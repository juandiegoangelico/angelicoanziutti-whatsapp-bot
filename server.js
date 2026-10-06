import express from 'express';
import cors from 'cors';
import axios from 'axios';
import sharp from 'sharp';
import qrcode from 'qrcode';
import pino from 'pino';
import fs from 'node:fs';
import path from 'node:path';
import pg from 'pg';
import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';
import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  proto,
  initAuthCreds,
  BufferJSON,
  Browsers,
} from '@whiskeysockets/baileys';

dotenv.config();

const { Pool } = pg;

process.on('uncaughtException', (err) => {
  console.error('⚠️ [WhatsApp Advocacia] Erro não capturado:', err);
});
process.on('unhandledRejection', (reason) => {
  console.error('⚠️ [WhatsApp Advocacia] Rejeição não tratada:', reason);
});

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

const PORT = parseInt(process.env.PORT || '8080', 10);
const HOST = '0.0.0.0';
const AUTH_DIR = path.resolve('auth_info_baileys');
const API_KEY = process.env.AUTHENTICATION_API_KEY || process.env.WA_API_KEY || '';
const SELF_URL = process.env.RENDER_EXTERNAL_URL || process.env.SERVER_URL || '';
const AUTH_TABLE = process.env.AUTH_TABLE || 'baileys_auth_advocacia';

const PG_URI = process.env.DATABASE_URL || process.env.DATABASE_CONNECTION_URI || process.env.POSTGRES_URL || '';
const MONGO_URI = process.env.MONGODB_URI || process.env.MONGO_URL || '';

let sock = null;
let currentQrCode = null;
let isConnected = false;
let userPhone = null;
let activeStorageType = 'Local File System (Temporário)';
let lastError = null;

// Singleton Pool para PostgreSQL para evitar estouro de conexões no Render
let pgPool = null;
function getPgPool() {
  if (!pgPool && PG_URI) {
    const isInternalRender = PG_URI.includes('.render.com') || PG_URI.includes('dpg-');
    pgPool = new Pool({
      connectionString: PG_URI,
      ssl: isInternalRender ? { rejectUnauthorized: false } : false,
      max: 10,
    });
    pgPool.on('error', (err) => {
      lastError = `PG Pool: ${err.message}`;
      console.error('⚠️ [PostgreSQL Pool Error]:', err.message);
    });
  }
  return pgPool;
}

// 1. Auth State Provider para PostgreSQL Cloud (com isolamento de tabela por escritório)
async function usePostgresAuthState() {
  const pool = getPgPool();
  if (!pool) throw new Error('DATABASE_URL do PostgreSQL não configurada.');

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${AUTH_TABLE} (
      id VARCHAR(255) PRIMARY KEY,
      data TEXT NOT NULL,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  const writeData = async (data, id) => {
    try {
      const jsonStr = JSON.stringify(data, BufferJSON.replacer);
      await pool.query(
        `INSERT INTO ${AUTH_TABLE} (id, data, updated_at) 
         VALUES ($1, $2, CURRENT_TIMESTAMP) 
         ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = CURRENT_TIMESTAMP`,
        [id, jsonStr]
      );
    } catch (err) {
      console.error(`❌ Erro ao salvar chave ${id} no PostgreSQL (${AUTH_TABLE}):`, err.message);
    }
  };

  const readData = async (id) => {
    try {
      const res = await pool.query(`SELECT data FROM ${AUTH_TABLE} WHERE id = $1`, [id]);
      if (!res.rows[0]?.data) return null;
      return JSON.parse(res.rows[0].data, BufferJSON.reviver);
    } catch (err) {
      console.error(`❌ Erro ao ler chave ${id} no PostgreSQL (${AUTH_TABLE}):`, err.message);
      return null;
    }
  };

  const removeData = async (id) => {
    try {
      await pool.query(`DELETE FROM ${AUTH_TABLE} WHERE id = $1`, [id]);
    } catch (err) {
      console.error(`❌ Erro ao deletar chave ${id} no PostgreSQL (${AUTH_TABLE}):`, err.message);
    }
  };

  const creds = (await readData('creds')) || initAuthCreds();

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {};
          await Promise.all(
            ids.map(async (id) => {
              let value = await readData(`${type}-${id}`);
              if (type === 'app-state-sync-key' && value) {
                value = proto.Message.AppStateSyncKeyData.fromObject(value);
              }
              data[id] = value;
            })
          );
          return data;
        },
        set: async (data) => {
          const tasks = [];
          for (const category in data) {
            for (const id in data[category]) {
              const value = data[category][id];
              const key = `${category}-${id}`;
              tasks.push(value ? writeData(value, key) : removeData(key));
            }
          }
          await Promise.all(tasks);
        },
      },
    },
    saveCreds: () => writeData(creds, 'creds'),
  };
}

// 2. Auth State Provider para MongoDB Atlas
async function useMongoAuthState(collection) {
  const writeData = (data, id) => {
    return collection.replaceOne(
      { _id: id },
      { _id: id, data: JSON.stringify(data, BufferJSON.replacer) },
      { upsert: true }
    );
  };

  const readData = async (id) => {
    const doc = await collection.findOne({ _id: id });
    if (!doc?.data) return null;
    return JSON.parse(doc.data, BufferJSON.reviver);
  };

  const removeData = async (id) => {
    await collection.deleteOne({ _id: id });
  };

  const creds = (await readData('creds')) || initAuthCreds();

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {};
          await Promise.all(
            ids.map(async (id) => {
              let value = await readData(`${type}-${id}`);
              if (type === 'app-state-sync-key' && value) {
                value = proto.Message.AppStateSyncKeyData.fromObject(value);
              }
              data[id] = value;
            })
          );
          return data;
        },
        set: async (data) => {
          const tasks = [];
          for (const category in data) {
            for (const id in data[category]) {
              const value = data[category][id];
              const key = `${category}-${id}`;
              tasks.push(value ? writeData(value, key) : removeData(key));
            }
          }
          await Promise.all(tasks);
        },
      },
    },
    saveCreds: () => writeData(creds, 'creds'),
  };
}

// Inicializa o cliente WhatsApp (Baileys)
let isConnecting = false;

async function connectToWhatsApp() {
  if (isConnecting) {
    console.log('⏳ [WhatsApp Advocacia] Tentativa de conexão já em andamento...');
    return;
  }
  isConnecting = true;

  try {
    // Limpa socket e ouvintes anteriores se existirem
    if (sock) {
      try {
        sock.ev.removeAllListeners();
        sock.end(undefined);
      } catch (e) {}
      sock = null;
    }

    let authStateResult;

    if (PG_URI) {
      console.log(`🐘 [WhatsApp Advocacia] Conectando ao PostgreSQL (tabela: ${AUTH_TABLE})...`);
      authStateResult = await usePostgresAuthState();
      activeStorageType = `PostgreSQL Cloud (${AUTH_TABLE})`;
      console.log('✅ [WhatsApp Advocacia] Sessão vinculada ao PostgreSQL com sucesso!');
    } else if (MONGO_URI) {
      console.log('🍃 [WhatsApp Advocacia] Conectando ao MongoDB Atlas...');
      const client = new MongoClient(MONGO_URI);
      await client.connect();
      const db = client.db('advocacia_bot');
      const collection = db.collection('baileys_auth_advocacia');
      authStateResult = await useMongoAuthState(collection);
      activeStorageType = 'MongoDB Atlas Cloud';
      console.log('✅ [WhatsApp Advocacia] Sessão vinculada ao MongoDB Atlas!');
    } else {
      if (!fs.existsSync(AUTH_DIR)) {
        fs.mkdirSync(AUTH_DIR, { recursive: true });
      }
      authStateResult = await useMultiFileAuthState(AUTH_DIR);
      activeStorageType = 'Local File System (Temporário)';
    }

    const { state, saveCreds } = authStateResult;

    let version = [2, 3000, 1043857760];
    try {
      const v = await fetchLatestBaileysVersion();
      if (v?.version) version = v.version;
    } catch (e) {
      console.warn('ℹ️ [WhatsApp Advocacia] Usando versão padrão Baileys:', version);
    }

    sock = makeWASocket({
      version,
      logger: pino({ level: 'warn' }),
      auth: state,
      browser: Browsers.macOS('Desktop'),
      generateHighQualityLinkPreview: true,
      defaultQueryTimeoutMs: undefined,
      connectTimeoutMs: 60000,
      keepAliveIntervalMs: 25000,
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        try {
          currentQrCode = await qrcode.toDataURL(qr, { scale: 8 });
          isConnected = false;
          console.log('📱 [WhatsApp Advocacia] Novo QR Code gerado! Pronto para leitura.');
        } catch (eQr) {
          console.error('Erro ao converter QR Code:', eQr.message);
        }
      }

      if (connection === 'close') {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const isLoggedOut = statusCode === DisconnectReason.loggedOut || statusCode === 401;
        const shouldReconnect = !isLoggedOut;

        isConnected = false;
        console.log(`⚠️ [WhatsApp Advocacia] Conexão fechada (Código: ${statusCode}). Reconectando: ${shouldReconnect}`);

        if (isLoggedOut) {
          console.log('🗑️ [WhatsApp Advocacia] Sessão deslogada pelo WhatsApp. Resetando credenciais...');
          currentQrCode = null;
          userPhone = null;
          try {
            await resetAuthState();
          } catch (e) {}
          setTimeout(connectToWhatsApp, 2000);
        } else {
          // Mantém currentQrCode disponível na tela para o usuário escanear
          setTimeout(connectToWhatsApp, 5000);
        }
      } else if (connection === 'open') {
        isConnected = true;
        currentQrCode = null;
        userPhone = sock.user?.id ? sock.user.id.split(':')[0] : 'Conectado';
        console.log(`✅ [WhatsApp Advocacia] Conectado com sucesso como: ${userPhone}! (Armazenamento: ${activeStorageType})`);
      }
    });
  } catch (err) {
    lastError = `Baileys Init: ${err.message}`;
    console.error('💥 [WhatsApp Advocacia] Falha ao iniciar Baileys:', err.message);
    setTimeout(connectToWhatsApp, 5000);
  } finally {
    isConnecting = false;
  }
}

// Keep-alive automático para evitar hibernação no Render (a cada 4 minutos)
if (SELF_URL) {
  setInterval(async () => {
    try {
      await axios.get(`${SELF_URL}/ping`, { timeout: 10000 });
      console.log('💓 [Keep-Alive] Ping interno executado com sucesso.');
    } catch (err) {}
  }, 4 * 60 * 1000);
}

// Middleware de Autenticação
function authMiddleware(req, res, next) {
  if (!API_KEY) return next();
  const token =
    req.headers['apikey'] ||
    req.headers['authorization']?.replace('Bearer ', '') ||
    req.query.apikey;

  if (token && token === API_KEY) {
    return next();
  }
  return res.status(401).json({ error: 'Não autorizado. Chave de API inválida.' });
}

// Rota 1: Dashboard de Status
app.get('/', (req, res) => {
  if (req.query.json === 'true' || req.query.format === 'json' || (req.headers.accept && req.headers.accept.includes('application/json') && !req.headers.accept.includes('text/html'))) {
    return res.json({
      status: 'online',
      service: 'angelicoanziutti-whatsapp-bot',
      office: 'Angélico & Anziutti Advogados Associados',
      connected: isConnected,
      hasQr: !!currentQrCode,
      user: userPhone,
      storage: activeStorageType,
      hasPgUri: !!PG_URI,
      lastError,
      serverTime: new Date().toISOString(),
    });
  }
  return res.redirect('/qr');
});

app.get('/status', (req, res) => {
  return res.json({
    status: 'online',
    service: 'angelicoanziutti-whatsapp-bot',
    office: 'Angélico & Anziutti Advogados Associados',
    connected: isConnected,
    hasQr: !!currentQrCode,
    user: userPhone,
    storage: activeStorageType,
    hasPgUri: !!PG_URI,
    lastError,
    serverTime: new Date().toISOString(),
  });
});

app.get('/ping', (req, res) => {
  res.send('pong');
});

// Rota 2: Limpar sessão para trocar de número / forçar novo QR Code
async function resetAuthState() {
  try {
    if (PG_URI) {
      const pool = getPgPool();
      if (pool) {
        await pool.query(`DELETE FROM ${AUTH_TABLE};`);
        console.log(`🗑️ [WhatsApp Advocacia] Sessão limpa do PostgreSQL (${AUTH_TABLE}).`);
      }
    } else if (MONGO_URI) {
      const client = new MongoClient(MONGO_URI);
      await client.connect();
      const db = client.db('advocacia_bot');
      await db.collection('baileys_auth_advocacia').deleteMany({});
      await client.close();
      console.log('🗑️ [WhatsApp Advocacia] Sessão limpa do MongoDB.');
    } else {
      if (fs.existsSync(AUTH_DIR)) {
        fs.rmSync(AUTH_DIR, { recursive: true, force: true });
        fs.mkdirSync(AUTH_DIR, { recursive: true });
      }
      console.log('🗑️ [WhatsApp Advocacia] Sessão limpa do sistema local.');
    }
  } catch (err) {
    console.error('❌ Erro ao limpar sessão:', err.message);
  }
}

app.get('/logout', async (req, res) => {
  try {
    console.log('🔄 [WhatsApp Advocacia] Desconectando sessão atual de forma não-bloqueante...');
    if (sock) {
      try {
        if (isConnected && sock.ws?.readyState === 1) {
          await Promise.race([
            sock.logout(),
            new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 2000))
          ]);
        } else {
          sock.end(undefined);
        }
      } catch (e) {
        try { sock.end(undefined); } catch (e2) {}
      }
      sock = null;
    }

    await resetAuthState();
    isConnected = false;
    userPhone = null;
    currentQrCode = null;

    setTimeout(() => {
      connectToWhatsApp().catch(() => {});
    }, 1000);

    return res.send(`
      <!DOCTYPE html>
      <html>
        <head>
          <title>WhatsApp Desconectado | Angélico & Anziutti</title>
          <meta http-equiv="refresh" content="3; url=/qr">
          <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; background: #0b1528; color: white; text-align: center; }
            .card { background: #132238; border: 1px solid rgba(197,168,128,0.3); padding: 2.5rem; border-radius: 1rem; box-shadow: 0 15px 35px rgba(0,0,0,0.5); }
            h2 { color: #c5a880; }
          </style>
        </head>
        <body>
          <div class="card">
            <h2>✅ Sessão Desconectada com Sucesso!</h2>
            <p style="color: #94a3b8;">Gerando novo QR Code para o WhatsApp do escritório... Redirecionando em 3 segundos.</p>
          </div>
        </body>
      </html>
    `);
  } catch (err) {
    return res.status(500).send(`Erro ao desconectar: ${err.message}`);
  }
});

// Rota 3: Visualização do QR Code no Navegador
app.get('/qr', (req, res) => {
  if (isConnected) {
    return res.send(`
      <!DOCTYPE html>
      <html>
        <head>
          <title>WhatsApp Conectado | Angélico & Anziutti</title>
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #0b1528; color: white; padding: 1.5rem; }
            .card { background: #132238; border: 1px solid rgba(197,168,128,0.3); padding: 2.5rem; border-radius: 1rem; text-align: center; box-shadow: 0 15px 35px rgba(0,0,0,0.6); max-width: 520px; width: 100%; box-sizing: border-box; }
            .badge { background: #10b981; color: white; padding: 0.5rem 1.2rem; border-radius: 9999px; font-weight: bold; display: inline-block; margin-bottom: 1rem; font-size: 0.9rem; }
            .badge-storage { background: rgba(197,168,128,0.2); color: #c5a880; border: 1px solid #c5a880; padding: 0.4rem 0.9rem; border-radius: 9999px; font-size: 0.8rem; font-weight: bold; display: inline-block; margin-bottom: 1rem; }
            h2 { color: #f8fafc; margin-top: 0.5rem; }
            .gold-text { color: #c5a880; font-weight: 600; }
            .btn { background: #2563eb; color: white; text-decoration: none; padding: 0.75rem 1.25rem; border-radius: 0.5rem; font-weight: bold; display: inline-block; margin: 0.5rem 0.25rem; }
            .btn-red { background: #ef4444; }
          </style>
        </head>
        <body>
          <div class="card">
            <div style="font-size: 2.2rem; margin-bottom: 0.5rem;">⚖️</div>
            <div class="gold-text" style="font-size: 0.9rem; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 0.75rem;">Angélico & Anziutti Advogados Associados</div>
            <div class="badge">✅ WHATSAPP DO ESCRITÓRIO CONECTADO</div><br/>
            <div class="badge-storage">💾 ${activeStorageType}</div>
            <h2>Canal "Atualizações dos Tribunais" Pronto!</h2>
            <p style="color: #94a3b8; font-size: 0.95rem; line-height: 1.5;">O robô está pronto para disparar boletins jurídicos automaticamente pelo WhatsApp corporativo.</p>
            <p style="color: #cbd5e1; font-size: 0.9rem;">Telefone Ativo: <b class="gold-text">${userPhone || 'Ativo'}</b></p>
            <div style="margin-top: 2rem;">
              <a href="/logout" onclick="return confirm('Deseja desconectar este número para conectar outro celular do escritório?')" class="btn btn-red">🔴 Desconectar / Trocar Celular</a>
            </div>
          </div>
        </body>
      </html>
    `);
  }

  if (currentQrCode) {
    return res.send(`
      <!DOCTYPE html>
      <html>
        <head>
          <title>Conectar WhatsApp Escritório | Angélico & Anziutti</title>
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <meta http-equiv="refresh" content="6">
          <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #0b1528; color: white; padding: 1.5rem; }
            .card { background: #132238; border: 1px solid rgba(197,168,128,0.3); padding: 2.5rem; border-radius: 1rem; text-align: center; box-shadow: 0 15px 35px rgba(0,0,0,0.6); max-width: 440px; width: 100%; box-sizing: border-box; }
            img { width: 280px; height: 280px; border-radius: 0.75rem; background: white; padding: 0.75rem; border: 3px solid #c5a880; }
            .gold-text { color: #c5a880; font-weight: 600; }
            .pulse { animation: pulse 2s infinite; }
            @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.5; } 100% { opacity: 1; } }
          </style>
        </head>
        <body>
          <div class="card">
            <div style="font-size: 2rem; margin-bottom: 0.25rem;">⚖️</div>
            <div class="gold-text" style="font-size: 0.85rem; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 1rem;">Angélico & Anziutti Advogados</div>
            <h2 style="margin: 0 0 0.5rem 0;">Conectar Celular do Escritório</h2>
            <p style="color: #94a3b8; font-size: 0.9rem; margin-bottom: 1.5rem;">
              No celular do escritório, abra o WhatsApp > <b>Aparelhos Conectados</b> > <b>Conectar Aparelho</b> e aponte para o QR Code abaixo:
            </p>
            <img src="${currentQrCode}" alt="QR Code WhatsApp Escritório" />
            <p style="color: #64748b; font-size: 0.8rem; margin-top: 1.5rem;" class="pulse">🔄 Atualizando automaticamente...</p>
          </div>
        </body>
      </html>
    `);
  }

  return res.send(`
    <!DOCTYPE html>
    <html>
      <head>
        <title>Iniciando WhatsApp | Angélico & Anziutti</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta http-equiv="refresh" content="3">
        <style>
          body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #0b1528; color: white; padding: 1.5rem; }
          .card { background: #132238; border: 1px solid rgba(197,168,128,0.3); padding: 2.5rem; border-radius: 1rem; text-align: center; box-shadow: 0 15px 35px rgba(0,0,0,0.6); max-width: 440px; width: 100%; box-sizing: border-box; }
          .spinner { border: 4px solid rgba(255,255,255,0.1); border-left-color: #c5a880; border-radius: 50%; width: 44px; height: 44px; animation: spin 1s linear infinite; margin: 0 auto 1.5rem auto; }
          @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
          .btn-reset { display: inline-block; background: #ef4444; color: white; text-decoration: none; padding: 0.65rem 1.25rem; border-radius: 0.5rem; font-weight: bold; font-size: 0.9rem; margin-top: 1rem; }
        </style>
      </head>
      <body>
        <div class="card">
          <div class="spinner"></div>
          <h2>Iniciando WhatsApp...</h2>
          <p style="color: #94a3b8; font-size: 0.95rem; line-height: 1.5;">Aguarde alguns segundos enquanto o servidor estabelece a conexão e gera o QR Code. Esta página atualiza a cada 3 segundos.</p>
          <a href="/logout" class="btn-reset">🔄 Forçar Novo QR Code</a>
        </div>
      </body>
    </html>
  `);
});

// Rota 4: Resolver ID do Canal (Newsletter)
app.get('/channel-id', async (req, res) => {
  if (!isConnected || !sock) {
    return res.status(503).json({ error: 'WhatsApp não está conectado. Escaneie o QR Code em /qr' });
  }

  const rawInput = req.query.url || req.query.code || req.query.link || '';
  if (!rawInput) {
    return res.status(400).json({ error: 'Parâmetro url ou code é obrigatório.' });
  }

  let code = String(rawInput).trim();
  const match = code.match(/whatsapp\.com\/channel\/([a-zA-Z0-9_-]+)/i);
  if (match && match[1]) {
    code = match[1];
  }

  try {
    const metadata = await sock.newsletterMetadata('invite', code);
    const channelId = metadata.id || metadata.jid;
    const channelName = metadata.name || metadata.thread_metadata?.name?.text || 'Canal WhatsApp';

    return res.json({
      success: true,
      name: channelName,
      id: channelId,
      inviteCode: code,
      raw: metadata,
    });
  } catch (err) {
    console.error('❌ Erro ao buscar dados do Canal:', err.message);
    return res.status(500).json({ error: 'Não foi possível resolver o canal.', details: err.message });
  }
});

// Rota 4.1: Verificar Permissões de Administrador do Canal Oficial
app.get('/channel-status', async (req, res) => {
  if (!isConnected || !sock) {
    return res.status(503).json({ error: 'WhatsApp não está conectado. Escaneie o QR Code em /qr' });
  }

  const jid = req.query.jid || '120363413003896686@newsletter';
  try {
    const meta = await sock.newsletterMetadata('jid', jid);
    const role = meta.viewer_metadata?.role || 'NONE';
    const isAdmin = role === 'ADMIN' || role === 'OWNER';

    return res.json({
      success: true,
      jid,
      name: meta.name || meta.thread_metadata?.name?.text,
      userPhone,
      role,
      isAdmin,
      subscribers: meta.subscribers_count || meta.thread_metadata?.subscribers_count || 0,
      description: meta.thread_metadata?.description?.text || '',
    });
  } catch (err) {
    console.error('❌ Erro ao consultar status do Canal:', err.message);
    return res.status(500).json({ 
      error: 'Não foi possível verificar status do canal. Certifique-se de que o número do escritório é administrador ou seguidor.', 
      details: err.message 
    });
  }
});

// ==========================================
// 🎨 GERADOR DINÂMICO DE CAPAS PERSONALIZADAS (ON-DEMAND)
// ==========================================
function wrapText(text, maxChars) {
  const words = String(text || '').trim().split(/\s+/);
  const lines = [];
  let current = '';
  for (const w of words) {
    if (!current) {
      current = w;
    } else if ((current + ' ' + w).length <= maxChars) {
      current = current + ' ' + w;
    } else {
      lines.push(current);
      current = w;
    }
  }
  if (current) lines.push(current);
  return lines;
}

function escapeXml(unsafe) {
  return String(unsafe || '').replace(/[<>&'"]/g, c => {
    switch (c) {
      case '<': return '&lt;';
      case '>': return '&gt;';
      case '&': return '&amp;';
      case '\'': return '&apos;';
      case '"': return '&quot;';
      default: return c;
    }
  });
}

let LOGO_BASE64_CACHE = '';
function obterLogoBase64() {
  if (LOGO_BASE64_CACHE) return LOGO_BASE64_CACHE;
  try {
    const logoFile = path.resolve('logo.png');
    if (fs.existsSync(logoFile)) {
      LOGO_BASE64_CACHE = fs.readFileSync(logoFile).toString('base64');
    }
  } catch (e) {
    console.warn('Aviso ao carregar logo.png para capa:', e.message);
  }
  return LOGO_BASE64_CACHE;
}

function gerarSvgCapa(titulo, subtitulo, categoria, formato) {
  const isStories = formato === 'stories';
  const width = 1080;
  const height = isStories ? 1920 : 1080;
  const catText = (categoria || 'ANÁLISE JURÍDICA').toUpperCase();

  const titleLines = wrapText(titulo, isStories ? 22 : 28).slice(0, 4);
  const subLines = subtitulo ? wrapText(subtitulo, isStories ? 32 : 44).slice(0, 3) : [];

  const startY = isStories ? 560 : 320;
  const titleLineHeight = isStories ? 76 : 72;
  const subLineHeight = 40;

  let titleSvg = '';
  titleLines.forEach((line, idx) => {
    const y = startY + (idx * titleLineHeight);
    titleSvg += `<text x="80" y="${y}" font-family="Georgia, serif" font-size="${isStories ? 56 : 52}" font-weight="bold" fill="#ffffff">${escapeXml(line)}</text>\n`;
  });

  const subStartY = startY + (titleLines.length * titleLineHeight) + 35;
  let subSvg = '';
  subLines.forEach((line, idx) => {
    const y = subStartY + (idx * subLineHeight);
    subSvg += `<text x="80" y="${y}" font-family="Helvetica, Arial, sans-serif" font-size="28" fill="#94a3b8">${escapeXml(line)}</text>\n`;
  });

  const footerY = height - 80;
  const logoBase64 = obterLogoBase64();
  const logoSvg = logoBase64 
    ? `<image x="${width - 80 - 130}" y="${isStories ? 120 : 65}" width="130" height="130" href="data:image/png;base64,${logoBase64}" />`
    : '';

  return `
  <svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg">
    <!-- Fundo Azul Marinho Nobre -->
    <rect width="${width}" height="${height}" fill="#051124"/>
    
    <!-- Moldura Dourada e Borda de Acabamento -->
    <rect x="32" y="32" width="${width - 64}" height="${height - 64}" fill="none" stroke="#d4af37" stroke-width="3"/>
    <rect x="44" y="44" width="${width - 88}" height="${height - 88}" fill="none" stroke="#1e3250" stroke-width="1"/>

    <!-- Header: Logo e Categoria -->
    ${logoSvg}
    <text x="80" y="${isStories ? 430 : 125}" font-family="Helvetica, Arial, sans-serif" font-size="24" font-weight="bold" fill="#d4af37" letter-spacing="2">
      ${escapeXml(catText)} • ANÁLISE JURÍDICA
    </text>
    <line x1="80" y1="${isStories ? 465 : 160}" x2="${width - 80 - (logoBase64 && !isStories ? 150 : 0)}" y2="${isStories ? 465 : 160}" stroke="#1e3250" stroke-width="1"/>

    <!-- Conteúdo: Título e Subtítulo -->
    ${titleSvg}
    ${subSvg}

    <!-- Rodapé Institucional -->
    <line x1="80" y1="${footerY - 40}" x2="${width - 80}" y2="${footerY - 40}" stroke="#1e3250" stroke-width="1"/>
    <text x="80" y="${footerY}" font-family="Georgia, serif" font-size="24" font-weight="bold" fill="#d4af37" letter-spacing="1">
      ANGÉLICO &amp; ANZIUTTI ADVOGADOS
    </text>
    <text x="${width - 80}" y="${footerY}" font-family="Helvetica, Arial, sans-serif" font-size="22" fill="#94a3b8" text-anchor="end">
      www.angelicoanziutti.com
    </text>
  </svg>`;
}

// Rota Pública: Gerador Dinâmico de Capas Personalizadas (Feed 1:1, Stories 9:16 ou Horizontal)
app.get('/capa', async (req, res) => {
  try {
    const titulo = req.query.titulo || req.query.title || 'Angélico & Anziutti Advogados Associados';
    const subtitulo = req.query.subtitulo || req.query.subtitle || req.query.lead || '';
    const categoria = req.query.categoria || req.query.category || 'DIREITO';
    const formato = (req.query.formato || req.query.format || 'quadrada').toLowerCase();

    const svg = gerarSvgCapa(titulo, subtitulo, categoria, formato);
    const pngBuffer = await sharp(Buffer.from(svg)).png({ compressionLevel: 8 }).toBuffer();

    res.set({
      'Content-Type': 'image/png',
      'Content-Length': pngBuffer.length,
      'Cache-Control': 'public, max-age=604800, immutable',
    });
    return res.send(pngBuffer);
  } catch (err) {
    console.error('❌ Erro ao gerar capa dinâmica:', err.message);
    return res.status(500).json({ error: 'Erro ao gerar capa.', details: err.message });
  }
});

// Rota 5: Disparo de Mensagem e Imagem
async function handleSend(req, res) {
  if (!isConnected || !sock) {
    return res.status(503).json({ error: 'WhatsApp não está conectado. Escaneie o QR Code em /qr' });
  }

  const body = req.body || {};
  const target = body.number || body.recipient || req.params.number;
  let messageText = String(body.caption || body.message || body.text || '')
    .replace(/https?:\/\/(www\.)?angelicoanziutti\.adv\.br/gi, 'https://www.angelicoanziutti.com')
    .replace(/https?:\/\/(www\.)?angelicoanziutti\.com\.br/gi, 'https://www.angelicoanziutti.com')
    .replace(/angelicoanziutti\.adv\.br/gi, 'angelicoanziutti.com')
    .replace(/angelicoanziutti\.com\.br/gi, 'angelicoanziutti.com');

  let imageUrl = body.media || body.image || body.imageUrl || null;
  if (imageUrl && typeof imageUrl === 'string') {
    if (imageUrl.includes('capa_institucional') && !imageUrl.includes('-quadrada')) {
      imageUrl = 'https://www.angelicoanziutti.com/imagens/capa_institucional-quadrada.png';
    }
  }

  if (!target) {
    return res.status(400).json({ error: 'Destinatário (number ou recipient) é obrigatório.' });
  }

  let formattedJid = String(target).trim();
  if (!formattedJid.includes('@')) {
    formattedJid = `${formattedJid}@s.whatsapp.net`;
  }

  try {
    let result;

    if (imageUrl && (imageUrl.startsWith('http://') || imageUrl.startsWith('https://'))) {
      try {
        console.log(`📥 [WhatsApp Advocacia] Baixando imagem de capa: ${imageUrl}`);
        const response = await axios.get(imageUrl, {
          responseType: 'arraybuffer',
          timeout: 20000,
        });
        const imageBuffer = Buffer.from(response.data);

        result = await sock.sendMessage(formattedJid, {
          image: imageBuffer,
          caption: messageText,
        });
      } catch (imgErr) {
        console.warn(`⚠️ [WhatsApp Advocacia] Falha ao baixar imagem (${imgErr.message}). Tentando capa institucional quadrada de segurança...`);
        try {
          const fallbackRes = await axios.get('https://www.angelicoanziutti.com/imagens/capa_institucional-quadrada.png', {
            responseType: 'arraybuffer',
            timeout: 15000,
          });
          result = await sock.sendMessage(formattedJid, {
            image: Buffer.from(fallbackRes.data),
            caption: messageText,
          });
          console.log(`✅ [WhatsApp Advocacia] Imagem institucional de contingência enviada com sucesso.`);
        } catch (fallbackErr) {
          console.warn(`⚠️ [WhatsApp Advocacia] Falha também no fallback institucional (${fallbackErr.message}). Enviando apenas texto.`);
          result = await sock.sendMessage(formattedJid, {
            text: messageText,
          });
        }
      }
    } else {
      result = await sock.sendMessage(formattedJid, {
        text: messageText,
      });
    }

    console.log(`📤 [WhatsApp Advocacia] Mensagem enviada com sucesso para: ${formattedJid}`);
    return res.json({ success: true, id: result?.key?.id, jid: formattedJid });
  } catch (err) {
    console.error(`❌ [WhatsApp Advocacia] Erro ao enviar mensagem para ${formattedJid}:`, err.message);
    return res.status(500).json({ error: err.message });
  }
}

app.post('/send', authMiddleware, handleSend);
app.post('/message/sendMedia/:instance', authMiddleware, handleSend);
app.post('/message/sendText/:instance', authMiddleware, handleSend);

app.listen(PORT, HOST, () => {
  console.log(`🚀 [WhatsApp Advocacia] Servidor rodando em http://${HOST}:${PORT}`);
  connectToWhatsApp().catch((err) => {
    console.error('💥 Erro ao inicializar Baileys:', err);
  });
});
