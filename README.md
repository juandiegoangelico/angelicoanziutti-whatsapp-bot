# ⚖️ Angélico & Anziutti Advogados Associados | WhatsApp Gateway Oficial

Microserviço Node.js 24/7 de alta performance baseado em **Baileys**, projetado exclusivamente para a publicação automatizada de decisões jurídicas e boletins no Canal Oficial do WhatsApp: **"Atualizações dos Tribunais | Angélico & Anziutti"**.

---

## 🏛️ Funcionalidades

* 📱 **Conexão Direta com WhatsApp Web:** Vinculado ao celular corporativo do escritório via QR Code simples no navegador.
* 💾 **Persistência Cloud no PostgreSQL:** Sessão 100% permanente salva no banco PostgreSQL em tabela isolada (`baileys_auth_advocacia`). Mesmo se o servidor reiniciar ou atualizar, a conexão com o WhatsApp não é perdida.
* 📢 **Suporte Nativo a Canais (Newsletters):** Suporta postagem com texto formatado em negrito, tópicos e imagem de capa de alta resolução.
* 🔒 **Segurança por API Key:** Rotas de disparo protegidas por token Bearer / apikey.
* 💓 **Keep-Alive 24/7:** Ping periódico para manter a instância sempre acordada.

---

## 🚀 Como Subir no Render em 1 Minuto

1. Acesse o **[Dashboard do Render](https://dashboard.render.com/)**;
2. Clique em **New +** > **Web Service**;
3. Conecte o repositório GitHub: `juandiegoangelico/angelicoanziutti-whatsapp-bot`;
4. Defina as configurações básicas:
   * **Name:** `angelicoanziutti-whatsapp-bot`
   * **Runtime:** `Node`
   * **Build Command:** `npm install`
   * **Start Command:** `node server.js`
   * **Plan:** `Free`
5. Em **Environment Variables**, adicione:
   * `AUTHENTICATION_API_KEY`: *(clique em Generate ou digite um token secreto)*
   * `DATABASE_URL`: *(Cole a URL de conexão do PostgreSQL do Render para persistência permanente)*
   * `AUTH_TABLE`: `baileys_auth_advocacia`
6. Clique em **Deploy Web Service**.

---

## 📱 Como Conectar o Celular do Escritório

1. Assim que o serviço estiver "Live", abra a URL no navegador:
   `https://seu-servico.onrender.com/qr`
2. No celular do escritório, abra o WhatsApp > **Aparelhos Conectados** > **Conectar um Aparelho**;
3. Aponte a câmera para o QR Code da tela;
4. Pronto! O painel exibirá o selo verde **WHATSAPP DO ESCRITÓRIO CONECTADO**.

---

## 🌐 Endpoints da API

* `GET /` - Status do serviço e dados da conexão (JSON).
* `GET /qr` - Interface visual de escaneamento de QR Code com a identidade visual do escritório.
* `GET /logout` - Desconecta a sessão atual para permitir a troca de aparelho.
* `GET /channel-id?url=<link_convite>` - Retorna o ID `@newsletter` oficial de qualquer canal do WhatsApp.
* `POST /send` - Dispara mensagem e imagem para um canal ou grupo (requer cabeçalho `apikey: SUA_CHAVE`).
