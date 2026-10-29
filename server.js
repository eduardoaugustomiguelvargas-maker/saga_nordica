// Servidor da Saga de Ragnarök: serve o jogo (jogo.html) e liga os jogadores de cada sala por WebSocket.
// Ele só repassa mensagens (presença dos jogadores e eventos); toda a lógica do jogo roda nos navegadores.
const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORTA = process.env.PORT || 3000;
const ARQUIVO_JOGO = path.join(__dirname, "jogo.html");
const MAX_POR_SALA = 8;
const MAX_MSG = 8192;          // bytes por mensagem
const MAX_MSG_POR_SEG = 120;   // por jogador

const salas = new Map();       // nome -> Map(id -> cliente)
let proximoId = 1;

const servidor = http.createServer((req, res) => {
    const url = (req.url || "/").split("?")[0];
    if (url === "/health") { res.writeHead(200, { "Content-Type": "text/plain" }); res.end("ok"); return; }
    if (url === "/" || url === "/jogo.html" || url === "/index.html") {
        fs.readFile(ARQUIVO_JOGO, (err, dados) => {
            if (err) { res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" }); res.end("jogo.html não encontrado ao lado do server.js"); return; }
            res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
            res.end(dados);
        });
        return;
    }
    res.writeHead(404, { "Content-Type": "text/plain" }); res.end("não encontrado");
});

const wss = new WebSocketServer({ server: servidor, path: "/ws", maxPayload: MAX_MSG });

function enviar(c, obj) { if (c.ws.readyState === 1) c.ws.send(JSON.stringify(obj)); }
function paraSala(c, obj, incluirEu) {
    const sala = salas.get(c.sala); if (!sala) return;
    const txt = JSON.stringify(obj);
    for (const o of sala.values()) if ((incluirEu || o !== c) && o.ws.readyState === 1) o.ws.send(txt);
}
function sair(c) {
    if (!c.sala) return;
    const sala = salas.get(c.sala);
    if (sala) { sala.delete(c.id); paraSala(c, { t: "left", peer: c.id }); if (!sala.size) salas.delete(c.sala); }
    c.sala = null;
}

wss.on("connection", ws => {
    const c = { ws, id: "j" + (proximoId++).toString(36), sala: null, pres: {}, vivo: true, janela: Date.now(), conta: 0 };
    ws.on("pong", () => { c.vivo = true; });
    ws.on("message", buf => {
        const agora = Date.now();
        if (agora - c.janela > 1000) { c.janela = agora; c.conta = 0; }
        if (++c.conta > MAX_MSG_POR_SEG) return;   // excesso: ignora
        let m; try { m = JSON.parse(buf.toString()); } catch (e) { return; }
        if (!m || typeof m !== "object") return;
        if (m.t === "join") {
            const nome = String(m.room || "").toLowerCase().replace(/[^a-z0-9_.-]/g, "").slice(0, 48);
            if (!nome) return;
            sair(c);
            let sala = salas.get(nome);
            if (!sala) { sala = new Map(); salas.set(nome, sala); }
            if (sala.size >= MAX_POR_SALA) { enviar(c, { t: "erro", msg: "sala cheia" }); return; }
            c.sala = nome; sala.set(c.id, c);
            enviar(c, { t: "hello", id: c.id, peers: [...sala.values()].filter(o => o !== c).map(o => ({ peer: o.id, p: o.pres })) });
        } else if (!c.sala) {
            return;
        } else if (m.t === "pres") {
            if (!m.p || typeof m.p !== "object" || JSON.stringify(m.p).length > 2048) return;
            c.pres = m.p;
            paraSala(c, { t: "pres", peer: c.id, p: c.pres });
        } else if (m.t === "emit") {
            if (typeof m.topic !== "string" || !/^[a-z][a-z0-9_.-]{0,47}$/.test(m.topic)) return;
            paraSala(c, { t: "ev", peer: c.id, topic: m.topic, data: m.data });
        } else if (m.t === "leave") {
            sair(c);
        }
    });
    ws.on("close", () => sair(c));
    ws.on("error", () => sair(c));
});

// derruba conexões mortas
setInterval(() => {
    for (const ws of wss.clients) {
        if (ws._morto) { ws.terminate(); continue; }
        ws._morto = true; ws.ping();
        ws.once("pong", () => { ws._morto = false; });
    }
}, 30000);

servidor.listen(PORTA, () => console.log("Saga de Ragnarök no ar na porta " + PORTA));
