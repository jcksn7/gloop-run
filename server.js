'use strict';
// GLOOP RUN — сервер-ретранслятор комнат (WebSocket).
// Игровая логика остаётся в браузере хоста; сервер только пересылает сообщения между хостом и гостями.
// Заодно раздаёт саму игру (gloop-run.html) по адресу "/", поэтому достаточно развернуть один сервис.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const PAGE = path.join(__dirname, 'gloop-run.html');
const MAX_ROOMS = 500, MAX_GUESTS = 8;
const rooms = new Map(); // код -> { host: ws, guests: Map(id -> ws), next }

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/health') {
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'cache-control': 'no-store' });
    return res.end(JSON.stringify({ gloop: 1, rooms: rooms.size }));
  }
  if (u === '/' || u === '/index.html' || u === '/gloop-run.html') {
    return fs.readFile(PAGE, (e, b) => {
      if (e) { res.writeHead(404); return res.end('gloop-run.html должен лежать рядом с server.js'); }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
      res.end(b);
    });
  }
  res.writeHead(404); res.end('Not found');
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 });
server.on('upgrade', (req, sock, head) => {
  let url;
  try { url = new URL(req.url, 'http://x'); } catch (e) { return sock.destroy(); }
  if (url.pathname !== '/ws') return sock.destroy();
  wss.handleUpgrade(req, sock, head, ws => wss.emit('connection', ws, req, url));
});

wss.on('connection', (ws, req, url) => {
  ws.miss = 0;
  ws.on('pong', () => { ws.miss = 0; });
  ws.on('error', () => {});
  const code = (url.searchParams.get('room') || '').toUpperCase();
  const role = url.searchParams.get('role');
  const bad = e => { send(ws, { k: 'err', e }); ws.close(4000, e); };
  if (!/^[A-Z0-9]{4}$/.test(code)) return bad('bad');

  if (role === 'host') {
    if (rooms.has(code)) return bad('taken');
    if (rooms.size >= MAX_ROOMS) return bad('busy');
    const R = { host: ws, guests: new Map(), next: 1 };
    rooms.set(code, R);
    send(ws, { k: 'ready' });
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch (e) { return; }
      if (m.kick != null) { // выгнать гостя (например, «комната заполнена»), при желании с последним сообщением
        const g = R.guests.get(m.kick);
        if (g) { R.guests.delete(m.kick); if (m.d) send(g, { k: 'm', d: m.d }); g.close(4003, 'kicked'); }
        return;
      }
      if (m.to === '*') { // рассылка всем; lossy — можно пропустить для медленных клиентов (устаревшие снимки)
        const s = JSON.stringify({ k: 'm', d: m.d });
        for (const g of R.guests.values()) if (g.readyState === 1 && !(m.lossy && g.bufferedAmount > 32768)) g.send(s);
      } else {
        const g = R.guests.get(m.to);
        if (g) send(g, { k: 'm', d: m.d });
      }
    });
    ws.on('close', () => {
      if (rooms.get(code) === R) rooms.delete(code);
      for (const g of R.guests.values()) g.close(4002, 'host-left');
    });
  } else if (role === 'guest') {
    const R = rooms.get(code);
    if (!R || R.host.readyState !== 1) return bad('none');
    if (R.guests.size >= MAX_GUESTS) return bad('full');
    const id = R.next++;
    R.guests.set(id, ws);
    send(ws, { k: 'ready', id });
    send(R.host, { k: 'join', id });
    ws.on('message', raw => {
      let d; try { d = JSON.parse(raw); } catch (e) { return; }
      send(R.host, { k: 'm', id, d });
    });
    ws.on('close', () => {
      if (R.guests.get(id) === ws) { R.guests.delete(id); send(R.host, { k: 'leave', id }); }
    });
  } else bad('bad');
});

// пинги: держат соединение живым за прокси и убирают «мёртвые» сокеты
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.miss >= 2) { ws.terminate(); continue; }
    ws.miss++;
    try { ws.ping(); } catch (e) {}
  }
}, 20000).unref();

server.listen(PORT, '0.0.0.0', () => console.log('GLOOP RUN server: http://localhost:' + PORT));
