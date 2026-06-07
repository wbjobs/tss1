const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  let filePath = '.' + req.url;
  if (filePath === './') {
    filePath = './client/index.html';
  }

  const extname = String(path.extname(filePath)).toLowerCase();
  const mimeTypes = {
    '.html': 'text/html',
    '.js': 'text/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpg',
    '.gif': 'image/gif',
    '.svg': 'image/svg+xml',
  };

  const contentType = mimeTypes[extname] || 'application/octet-stream';

  fs.readFile(filePath, (error, content) => {
    if (error) {
      if (error.code === 'ENOENT') {
        res.writeHead(404);
        res.end('File not found');
      } else {
        res.writeHead(500);
        res.end('Server Error: ' + error.code);
      }
    } else {
      res.writeHead(200, { 'Content-Type': contentType });
      res.end(content, 'utf-8');
    }
  });
});

const wss = new WebSocket.Server({ server });

const rooms = new Map();

wss.on('connection', (ws, req) => {
  let clientId = null;
  let currentRoom = null;

  console.log('New client connected');

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message.toString());

      switch (data.type) {
        case 'join':
          handleJoin(ws, data);
          break;
        case 'offer':
        case 'answer':
        case 'ice-candidate':
          forwardSignaling(data);
          break;
        case 'ping':
          ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
          break;
        default:
          console.log('Unknown message type:', data.type);
      }
    } catch (err) {
      console.error('Error parsing message:', err);
    }
  });

  ws.on('close', () => {
    handleDisconnect();
  });

  function handleJoin(ws, data) {
    clientId = data.clientId;
    const roomId = data.roomId;
    currentRoom = roomId;

    if (!rooms.has(roomId)) {
      rooms.set(roomId, new Map());
    }

    const room = rooms.get(roomId);
    const existingClients = Array.from(room.keys());

    room.set(clientId, ws);

    ws.send(JSON.stringify({
      type: 'joined',
      clientId: clientId,
      roomId: roomId,
      peers: existingClients.filter(id => id !== clientId)
    }));

    room.forEach((peerWs, peerId) => {
      if (peerId !== clientId && peerWs.readyState === WebSocket.OPEN) {
        peerWs.send(JSON.stringify({
          type: 'peer-joined',
          peerId: clientId
        }));
      }
    });

    console.log(`Client ${clientId} joined room ${roomId}`);
  }

  function forwardSignaling(data) {
    const room = rooms.get(data.roomId);
    if (!room) return;

    const targetWs = room.get(data.targetId);
    if (targetWs && targetWs.readyState === WebSocket.OPEN) {
      targetWs.send(JSON.stringify({
        type: data.type,
        from: data.from,
        to: data.targetId,
        payload: data.payload,
        roomId: data.roomId
      }));
    }
  }

  function handleDisconnect() {
    if (currentRoom && clientId) {
      const room = rooms.get(currentRoom);
      if (room) {
        room.delete(clientId);

        room.forEach((peerWs, peerId) => {
          if (peerWs.readyState === WebSocket.OPEN) {
            peerWs.send(JSON.stringify({
              type: 'peer-left',
              peerId: clientId
            }));
          }
        });

        if (room.size === 0) {
          rooms.delete(currentRoom);
        }
      }
    }
    console.log(`Client ${clientId} disconnected`);
  }
});

server.listen(PORT, () => {
  console.log(`Signaling server running on http://localhost:${PORT}`);
  console.log(`Open multiple browser tabs to test P2P synchronization`);
});
