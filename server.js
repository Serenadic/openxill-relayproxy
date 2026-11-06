const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 9090;
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'online', service: 'openxill-core-relay' }));
});

const wss = new WebSocket.Server({ server });
const rooms = new Map();

function broadcastToRoom(roomCode, message, senderWs) {
    const clients = rooms.get(roomCode);
    if (!clients) return;
    for (const client of clients) {
        if (client !== senderWs && client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    }
}

function updatePeerCount(roomCode) {
    const clients = rooms.get(roomCode);
    const count = clients ? clients.size : 0;
    const msg = JSON.stringify({ type: 'peer_status', count: count });
    if (clients) {
        for (const client of clients) {
            if (client.readyState === WebSocket.OPEN) {
                client.send(msg);
            }
        }
    }
}

wss.on('connection', (ws) => {
    let currentRoom = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            if (data.type === 'join') {
                currentRoom = data.code;
                if (!rooms.has(currentRoom)) {
                    rooms.set(currentRoom, new Set());
                }
                rooms.get(currentRoom).add(ws);
                updatePeerCount(currentRoom);
            } else if (data.type === 'packet' && currentRoom) {
                broadcastToRoom(currentRoom, JSON.stringify(data), ws);
            }
        } catch (e) {}
    });

    ws.on('close', () => {
        if (currentRoom && rooms.has(currentRoom)) {
            const roomSet = rooms.get(currentRoom);
            roomSet.delete(ws);
            if (roomSet.size === 0) {
                rooms.delete(currentRoom);
            } else {
                updatePeerCount(currentRoom);
            }
        }
    });
});

server.listen(PORT);
