const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 9090;
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'online', service: 'openxill-core-proxy' }));
});

const wss = new WebSocket.Server({ server });
const rooms = new Map();
const nodeRegistry = new Map();

function broadcastPoolState(roomCode) {
    const clients = rooms.get(roomCode);
    if (!clients || clients.size === 0) return;

    let totalRamMb = 0;
    let nodeCount = 0;

    for (const client of clients) {
        const stats = nodeRegistry.get(client);
        if (stats) {
            totalRamMb += stats.ramMb || 0;
            nodeCount++;
        }
    }

    const payload = JSON.stringify({
        type: 'pool_update',
        room: roomCode,
        totalRamMb: totalRamMb,
        nodeCount: nodeCount,
        timestamp: Date.now()
    });

    for (const client of clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    }
}

wss.on('connection', (ws) => {
    let currentRoom = null;

    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            if (data.type === 'join') {
                currentRoom = data.code;
                if (!rooms.has(currentRoom)) {
                    rooms.set(currentRoom, new Set());
                }
                rooms.get(currentRoom).add(ws);

                if (data.ramMb) {
                    nodeRegistry.set(ws, { ramMb: Number(data.ramMb) || 0 });
                }

                broadcastPoolState(currentRoom);
            } else if (data.type === 'telemetry') {
                if (currentRoom) {
                    nodeRegistry.set(ws, { ramMb: Number(data.ramMb) || 0 });
                    broadcastPoolState(currentRoom);
                }
            } else if (data.type === 'packet' && currentRoom) {
                const clients = rooms.get(currentRoom);
                if (clients) {
                    for (const client of clients) {
                        if (client !== ws && client.readyState === WebSocket.OPEN) {
                            client.send(JSON.stringify(data));
                        }
                    }
                }
            }
        } catch (e) {}
    });

    ws.on('close', () => {
        nodeRegistry.delete(ws);
        if (currentRoom && rooms.has(currentRoom)) {
            const roomSet = rooms.get(currentRoom);
            roomSet.delete(ws);
            if (roomSet.size === 0) {
                rooms.delete(currentRoom);
            } else {
                broadcastPoolState(currentRoom);
            }
        }
    });
});

const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 25000);

wss.on('close', () => {
    clearInterval(interval);
});

server.listen(PORT);
