const WebSocket = require('ws');
const server = new WebSocket.Server({ port: process.env.PORT || 9090 });
const rooms = new Map();

server.on('connection', (ws) => {
    let currentRoom = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            if (data.type === 'join') {
                currentRoom = data.code;
                if (!rooms.has(currentRoom)) rooms.set(currentRoom, new Set());
                rooms.get(currentRoom).add(ws);
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
        if (currentRoom && rooms.has(currentRoom)) {
            rooms.get(currentRoom).delete(ws);
            if (rooms.get(currentRoom).size === 0) rooms.delete(currentRoom);
        }
    });
});
