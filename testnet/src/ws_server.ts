import WebSocket from 'ws';

const port = 4500;
const server = new WebSocket.Server({ port });

server.on('connection', (ws) => {
  ws.on('message', (message) => {
    try {
      const jsonMessage = JSON.parse(message.toString());

      switch (jsonMessage.event) {
        case 'subscribe':
          
          ws.send(jsonMessage.default_message);

          setInterval(() => {
            ws.send(jsonMessage.default_message);
          }, 1000);

          break;

        default:
          console.log('Testnet received unknown event:', jsonMessage.event);
          break;
      }
    } catch (error) {
      console.error('Testnet error parsing JSON message:', error);
    }
  });

  ws.on('close', () => {
    console.log('Testnet client disconnected');
  });
});

console.log(`Testnet webSocket server is listening on ws://localhost:${port}`);