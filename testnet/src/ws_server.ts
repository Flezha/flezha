import WebSocket from 'ws';

const port = 4500;
const server = new WebSocket.Server({ port });

type Payload = {
  event_type: string;
  event_data: SubscriptionEvent
}

type SubscriptionEvent = {
  default_message: string,
  interval: number
}

server.on('connection', (ws) => {
  console.log('Testnet client connected');

  ws.on('message', (message: string) => handle_message(ws, message));
  ws.on('close', () => handle_close(ws));
});

function handle_close(ws: WebSocket) {
  console.log('Testnet client disconnected');
}

function handle_message(ws: WebSocket, message: string) {
  try {
    const payload: Payload = JSON.parse(message.toString());

    switch (payload.event_type) {
      case 'subscribe':
        console.log(payload)

        const subscription: SubscriptionEvent = payload.event_data;


        ws.send(subscription.default_message);

        setInterval(() => {
          ws.send(subscription.default_message);
        }, subscription.interval);

        break;

      default:
        console.log('Testnet received unknown event:', payload.event_type);
        break;
    }
  } catch (error) {
    console.error('Testnet error parsing JSON message:', error);
  }
}

console.log(`Testnet webSocket server is listening on ws://localhost:${port}`);