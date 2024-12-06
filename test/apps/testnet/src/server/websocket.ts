import WebSocket from 'ws';
import ServerContract from './server_contract';

type Payload = {
  event_type: string;
  event_data: SubscriptionEvent
}

type SubscriptionEvent = {
  default_message: string,
  interval: number
}

export default class ServerImpl implements ServerContract{
  private port: number;

  constructor(port: number) {
    this.port = port;
  }

  public start() {
    const server = new WebSocket.Server({
      port: this.port
    });
  
    server.on('connection', (ws) => {
      console.log('Testnet client connected');
    
      ws.on('message', (message: string) => this.handle_message(ws, message));
      ws.on('close', () => this.handle_close(ws));
    });
  
    console.log(`Testnet webSocket server is listening on ws://localhost:${this.port}`);
  }

  handle_close(ws: WebSocket) {
    console.log('Testnet client disconnected');
  }
  
  handle_message(ws: WebSocket, message: string) {
    try {
      const payload: Payload = JSON.parse(message.toString());
  
      switch (payload.event_type) {
        case 'subscribe':
          this.handle_subscription(ws, payload.event_data);
          break;
  
        default:
          console.log('Testnet received unknown event:', payload.event_type);
          break;
      }
    } catch (error) {
      console.error('Testnet error parsing JSON message:', error);
    }
  }
  
  handle_subscription(ws: WebSocket, subscription: SubscriptionEvent) {
    ws.send(subscription.default_message);
  
    setInterval(() => {
      ws.send(subscription.default_message);
    }, subscription.interval);
  }
}


