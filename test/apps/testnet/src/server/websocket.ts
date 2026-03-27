import WebSocket from 'ws';
import ServerContract from './server_contract';
import { Generator, GeneratorSubscription } from '../generator/generator_contract';
import { loadGenerators } from '../generator/generator_loader';

type Payload = {
  event_type: string;
  event_data: SubscriptionEvent;
};

type SubscriptionEvent = {
  /** Legacy: echo this message at interval. */
  default_message?: string;
  /** Legacy: interval for default_message echo. */
  interval?: number;
  /** New: activate named generators with per-generator intervals and config. */
  generators?: GeneratorSubscription[];
};

export default class ServerImpl implements ServerContract {
  private port: number;
  private generators: Map<string, Generator>;
  private intervals: NodeJS.Timeout[] = [];

  constructor(port: number, generatorsPath: string) {
    this.port = port;
    this.generators = loadGenerators(generatorsPath);
  }

  public start() {
    const server = new WebSocket.Server({
      port: this.port,
    });

    server.on('connection', (ws) => {
      console.log('Testnet client connected');

      ws.on('message', (message: string) => this.handle_message(ws, message));
      ws.on('close', () => this.handle_close(ws));
    });

    console.log(
      `Testnet webSocket server is listening on ws://localhost:${this.port}`
    );
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
    // New generator-based subscriptions
    if (subscription.generators && subscription.generators.length > 0) {
      this.handle_generator_subscription(ws, subscription.generators);
      return;
    }

    // Legacy: echo default_message at interval
    if (subscription.default_message && subscription.interval) {
      ws.send(subscription.default_message);

      const intervalId = setInterval(() => {
        ws.send(subscription.default_message!);
      }, subscription.interval);

      this.intervals.push(intervalId);
    }
  }

  private handle_generator_subscription(
    ws: WebSocket,
    generatorSubs: GeneratorSubscription[]
  ) {
    for (const sub of generatorSubs) {
      const generator = this.generators.get(sub.type);

      if (!generator) {
        console.warn(
          `No generator found for type "${sub.type}". Available: [${Array.from(this.generators.keys()).join(', ')}]`
        );
        continue;
      }

      const config = sub.config || generator.defaultConfig;
      const interval = sub.interval || 3000;

      console.log(
        `Activating generator "${sub.type}" with interval ${interval}ms`
      );

      // Send first message immediately
      try {
        const msg = generator.generate(config);
        ws.send(msg);
      } catch (err) {
        console.error(`Generator "${sub.type}" error on initial send:`, err);
      }

      // Schedule periodic generation
      const intervalId = setInterval(() => {
        try {
          const msg = generator.generate(config);
          ws.send(msg);
        } catch (err) {
          console.error(`Generator "${sub.type}" error:`, err);
        }
      }, interval);

      this.intervals.push(intervalId);
    }
  }
}
