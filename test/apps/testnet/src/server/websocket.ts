import WebSocket from 'ws';
import ServerContract, { ConnectedSocket, InputSockets } from './server_contract';
import { ConnectionRole, requestPath, roleFor } from './connection_paths';
import { formatOutputFrame } from './output_frame';
import { Generator, GeneratorSubscription } from '../generator/generator_contract';
import { loadGenerators } from '../generator/generator_loader';
import { loadTestnetConfig, TestnetConfig } from './testnet_config';
import { SubscriptionRouter } from './subscription_router';

type Payload = {
  event_type: string;
  event_data: SubscriptionEvent;
};

type SubscriptionEvent = {
  /** Legacy: echo this message at interval. */
  default_message?: string;
  /** Legacy: interval for default_message echo. */
  interval?: number;
  /** Legacy: activate named generators with per-generator intervals and config. */
  generators?: GeneratorSubscription[];
};

export default class ServerImpl implements ServerContract, InputSockets {
  private port: number;
  private generators: Map<string, Generator>;
  private config: TestnetConfig;
  /** Per-connection state, so a disconnect stops exactly that socket's timers. */
  private connections: Map<
    WebSocket,
    { router: SubscriptionRouter; legacy: NodeJS.Timeout[]; path: string; role: ConnectionRole }
  > = new Map();

  constructor(port: number, generatorsPath: string, configPath?: string) {
    this.port = port;
    this.generators = loadGenerators(generatorsPath);
    this.config = loadTestnetConfig(configPath);
  }

  public start() {
    const server = new WebSocket.Server({
      port: this.port,
    });

    server.on('connection', (ws, request) => {
      const path = requestPath(request.url);
      console.log(`Testnet client connected on ${path}`);
      this.connections.set(ws, {
        router: new SubscriptionRouter(this.generators, this.config, (message) => ws.send(message)),
        legacy: [],
        path,
        role: roleFor(path),
      });

      ws.on('message', (message: string) => this.handle_message(ws, message));
      ws.on('close', () => this.handle_close(ws));
    });

    console.log(
      `Testnet webSocket server is listening on ws://localhost:${this.port}`
    );
  }

  /** @see InputSockets.broadcast */
  public broadcast(message: string): number {
    let recipients = 0;
    this.connections.forEach((connection, ws) => {
      if (connection.role === 'output') return;
      if (ws.readyState !== WebSocket.OPEN) return;
      ws.send(message);
      recipients += 1;
    });
    return recipients;
  }

  /** @see InputSockets.connected */
  public connected(): ConnectedSocket[] {
    return [...this.connections.values()].map(({ path, role }) => ({ path, role }));
  }

  handle_close(ws: WebSocket) {
    const connection = this.connections.get(ws);
    if (connection) {
      connection.router.close();
      connection.legacy.forEach(clearInterval);
      this.connections.delete(ws);
    }
    console.log('Testnet client disconnected');
  }

  handle_message(ws: WebSocket, message: string) {
    const raw = message.toString();
    const connection = this.connections.get(ws);

    // An output socket is a sink reporting what an operative emitted: print it
    // and stop. Before the router, so a payload that happens to look like a
    // subscription frame cannot start generators on the sink's own socket.
    if (connection?.role === 'output') {
      console.log(formatOutputFrame(raw));
      return;
    }

    // Kraken-v2 subscriptions first: they are what the reconciling WS source
    // sends, and a partitioned member's applied set is exactly these frames.
    if (connection?.router.handle(raw)) return;

    try {
      const payload: Payload = JSON.parse(raw);

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

      this.connections.get(ws)?.legacy.push(intervalId);
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

      this.connections.get(ws)?.legacy.push(intervalId);
    }
  }
}
