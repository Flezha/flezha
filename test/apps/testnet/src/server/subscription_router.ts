/**
 * Per-connection Kraken-v2 subscription state: which symbols this socket has
 * asked for on which channel, and the generator timer serving each one.
 *
 * One router per connection is what makes the testnet usable by a partitioned
 * operative: each member subscribes only the symbols in its own key-group
 * range, and must then receive only those. A server that generated for every
 * configured symbol would hand every member the whole set, and the partition
 * guard would park the out-of-range half.
 */

import { Generator } from '../generator/generator_contract';
import { ackFrames, parseSubscriptionRequest } from './kraken_subscription';
import { generatorConfigFor, intervalFor, TestnetConfig } from './testnet_config';

export type Send = (message: string) => void;

/** Timer surface, injected so tests drive generation without wall-clock waits. */
export interface Timers {
  set(fn: () => void, ms: number): unknown;
  clear(handle: unknown): void;
}

const REAL_TIMERS: Timers = {
  set: (fn, ms) => setInterval(fn, ms),
  clear: (handle) => clearInterval(handle as NodeJS.Timeout),
};

export class SubscriptionRouter {
  /** `channel -> symbol -> timer handle`. Presence is the subscription. */
  private readonly active = new Map<string, Map<string, unknown>>();

  constructor(
    private readonly generators: Map<string, Generator>,
    private readonly config: TestnetConfig,
    private readonly send: Send,
    private readonly timers: Timers = REAL_TIMERS,
    private readonly now: () => string = () => new Date().toISOString()
  ) {}

  /**
   * Apply a raw inbound frame. Returns `false` when it is not a Kraken-v2
   * subscription frame, so the caller can fall through to the legacy
   * `event_type` handler.
   */
  handle(message: string): boolean {
    const request = parseSubscriptionRequest(message);
    if (!request) return false;

    const channel = this.config.channels[request.channel];
    if (!channel) {
      console.warn(
        `No generator wired for channel "${request.channel}". ` +
          `Configured: [${Object.keys(this.config.channels).join(', ')}]`
      );
      return true;
    }

    for (const symbol of request.symbols) {
      if (request.method === 'subscribe') {
        this.subscribe(request.channel, symbol);
      } else {
        this.unsubscribe(request.channel, symbol);
      }
    }

    // Ack last: the applied set commits on this frame, so a generator that
    // throws on activation surfaces before the source believes it is streaming.
    for (const frame of ackFrames(request, this.now())) {
      this.send(frame);
    }
    return true;
  }

  /** Symbols currently generating on `channel`, sorted. */
  activeSymbols(channel: string): string[] {
    return [...(this.active.get(channel)?.keys() ?? [])].sort();
  }

  /** Stop every generator this connection started. */
  close(): void {
    for (const symbols of this.active.values()) {
      for (const handle of symbols.values()) {
        this.timers.clear(handle);
      }
    }
    this.active.clear();
  }

  private subscribe(channelName: string, symbol: string): void {
    const symbols = this.active.get(channelName) ?? new Map<string, unknown>();
    this.active.set(channelName, symbols);
    // Re-subscribing a live symbol is a no-op: the source re-asserts its whole
    // applied set on every reconnect, and a second timer would double its rate.
    if (symbols.has(symbol)) return;

    const channel = this.config.channels[channelName];
    const generator = this.generators.get(channel.generator);
    if (!generator) {
      console.warn(
        `Channel "${channelName}" wants generator "${channel.generator}", which is not loaded. ` +
          `Available: [${[...this.generators.keys()].join(', ')}]`
      );
      return;
    }

    const generatorConfig = generatorConfigFor(this.config, channel, symbol);
    const interval = intervalFor(this.config, channel, symbol);
    const tick = () => {
      try {
        this.send(generator.generate(generatorConfig));
      } catch (error) {
        console.error(`Generator "${channel.generator}" error for ${symbol}:`, error);
      }
    };

    console.log(`Subscribing ${channelName}/${symbol} (${channel.generator} every ${interval}ms)`);
    symbols.set(symbol, this.timers.set(tick, interval));
    tick();
  }

  private unsubscribe(channelName: string, symbol: string): void {
    const symbols = this.active.get(channelName);
    const handle = symbols?.get(symbol);
    if (!symbols || handle === undefined) return;
    this.timers.clear(handle);
    symbols.delete(symbol);
    console.log(`Unsubscribing ${channelName}/${symbol}`);
  }
}
