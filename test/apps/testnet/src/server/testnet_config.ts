/**
 * Server-side generator tuning, read once at start from the JSON file named
 * by `FLEZHA_TESTNET_CONFIG`.
 *
 * With Kraken-v2 subscriptions the client sends only a channel and a symbol
 * list — there is no room in the frame for a base price or a tick interval,
 * and inventing one would stop the dev feed being shaped like the real
 * exchange. So the tuning lives here instead:
 *
 * ```json
 * {
 *   "channels": {
 *     "trade": { "generator": "trade", "interval_ms": 1500, "config": { "volatility": 0.002 } },
 *     "book":  { "generator": "order_book", "interval_ms": 1000, "config": { "levels": 100 } }
 *   },
 *   "symbol_defaults": { "basePrice": 42000 },
 *   "symbols": { "ETH/DEMO1": { "basePrice": 2500 }, "BENCH/USD": { "interval_ms": 25 } }
 * }
 * ```
 *
 * A symbol nobody configured still streams, on `symbol_defaults` — an admin
 * subscribing a fresh pair gets data without a server restart.
 */

import * as fs from 'fs';

/** Per-Kraken-channel wiring: which generator serves it, and how fast. */
export interface ChannelConfig {
  /** Loaded generator's `name` (e.g. `trade`, `order_book`). */
  generator: string;
  /** Milliseconds between generated frames, unless the symbol overrides it. */
  interval_ms: number;
  /** Generator config merged under the per-symbol entry. */
  config?: Record<string, unknown>;
}

/** Per-symbol tuning. `interval_ms` overrides the channel's cadence. */
export type SymbolConfig = Record<string, unknown> & { interval_ms?: number };

export interface TestnetConfig {
  channels: Record<string, ChannelConfig>;
  symbol_defaults: SymbolConfig;
  symbols: Record<string, SymbolConfig>;
}

export const DEFAULT_CONFIG: TestnetConfig = {
  channels: {
    trade: { generator: 'trade', interval_ms: 3000 },
    book: { generator: 'order_book', interval_ms: 3000 },
  },
  symbol_defaults: {},
  symbols: {},
};

/** Read the config file, or fall back to {@link DEFAULT_CONFIG}. Never throws. */
export function loadTestnetConfig(configPath?: string): TestnetConfig {
  if (!configPath) {
    console.log('No FLEZHA_TESTNET_CONFIG set — using built-in channel defaults');
    return DEFAULT_CONFIG;
  }
  if (!fs.existsSync(configPath)) {
    console.warn(`Testnet config not found at ${configPath} — using built-in channel defaults`);
    return DEFAULT_CONFIG;
  }
  try {
    const parsed = JSON.parse(fs.readFileSync(configPath, 'utf8')) as Partial<TestnetConfig>;
    const config: TestnetConfig = {
      channels: { ...DEFAULT_CONFIG.channels, ...(parsed.channels ?? {}) },
      symbol_defaults: parsed.symbol_defaults ?? {},
      symbols: parsed.symbols ?? {},
    };
    console.log(
      `Loaded testnet config from ${configPath}: channels [${Object.keys(config.channels).join(', ')}], ` +
        `${Object.keys(config.symbols).length} tuned symbol(s)`
    );
    return config;
  } catch (error) {
    console.error(`Failed to parse testnet config ${configPath}:`, error);
    return DEFAULT_CONFIG;
  }
}

/**
 * The generator config for one `(channel, symbol)` pair: channel config,
 * overlaid with the symbol's own tuning, pinned to that single symbol so a
 * generator serves exactly the symbol it was activated for.
 */
export function generatorConfigFor(
  config: TestnetConfig,
  channel: ChannelConfig,
  symbol: string
): Record<string, unknown> {
  const { interval_ms: _interval, ...tuning } = symbolTuning(config, symbol);
  return { ...(channel.config ?? {}), ...tuning, pairs: [symbol] };
}

/** Tick interval for one `(channel, symbol)` pair, in milliseconds. */
export function intervalFor(config: TestnetConfig, channel: ChannelConfig, symbol: string): number {
  return symbolTuning(config, symbol).interval_ms ?? channel.interval_ms;
}

/** A symbol's own entry laid over the shared defaults, so one override never drops the rest. */
function symbolTuning(config: TestnetConfig, symbol: string): SymbolConfig {
  return { ...config.symbol_defaults, ...(config.symbols[symbol] ?? {}) };
}
