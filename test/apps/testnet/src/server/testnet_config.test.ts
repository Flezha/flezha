import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { test } from 'node:test';

import {
  DEFAULT_CONFIG,
  generatorConfigFor,
  intervalFor,
  loadTestnetConfig,
} from './testnet_config';

function writeConfig(contents: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'testnet-config-'));
  const file = path.join(dir, 'testnet-config.json');
  fs.writeFileSync(file, contents);
  return file;
}

// fails when: a missing or malformed config file takes the server down
// instead of degrading — a typo in the mounted file would kill the dev feed.
test('a missing or malformed config falls back to the built-in defaults', () => {
  assert.deepEqual(loadTestnetConfig(undefined), DEFAULT_CONFIG);
  assert.deepEqual(loadTestnetConfig('/nonexistent/testnet-config.json'), DEFAULT_CONFIG);
  assert.deepEqual(loadTestnetConfig(writeConfig('{ not json')), DEFAULT_CONFIG);
});

// fails when: a config file's channels stop reaching the router — every
// symbol would then stream at the built-in cadence, not the tuned one.
test('a config file overrides channels and keeps the unlisted defaults', () => {
  const config = loadTestnetConfig(
    writeConfig(
      JSON.stringify({
        channels: { trade: { generator: 'trade', interval_ms: 1500 } },
        symbol_defaults: { basePrice: 42000 },
        symbols: { 'ETH/DEMO1': { basePrice: 2500 } },
      })
    )
  );
  assert.equal(config.channels.trade.interval_ms, 1500);
  assert.equal(config.channels.book.generator, 'order_book', 'unlisted channels keep their default');
});

// fails when: a per-symbol override drops the shared defaults — a symbol that
// tunes only its interval would lose its base price and stream at 0.
test('per-symbol tuning layers over the shared defaults', () => {
  const config = {
    channels: { trade: { generator: 'trade', interval_ms: 1500, config: { volatility: 0.002 } } },
    symbol_defaults: { basePrice: 42000 },
    symbols: { 'BENCH/USD': { interval_ms: 25 }, 'ETH/DEMO1': { basePrice: 2500 } },
  };

  assert.deepEqual(generatorConfigFor(config, config.channels.trade, 'BENCH/USD'), {
    volatility: 0.002,
    basePrice: 42000,
    pairs: ['BENCH/USD'],
  });
  assert.deepEqual(generatorConfigFor(config, config.channels.trade, 'ETH/DEMO1'), {
    volatility: 0.002,
    basePrice: 2500,
    pairs: ['ETH/DEMO1'],
  });
  assert.deepEqual(generatorConfigFor(config, config.channels.trade, 'NEW/PAIR'), {
    volatility: 0.002,
    basePrice: 42000,
    pairs: ['NEW/PAIR'],
  });

  assert.equal(intervalFor(config, config.channels.trade, 'BENCH/USD'), 25);
  assert.equal(intervalFor(config, config.channels.trade, 'ETH/DEMO1'), 1500);
});
