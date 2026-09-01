import assert from 'node:assert/strict';
import { test } from 'node:test';

import { Generator } from '../generator/generator_contract';
import { SubscriptionRouter, Timers } from './subscription_router';
import { TestnetConfig } from './testnet_config';

/** Timer stub: records every scheduled tick so a test can fire it by hand. */
class FakeTimers implements Timers {
  readonly ticks = new Map<number, { fn: () => void; ms: number }>();
  private next = 1;

  set(fn: () => void, ms: number): unknown {
    const handle = this.next++;
    this.ticks.set(handle, { fn, ms });
    return handle;
  }

  clear(handle: unknown): void {
    this.ticks.delete(handle as number);
  }

  fireAll(): void {
    [...this.ticks.values()].forEach((t) => t.fn());
  }
}

const tradeGenerator: Generator = {
  name: 'trade',
  generate: (config: { pairs: string[]; basePrice?: number }) =>
    JSON.stringify({
      channel: 'trade',
      type: 'update',
      data: [{ symbol: config.pairs[0], price: config.basePrice ?? 0 }],
    }),
  defaultConfig: {},
};

const CONFIG: TestnetConfig = {
  channels: {
    trade: { generator: 'trade', interval_ms: 1500, config: { volatility: 0.002 } },
    book: { generator: 'order_book', interval_ms: 1000 },
  },
  symbol_defaults: { basePrice: 42000 },
  symbols: { 'ETH/DEMO1': { basePrice: 2500 }, 'BENCH/USD': { interval_ms: 25 } },
};

function router(): {
  router: SubscriptionRouter;
  sent: string[];
  timers: FakeTimers;
} {
  const sent: string[] = [];
  const timers = new FakeTimers();
  return {
    router: new SubscriptionRouter(
      new Map([[tradeGenerator.name, tradeGenerator]]),
      CONFIG,
      (m) => sent.push(m),
      timers,
      () => '2026-08-16T00:00:00.000Z'
    ),
    sent,
    timers,
  };
}

function dataFrames(sent: string[]): Record<string, unknown>[] {
  return sent.map((s) => JSON.parse(s)).filter((f) => f.channel === 'trade' && f.data);
}

// fails when: a subscribed symbol produces no data — the dev feed goes silent
// and every chart/trade-feed journey fails with an empty chart.
test('a subscribed symbol generates immediately and on every tick', () => {
  const { router: r, sent, timers } = router();
  r.handle('{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/DEMO1"]},"req_id":1}');

  assert.deepEqual(r.activeSymbols('trade'), ['BTC/DEMO1']);
  assert.equal(dataFrames(sent).length, 1, 'first frame is sent on subscribe, not one interval later');
  timers.fireAll();
  assert.equal(dataFrames(sent).length, 2);
});

// fails when: the server generates for symbols this connection never asked
// for — a partitioned member would receive out-of-range symbols and the
// partition guard would park them to the DLQ.
test('only subscribed symbols are generated for this connection', () => {
  const { router: r, sent, timers } = router();
  r.handle('{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/DEMO2"]},"req_id":1}');
  timers.fireAll();

  const symbols = new Set(dataFrames(sent).map((f) => (f.data as { symbol: string }[])[0].symbol));
  assert.deepEqual([...symbols], ['BTC/DEMO2']);
});

// fails when: per-symbol server-side tuning stops being applied — every demo
// pair would then stream at one base price and ETH candles read as BTC ones.
test('per-symbol config from the server config reaches the generator', () => {
  const { router: r, sent } = router();
  r.handle(
    '{"method":"subscribe","params":{"channel":"trade","symbol":["ETH/DEMO1","BTC/DEMO1"]},"req_id":1}'
  );

  const prices = Object.fromEntries(
    dataFrames(sent).map((f) => {
      const entry = (f.data as { symbol: string; price: number }[])[0];
      return [entry.symbol, entry.price];
    })
  );
  assert.deepEqual(prices, { 'ETH/DEMO1': 2500, 'BTC/DEMO1': 42000 });
});

// fails when: a per-symbol interval override is ignored — the back-pressure
// bench cannot reach its load target from server config alone.
test('a per-symbol interval overrides the channel cadence', () => {
  const { router: r, timers } = router();
  r.handle(
    '{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/DEMO1","BENCH/USD"]},"req_id":1}'
  );
  assert.deepEqual(
    [...timers.ticks.values()].map((t) => t.ms),
    [1500, 25]
  );
});

// fails when: unsubscribe leaves the generator running — a symbol an admin
// removed would keep producing candles forever.
test('unsubscribe stops that symbol and leaves the others streaming', () => {
  const { router: r, timers } = router();
  r.handle(
    '{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/DEMO1","ETH/DEMO1"]},"req_id":1}'
  );
  r.handle('{"method":"unsubscribe","params":{"channel":"trade","symbol":["BTC/DEMO1"]},"req_id":2}');

  assert.deepEqual(r.activeSymbols('trade'), ['ETH/DEMO1']);
  assert.equal(timers.ticks.size, 1);
});

// fails when: a re-assert doubles the timer — the connect-time re-subscribe
// would multiply the feed rate on every reconnect.
test('re-subscribing a live symbol does not double its rate', () => {
  const { router: r, timers } = router();
  const frame = '{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/DEMO1"]},"req_id":1}';
  r.handle(frame);
  r.handle(frame.replace('"req_id":1', '"req_id":0'));

  assert.equal(timers.ticks.size, 1);
});

// fails when: acks stop being sent — the source never commits its delta and
// re-sends the same subscribe every poll, forever.
test('every request is acked once per symbol', () => {
  const { router: r, sent } = router();
  r.handle(
    '{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/DEMO1","ETH/DEMO1"]},"req_id":4}'
  );
  const acks = sent.map((s) => JSON.parse(s)).filter((f) => f.method === 'subscribe');
  assert.equal(acks.length, 2);
  assert.deepEqual(
    acks.map((a) => a.req_id),
    [4, 4]
  );
});

// fails when: a disconnect leaves timers running — every reconnect would leak
// a generator and the feed rate would climb without bound.
test('closing a connection stops all of its generators', () => {
  const { router: r, timers } = router();
  r.handle(
    '{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/DEMO1","ETH/DEMO1"]},"req_id":1}'
  );
  r.close();

  assert.equal(timers.ticks.size, 0);
  assert.deepEqual(r.activeSymbols('trade'), []);
});

// fails when: a legacy `event_type` payload is swallowed by the v2 path,
// breaking every existing testnet consumer.
test('a legacy payload is left to the legacy handler', () => {
  const { router: r } = router();
  assert.equal(r.handle('{"event_type":"subscribe","event_data":{"generators":[]}}'), false);
});

// fails when: an unconfigured channel is acked anyway — the source would
// believe it is streaming a channel nothing serves, which reads downstream as
// a dead pipeline rather than a misconfigured testnet.
test('an unknown channel starts nothing and is not acked', () => {
  const { router: r, sent, timers } = router();
  assert.equal(
    r.handle('{"method":"subscribe","params":{"channel":"ohlc","symbol":["BTC/DEMO1"]},"req_id":1}'),
    true
  );
  assert.equal(timers.ticks.size, 0);
  assert.equal(sent.length, 0);
});
