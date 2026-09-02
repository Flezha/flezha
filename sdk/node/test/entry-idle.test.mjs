// The observer's host half — registration, the tick, contained delivery and
// the accessor's mapping — against a scripted core. The latch itself is the
// core's and is proved there; what this proves is the wiring over it.
import assert from 'node:assert/strict';
import test from 'node:test';

import { EntryIdleWatcher, lastEntryDelivery, tickPeriodMs } from '../dist/esm/entry-idle.js';
import { consoleLogger, setLogger } from '../dist/esm/logger.js';

const GAP_NANOS = 1_500_000_000n;

/** A core whose polls answer a script — the ABI's statuses in order, then none. */
function scriptedNative(answers) {
  const freed = [];
  let next = 1n;
  return {
    freed,
    monotonicNanos: () => 10n ** 12n,
    entryLastDeliveryNanos: () => 0n,
    idleWatchNew: (channel) => (channel === '' ? 0n : next++),
    idleWatchPoll: () => {
      const status = answers.shift() ?? 0;
      return { status, gapNanos: status > 0 ? GAP_NANOS : 0n };
    },
    idleWatchFree: (id) => {
      freed.push(id);
    },
  };
}

function recordingLogger() {
  const lines = [];
  const logger = { debug() {}, info() {}, warn: (m) => lines.push(m), error() {} };
  return { lines, logger };
}

test('one idle and one resumed reach the observer, in the order the core answers', () => {
  const watcher = new EntryIdleWatcher(scriptedNative([0, 1, 0, 2]));
  const seen = [];
  watcher.register('trades_in', 1_000, {
    idle: (gap) => seen.push(['idle', gap]),
    resumed: (gap) => seen.push(['resumed', gap]),
  });
  for (let i = 0; i < 4; i++) watcher.tick();
  assert.deepEqual(seen, [
    ['idle', 1500],
    ['resumed', 1500],
  ]);
  watcher.stop();
});

test('a bare function is an observer whose resume does nothing', () => {
  const watcher = new EntryIdleWatcher(scriptedNative([1, 2, 1]));
  const gaps = [];
  watcher.register('trades_in', 1_000, (gap) => gaps.push(gap));
  for (let i = 0; i < 3; i++) watcher.tick();
  assert.deepEqual(gaps, [1500, 1500]);
  watcher.stop();
});

test('a throwing observer is logged and the next episode still reaches it', () => {
  const { lines, logger } = recordingLogger();
  setLogger(logger);
  try {
    const watcher = new EntryIdleWatcher(scriptedNative([1, 2, 1]));
    let calls = 0;
    watcher.register('book_in', 1_000, {
      idle() {
        calls += 1;
        throw new Error('observer under test');
      },
    });
    for (let i = 0; i < 3; i++) watcher.tick();
    assert.equal(calls, 2, 'the episode after the throw still reaches the observer');
    assert.equal(lines.length, 2);
    assert.match(
      lines[0],
      /entry-idle observer threw on the idle episode of channel 'book_in': observer under test/,
    );
    watcher.stop();
  } finally {
    setLogger(consoleLogger());
  }
});

test('stop frees every watch the core handed out, and is idempotent', () => {
  const native = scriptedNative([]);
  const watcher = new EntryIdleWatcher(native);
  watcher.register('a', 1_000, () => {});
  watcher.register('b', 2_000, () => {});
  watcher.stop();
  assert.deepEqual(native.freed, [1n, 2n]);
  watcher.stop();
  assert.deepEqual(native.freed, [1n, 2n]);
});

test('a refused channel name and a non-positive threshold are refused at registration', () => {
  const watcher = new EntryIdleWatcher(scriptedNative([]));
  assert.throws(() => watcher.register('', 1_000, () => {}), /refused the channel name/);
  assert.throws(() => watcher.register('x', 0, () => {}), RangeError);
});

test('the tick is a quarter of the threshold within its bounds', () => {
  assert.equal(tickPeriodMs(60_000), 1_000);
  assert.equal(tickPeriodMs(2_000), 500);
  assert.equal(tickPeriodMs(10), 250);
});

test('the accessor is absent before the first frame, and an instant derived from the gap after', () => {
  const stamped = 5n * 10n ** 9n;
  const native = {
    monotonicNanos: () => stamped + 2n * 10n ** 9n,
    entryLastDeliveryNanos: (channel) => (channel === 'fed' ? stamped : 0n),
  };
  assert.equal(lastEntryDelivery(native, 'never'), null);
  const before = Date.now();
  const at = lastEntryDelivery(native, 'fed');
  const after = Date.now();
  assert.ok(at instanceof Date);
  assert.ok(at.getTime() >= before - 2_000 && at.getTime() <= after - 2_000, 'two seconds ago');
});
