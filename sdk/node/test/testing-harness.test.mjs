// The procedure test harness, and the replay-equality gate it exists for.
import assert from 'node:assert/strict';
import test from 'node:test';

import { defineProcedure, encodeValue } from '../dist/esm/index.js';
import { ProcedureTestHarness, ReplayMismatch } from '../dist/esm/testing.js';

const Window = defineProcedure({
  name: 'rolling_window',
  dynamicEntities: {},
  initialState: () => ({ samples: [] }),
  applyChange: (state, change) => {
    if (change.push !== undefined) state.samples.push(change.push);
    if (change.popFront !== undefined) state.samples.splice(0, change.popFront);
  },
  handle: (value, ctx) => {
    ctx.change({ push: value });
    const over = ctx.state.samples.length - 3;
    if (over > 0) ctx.change({ popFront: over });
    if (ctx.state.samples.length < 3) return undefined;
    return ctx.state.samples.reduce((a, b) => a + b, 0) / 3;
  },
});

test('a defined handler runs with no runner and no controller', async () => {
  // Nothing here starts a runner or loads the native binary: a handler is an
  // ordinary function, and this is the proof of it.
  const ctx = {
    state: { samples: [1, 2] },
    change() {},
    now: () => 0n,
  };
  const returned = await Window.definition.handle(3, {
    ...ctx,
    state: { samples: [1, 2, 3] },
    change() {},
  });
  assert.equal(returned, 2);
});

test('inputs, emits and the recorded change log are inspectable', async () => {
  const harness = ProcedureTestHarness.of(Window, { entityKey: 'AAPL' });
  assert.deepEqual((await harness.input(3)).emits, []);
  assert.deepEqual((await harness.input(6)).emits, []);
  assert.deepEqual((await harness.input(9)).emits, [6]);
  assert.deepEqual((await harness.input(12)).emits, [9]);
  assert.deepEqual(harness.currentState.samples, [6, 9, 12]);
  assert.deepEqual(harness.recordedChanges, [
    { push: 3 },
    { push: 6 },
    { push: 9 },
    { push: 12 },
    { popFront: 1 },
  ]);
  harness.replayEquals();
});

test('the replay gate reports both states when the fold does not rebuild one', async () => {
  // A handler cannot drift by mutating state directly — the guard refuses
  // that. What is left, and what this gate is for here, is an `applyChange`
  // that is not a pure function of (state, change): the live fold and the
  // replay fold then disagree, and only a restart would otherwise show it.
  let folds = 0;
  const Impure = defineProcedure({
    name: 'impure_fold',
    initialState: () => ({ total: 0 }),
    applyChange: (state, change) => {
      folds += 1;
      state.total += change.by * folds;
    },
    handle: (value, ctx) => {
      ctx.change({ by: value });
      return undefined;
    },
  });
  const harness = ProcedureTestHarness.of(Impure);
  await harness.input(2);
  await harness.input(2);
  assert.throws(
    () => harness.replayEquals(),
    (error) =>
      error instanceof ReplayMismatch &&
      error.message.includes('live:') &&
      error.message.includes('replayed:') &&
      error.message.includes('impure_fold'),
  );
});

test('a timer fires with ctx.now() at its scheduled time', async () => {
  const Session = defineProcedure({
    name: 'session',
    initialState: () => ({ closedAt: null }),
    applyChange: (state, change) => {
      state.closedAt = change.closedAt;
    },
    handle: (_input, ctx) => {
      ctx.timerAfter(5_000n);
    },
    onTimer: (firing, ctx) => {
      assert.equal(ctx.now(), firing.scheduledAt);
      ctx.change({ closedAt: ctx.now() });
    },
  });
  const harness = ProcedureTestHarness.of(Session, { now: 1_000n });
  const applied = await harness.input('hello');
  assert.deepEqual(applied.timers, [
    { kind: 'processing_time', scheduledAt: 6_000n, register: true },
  ]);
  await harness.fireTimer('processing_time', 6_000n);
  assert.equal(harness.currentState.closedAt, 6_000n);
  harness.replayEquals();
});

test('the clock the next input is served at can be advanced', async () => {
  const Clocked = defineProcedure({ name: 'clocked', handle: (_i, ctx) => ctx.now() });
  const harness = ProcedureTestHarness.of(Clocked, { now: 10n });
  assert.deepEqual((await harness.input(1)).emits, [10n]);
  harness.advanceBy(5n);
  assert.deepEqual((await harness.input(1)).emits, [15n]);
  harness.advanceTo(100n);
  assert.equal(harness.now, 100n);
});

test('a harness can be built from a restore payload', async () => {
  const harness = ProcedureTestHarness.withPayload(Window, {
    snapshot: encodeValue({ samples: [1, 2] }),
    changes: [encodeValue({ push: 3 })],
  });
  assert.deepEqual(harness.currentState.samples, [1, 2, 3]);
  assert.deepEqual((await harness.input(4)).emits, [3]);
});

test('a payload stamped at an older version reaches the declared migration', () => {
  const Migrating = defineProcedure({
    name: 'migrating',
    schemaVersion: 2,
    minReadableSchemaVersion: 1,
    migrateState: (from, bytes) => {
      assert.equal(from, 1);
      return { items: bytes.length, currency: 'EUR' };
    },
    initialState: () => ({ items: 0, currency: 'EUR' }),
    applyChange: (state, change) => {
      state.items += change.by;
    },
    handle: () => undefined,
  });
  const harness = ProcedureTestHarness.withPayload(Migrating, {
    snapshot: new Uint8Array([1, 2, 3]),
    fromVersion: 1,
  });
  assert.deepEqual(harness.currentState, { items: 3, currency: 'EUR' });
});

test('stableId is distinct within an invocation and stable across a replay', async () => {
  const seen = [];
  const Ids = defineProcedure({
    name: 'ids',
    handle: (_input, ctx) => {
      seen.push(ctx.stableId(), ctx.stableId());
    },
  });
  await ProcedureTestHarness.of(Ids, { entityKey: 'k' }).input(1);
  await ProcedureTestHarness.of(Ids, { entityKey: 'k' }).input(1);
  assert.notEqual(seen[0], seen[1], 'two calls in one invocation differ');
  assert.equal(seen[0], seen[2], 'the first call of a replayed invocation matches');
  assert.equal(seen[1], seen[3]);
  assert.match(seen[0], /^[0-9a-f]{32}$/);
});

test('a reserved metadata key is refused naming the key', async () => {
  const Meta = defineProcedure({
    name: 'meta',
    handle: (_input, ctx) => {
      ctx.emit('v', { metadata: { 'flz.key': 'mine' } });
    },
  });
  await assert.rejects(
    () => ProcedureTestHarness.of(Meta).input(1),
    (error) => error.message.includes('flz.key') && error.message.includes('reserved'),
  );
});
