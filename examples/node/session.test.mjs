// The example's own test — no runner, no controller, no native binary.
import assert from 'node:assert/strict';
import test from 'node:test';

import { TaskletTestHarness } from '@flezha/sdk/testing';

import { Session } from './session.mjs';

test('a quiet session closes on its timer and reports what it saw', async () => {
  const harness = TaskletTestHarness.of(Session, { cellKey: 'ada', now: 1_000n });

  const first = await harness.input({ user: 'ada', action: 'login' });
  assert.deepEqual(first.timers, [
    { kind: 'processing_time', scheduledAt: 31_000n, register: true },
  ]);

  harness.advanceTo(10_000n);
  const second = await harness.input({ user: 'ada', action: 'search' });
  assert.deepEqual(second.timers, [
    // The deadline the input replaced is cancelled, then the next is armed.
    { kind: 'processing_time', scheduledAt: 31_000n, register: false },
    { kind: 'processing_time', scheduledAt: 40_000n, register: true },
  ]);

  const closed = await harness.fireTimer('processing_time', 40_000n);
  assert.equal(closed.emits.length, 1);
  assert.equal(closed.emits[0].user, 'ada');
  assert.equal(closed.emits[0].events, 2);
  assert.equal(closed.emits[0].closedAt, '40000');

  // The gate: the recorded change log rebuilds exactly the state the handler
  // produced, which is how the engine rebuilds this cell after a restart.
  harness.replayEquals();
});

test('a session that keeps talking is never closed twice', async () => {
  const harness = TaskletTestHarness.of(Session, { cellKey: 'grace' });
  await harness.input({ user: 'grace', action: 'login' });
  const closed = await harness.fireTimer('processing_time', 30_000n);
  assert.equal(closed.emits.length, 1);
  const again = await harness.fireTimer('processing_time', 30_000n);
  assert.deepEqual(again.emits, [], 'a closed session emits once');
  harness.replayEquals();
});
