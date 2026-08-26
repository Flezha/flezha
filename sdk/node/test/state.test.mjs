// Declared state is mutated only through declared changes.
import assert from 'node:assert/strict';
import test from 'node:test';

import { defineProcedure, StateMutationError } from '../dist/esm/index.js';
import { ProcedureTestHarness } from '../dist/esm/testing.js';

const Counter = defineProcedure({
  name: 'counter',
  initialState: () => ({ total: 0, tags: { last: '' } }),
  applyChange: (state, change) => {
    if (change.kind === 'add') state.total += change.by;
    if (change.kind === 'tag') state.tags.last = change.value;
  },
  handle: (input, ctx) => {
    ctx.change({ kind: 'add', by: input });
    return ctx.state.total;
  },
});

test('a change folds live and the handler reads the folded state', async () => {
  const harness = ProcedureTestHarness.of(Counter);
  assert.deepEqual((await harness.input(3)).emits, [3]);
  assert.deepEqual((await harness.input(4)).emits, [7]);
  assert.equal(harness.currentState.total, 7);
});

test('a direct assignment is refused naming the field and the change path', async () => {
  const Bad = defineProcedure({
    name: 'bad',
    initialState: () => ({ total: 0 }),
    applyChange: (state, change) => {
      state.total += change.by;
    },
    handle: (_input, ctx) => {
      ctx.state.total = 99;
    },
  });
  const harness = ProcedureTestHarness.of(Bad);
  await assert.rejects(
    () => harness.input(1),
    (error) =>
      error instanceof StateMutationError &&
      error.message.includes('state.total') &&
      error.message.includes('ctx.change'),
  );
});

test('the guard reaches a nested field, where the loss is the same', async () => {
  const Nested = defineProcedure({
    name: 'nested',
    initialState: () => ({ tags: { last: '' } }),
    applyChange: (state, change) => {
      state.tags.last = change.value;
    },
    handle: (_input, ctx) => {
      ctx.state.tags.last = 'x';
    },
  });
  const harness = ProcedureTestHarness.of(Nested);
  await assert.rejects(
    () => harness.input(1),
    (error) => error.message.includes('state.tags.last'),
  );
});

test('applyChange retains its ability to mutate', async () => {
  const harness = ProcedureTestHarness.of(Counter);
  await harness.input(5);
  harness.replayEquals();
  assert.equal(harness.currentState.total, 5);
});

test('deleting a field is refused too', async () => {
  const Deleter = defineProcedure({
    name: 'deleter',
    initialState: () => ({ total: 0 }),
    applyChange: (state, change) => {
      state.total += change.by;
    },
    handle: (_input, ctx) => {
      delete ctx.state.total;
    },
  });
  await assert.rejects(
    () => ProcedureTestHarness.of(Deleter).input(1),
    (error) => error instanceof StateMutationError,
  );
});
