// One activation, one answer — and the guard that makes it exactly one.
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { decodeValue, defineOperative, defineProcedure, encodeValue } from '../dist/esm/index.js';
import { Registry, answer, dispatch } from '../dist/esm/dispatch.js';
import { importModule, loadRegistry, specsFrom } from '../dist/esm/runner.js';

const Counter = defineProcedure({
  name: 'counter',
  dynamicEntities: {},
  initialState: () => ({ total: 0 }),
  applyChange: (state, change) => {
    state.total += change.by;
  },
  handle: (input, ctx) => {
    ctx.change({ by: input });
    return ctx.state.total;
  },
});

const Spec = defineOperative({
  factoryId: 'counters',
  channels: (c) => {
    c.input('in').keyBy((value) => String(value)).into(Counter).keyed().sink('out');
  },
});

function fixture() {
  const registry = new Registry();
  registry.add(Spec);
  return {
    registry,
    bridge: { spawnWorkflow: async () => 'spawned' },
    workflowCtx: () => ({}),
  };
}

const activation = (overrides) => ({
  token: 1n,
  operative: 'op-1',
  procedure: 0,
  entityKey: 'k',
  changeSeq: 0n,
  factory: 'counters',
  kind: 'work',
  ...overrides,
});

const inputItem = (value, overrides = {}) => ({
  kind: 'input',
  channel: 0,
  payload: Buffer.from(encodeValue(value)),
  metadata: [],
  replay: false,
  now: 1_000n,
  ...overrides,
});

test('a work batch answers one effect per item, in order', async () => {
  const result = await answer(activation({ items: [inputItem(2), inputItem(3)] }), fixture());
  assert.equal(result.outcome, 'work');
  assert.equal(result.effects.length, 2);
  assert.deepEqual(
    result.effects.map((effect) => effect.verdict),
    ['ok', 'ok'],
  );
  assert.deepEqual(
    result.effects.flatMap((effect) => effect.outputs.map((o) => decodeValue(o.payload))),
    [2, 5],
  );
  assert.equal(result.effects[0].outputs[0].target, 'plugin');
  assert.equal(result.effects[0].outputs[0].pluginChannel, 'out');
  assert.equal(result.effects[0].outputs[0].key, 'k');
  assert.equal(result.effects[0].eventTime, 1_000n);
});

test('a throwing handler is a retryable verdict and stops the batch', async () => {
  const Throwing = defineProcedure({
    name: 'throwing',
    handle: (input) => {
      if (input === 'boom') throw new Error('handler exploded');
      return input;
    },
  });
  const spec = defineOperative({
    factoryId: 'throwing_op',
    channels: (c) => c.input('in').into(Throwing).sink('out'),
  });
  const registry = new Registry();
  registry.add(spec);
  const deps = { registry, bridge: { spawnWorkflow: async () => '' }, workflowCtx: () => ({}) };

  const result = await answer(
    activation({
      factory: 'throwing_op',
      entityKey: undefined,
      items: [inputItem('ok'), inputItem('boom'), inputItem('never')],
    }),
    deps,
  );
  assert.equal(result.effects.length, 2, 'items after the failure are re-handed by the core');
  assert.equal(result.effects[1].verdict, 'retryable');
  assert.match(result.effects[1].message, /handler exploded/);
});

test('a rejected promise is answered the same way a throw is', async () => {
  const Rejecting = defineProcedure({
    name: 'rejecting',
    handle: async () => {
      throw Object.assign(new Error('gave up'), { nonRetryable: true });
    },
  });
  const spec = defineOperative({
    factoryId: 'rejecting_op',
    channels: (c) => c.input('in').into(Rejecting).sink('out'),
  });
  const registry = new Registry();
  registry.add(spec);
  const result = await answer(
    activation({ factory: 'rejecting_op', entityKey: undefined, items: [inputItem(1)] }),
    { registry, bridge: { spawnWorkflow: async () => '' }, workflowCtx: () => ({}) },
  );
  assert.equal(result.effects[0].verdict, 'non_retryable');
  assert.match(result.effects[0].message, /gave up/);
});

test('a restore fold rebuilds the state, and a snapshot encodes it back', async () => {
  const deps = fixture();
  const restored = await answer(
    activation({
      kind: 'restore_fold',
      snapshot: { bytes: Buffer.from(encodeValue({ total: 10 })) },
      changes: [
        { bytes: Buffer.from(encodeValue({ by: 5 })) },
        { bytes: Buffer.from(encodeValue({ by: 2 })) },
      ],
    }),
    deps,
  );
  assert.equal(restored.outcome, 'restored');

  const snapshot = await answer(activation({ kind: 'snapshot_request' }), deps);
  assert.deepEqual(decodeValue(snapshot.state), { total: 17 });

  const read = await answer(activation({ kind: 'live_state_read' }), deps);
  assert.deepEqual(JSON.parse(read.json.toString()), { total: 17 });
});

test('a migration handover round-trips through the JSON form', async () => {
  const deps = fixture();
  await answer(
    activation({
      kind: 'restore_fold',
      changes: [{ bytes: Buffer.from(encodeValue({ by: 4 })) }],
    }),
    deps,
  );
  const serialized = await answer(activation({ kind: 'migration_serialize' }), deps);

  const receiving = fixture();
  const applied = await answer(
    activation({ kind: 'apply_state', stateJson: serialized.json }),
    receiving,
  );
  assert.equal(applied.outcome, 'state_applied');
  const snapshot = await answer(activation({ kind: 'snapshot_request' }), receiving);
  assert.deepEqual(decodeValue(snapshot.state), { total: 4 });
});

test('the JSON handover carries a bigint and a byte array without narrowing', async () => {
  const Wide = defineProcedure({
    name: 'wide',
    initialState: () => ({ offset: 0n, blob: new Uint8Array() }),
    applyChange: (state, change) => {
      state.offset = change.offset;
      state.blob = change.blob;
    },
    handle: () => undefined,
  });
  const spec = defineOperative({
    factoryId: 'wide_op',
    channels: (c) => c.input('in').into(Wide).sink('out'),
  });
  const source = { registry: new Registry(), bridge: {}, workflowCtx: () => ({}) };
  source.registry.add(spec);
  const huge = (1n << 63n) + 7n;
  await answer(
    activation({
      factory: 'wide_op',
      entityKey: undefined,
      kind: 'restore_fold',
      changes: [{ bytes: Buffer.from(encodeValue({ offset: huge, blob: new Uint8Array([9]) })) }],
    }),
    source,
  );
  const serialized = await answer(
    activation({ factory: 'wide_op', entityKey: undefined, kind: 'migration_serialize' }),
    source,
  );

  const target = { registry: new Registry(), bridge: {}, workflowCtx: () => ({}) };
  target.registry.add(spec);
  await answer(
    activation({
      factory: 'wide_op',
      entityKey: undefined,
      kind: 'apply_state',
      stateJson: serialized.json,
    }),
    target,
  );
  const snapshot = await answer(
    activation({ factory: 'wide_op', entityKey: undefined, kind: 'snapshot_request' }),
    target,
  );
  const state = decodeValue(snapshot.state);
  assert.equal(state.offset, huge);
  assert.deepEqual([...state.blob], [9]);
});

test('a prepare crossing answers one key per payload, in order', async () => {
  const result = await answer(
    activation({
      kind: 'key_request',
      entityKey: undefined,
      keyRequest: {
        channel: 0,
        payloads: [encodeValue(7), encodeValue(8)].map(Buffer.from),
        wantKeys: true,
        wantEventTime: false,
      },
    }),
    fixture(),
  );
  assert.equal(result.outcome, 'prepared');
  assert.deepEqual(result.prepared.map((answerFor) => answerFor.keys), [['7'], ['8']]);
});

test('one payload failing its prepare does not fail the others', async () => {
  const Windowed = defineProcedure({
    name: 'windowed',
    dynamicEntities: {},
    clock: 'event_time',
    eventTime: (input) => {
      if (input.at === undefined) throw new Error('no event time on this payload');
      return BigInt(input.at);
    },
    handle: (input) => input,
  });
  const spec = defineOperative({
    factoryId: 'windowed_op',
    channels: (c) => c.input('in').keyBy((v) => v.k).into(Windowed).keyed().sink('out'),
  });
  const registry = new Registry();
  registry.add(spec);
  const result = await answer(
    activation({
      factory: 'windowed_op',
      entityKey: undefined,
      kind: 'key_request',
      keyRequest: {
        channel: 0,
        payloads: [{ k: 'a', at: 5 }, { k: 'b' }].map((v) => Buffer.from(encodeValue(v))),
        wantKeys: true,
        wantEventTime: true,
      },
    }),
    { registry, bridge: {}, workflowCtx: () => ({}) },
  );
  assert.deepEqual(result.prepared[0], { keys: ['a'], eventTime: 5n });
  assert.match(result.prepared[1].error, /no event time/);
});

test('an unanswerable activation abandons rather than retiring its entity', async () => {
  const answered = [];
  const fake = {
    complete: async () => {
      throw new Error('the core refused the completion');
    },
    abandon: (token) => answered.push(token),
  };
  await dispatch(activation({ items: [inputItem(1)] }), fake, fixture());
  assert.deepEqual(answered, [1n], 'the finally abandoned what complete could not answer');
});

test('a completed activation is not also abandoned', async () => {
  const answered = [];
  const completed = [];
  const fake = {
    complete: async (result) => completed.push(result.token),
    abandon: (token) => answered.push(token),
  };
  await dispatch(activation({ items: [inputItem(1)] }), fake, fixture());
  assert.deepEqual(completed, [1n]);
  assert.deepEqual(answered, []);
});

test('an activation for an unknown factory is answered, not dropped', async () => {
  const completed = [];
  const fake = { complete: async (r) => completed.push(r), abandon: () => {} };
  await dispatch(activation({ factory: 'never_declared' }), fake, fixture());
  assert.equal(completed[0].outcome, 'failed');
  assert.match(completed[0].error, /no class registered/);
  assert.equal(completed[0].nonRetryable, true);
});

test('a module specifier is imported and its operatives registered', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'flezha-specifier-'));
  const module = join(dir, 'handlers.mjs');
  writeFileSync(
    module,
    `import { defineOperative, defineProcedure } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `const Echo = defineProcedure({ name: 'echo', handle: (i) => i });\n` +
      `export default defineOperative({ factoryId: 'from_specifier', channels: (c) => c.input('in').into(Echo).sink('out') });\n`,
  );
  const registry = await loadRegistry([module]);
  assert.ok(registry.get('from_specifier'), 'the module\'s operative was registered');
  assert.deepEqual(specsFrom(module, await importModule(module)).map((s) => s.factoryId), [
    'from_specifier',
  ]);
});

test('an unimportable specifier is named, with the original error chained', async () => {
  await assert.rejects(
    () => importModule('./definitely-not-here.mjs'),
    (error) =>
      error.message.includes('./definitely-not-here.mjs') &&
      error.message.includes('module specifier') &&
      error.cause instanceof Error,
  );
});

test('a module exporting no operative is refused naming the specifier', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'flezha-empty-'));
  const module = join(dir, 'empty.mjs');
  writeFileSync(module, 'export const nothing = 1;\n');
  await assert.rejects(
    () => loadRegistry([module]),
    (error) => error.message.includes(module) && error.message.includes('exports no operative'),
  );
});

test('a duplicate factory id fails at the second declaration, naming it', () => {
  const registry = new Registry();
  registry.add(Spec);
  assert.throws(
    () => registry.add(Spec),
    (error) => error.message.includes('duplicate factory id') && error.message.includes('counters'),
  );
});
