// One activation, one answer — and the guard that makes it exactly one.
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test, { after } from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  decodeValue,
  defineOperative,
  defineProcedure,
  encodeValue,
  loadNative,
} from '../dist/esm/index.js';
import { Registry, answer, dispatch } from '../dist/esm/dispatch.js';
import { importModule, loadRegistry, specsFrom } from '../dist/esm/runner.js';

import { activationFor, depsFor, inputItem } from './dispatch-fixtures.mjs';

// The handout-contract tests below answer against the core itself, so the
// refusal they assert is the core's own. Nothing starts a runner, so no
// controller connection is attempted.
const native = loadNative();
native.contextNew(ACTIVATION_SCHEMA_VERSION);
after(() => native.contextFree());

/**
 * What the SDK says while `run` executes.
 *
 * Read off the console rather than through `setLogger`, so what is asserted is
 * what a host with no logger of its own would actually see.
 */
async function said(run) {
  const lines = [];
  const console_ = { error: console.error, warn: console.warn, log: console.log };
  console.error = (line) => lines.push(line);
  console.warn = (line) => lines.push(line);
  console.log = (line) => lines.push(line);
  try {
    await run();
  } finally {
    Object.assign(console, console_);
  }
  return lines.join('\n');
}

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
    c.input('in')
      .keyBy((value) => String(value))
      .into(Counter)
      .keyed()
      .sink('out');
  },
});

const fixture = () => depsFor(Spec);
const activation = activationFor('counters');

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
  const result = await answer(
    activation({
      factory: 'throwing_op',
      entityKey: undefined,
      items: [inputItem('ok'), inputItem('boom'), inputItem('never')],
    }),
    depsFor(spec),
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
  const result = await answer(
    activation({ factory: 'rejecting_op', entityKey: undefined, items: [inputItem(1)] }),
    depsFor(spec),
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
  const source = depsFor(spec);
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

  const target = depsFor(spec);
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
  assert.deepEqual(
    result.prepared.map((answerFor) => answerFor.keys),
    [['7'], ['8']],
  );
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
    channels: (c) =>
      c
        .input('in')
        .keyBy((v) => v.k)
        .into(Windowed)
        .keyed()
        .sink('out'),
  });
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
    depsFor(spec),
  );
  assert.deepEqual(result.prepared[0], { keys: ['a'], eventTime: 5n });
  assert.match(result.prepared[1].error, /no event time/);
});

test('an unanswerable activation abandons rather than retiring its entity', async () => {
  // A token the core never handed out: it refuses the completion, and refuses
  // the abandon the guard falls back to. Neither refusal escapes the guard.
  const reported = await said(() =>
    dispatch(activation({ token: 42n, items: [inputItem(1)] }), native, fixture()),
  );
  assert.match(reported, /unknown activation token 42/);
  assert.match(reported, /abandoning activation 42 failed/);
});

test('a completed activation is not also abandoned', async () => {
  // Only `complete` is scripted, and only to reach the path a real handout
  // would reach: the abandon is the core's own, so a guard that abandoned an
  // answered activation would earn its refusal and report it.
  const peer = { complete: async () => {}, abandon: (token) => native.abandon(token) };
  const reported = await said(() =>
    dispatch(activation({ token: 7n, items: [inputItem(1)] }), peer, fixture()),
  );
  assert.equal(reported, '');
});

test('an activation for an unknown factory is answered, not dropped', async () => {
  // The failure reaches the core: it refuses the token the answer carries,
  // which is what tells the completion apart from a dropped activation.
  const completed = [];
  const peer = {
    complete: async (result) => {
      completed.push(result);
      await native.complete(result);
    },
    abandon: (token) => native.abandon(token),
  };
  const reported = await said(() =>
    dispatch(activation({ token: 13n, factory: 'never_declared' }), peer, fixture()),
  );
  assert.equal(completed[0].outcome, 'failed');
  assert.match(completed[0].error, /no class registered/);
  assert.equal(completed[0].nonRetryable, true);
  assert.match(reported, /unknown activation token 13/);
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
  assert.ok(registry.get('from_specifier'), "the module's operative was registered");
  assert.deepEqual(
    specsFrom(module, await importModule(module)).map((s) => s.factoryId),
    ['from_specifier'],
  );
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
