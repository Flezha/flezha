// One activation, one answer — and the guard that makes it exactly one.
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';
import test, { after } from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  DeclarationError,
  decodeValue,
  defineOperative,
  defineProcedure,
  defineWorkflow,
  encodeValue,
  loadNative,
} from '../dist/esm/index.js';
import { Registry, answer, dispatch } from '../dist/esm/dispatch.js';
import { importModule, loadRegistry, specsFrom } from '../dist/esm/runner.js';

import { activationFor, depsFor, inputItem, jsonOf } from './dispatch-fixtures.mjs';

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
    result.effects.flatMap((effect) => effect.outputs.map((o) => jsonOf(o.payload))),
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
    channels: (c) => c.input('in').into(Throwing).sink(),
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
    channels: (c) => c.input('in').into(Rejecting).sink(),
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
    channels: (c) => c.input('in').into(Wide).sink(),
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
        payloads: ['7', '8'].map((text) => Buffer.from(text)),
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
        .sink(),
  });
  const result = await answer(
    activation({
      factory: 'windowed_op',
      entityKey: undefined,
      kind: 'key_request',
      keyRequest: {
        channel: 0,
        payloads: [{ k: 'a', at: 5 }, { k: 'b' }].map((v) => Buffer.from(JSON.stringify(v))),
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
      `export default defineOperative({ factoryId: 'from_specifier', channels: (c) => c.input('in').into(Echo).sink() });\n`,
  );
  const registry = await loadRegistry([module]);
  assert.ok(registry.get('from_specifier'), "the module's operative was registered");
  assert.deepEqual(
    specsFrom(module, await importModule(module)).map((s) => s.factoryId),
    ['from_specifier'],
  );
});

// Each build is its own compile of `importModule`, so each loads a handler by a
// relative path, the spelling an author passes to `defineStream`.
for (const [build, load, file, bind, exporting] of [
  [
    'esm',
    loadRegistry,
    'handlers.mjs',
    (sdk) => `import { defineOperative, defineProcedure } from '${sdk}';`,
    'export default',
  ],
  [
    'cjs',
    createRequire(import.meta.url)('../dist/cjs/runner.js').loadRegistry,
    'handlers.cjs',
    (sdk) => `const { defineOperative, defineProcedure } = require('${sdk}');`,
    'module.exports =',
  ],
]) {
  test(`the ${build} build imports a handler module by relative path`, async () => {
    const module = join(mkdtempSync(join(tmpdir(), 'flezha-specifier-')), file);
    writeFileSync(
      module,
      `${bind(new URL(`../dist/${build}/index.js`, import.meta.url).pathname)}\n` +
        `const Echo = defineProcedure({ name: 'echo', handle: (i) => i });\n` +
        `${exporting} defineOperative({ factoryId: 'by_relative_path', channels: (c) => c.input('in').into(Echo).sink() });\n`,
    );
    const registry = await load([`./${relative(process.cwd(), module)}`]);
    assert.ok(registry.get('by_relative_path'), "the module's operative was registered");
  });
}

test('an unimportable specifier is named, with the original error chained', async () => {
  await assert.rejects(
    () => importModule('./definitely-not-here.mjs'),
    (error) =>
      error.message.includes('./definitely-not-here.mjs') &&
      error.message.includes('module specifier') &&
      error.cause instanceof Error,
  );
});

// A graph is built at module scope, so a builder refusal is thrown while the
// module is imported, and it reaches the author as itself.
test('a refusal raised while a handler module is imported keeps its own name', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'flezha-module-scope-'));
  const module = join(dir, 'handlers.mjs');
  writeFileSync(
    module,
    `import { defineOperative, defineProcedure } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `const A = defineProcedure({ name: 'twin', handle: (i) => i });\n` +
      `const B = defineProcedure({ name: 'twin', handle: (i) => i });\n` +
      `export default defineOperative({ factoryId: 'module_scope', channels: (c) => {\n` +
      `  const s = c.input('in'); s.into(A); s.into(B);\n` +
      `} });\n`,
  );
  await assert.rejects(
    () => loadRegistry([module]),
    (error) =>
      error instanceof DeclarationError && error.refusals.includes('NodeIdentityCollision'),
  );
});

test('a module that throws anything but a refusal is named, with the original error chained', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'flezha-broken-'));
  const module = join(dir, 'broken.mjs');
  writeFileSync(module, 'export default { x: , };\n');
  await assert.rejects(
    () => loadRegistry([module]),
    (error) =>
      error.message.includes(module) &&
      error.message.includes('module specifier') &&
      error.cause instanceof SyntaxError,
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

// --- output identity stamps -------------------------------------------------
//
// One invocation, several emits: the per-emit ordinal is the half a single-emit
// assertion cannot see, and the e2e golden checks stamp NAMES only.

const StampEmitter = defineProcedure({
  name: 'stamp_emitter',
  handle: (input, ctx) => {
    if (input === 'twice') {
      ctx.emitTo('keyed_out', 'a');
      ctx.emitTo('keyed_out', 'b');
    } else {
      ctx.emitTo('plain_out', 'p');
      ctx.emitTo('keyed_out', 'k');
    }
  },
});

// One keyed plugin channel: the topology every SDK can express, and where the
// per-emit ordinal has to be right.
const StampSpec = defineOperative({
  factoryId: 'stamps',
  channels: (c) => {
    c.input('in')
      .keyBy(() => 'ek')
      .into(StampEmitter)
      .keyed()
      .sink('keyed_out');
  },
});

// A procedure owns ONE output channel, so a keyed and a non-keyed plugin
// channel are two procedures. The unkeyed one carries no stamps at all.
const PlainEmitter = defineProcedure({
  name: 'plain_emitter',
  handle: (_input, ctx) => {
    ctx.emitTo('plain_out', 'p');
  },
});

const MixedSpec = defineOperative({
  factoryId: 'stamps_mixed',
  channels: (c) => {
    c.input('in')
      .keyBy(() => 'ek')
      .branch([
        (edge) => edge.into(PlainEmitter).sink('plain_out'),
        (edge) => edge.into(StampEmitter).keyed().sink('keyed_out'),
      ]);
  },
});

const stampActivation = activationFor('stamps');
const mixedActivation = activationFor('stamps_mixed');
const metaOf = (output) => Object.fromEntries(output.metadata.map((m) => [m.key, m.value]));

test('a keyed plugin emit carries the identity stamps, and the ordinal advances per emit', async () => {
  const result = await answer(
    stampActivation({ entityKey: 'ek', changeSeq: 5n, items: [inputItem('twice')] }),
    depsFor(StampSpec),
  );
  assert.equal(result.outcome, 'work');
  const outputs = result.effects[0].outputs;
  assert.equal(outputs.length, 2);
  assert.deepEqual(metaOf(outputs[0]), {
    'flz.entity': 'op-1/stamp_emitter/ek',
    'flz.seq': '5.0',
    'flz.key': 'ek',
  });
  assert.deepEqual(metaOf(outputs[1]), {
    'flz.entity': 'op-1/stamp_emitter/ek',
    'flz.seq': '5.1',
    'flz.key': 'ek',
  });
});

test('a non-keyed plugin emit carries no stamps', async () => {
  const result = await answer(
    mixedActivation({ entityKey: 'ek', changeSeq: 5n, items: [inputItem('mixed')] }),
    depsFor(MixedSpec),
  );
  const [plain] = result.effects[0].outputs;
  assert.equal(plain.pluginChannel, 'plain_out');
  assert.deepEqual(plain.metadata, []);
});

test('a fresh work item restarts the emit ordinal', async () => {
  const result = await answer(
    stampActivation({
      entityKey: 'ek',
      changeSeq: 5n,
      items: [inputItem('twice'), inputItem('twice')],
    }),
    depsFor(StampSpec),
  );
  assert.deepEqual(
    result.effects.map((effect) => effect.outputs.map((o) => metaOf(o)['flz.seq'])),
    [
      ['5.0', '5.1'],
      ['5.0', '5.1'],
    ],
  );
});

test('user metadata survives beside the stamps and cannot spoof one', async () => {
  const UserMeta = defineProcedure({
    name: 'stamp_user_meta',
    handle: (input, ctx) => {
      if (input === 'spoof') {
        ctx.emit('v', { metadata: { 'flz.seq': '999.0' } });
        return;
      }
      ctx.emit('v', { metadata: { user: 'meta' } });
    },
  });
  const spec = defineOperative({
    factoryId: 'stamps_user',
    channels: (c) => {
      c.input('in')
        .keyBy(() => 'ek')
        .into(UserMeta)
        .keyed()
        .sink();
    },
  });
  const activation = activationFor('stamps_user');

  const ok = await answer(
    activation({ entityKey: 'ek', changeSeq: 2n, items: [inputItem('plain')] }),
    depsFor(spec),
  );
  assert.deepEqual(metaOf(ok.effects[0].outputs[0]), {
    user: 'meta',
    'flz.entity': 'op-1/stamp_user_meta/ek',
    'flz.seq': '2.0',
    'flz.key': 'ek',
  });

  const spoofed = await answer(
    activation({ entityKey: 'ek', changeSeq: 2n, items: [inputItem('spoof')] }),
    depsFor(spec),
  );
  assert.equal(spoofed.effects[0].verdict, 'retryable');
});

// --- the structured-workflow emit arm ---------------------------------------
//
// One released value is ONE edge output on a workflow class, untransformed and
// unkeyed, naming the producing channel and its first consumer: the driver
// fans it out from the journaled handoff. On a stream class the same topology
// releases one output per consumer, transformed and keyed. The plugin output
// rides alongside either way.

const WorkflowProducer = defineProcedure({
  name: 'wf_producer',
  handle: (input, ctx) => {
    ctx.emit(input);
  },
});

/** What a consumer of the `handoff` channel was handed, in delivery order. */
const DELIVERED = [];

const record = (name) =>
  defineProcedure({
    name,
    handle: (input) => {
      DELIVERED.push(input);
    },
  });

const workflowGraph = (c) => {
  const edge = c.input('in').into(WorkflowProducer).named('handoff');
  edge.map((value) => value + 1).into(record('wf_b'));
  edge.into(record('wf_c'));
  edge.sink();
};

/**
 * An edge payload is the class codec's MessagePack; the `handoff` sink names no
 * codec, so its frame for a number is JSON.
 */
const payloadOf = (output) =>
  output.target === 'plugin' ? jsonOf(output.payload) : decodeValue(output.payload);

const WorkflowSpec = defineWorkflow({ factoryId: 'wf_emit', channels: workflowGraph });
const StreamTwinSpec = defineOperative({ factoryId: 'wf_emit_stream', channels: workflowGraph });

test('a structured workflow releases one untransformed edge output per emit', async () => {
  const result = await answer(
    activationFor('wf_emit')({ items: [inputItem(10)] }),
    depsFor(WorkflowSpec),
  );
  assert.equal(result.outcome, 'work');
  const outputs = result.effects[0].outputs;
  assert.deepEqual(
    outputs.map((o) => [o.target, o.consumer, o.pluginChannel, payloadOf(o)]),
    [
      ['edge', 1, undefined, 10],
      ['plugin', undefined, 'handoff', 10],
    ],
  );
  assert.equal(outputs[0].key, undefined);
  assert.equal(outputs[0].eventTime, undefined);
});

test("an edge payload is the encoder's own bytes, with nothing layered on top", async () => {
  const result = await answer(
    activationFor('wf_emit_stream')({ items: [inputItem(10)] }),
    depsFor(StreamTwinSpec),
  );
  const outputs = result.effects[0].outputs;
  for (const output of outputs.filter((o) => o.target === 'edge')) {
    assert.ok(output.payload instanceof Uint8Array);
    assert.equal(
      Buffer.isBuffer(output.payload),
      false,
      "msgpack's own bytes were copied into a Buffer on the way to the core",
    );
  }
  assert.deepEqual(outputs[1].payload, encodeValue(10));
});

test('the same graph on a stream class releases one output per consumer', async () => {
  const result = await answer(
    activationFor('wf_emit_stream')({ items: [inputItem(10)] }),
    depsFor(StreamTwinSpec),
  );
  const outputs = result.effects[0].outputs;
  assert.deepEqual(
    outputs.map((o) => [o.target, o.consumer, payloadOf(o)]),
    [
      ['edge', 1, 11],
      ['edge', 2, 10],
      ['plugin', undefined, 10],
    ],
  );
});

// The other end of the same two arms: what the CONSUMER is handed. A stream
// producer already applied the edge, so delivery applies nothing; a workflow
// stage released the value raw, so delivery is the one application.

/** Hand `payload` to `procedure` on the `handoff` channel and answer what it saw. */
async function delivered(spec, procedure, payload) {
  DELIVERED.length = 0;
  const result = await answer(
    activationFor(spec.factoryId)({
      procedure,
      items: [inputItem(0, { channel: 1, payload })],
    }),
    depsFor(spec),
  );
  assert.equal(result.effects[0].verdict, 'ok');
  return [...DELIVERED];
}

test('a stream inter-proc edge applies its map exactly once', async () => {
  const produced = await answer(
    activationFor('wf_emit_stream')({ items: [inputItem(10)] }),
    depsFor(StreamTwinSpec),
  );
  const mapped = produced.effects[0].outputs[0];
  assert.deepEqual(await delivered(StreamTwinSpec, 1, mapped.payload), [11]);
});

test('a workflow consumer applies its own edge exactly once, at delivery', async () => {
  const stage = await answer(
    activationFor('wf_emit')({ items: [inputItem(10)] }),
    depsFor(WorkflowSpec),
  );
  const released = stage.effects[0].outputs[0].payload;
  // One released value reaches BOTH children — the driver's fan-out — and each
  // evaluates its own edge here.
  assert.deepEqual(await delivered(WorkflowSpec, 1, released), [11]);
  assert.deepEqual(await delivered(WorkflowSpec, 2, released), [10]);
});

// An entry naming no codec reads JSON, so plain text fails its decode: the
// item rides the retry ladder and the handler is never handed it.
test('plain text into an entry naming no codec never reaches the handler', async () => {
  const spec = defineOperative({
    factoryId: 'json_entry',
    channels: (c) => {
      c.input('in').into(record('json_entry_procedure'));
    },
  });
  DELIVERED.length = 0;
  const result = await answer(
    activationFor('json_entry')({
      entityKey: undefined,
      items: [inputItem({ a: 1 }), inputItem(0, { payload: Buffer.from('hello') })],
    }),
    depsFor(spec),
  );
  assert.deepEqual(
    result.effects.map((effect) => effect.verdict),
    ['ok', 'retryable'],
  );
  assert.match(result.effects[1].message, /not valid JSON/);
  assert.deepEqual(DELIVERED, [{ a: 1 }]);
});
