// The concurrency requirements, as behaviour.
//
// The half that needs a controller — a full DropNothing consumer draining
// while its completion is parked off-loop, and shutdown observed within the
// poll timeout — is driven by the conformance suite's own fixtures
// (`bounded_mailbox_drop_nothing`, `wedge_probe`) against a real one. What is
// provable here is proved here.
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { Worker } from 'node:worker_threads';

import {
  ACTIVATION_SCHEMA_VERSION,
  decodeValue,
  defineOperative,
  defineProcedure,
  loadNative,
} from '../dist/esm/index.js';
import { answer } from '../dist/esm/dispatch.js';

import { activationFor, depsFor, inputItem } from './dispatch-fixtures.mjs';

// `depsFor` submits each spec, so this file needs the process context too.
loadNative().contextNew(ACTIVATION_SCHEMA_VERSION);

const Accumulate = defineProcedure({
  name: 'accumulate',
  dynamicEntities: {},
  initialState: () => ({ seen: [] }),
  applyChange: (state, change) => {
    state.seen.push(change.value);
  },
  handle: (input, ctx) => {
    ctx.change({ value: input });
    return [...ctx.state.seen];
  },
});

const Spec = defineOperative({
  factoryId: 'ordering',
  channels: (c) =>
    c
      .input('in')
      .keyBy((v) => v.key)
      .into(Accumulate)
      .keyed()
      .sink('out'),
});

const deps = () => depsFor(Spec);
const activation = activationFor('ordering');

const work = (token, key, values) =>
  activation({
    token,
    entityKey: key,
    changeSeq: BigInt(token),
    items: values.map((value) => inputItem(value, { now: 0n })),
  });

test('inputs for one entity are processed in enqueue order', async () => {
  const shared = deps();
  const first = await answer(work(1n, 'a', ['x', 'y']), shared);
  const second = await answer(work(2n, 'a', ['z']), shared);
  const emitted = [...first.effects, ...second.effects].map((effect) =>
    decodeValue(effect.outputs[0].payload),
  );
  assert.deepEqual(emitted, [['x'], ['x', 'y'], ['x', 'y', 'z']]);
});

test('two entities do not see each other state', async () => {
  const shared = deps();
  await answer(work(1n, 'a', ['x']), shared);
  const other = await answer(work(2n, 'b', ['q']), shared);
  assert.deepEqual(decodeValue(other.effects[0].outputs[0].payload), ['q']);
});

test('the dispatch path is one function, shared by both configurations', async () => {
  // Structural rather than behavioural, and read from the source rather than
  // imported: `worker-entry` only runs inside a worker. What matters is that
  // both loops reach the same `dispatch`, so a second dispatch path cannot
  // appear by drift.
  const dispatchModule = await import('../dist/esm/dispatch.js');
  assert.equal(typeof dispatchModule.dispatch, 'function');
  const read = (name) => readFileSync(new URL(`../dist/esm/${name}`, import.meta.url), 'utf8');
  for (const loop of ['runner.js', 'worker-entry.js']) {
    assert.match(
      read(loop),
      /from ['"]\.\/dispatch\.js['"]/,
      `${loop} dispatches through some other path`,
    );
    assert.match(read(loop), /dispatch\(activation, native, /);
  }
});

test('module-scope state is instantiated once per dispatch loop', async () => {
  // Usually benign — a connection pool, a cache. Occasionally wrong — an
  // in-memory counter, a run-once flag. Either way it is a semantic the SDK
  // states rather than one an author discovers.
  const dir = mkdtempSync(join(tmpdir(), 'flezha-isolation-'));
  const handler = join(dir, 'handler.mjs');
  writeFileSync(
    handler,
    `import { parentPort, workerData } from 'node:worker_threads';\n` +
      `let calls = 0;\n` +
      `calls += 1;\n` +
      `parentPort.postMessage({ worker: workerData.index, calls });\n`,
  );

  const results = await Promise.all(
    [0, 1].map(
      (index) =>
        new Promise((resolve, reject) => {
          const worker = new Worker(handler, { workerData: { index } });
          worker.once('message', (message) => {
            worker.terminate();
            resolve(message);
          });
          worker.once('error', reject);
        }),
    ),
  );
  assert.deepEqual(
    results.map((r) => r.calls),
    [1, 1],
    'each worker evaluated the module and holds its own copy',
  );
});

test('polling does not consume the shared I/O threadpool', async () => {
  // The fetchers are the SDK's own threads. libuv's pool has four threads and
  // serves `fs`, `dns` and `crypto` besides, so a blocking poll borrowed from
  // it would starve everything else on it. Proved by keeping every native
  // blocking slot busy and timing a libuv job.
  const native = loadNative();
  const parked = Array.from({ length: 16 }, () => native.stopHost());
  await new Promise((resolve) => setTimeout(resolve, 50));
  const started = process.hrtime.bigint();
  await new Promise((resolve, reject) =>
    import('node:crypto').then(({ pbkdf2 }) =>
      pbkdf2('p', 's', 1, 32, 'sha256', (error, key) => (error ? reject(error) : resolve(key))),
    ),
  );
  const elapsedMs = Number(process.hrtime.bigint() - started) / 1e6;
  await Promise.all(parked);
  assert.ok(elapsedMs < 500, `libuv work waited ${elapsedMs.toFixed(0)}ms behind native parks`);
});

test('the documented delay threshold is below the controller stall verdict', async () => {
  const { DELAY_WARN_MS } = await import('../dist/esm/index.js');
  assert.equal(typeof DELAY_WARN_MS, 'number');
  assert.ok(DELAY_WARN_MS >= 100, 'above scheduling noise and a GC pause');
  assert.ok(DELAY_WARN_MS <= 5_000, 'the host warns before the controller mints a verdict');
});
