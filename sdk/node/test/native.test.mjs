// The native surface, and the two properties asserted rather than reviewed:
// the veneer adds no ABI surface, and no blocking entry point is
// synchronously reachable from an event loop.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  ACTIVATION_SCHEMA_VERSION,
  defineOperative,
  defineProcedure,
  loadNative,
  toHostClassDecl,
} from '../dist/esm/index.js';

const here = dirname(fileURLToPath(import.meta.url));
const native = loadNative();

test('the addon exports exactly what its surface table names', () => {
  const surface = native.nativeSurface();
  const exported = Object.keys(native).sort();
  const named = surface.map((entry) => entry.name).sort();
  assert.deepEqual(
    exported.filter((name) => !named.includes(name)),
    [],
    'an export with no entry in the surface table',
  );
  assert.deepEqual(
    named.filter((name) => !exported.includes(name)),
    [],
    'a surface-table entry that is not exported',
  );
});

test('every export names a C ABI entry point or is host bookkeeping', () => {
  // The veneer adds no ABI surface: each entry either corresponds to an
  // existing C entry point, or reaches none at all.
  // Two files, because the C ABI is two: the general entry points and the
  // local-mode ones. Reading only the first passes a name the crate exports
  // and fails one it exports from the other module.
  const abi = ['lib.rs', 'local.rs']
    .map((file) => readFileSync(resolve(here, `../../../flz_core_ffi/src/${file}`), 'utf8'))
    .join('\n');
  for (const entry of native.nativeSurface()) {
    if (!entry.abi) continue;
    assert.ok(
      abi.includes(`fn ${entry.abi}(`),
      `${entry.name} names ${entry.abi}, which flz_core_ffi does not export`,
    );
  }
});

test('poll and the starvation wait are not reachable from JavaScript at all', () => {
  // The rule this closes: under the default overflow policy a completion parks
  // until the consumer's mailbox drains, and the consumer drains only when its
  // handler runs. An event loop parked inside the core is the one thread that
  // could run it.
  for (const forbidden of ['poll', 'waitStarvation', 'flzCorePoll']) {
    assert.equal(native[forbidden], undefined, `${forbidden} must not be exported`);
  }
});

test('every blocking entry point is asynchronous', () => {
  const blocking = native.nativeSurface().filter((entry) => entry.blocking);
  // Held against the package's own typing of the addon rather than a list kept
  // here: the table's blocking entries are exactly the `Native` members typed
  // as a promise.
  const typescript = readFileSync(resolve(here, '../src/native.ts'), 'utf8');
  const at = typescript.indexOf('export interface Native {');
  assert.notEqual(at, -1, 'src/native.ts declares no interface Native');
  const body = typescript.slice(at, typescript.indexOf('\n}', at));
  const promised = [...body.matchAll(/^ {2}(\w+)\([^;]*?\): Promise</gm)].map(([, name]) => name);
  assert.deepEqual(blocking.map((entry) => entry.name).sort(), promised.sort());
  for (const entry of blocking) {
    assert.equal(typeof native[entry.name], 'function');
  }
  // `stopHost` is the one that can be called with no context and observed
  // returning a promise; the others need a live context and a running core,
  // and are exercised end to end by the conformance suite.
  const returned = native.stopHost();
  assert.ok(returned instanceof Promise, 'stopHost returns a promise');
  return returned;
});

test('a skewed binary refuses to start, naming both versions', () => {
  assert.throws(
    () => native.contextNew(ACTIVATION_SCHEMA_VERSION + 1),
    (error) =>
      /schema version mismatch/.test(error.message) &&
      error.message.includes(String(ACTIVATION_SCHEMA_VERSION + 1)) &&
      error.message.includes(String(native.schemaVersion())),
  );
});

test('the package and the binary agree on the activation schema', () => {
  assert.equal(
    native.schemaVersion(),
    ACTIVATION_SCHEMA_VERSION,
    'the package was built against a different activation schema than the binary',
  );
});

test('a core refusal carries the engine detail', () => {
  native.contextNew(ACTIVATION_SCHEMA_VERSION);
  try {
    assert.throws(
      () => native.declareClass(Buffer.from('{"factory_id":"x"}')),
      (error) => error.message.includes('sdk_lang'),
    );
    assert.match(native.lastError(), /sdk_lang/);
  } finally {
    native.contextFree();
  }
});

// The checkpoint options the core will accept together. Asserted here rather
// than on the builder's fields because the core is what refuses AND what
// implies: the builder writes each raw flag, and the acknowledgement
// granularity a declaration registers under is derived from any of them.
const checkpointClass = (factoryId, declare) =>
  defineOperative({
    factoryId,
    channels: (c) => {
      c.effectivelyOnce();
      declare(c);
      c.input('input')
        .into(defineProcedure({ name: `${factoryId}_procedure`, handle: (input) => input }))
        .named('sink')
        .keyed()
        .sink('tier_sink');
    },
  });

const declaresTo = (native, spec) =>
  native.declareClass(Buffer.from(JSON.stringify(toHostClassDecl(spec))));

test('holding outputs at the checkpoint acknowledges at the checkpoint', () => {
  native.contextNew(ACTIVATION_SCHEMA_VERSION);
  try {
    const spec = checkpointClass('outputs_only', (c) => c.outputsAtCheckpoint(1500));
    const decl = toHostClassDecl(spec);
    assert.equal(decl.checkpoint_ack, false, 'the flag is raw: nothing amends it');
    assert.equal(decl.checkpoint_outputs, 'at_checkpoint');
    assert.equal(decl.checkpoint_max_delay_ms, 1500);
    // Registering is the assertion: the core implies the checkpoint
    // acknowledgement from the outputs flag, and refuses the pair otherwise.
    declaresTo(native, spec);
  } finally {
    native.contextFree();
  }
});

test('the bare checkpoint opt-in registers with no cadence', () => {
  native.contextNew(ACTIVATION_SCHEMA_VERSION);
  try {
    const spec = checkpointClass('ack_only', (c) => c.checkpointAck());
    const decl = toHostClassDecl(spec);
    assert.equal(decl.checkpoint_ack, true);
    assert.equal(decl.checkpoint_every, undefined, 'the bare opt-in declares no cadence');
    declaresTo(native, spec);
  } finally {
    native.contextFree();
  }
});

test('the monotonic clock advances', () => {
  const first = native.monotonicNanos();
  assert.equal(typeof first, 'bigint');
  assert.ok(native.monotonicNanos() >= first);
});

test('both module systems import the package and expose the same surface', () => {
  const script = `
    const cjs = require('${resolve(here, '../dist/cjs/index.js')}');
    import('${resolve(here, '../dist/esm/index.js')}').then((esm) => {
      const cjsKeys = Object.keys(cjs).sort();
      const esmKeys = Object.keys(esm).filter((k) => k !== 'default').sort();
      if (JSON.stringify(cjsKeys) !== JSON.stringify(esmKeys)) {
        console.error('surfaces differ', cjsKeys, esmKeys);
        process.exit(1);
      }
      if (cjs.ACTIVATION_SCHEMA_VERSION !== esm.ACTIVATION_SCHEMA_VERSION) process.exit(1);
      if (typeof cjs.createRunner !== 'function' || typeof esm.createRunner !== 'function') {
        process.exit(1);
      }
      console.log(cjsKeys.length);
    });
  `;
  const out = execFileSync(process.execPath, ['-e', script], { encoding: 'utf8' });
  assert.ok(Number(out.trim()) > 15, `expected a populated surface, got ${out}`);
});
