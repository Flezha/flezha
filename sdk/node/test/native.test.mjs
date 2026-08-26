// The native surface, and the two properties asserted rather than reviewed:
// the veneer adds no ABI surface, and no blocking entry point is
// synchronously reachable from an event loop.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { ACTIVATION_SCHEMA_VERSION, loadNative, nativePackageName } from '../dist/esm/index.js';

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
  const abi = readFileSync(resolve(here, '../../../flz_core_ffi/src/lib.rs'), 'utf8');
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
  assert.deepEqual(
    blocking.map((entry) => entry.name).sort(),
    ['complete', 'spawnWorkflow', 'start', 'stopHost', 'workflowRecv'],
  );
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

test('the monotonic clock advances and an unknown entry channel reads zero', () => {
  const first = native.monotonicNanos();
  assert.equal(typeof first, 'bigint');
  assert.ok(native.monotonicNanos() >= first);
  assert.equal(native.entryLastDeliveryNanos('never_declared'), 0n);
});

test('this platform has a per-platform binary package name', () => {
  assert.equal(nativePackageName('linux', 'x64'), '@flezha/sdk-linux-x64-gnu');
  assert.equal(nativePackageName('darwin', 'arm64'), '@flezha/sdk-darwin-arm64');
  assert.equal(nativePackageName('win32', 'x64'), '@flezha/sdk-win32-x64-msvc');
  assert.equal(nativePackageName('sunos', 'sparc'), null);
});

test('a supported platform resolves one binary package, not five', async () => {
  const { nativeCandidates } = await import('../dist/esm/native.js');
  const candidates = nativeCandidates('linux', 'x64', undefined);
  assert.equal(candidates[0], '@flezha/sdk-linux-x64-gnu');
  assert.equal(
    candidates.filter((candidate) => candidate.startsWith('@flezha/sdk-')).length,
    1,
    'exactly one per-platform package is consulted',
  );
  assert.match(candidates.at(-1), /native\/flz_core_ffi\.node$/);
});

test('an unsupported platform is told what a source build needs', async () => {
  const { nativeCandidates, nativeResolutionError } = await import('../dist/esm/native.js');
  const candidates = nativeCandidates('sunos', 'sparc', undefined);
  assert.deepEqual(
    candidates.filter((candidate) => candidate.startsWith('@flezha/sdk-')),
    [],
    'no per-platform package is published for this platform',
  );
  const error = nativeResolutionError(candidates, 'sunos', 'sparc');
  assert.match(error.message, /No prebuilt binary is published/);
  assert.match(error.message, /cargo build -p flz_core_ffi --features napi/);
  assert.match(error.message, /FLEZHA_NATIVE_BINARY/);
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
