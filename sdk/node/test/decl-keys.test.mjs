// The declaration key set is a cross-SDK contract pinned in one file. The core
// proves the file matches what it accepts and the Java builder proves it emits
// every key; this is the third reader. A key the core learns that Node cannot
// spell fails here.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  defineOperative,
  defineProcedure,
  defineWorkflow,
  toHostClassDecl,
} from '../dist/esm/index.js';

const FIXTURE = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../../test/fixtures/host_class_decl_keys.txt',
);

function pinned(section) {
  return readFileSync(FIXTURE, 'utf8')
    .split('\n')
    .filter((line) => line && !line.startsWith('#'))
    .filter((line) => line.startsWith(`${section}.`))
    .map((line) => line.slice(section.length + 1));
}

/** Every stream-side option the Node surface has a spelling for. */
function fullyDeclaredStream() {
  const dynamic = defineProcedure({
    name: 'full_dynamic',
    dynamicEntities: { idleTtlMs: 30_000, maxEntities: 64 },
    clock: 'event_time',
    eventTime: (input) => BigInt(input.at),
    mailboxCapacity: 512,
    schemaVersion: 2,
    minReadableSchemaVersion: 1,
    migrateState: (_from, bytes) => ({ seen: bytes.length }),
    migrateChange: () => ({ by: 1 }),
    initialState: () => ({ seen: 0 }),
    applyChange: (state, change) => {
      state.seen += change.by;
    },
    compensate: () => {},
    handle: (input, ctx) => {
      ctx.change({ by: 1 });
      return input;
    },
  });
  return defineOperative({
    factoryId: 'node_decl_full',
    persistent: true,
    channels: (c) => {
      c.effectivelyOnce();
      c.checkpointEvery(500);
      c.outputsAtCheckpoint(50);
      c.correctionWindow(600_000);
      c.beyondWindowPolicy('drop');
      c.compactAfterBytes(1_048_576);
      c.input('in')
        .retryMax(3)
        .retryBackoffMs(300)
        .keyBy((input) => input.symbol)
        .into(dynamic)
        .keyed()
        .sink('out');
    },
  });
}

/** Every static-mode procedure key, which the dynamic one cannot carry. */
function fullyDeclaredStatic() {
  const statik = defineProcedure({
    name: 'full_static',
    entities: { keys: ['a', 'b'], entitiesFor: () => ['a'] },
    handle: (input) => input,
  });
  return defineOperative({
    factoryId: 'node_decl_static',
    channels: (c) => c.input('in').into(statik).sink('out'),
  });
}

function fullyDeclaredWorkflow() {
  return defineWorkflow({
    factoryId: 'node_decl_workflow',
    loopMode: 'per_input',
    rollbackMode: 'keep_progress',
    retryAttempts: 5,
    retryBackoffMs: 250,
    barrierRetryAttempts: 4,
    maxDurationMs: 120_000,
    wakeAfterMs: 30_000,
    syncDurability: true,
    run: async () => undefined,
  });
}

test('every class key the fixture pins has a Node spelling', () => {
  // The kind splits the class keys the way entity mode splits the procedure
  // ones: only a workflow class carries `workflow`.
  const emitted = new Set(
    [fullyDeclaredStream(), fullyDeclaredWorkflow()]
      .map(toHostClassDecl)
      .flatMap((decl) => Object.keys(decl)),
  );
  const missing = pinned('class').filter((key) => !emitted.has(key));
  assert.deepEqual(
    missing,
    [],
    `class keys the fixture pins that the Node builder never emits: ${missing.join(', ')}`,
  );
});

test('every channel key the fixture pins has a Node spelling', () => {
  const decl = toHostClassDecl(fullyDeclaredStream());
  const emitted = new Set(decl.channels.flatMap((channel) => Object.keys(channel)));
  const missing = pinned('channel').filter((key) => !emitted.has(key));
  assert.deepEqual(missing, [], `channel keys with no Node spelling: ${missing.join(', ')}`);
});

test('every procedure key the fixture pins has a Node spelling', () => {
  // Entity mode splits the procedure keys: a dynamic procedure carries
  // `idle_ttl_ms` and `max_entities`, a static one `entity_keys`, and neither
  // carries the other's.
  const emitted = new Set(
    [fullyDeclaredStream(), fullyDeclaredStatic()]
      .map(toHostClassDecl)
      .flatMap((decl) => decl.procedures)
      .flatMap((procedure) => Object.keys(procedure)),
  );
  const missing = pinned('procedure').filter((key) => !emitted.has(key));
  assert.deepEqual(missing, [], `procedure keys with no Node spelling: ${missing.join(', ')}`);
});

test('every workflow key the fixture pins has a Node spelling', () => {
  const decl = toHostClassDecl(fullyDeclaredWorkflow());
  const missing = pinned('workflow').filter((key) => !(key in decl.workflow));
  assert.deepEqual(missing, [], `workflow keys with no Node spelling: ${missing.join(', ')}`);
});

test('the builder invents no key the core never learned', () => {
  // `deny_unknown_fields` on the core's side makes an invented key a parse
  // error naming it; catching it here names it at the source instead.
  const stream = toHostClassDecl(fullyDeclaredStream());
  const workflow = toHostClassDecl(fullyDeclaredWorkflow());
  const check = (section, keys) => {
    const allowed = new Set(pinned(section));
    const extra = [...keys].filter((key) => !allowed.has(key));
    assert.deepEqual(extra, [], `${section} keys the core never learned: ${extra.join(', ')}`);
  };
  check('class', new Set([...Object.keys(stream), ...Object.keys(workflow)]));
  check('channel', new Set(stream.channels.flatMap((c) => Object.keys(c))));
  check(
    'procedure',
    new Set(
      [stream, toHostClassDecl(fullyDeclaredStatic())]
        .flatMap((decl) => decl.procedures)
        .flatMap((p) => Object.keys(p)),
    ),
  );
  check('workflow', new Set(Object.keys(workflow.workflow)));
});
