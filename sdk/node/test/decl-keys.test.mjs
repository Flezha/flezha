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

// --- the keys a workflow class may not put on the wire ----------------------
//
// `flz_core::host::register::refuse_workflow_facts` refuses `effectively_once`
// and `correction_window_ms` on a workflow class outright, and
// `refuse_tier_facts` refuses the checkpoint family without the checkpoint
// acknowledgement the first refusal leaves unreachable there. One builder
// serves both kinds, so the gate lives in it.

const REFUSED_ON_WORKFLOW = [
  'effectively_once',
  'ack_granularity',
  'checkpoint_every',
  'checkpoint_outputs',
  'checkpoint_max_delay_ms',
  'correction_window_ms',
  'beyond_window_policy',
];

/** The stream fixture's spec re-kinded: every refused key populated at once. */
function tierSpec(kind) {
  return {
    ...fullyDeclaredStream(),
    factoryId: `node_decl_${kind}_tier`,
    kind,
    workflow: kind === 'workflow' ? fullyDeclaredWorkflow().workflow : undefined,
  };
}

test('a workflow class declares none of the keys the core refuses on one', () => {
  const decl = toHostClassDecl(tierSpec('workflow'));
  const emitted = REFUSED_ON_WORKFLOW.filter((key) => key in decl);
  assert.deepEqual(emitted, [], `keys the core refuses on a workflow class: ${emitted.join(', ')}`);
});

test('a stream class still declares every one of them', () => {
  const decl = toHostClassDecl(tierSpec('stream'));
  const missing = REFUSED_ON_WORKFLOW.filter((key) => !(key in decl));
  assert.deepEqual(missing, [], `stream keys the gate dropped: ${missing.join(', ')}`);
});

// --- the cross-language topology golden ------------------------------------
//
// `test/fixtures/reference_topologies.txt` is the Rust reference's channel
// topology for two fixtures, transcribed by hand. Four SDKs implement one
// design with no compiler between them, so the file is what keeps them from
// drifting: this builder declares both shapes in Node's spelling and the lines
// must be the golden's, in order.

const TOPOLOGIES = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../../test/fixtures/reference_topologies.txt',
);

function goldenLines(topology) {
  return readFileSync(TOPOLOGIES, 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'))
    .filter((line) => line.startsWith(`${topology} |`));
}

/** The declaration rendered in the golden's columns, one line per channel. */
function topologyLines(topology, spec) {
  const decl = toHostClassDecl(spec);
  return decl.channels.map((channel, index) => {
    const consumers = channel.consumers ?? [];
    return [
      topology,
      index,
      channel.name,
      channel.kind,
      channel.producer ?? '-',
      consumers.length > 0 ? consumers.join(',') : '-',
      spec.channels[index].sink ?? '-',
    ].join(' | ');
  });
}

const bytes = (value) => Buffer.from(String(value), 'utf8');

function hybridChannel() {
  const producer = defineProcedure({
    name: 'hybrid_producer_procedure',
    handle: (input, ctx) => ctx.emit(input),
  });
  const consumer = defineProcedure({ name: 'hybrid_consumer_procedure', handle: () => {} });
  return defineOperative({
    factoryId: 'hybrid_channel',
    channels: (c) => {
      c.input('input')
        .into(producer)
        .named('hybrid_indicator')
        .sink('hybrid_indicators', bytes)
        .into(consumer);
    },
  });
}

function commitBlockChain() {
  const commitA = defineProcedure({
    name: 'tier_commit_a',
    handle: (input, ctx) => ctx.emit(input, { key: 'job' }),
  });
  const blockB = defineProcedure({ name: 'block_b', handle: () => {} });
  return defineWorkflow({
    factoryId: 'commit_block_chain',
    channels: (c) => {
      c.effectivelyOnce();
      c.input('in').into(commitA).named('mid').keyed().sink('tier_sink', bytes).into(blockB);
    },
  });
}

test('the hybrid stream topology is the reference, channel for channel', () => {
  assert.deepEqual(topologyLines('hybrid_channel', hybridChannel()), goldenLines('hybrid_channel'));
});

test('the structured workflow topology is the reference, channel for channel', () => {
  assert.deepEqual(
    topologyLines('commit_block_chain', commitBlockChain()),
    goldenLines('commit_block_chain'),
  );
});
