// The graph builder, what it records, and the refusals it still makes itself.
//
// Every topology rule now lives in `flz_core::host::register::check`, and
// `test/fixtures/reference_topologies.txt` is where each one's case lives — one
// row per refusal, read by every SDK's conformance run. What is left here is
// the authoring surface: the shape a declaration builds, the names it derives,
// which closure reaches which edge, and the two defects the record cannot
// carry. `validate` is the call that submits, so it needs the process context.
import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  DeclarationError,
  defineOperative,
  defineProcedure,
  defineWorkflow,
  loadNative,
  toHostClassDecl,
  validate,
} from '../dist/esm/index.js';
import { ProcedureTestHarness } from '../dist/esm/testing.js';

loadNative().contextNew(ACTIVATION_SCHEMA_VERSION);

const echo = (name) => defineProcedure({ name, handle: (input) => input });

const refusal = (build) => {
  try {
    const spec = build();
    validate(spec);
  } catch (error) {
    assert.ok(error instanceof DeclarationError, `expected a DeclarationError, got ${error}`);
    return error;
  }
  assert.fail('expected a refusal');
};

/** The channels the core derived, as `[name, kind, producer, consumers]`. */
const derivedChannels = (spec) =>
  validate(spec).channels.map((channel) => [
    channel.name,
    channel.kind,
    channel.producer,
    channel.consumers.map((edge) => edge.procedure),
  ]);

test('a graph builds and its topology reaches the declaration', () => {
  const first = echo('first');
  const second = echo('second');
  const spec = defineOperative({
    factoryId: 'chain',
    channels: (c) => {
      c.input('in').into(first).into(second).sink();
    },
  });
  // The record carries the DECLARED kind; the plugin kind is the wiring rule,
  // which the core applies and hands back.
  assert.deepEqual(
    validate(spec).channels.map((channel) => [channel.name, channel.kind]),
    [
      ['in', 'entry'],
      ['first', 'inter_proc'],
      ['second', 'plugin'],
    ],
  );
  const decl = toHostClassDecl(spec);
  assert.deepEqual(
    decl.channels.map((channel) => channel.kind),
    ['entry', 'inter_proc', 'inter_proc'],
  );
  assert.deepEqual(
    decl.procedures.map((p) => p.name),
    ['first', 'second'],
  );
  assert.equal(decl.sdk_lang, 4);
  assert.equal(decl.change_format_id, 5);
});

test('keyBy declares a dynamic consumer and the key reaches the channel', () => {
  const counter = defineProcedure({
    name: 'counter',
    dynamicEntities: { idleTtlMs: 60_000, maxEntities: 100 },
    initialState: () => ({ seen: 0 }),
    applyChange: (state, change) => {
      state.seen += change.by;
    },
    handle: (input, ctx) => {
      ctx.change({ by: 1 });
      return input;
    },
  });
  const spec = defineOperative({
    factoryId: 'keyed',
    channels: (c) => {
      c.input('in')
        .keyBy((value) => value.symbol)
        .into(counter)
        .keyed()
        .sink();
    },
  });
  validate(spec);
  const decl = toHostClassDecl(spec);
  assert.equal(decl.procedures[0].entity_mode, 'dynamic');
  assert.equal(decl.procedures[0].idle_ttl_ms, 60_000);
  assert.equal(decl.channels[1].keyed, true);
});

// The record carries `change_is_nochange` per procedure: a handler that folds
// no change has the empty Change type, and a logical clock over it never
// advances. The refusal is the core's `UnservableClock`; what is asserted here
// is that this builder writes the fact it reads.
test('a procedure that folds no change declares it, and a logical clock over it is refused', () => {
  const spec = defineOperative({
    factoryId: 'unservable',
    channels: (c) =>
      c
        .input('in')
        .into(defineProcedure({ name: 'ticker', clock: 'logical', handle: (i) => i }))
        .sink(),
  });
  assert.equal(toHostClassDecl(spec).procedures[0].change_is_nochange, true);
  assert.deepEqual(refusal(() => spec).refusals, ['UnservableClock']);
});

// The verdict itself: one exception carrying every name the core answered with.
test('a refusal reaches the author under the core’s own names', () => {
  const target = echo('target');
  const refused = refusal(() =>
    defineOperative({
      factoryId: 'edge_already_wired',
      channels: (c) => {
        const edge = c.input('in');
        edge.into(target);
        edge.map((value) => `${value}!`).into(target);
      },
    }),
  );
  assert.deepEqual(refused.refusals, ['EdgeAlreadyWired']);
  assert.match(
    refused.message,
    /^edge_already_wired: topology rejected \[EdgeAlreadyWired\]:\n {2}- /,
  );
  assert.match(refused.message, /already feeds procedure `target`/);
});

// The one refusal this builder raises under a core name reads the way the core's do.
test('a node identity collision is refused under the core’s name, in the message too', () => {
  const refused = refusal(() =>
    defineOperative({
      factoryId: 'collision',
      channels: (c) => {
        const entry = c.input('in');
        entry.into(echo('twin'));
        entry.into(echo('twin'));
      },
    }),
  );
  assert.deepEqual(refused.refusals, ['NodeIdentityCollision']);
  assert.match(
    refused.message,
    /^collision: twin: topology rejected \[NodeIdentityCollision\]:\n {2}- two different procedures/,
  );
});

test('a named edge names the channel, not the consuming node', () => {
  const first = echo('first');
  const second = echo('second');
  const spec = defineOperative({
    factoryId: 'named_edge',
    channels: (c) => {
      c.input('in').into(first).named('hop').into(second).sink();
    },
  });
  validate(spec);
  const decl = toHostClassDecl(spec);
  assert.deepEqual(
    decl.channels.map((channel) => channel.name),
    ['in', 'hop', 'second'],
  );
  assert.deepEqual(
    decl.procedures.map((p) => p.name),
    ['first', 'second'],
  );
});

test('a second mention under its own node name is a second node', () => {
  const step = echo('step');
  const spec = defineOperative({
    factoryId: 'twice_over',
    channels: (c) => {
      c.input('in').into(step).into(step, 'step_again').sink();
    },
  });
  validate(spec);
  assert.deepEqual(
    toHostClassDecl(spec).procedures.map((p) => p.name),
    ['step', 'step_again'],
  );
});

test('two procedures claiming one node identity are refused', () => {
  const refused = refusal(() => {
    const first = defineProcedure({ name: 'same', handle: (i) => i });
    const second = defineProcedure({ name: 'same', handle: (i) => i });
    return defineOperative({
      factoryId: 'collide',
      channels: (c) => {
        c.input('in').branch([(edge) => edge.into(first), (edge) => edge.into(second)]);
      },
    });
  });
  assert.match(refused.message, /node identity/);
});

test('an event-time clock without an extractor is refused', () => {
  const refused = refusal(() => {
    const proc = defineProcedure({ name: 'windowed', clock: 'event_time', handle: (i) => i });
    return defineOperative({
      factoryId: 'no_extractor',
      channels: (c) => c.input('in').into(proc).sink(),
    });
  });
  assert.match(
    refused.message,
    /windowed: declares an event_time clock but no eventTime extractor/,
  );
});

// KI-122: three options, one implication. Each writes its own raw flag and the
// core derives the acknowledgement granularity from any of them, so no order
// of calls can clear another's.
test('the checkpoint options compose in any order, and none clears another', () => {
  const spec = defineOperative({
    factoryId: 'compose',
    channels: (c) => {
      c.effectivelyOnce();
      c.outputsAtCheckpoint(250);
      c.checkpointAck();
      c.checkpointEvery(4);
      c.input('in').into(echo('p')).keyed().sink();
    },
  });
  assert.equal(validate(spec).ack_granularity, 'checkpoint');
  const decl = toHostClassDecl(spec);
  assert.equal(decl.checkpoint_ack, true);
  assert.equal(decl.checkpoint_every, 4);
  assert.equal(decl.checkpoint_outputs, 'at_checkpoint');
  assert.equal(decl.checkpoint_max_delay_ms, 250);
});

test('two procedures each terminate on their own sink, named by their node', () => {
  const a = echo('a');
  const b = echo('b');
  const spec = defineOperative({
    factoryId: 'one_sink_twice',
    channels: (c) => {
      c.input('in').branch([(edge) => edge.into(a).sink(), (edge) => edge.into(b).sink()]);
    },
  });
  assert.deepEqual(
    validate(spec).channels.map((channel) => [channel.name, channel.kind, channel.sink]),
    [
      ['in', 'entry', null],
      ['a', 'plugin', 'a'],
      ['b', 'plugin', 'b'],
    ],
  );
});

test('the sink names the channel, and the no-argument form leaves it derived', () => {
  const named = defineOperative({
    factoryId: 'sink_named',
    channels: (c) => {
      c.input('in').into(echo('producer')).sink('items_output');
    },
  });
  const derived = defineOperative({
    factoryId: 'sink_derived',
    channels: (c) => {
      c.input('in').into(echo('producer')).sink();
    },
  });

  assert.deepEqual(
    validate(named).channels.map((channel) => [channel.name, channel.sink]),
    [
      ['in', null],
      ['items_output', 'items_output'],
    ],
  );
  assert.deepEqual(
    validate(derived).channels.map((channel) => [channel.name, channel.sink]),
    [
      ['in', null],
      ['producer', 'producer'],
    ],
  );
});

test('a channel named by both named() and sink() is refused naming both', () => {
  const spec = defineOperative({
    factoryId: 'named_twice',
    channels: (c) => {
      c.input('in').into(echo('producer')).named('a').sink('b');
    },
  });
  assert.throws(
    () => validate(spec),
    (error) => /ChannelNamedTwice/.test(error.message) && /both name it/.test(error.message),
  );
});

test('two channels naming one sink is refused as a duplicate sink name', () => {
  const spec = defineOperative({
    factoryId: 'one_sink_two_channels',
    channels: (c) => {
      c.input('in').branch([
        (edge) => edge.into(echo('a')).sink('shared'),
        (edge) => edge.into(echo('b')).sink('shared'),
      ]);
    },
  });
  assert.throws(
    () => validate(spec),
    (error) => /DuplicateSinkName/.test(error.message),
  );
});

test('a workflow declaring both a body and a graph is refused', () => {
  assert.throws(
    () =>
      defineWorkflow({
        factoryId: 'both',
        run: async () => undefined,
        channels: (c) => c.input('in').into(echo('p')),
      }),
    (error) => /one or the other/.test(error.message),
  );
});

test('a run-sugar workflow declares its lifecycle knobs', () => {
  const spec = defineWorkflow({
    factoryId: 'saga',
    loopMode: 'chained',
    rollbackMode: 'keep_progress',
    maxDurationMs: 60_000,
    run: async (_args, ctx) => ctx.complete('done'),
  });
  validate(spec);
  const decl = toHostClassDecl(spec);
  assert.equal(decl.workflow.loop_mode, 'chained');
  assert.equal(decl.workflow.rollback_mode, 'keep_progress');
  assert.equal(decl.workflow.max_duration_ms, 60_000);
  assert.deepEqual(decl.procedures, []);
});

test('state read but never folded is allowed; recording a change is not', async () => {
  // A value seeded at start and read is a real shape — the reference
  // `compare_filter` fixture is exactly it — so the declaration stands. What
  // is refused is recording a change with nothing to fold it through, at the
  // point of use rather than at declaration.
  const ReadOnly = defineProcedure({
    name: 'read_only',
    initialState: () => ({ comparable: 'default' }),
    handle: (input, ctx) => (ctx.state.comparable === input ? input : undefined),
  });
  const harness = ProcedureTestHarness.of(ReadOnly);
  assert.deepEqual((await harness.input('default')).emits, ['default']);
  assert.deepEqual((await harness.input('other')).emits, []);

  const Recording = defineProcedure({
    name: 'recording',
    initialState: () => ({ n: 0 }),
    handle: (_input, ctx) => ctx.change({ by: 1 }),
  });
  await assert.rejects(
    () => ProcedureTestHarness.of(Recording).input(1),
    (error) => /declares no applyChange/.test(error.message),
  );
});

test('applyChange with no state to fold into is refused at definition', () => {
  assert.throws(
    () => defineProcedure({ name: 'nowhere', applyChange: () => {}, handle: () => {} }),
    (error) => /no initialState/.test(error.message),
  );
});

test('a declared state that cannot be encoded is refused at definition', () => {
  assert.throws(
    () =>
      defineProcedure({
        name: 'unencodable',
        initialState: () => ({ pending: new Map() }),
        applyChange: () => {},
        handle: () => {},
      }),
    (error) => /unencodable\.state\.pending/.test(error.message),
  );
});

test('a sink names the channel a forwarded frame carries', () => {
  // The channel name is what a forwarded frame carries and what selects the
  // receiving operative's entry channel, so the name an author writes on the
  // sink has to be the one that rides the wire.
  const spec = defineOperative({
    factoryId: 'named_sink',
    channels: (c) => {
      c.input('in').into(echo('relay')).sink('input');
    },
  });
  validate(spec);
  assert.deepEqual(
    toHostClassDecl(spec).channels.map((channel) => channel.name),
    ['in', 'input'],
  );
});

// `sdk-graph-authoring`: an underived channel name comes from the PRODUCER
// NODE, never from the sink it terminates on — the sink's name is the plugin's,
// and several channels may name one sink.
test('an unnamed edge terminating on a sink takes the producing node’s name', () => {
  const spec = defineOperative({
    factoryId: 'unnamed_sink',
    channels: (c) => {
      c.input('in').into(echo('plain')).sink();
    },
  });
  validate(spec);
  assert.deepEqual(
    toHostClassDecl(spec).channels.map((channel) => channel.name),
    ['in', 'plain'],
  );
});

// --- one output channel per producing procedure ------------------------------
//
// The model Rust and Java carry: a procedure node owns ONE output channel,
// created on first wiring and reused after. A held handle wired twice appends
// a second consumer to it — it does not declare a second channel claiming the
// same producer, which the core resolves by first match
// (`host/register.rs` `procedure_wiring`) and so silently truncates.

// --- one output channel per producing procedure ------------------------------
//
// The model Rust and Java carry: a procedure node owns ONE output channel,
// created on first wiring and reused after. A held handle wired twice appends
// a second consumer to it — it does not declare a second channel claiming the
// same producer, which the core resolves by first match
// (`host/register.rs` `procedure_wiring`) and so silently truncates.

test('a held handle wired twice is one channel with two consumers', () => {
  const producer = echo('producer');
  const spec = defineOperative({
    factoryId: 'fan_out',
    channels: (c) => {
      const edge = c.input('in').into(producer);
      edge.into(echo('b'));
      edge.into(echo('c'));
    },
  });
  const channels = derivedChannels(spec);
  assert.deepEqual(channels, [
    ['in', 'entry', null, [0]],
    ['producer', 'inter_proc', 0, [1, 2]],
  ]);
  assert.equal(channels.filter((channel) => channel[2] === 0).length, 1);
});

test('branch(...) and two into(...) statements derive the identical topology', () => {
  // The record differs by one link — the sugar's declared arity — and the
  // topology the core derives from the two does not.
  const graph = (factoryId, wire) =>
    JSON.stringify(
      derivedChannels(
        defineOperative({
          factoryId,
          channels: (c) => wire(c.input('in').into(echo('producer'))),
        }),
      ),
    );

  const separate = graph('separate', (edge) => {
    edge.into(echo('b'));
    edge.into(echo('c'));
  });
  const branched = graph('branched', (edge) =>
    edge.branch([(arm) => arm.into(echo('b')), (arm) => arm.into(echo('c'))]),
  );
  assert.equal(separate, branched);
});

test('sink(...) returns the handle, so a consumer wires behind it', () => {
  const spec = defineOperative({
    factoryId: 'hybrid',
    channels: (c) => {
      c.input('in').into(echo('producer')).sink('hybrid_indicator').into(echo('consumer'));
    },
  });
  assert.deepEqual(
    validate(spec).channels.map((channel) => [
      channel.name,
      channel.kind,
      channel.producer,
      channel.consumers.map((edge) => edge.procedure),
      channel.sink,
    ]),
    [
      ['in', 'entry', null, [0], null],
      // A sink alongside a consumer stays inter-procedure: the value reaches
      // both, and the channel is journalled as the edge it still is.
      ['hybrid_indicator', 'inter_proc', 0, [1], 'hybrid_indicator'],
    ],
  );
});

test('keyed() reaches the channel of a handle that also declares a sink', () => {
  const spec = defineOperative({
    factoryId: 'keyed_hybrid',
    channels: (c) => {
      c.input('in').into(echo('producer')).keyed().sink('mid').into(echo('consumer'));
    },
  });
  validate(spec);
  assert.equal(toHostClassDecl(spec).channels[1].keyed, true);
});

test('a transform declared before one into(...) reaches that consumer only', () => {
  const spec = defineOperative({
    factoryId: 'per_edge_transform',
    channels: (c) => {
      const edge = c.input('in').into(echo('producer'));
      edge.map((value) => `${value}!`).into(echo('b'));
      edge.into(echo('c'));
    },
  });
  validate(spec);
  const [, channel] = spec.channels;
  assert.deepEqual(
    channel.consumers.map((consumer) => [consumer.procedure, consumer.transforms.length]),
    [
      [1, 1],
      [2, 0],
    ],
  );
});

// The refusal both references carry by name — Rust's
// `TopologyIssue::DanglingTransformChain` and Java's `danglingTransformChain`.
// A transform lives on the consumer edge it terminates in, so one no `into(...)`
// closed has nowhere to run and would be discarded without a word.

test('a bare re-declared edge is a no-op, not a second edge', () => {
  const target = echo('target');
  const spec = defineOperative({
    factoryId: 'edge_redeclared_bare',
    channels: (c) => {
      const edge = c.input('in');
      edge.into(target);
      edge.into(target);
    },
  });
  validate(spec);
  assert.deepEqual(
    spec.channels[0].consumers.map((edge) => [edge.procedure, edge.transforms.length]),
    [[0, 0]],
  );
});

// The reference declares exactly this — `tier_commit_block_workflow` opens with
// `c.effectively_once()` on a workflow class in Rust and Java — so the flag is
// read where the workflow spec is assembled rather than written as `false`. It
// stays off the wire: the core refuses `effectively_once` on a workflow class,
// and Java's `HostClassDeclaration.forWorkflow` never writes it either.

test('a workflow class carries its effectivelyOnce() declaration', () => {
  const spec = defineWorkflow({
    factoryId: 'tier_workflow',
    channels: (c) => {
      c.effectivelyOnce();
      c.input('in').into(echo('producer')).keyed().sink('out');
    },
  });
  validate(spec);
  assert.equal(spec.effectivelyOnce, true);
  assert.equal('effectively_once' in toHostClassDecl(spec), false);
});
