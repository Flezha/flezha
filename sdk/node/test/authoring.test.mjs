// The graph builder and the refusals static typing cannot make.
import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DeclarationError,
  defineOperative,
  defineProcedure,
  defineWorkflow,
  toHostClassDecl,
  validate,
} from '../dist/esm/index.js';
import { ProcedureTestHarness } from '../dist/esm/testing.js';

const echo = (name) => defineProcedure({ name, handle: (input) => input });

const refusal = (build) => {
  try {
    const spec = build();
    validate(spec);
  } catch (error) {
    assert.ok(error instanceof DeclarationError, `expected a DeclarationError, got ${error}`);
    return error.message;
  }
  assert.fail('expected a refusal');
};

test('a graph builds and its topology reaches the declaration', () => {
  const first = echo('first');
  const second = echo('second');
  const spec = defineOperative({
    factoryId: 'chain',
    channels: (c) => {
      c.input('in').into(first).into(second).sink('out');
    },
  });
  validate(spec);
  const decl = toHostClassDecl(spec);
  assert.deepEqual(
    decl.channels.map((channel) => [channel.name, channel.kind]),
    [
      ['in', 'entry'],
      ['first', 'inter_proc'],
      ['second', 'plugin'],
    ],
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
        .sink('out');
    },
  });
  validate(spec);
  const decl = toHostClassDecl(spec);
  assert.equal(decl.procedures[0].entity_mode, 'dynamic');
  assert.equal(decl.procedures[0].idle_ttl_ms, 60_000);
  assert.equal(decl.channels[1].keyed, true);
});

test('a cycle is refused naming a participating procedure', () => {
  const a = echo('a');
  const b = echo('b');
  const message = refusal(() =>
    defineOperative({
      factoryId: 'cyclic',
      channels: (c) => {
        // The third mention finds the node the first created, so this wires
        // b back into a.
        c.input('in').into(a).into(b).into(a);
      },
    }),
  );
  assert.match(message, /cyclic: (a|b): is on a cycle/);
});

test('an unreachable procedure is refused by name', () => {
  // Not reachable through the fluent builder, where every node is created by
  // an `into` that already has a path from an entry. It is reachable by a
  // hand-built declaration, which is exactly what the backstop is for.
  const reached = echo('reached');
  const orphan = echo('orphan');
  const spec = defineOperative({
    factoryId: 'orphaned',
    channels: (c) => {
      c.input('in').into(reached).sink('out');
    },
  });
  spec.procedures.push({ index: 1, name: 'orphan', procedure: orphan });
  spec.channels.push({
    index: spec.channels.length,
    name: 'dangling',
    kind: 'inter_proc',
    producer: 1,
    consumers: [0],
    overflow: 'drop_nothing',
    keyed: false,
    transforms: [],
  });
  assert.throws(
    () => validate(spec),
    (error) => error instanceof DeclarationError && /orphan: is not reachable/.test(error.message),
  );
});

test('a named edge names the channel, not the consuming node', () => {
  const first = echo('first');
  const second = echo('second');
  const spec = defineOperative({
    factoryId: 'named_edge',
    channels: (c) => {
      c.input('in').into(first).named('hop').into(second).sink('out');
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
      c.input('in').into(step).into(step, 'step_again').sink('out');
    },
  });
  validate(spec);
  assert.deepEqual(
    toHostClassDecl(spec).procedures.map((p) => p.name),
    ['step', 'step_again'],
  );
});

test('two procedures claiming one node identity are refused', () => {
  const message = refusal(() => {
    const first = defineProcedure({ name: 'same', handle: (i) => i });
    const second = defineProcedure({ name: 'same', handle: (i) => i });
    return defineOperative({
      factoryId: 'collide',
      channels: (c) => {
        c.input('in').branch([(edge) => edge.into(first), (edge) => edge.into(second)]);
      },
    });
  });
  assert.match(message, /node identity/);
});

test('an event-time clock without an extractor is refused', () => {
  const message = refusal(() => {
    const proc = defineProcedure({ name: 'windowed', clock: 'event_time', handle: (i) => i });
    return defineOperative({
      factoryId: 'no_extractor',
      channels: (c) => c.input('in').into(proc).sink('out'),
    });
  });
  assert.match(message, /windowed: declares an event_time clock but no eventTime extractor/);
});

test('a logical clock on a stateless procedure is unservable and refused', () => {
  const message = refusal(() => {
    const proc = defineProcedure({ name: 'ticker', clock: 'logical', handle: (i) => i });
    return defineOperative({
      factoryId: 'unservable',
      channels: (c) => c.input('in').into(proc).sink('out'),
    });
  });
  assert.match(message, /logical clock but no state/);
});

test('keyed() without a plugin wire is refused', () => {
  const message = refusal(() => {
    const a = echo('a');
    const b = echo('b');
    return defineOperative({
      factoryId: 'keyed_edge',
      channels: (c) => c.input('in').into(a).keyed().into(b).sink('out'),
    });
  });
  assert.match(message, /keyed\(\) but has no plugin wire/);
});

test('a tier class with an unkeyed sink is refused', () => {
  const message = refusal(() =>
    defineOperative({
      factoryId: 'tier_unkeyed',
      channels: (c) => {
        c.effectivelyOnce();
        c.input('in').into(echo('p')).sink('out');
      },
    }),
  );
  assert.match(message, /effectively-once tier deduplicates by output identity/);
});

for (const [option, declare] of [
  ['checkpointAck', (c) => c.checkpointAck()],
  ['checkpointEvery', (c) => c.checkpointEvery(4)],
  ['outputsAtCheckpoint', (c) => c.outputsAtCheckpoint(250)],
]) {
  test(`${option}() without the tier is refused`, () => {
    const message = refusal(() =>
      defineOperative({
        factoryId: `ckpt_${option}`,
        channels: (c) => {
          declare(c);
          c.input('in').into(echo('p')).keyed().sink('out');
        },
      }),
    );
    assert.match(message, /checkpoint acknowledgement without effectivelyOnce/);
  });
}

test('the checkpoint options compose in any order, and none clears another', () => {
  const spec = defineOperative({
    factoryId: 'compose',
    channels: (c) => {
      c.effectivelyOnce();
      c.outputsAtCheckpoint(250);
      c.checkpointAck();
      c.checkpointEvery(4);
      c.input('in').into(echo('p')).keyed().sink('out');
    },
  });
  validate(spec);
  const decl = toHostClassDecl(spec);
  assert.equal(decl.ack_granularity, 'checkpoint');
  assert.equal(decl.checkpoint_every, 4);
  assert.equal(decl.checkpoint_outputs, 'at_checkpoint');
  assert.equal(decl.checkpoint_max_delay_ms, 250);
});

test('a correction window without the tier is refused', () => {
  const message = refusal(() => {
    const proc = defineProcedure({
      name: 'candle',
      clock: 'event_time',
      eventTime: (input) => BigInt(input.at),
      handle: (i) => i,
    });
    return defineOperative({
      factoryId: 'correction',
      channels: (c) => {
        c.correctionWindow(60_000);
        c.input('in').into(proc).keyed().sink('out');
      },
    });
  });
  assert.match(message, /correction window without effectivelyOnce/);
});

test('a correction window with no event-time procedure is refused', () => {
  const message = refusal(() =>
    defineOperative({
      factoryId: 'correction_wall',
      channels: (c) => {
        c.effectivelyOnce();
        c.correctionWindow(60_000);
        c.input('in').into(echo('p')).keyed().sink('out');
      },
    }),
  );
  assert.match(message, /no procedure declares an event_time clock/);
});

test('a dropping overflow under the tier is refused', () => {
  const message = refusal(() =>
    defineOperative({
      factoryId: 'shedding_tier',
      channels: (c) => {
        c.effectivelyOnce();
        c.input('in').onFull('drop_oldest').into(echo('p')).keyed().sink('out');
      },
    }),
  );
  assert.match(message, /loss is permanent/);
});

test('branch below two arms is refused', () => {
  assert.throws(
    () =>
      defineOperative({
        factoryId: 'one_arm',
        channels: (c) => c.input('in').branch([(edge) => edge.into(echo('a'))]),
      }),
    (error) => error instanceof DeclarationError && /at least two arms/.test(error.message),
  );
});

// A procedure owns one output channel, so a second sink on it is the
// reference's `DuplicatePluginSink`. Two procedures may name one sink; that is
// several channels naming one plugin, which the reference permits.
test('a duplicate plugin sink is refused', () => {
  const message = refusal(() => {
    const a = echo('a');
    return defineOperative({
      factoryId: 'twice',
      channels: (c) => {
        c.input('in').into(a).sink('out').sink('out_again');
      },
    });
  });
  assert.match(message, /declares a plugin sink more than once/);
});

test('two procedures may name one plugin sink', () => {
  const a = echo('a');
  const b = echo('b');
  const spec = defineOperative({
    factoryId: 'one_sink_twice',
    channels: (c) => {
      c.input('in').branch([
        (edge) => edge.into(a).sink('out'),
        (edge) => edge.into(b).sink('out'),
      ]);
    },
  });
  validate(spec);
  assert.deepEqual(
    toHostClassDecl(spec).channels.map((channel) => [channel.name, channel.kind]),
    [
      ['in', 'entry'],
      ['a', 'plugin'],
      ['b', 'plugin'],
    ],
  );
});

test('an unkeyed edge into a dynamic procedure is refused', () => {
  const message = refusal(() => {
    const dynamic = defineProcedure({
      name: 'dyn',
      dynamicEntities: {},
      handle: (i) => i,
    });
    return defineOperative({
      factoryId: 'unkeyed_dynamic',
      channels: (c) => c.input('in').into(dynamic).sink('out'),
    });
  });
  assert.match(message, /without a keyBy/);
});

test('keyBy into a statically keyed procedure is refused', () => {
  const message = refusal(() => {
    const statik = defineProcedure({
      name: 'fixed',
      entities: { keys: ['a', 'b'], entitiesFor: () => ['a'] },
      handle: (i) => i,
    });
    return defineOperative({
      factoryId: 'both_keyings',
      channels: (c) =>
        c
          .input('in')
          .keyBy(() => 'a')
          .into(statik)
          .sink('out'),
    });
  });
  assert.match(message, /one or the other, not both/);
});

test('a static key set without entitiesFor is refused', () => {
  const message = refusal(() => {
    const statik = defineProcedure({
      name: 'fixed',
      entities: { keys: ['a'] },
      handle: (i) => i,
    });
    return defineOperative({
      factoryId: 'no_entities_for',
      channels: (c) => c.input('in').into(statik).sink('out'),
    });
  });
  assert.match(message, /static key set without an entitiesFor/);
});

test('a stream class with no entry channel is refused', () => {
  const message = refusal(() => defineOperative({ factoryId: 'no_entry', channels: () => {} }));
  assert.match(message, /declares no entry channel/);
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

test('a named edge terminating on a sink names the channel too, and the sink name is its default', () => {
  // `.named()` is honoured on the inter-proc path and was dropped on the
  // plugin path, so `.named('input').sink('relay_out')` produced a channel
  // called `relay_out` — and the channel name is what a forwarded frame
  // carries and what selects the receiving operative's entry channel.
  const spec = defineOperative({
    factoryId: 'named_sink',
    channels: (c) => {
      c.input('in').into(echo('relay')).named('input').sink('relay_out');
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
      c.input('in').into(echo('plain')).sink('relay_out');
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
  validate(spec);
  const channels = toHostClassDecl(spec).channels;
  assert.deepEqual(
    channels.map((channel) => [channel.name, channel.kind, channel.producer, channel.consumers]),
    [
      ['in', 'entry', undefined, [0]],
      ['producer', 'inter_proc', 0, [1, 2]],
    ],
  );
  assert.equal(channels.filter((channel) => channel.producer === 0).length, 1);
});

test('branch(...) and two into(...) statements build the identical declaration', () => {
  const graph = (factoryId, wire) =>
    JSON.stringify(
      toHostClassDecl(
        defineOperative({
          factoryId,
          channels: (c) => wire(c.input('in').into(echo('producer'))),
        }),
      ),
    ).replace(factoryId, '<id>');

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
      c.input('in')
        .into(echo('producer'))
        .named('hybrid_indicator')
        .sink('hybrid_indicators')
        .into(echo('consumer'));
    },
  });
  validate(spec);
  assert.deepEqual(
    toHostClassDecl(spec).channels.map((channel) => [
      channel.name,
      channel.kind,
      channel.producer,
      channel.consumers,
    ]),
    [
      ['in', 'entry', undefined, [0]],
      ['hybrid_indicator', 'inter_proc', 0, [1]],
    ],
  );
});

test('keyed() is refused without a plugin sink, and accepted with one', () => {
  const message = refusal(() =>
    defineOperative({
      factoryId: 'keyed_no_sink',
      channels: (c) => {
        c.input('in').into(echo('producer')).named('mid').keyed().into(echo('consumer'));
      },
    }),
  );
  assert.match(message, /is declared keyed\(\) but has no plugin wire/);

  const hybrid = defineOperative({
    factoryId: 'keyed_hybrid',
    channels: (c) => {
      c.input('in')
        .into(echo('producer'))
        .named('mid')
        .keyed()
        .sink('mid_sink')
        .into(echo('consumer'));
    },
  });
  validate(hybrid);
  assert.equal(toHostClassDecl(hybrid).channels[1].keyed, true);
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
test('a transform chain no into(...) closes is refused', () => {
  const message = refusal(() =>
    defineOperative({
      factoryId: 'dangling',
      channels: (c) => {
        c.input('in')
          .into(echo('validate'))
          .map((value) => `${value}!`)
          .sink('s');
      },
    }),
  );
  assert.match(
    message,
    /validate: DanglingTransformChain: declares a transform or keyBy\(\.\.\.\)/,
  );

  // A key dangles the same way. Declared on a channel another edge consumes, so
  // no other refusal reaches the declaration first.
  const keyed = refusal(() =>
    defineOperative({
      factoryId: 'dangling_key',
      channels: (c) => {
        const edge = c.input('in');
        edge.into(echo('kept'));
        edge.keyBy((value) => String(value));
      },
    }),
  );
  assert.match(keyed, /in: DanglingTransformChain:/);
});

// The three refusals both references raise from their own `extend_pending` —
// Rust's `TopologyIssue::{DuplicateKeyBy, TransformAfterKeyBy,
// TransformAfterTerminal}` and Java's factory methods of the same names. Each
// shape was ACCEPTED here before this: a second `keyBy` overwrote the author's
// first without a word, and neither a prior key nor an attached sink was
// inspected at all.
test('a second keyBy is refused rather than overwriting the first', () => {
  const keyed = defineProcedure({
    name: 'keyed',
    dynamicEntities: { idleTtlMs: 60_000, maxEntities: 100 },
    handle: (input) => input,
  });
  const message = refusal(() =>
    defineOperative({
      factoryId: 'dup_key_by',
      channels: (c) => {
        c.input('in')
          .keyBy(() => 'a')
          .keyBy(() => 'b')
          .into(keyed);
      },
    }),
  );
  assert.match(
    message,
    /in: DuplicateKeyBy: declares keyBy\(\.\.\.\) twice before one into\(\.\.\.\)/,
  );
});

// The key is evaluated on the final post-transform value, so it must be the
// last link before the `into(...)` that closes the chain.
test('a transform after keyBy is refused', () => {
  const keyed = defineProcedure({
    name: 'keyed',
    dynamicEntities: { idleTtlMs: 60_000, maxEntities: 100 },
    handle: (input) => input,
  });
  const message = refusal(() =>
    defineOperative({
      factoryId: 'transform_after_key_by',
      channels: (c) => {
        c.input('in')
          .keyBy(() => 'a')
          .map((value) => `${value}!`)
          .into(keyed);
      },
    }),
  );
  assert.match(message, /in: TransformAfterKeyBy: declares a transform after keyBy\(\.\.\.\)/);
});

// The sink serializes the channel's own value, so nothing declared behind it
// can reshape what it already wrote.
test('a transform after a plugin sink is refused', () => {
  const message = refusal(() =>
    defineOperative({
      factoryId: 'transform_after_terminal',
      channels: (c) => {
        c.input('in')
          .into(echo('emit'))
          .sink('s')
          .map((value) => `${value}!`)
          .into(echo('tail'));
      },
    }),
  );
  assert.match(
    message,
    /emit: TransformAfterTerminal: declares a transform or keyBy\(\.\.\.\) after a plugin sink/,
  );
});

// The refusal both references raise from consumer wiring rather than chain
// extension — Rust's `attach_consumer` and Java's `attachEdge`. Both shapes
// were ACCEPTED here before this, leaving two consumer edges naming one
// procedure, so the value reached it twice.
test('a re-declared edge carrying a transform is refused', () => {
  const target = echo('target');
  const message = refusal(() =>
    defineOperative({
      factoryId: 'edge_already_wired',
      channels: (c) => {
        const edge = c.input('in');
        edge.into(target);
        edge.map((value) => `${value}!`).into(target);
      },
    }),
  );
  assert.match(
    message,
    /in: EdgeAlreadyWired: already feeds procedure `target`; an edge carries exactly one/,
  );
});

// A re-declaration carrying neither transform nor key is the fan-in /
// repeated-`into` case both references make a true no-op: one consumer edge,
// not two, and not a refusal.
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
      c.input('in').into(echo('producer')).named('out').keyed().sink('out');
    },
  });
  validate(spec);
  assert.equal(spec.effectivelyOnce, true);
  assert.equal('effectively_once' in toHostClassDecl(spec), false);
});
