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
      ['first__second', 'inter_proc'],
      ['out', 'plugin'],
    ],
  );
  assert.deepEqual(decl.procedures.map((p) => p.name), ['first', 'second']);
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
      c.input('in').keyBy((value) => value.symbol).into(counter).keyed().sink('out');
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
    (error) =>
      error instanceof DeclarationError && /orphan: is not reachable/.test(error.message),
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
  assert.deepEqual(decl.channels.map((channel) => channel.name), ['in', 'hop', 'out']);
  assert.deepEqual(decl.procedures.map((p) => p.name), ['first', 'second']);
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

test('checkpoint acknowledgement without the tier is refused', () => {
  const message = refusal(() => {
    const spec = defineOperative({
      factoryId: 'ckpt',
      channels: (c) => {
        c.input('in').into(echo('p')).keyed().sink('out');
      },
    });
    spec.ackGranularity = 'checkpoint';
    return spec;
  });
  assert.match(message, /checkpoint acknowledgement without effectivelyOnce/);
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

test('a duplicate plugin sink is refused', () => {
  const message = refusal(() => {
    const a = echo('a');
    const b = echo('b');
    return defineOperative({
      factoryId: 'twice',
      channels: (c) => {
        c.input('in').branch([
          (edge) => edge.into(a).sink('out'),
          (edge) => edge.into(b).sink('out'),
        ]);
      },
    });
  });
  assert.match(message, /declared more than once/);
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
      channels: (c) => c.input('in').keyBy(() => 'a').into(statik).sink('out'),
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
