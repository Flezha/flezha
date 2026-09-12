// Twins of the rust_runner e2e fixtures (test/apps/rust_runner/src/main.rs)
// under the same factory ids, procedure names, channel names and output bytes.
//
// Entry payloads and plugin frames are UTF-8 text on the wire, the same as the
// reference: the SDK's default codec is MessagePack, so every fixture that
// exchanges bytes with another SDK or with a plugin declares its own decoder
// and encoder. An inter-procedure hop between two Node procedures carries
// MessagePack, which no one else reads.

import { BroadcastChannel } from 'node:worker_threads';

import { defineOperative, defineProcedure } from '@flezha/sdk';

import { bytes, prefix, text } from './fixture_support.mjs';

// The persisted-state family is registered from its own module.
export * from './fixtures_state.mjs';

// -- do_nothing / bench_echo ------------------------------------------------

const DoNothing = defineProcedure({
  name: 'do_nothing_procedure',
  handle: (input) => input,
});

export const doNothing = defineOperative({
  factoryId: 'do_nothing',
  channels: (c) => {
    c.input('input', text).into(DoNothing).named('echo').sink('do_nothing_echo', bytes);
  },
});

// Echo with a declared DropNothing on its entry mailbox. `do_nothing` is the
// merge-blocking bench factory and must not change; this keeps the round-trip
// bench's back-pressure posture independent of it.
export const benchEcho = defineOperative({
  factoryId: 'bench_echo',
  channels: (c) => {
    c.input('input', text)
      .onFull('drop_nothing')
      .into(DoNothing)
      .named('echo')
      .sink('bench_echo_sink', bytes);
  },
});

// -- compare_filter ---------------------------------------------------------

// State seeded by the controller's init values and then only read: there is
// nothing to journal, so the procedure declares no change fold.
const CompareFilter = defineProcedure({
  name: 'compare_filter_procedure',
  initialState: () => ({ comparable: 'default', inverted: false }),
  handle: (input, ctx) => (ctx.state.comparable === input ? input : undefined),
});

export const compareFilter = defineOperative({
  factoryId: 'compare_filter',
  channels: (c) => {
    c.input('input', text).into(CompareFilter).named('sink').sink('compare_filter_sink', bytes);
  },
});

// -- static_entity / sugar_static_entity / mixed_operative ------------------

const STATIC_KEYS = ['a', 'b', 'c', 'd'];
const staticEntitiesFor = (input) => {
  const first = String(input)[0];
  return [STATIC_KEYS.includes(first) ? first : 'd'];
};

const staticEntry = (name) =>
  defineProcedure({
    name,
    entities: { keys: STATIC_KEYS, entitiesFor: staticEntitiesFor },
    initialState: () => ({ seen: [] }),
    applyChange: (state, change) => {
      state.seen.push(change.push);
    },
    handle: (input, ctx) => {
      ctx.change({ push: input });
      return input;
    },
  });

const passThrough = (name) => defineProcedure({ name, handle: (input) => input });

const StaticEntry = staticEntry('static_entry_procedure');
const StaticDownstream = passThrough('static_downstream_procedure');

export const staticEntity = defineOperative({
  factoryId: 'static_entity',
  channels: (c) => {
    c.input('input', text)
      .into(StaticEntry)
      .named('hop')
      .into(StaticDownstream)
      .named('kafka')
      .sink('test_kafka', bytes);
  },
});

const MacroStaticEntry = staticEntry('macro_static_entry_procedure');
const MacroStaticDownstream = passThrough('macro_static_downstream_procedure');

// The reference pipeline sugar leaves the hop unnamed and names the plugin
// channel after the plugin.
export const sugarStaticEntity = defineOperative({
  factoryId: 'sugar_static_entity',
  channels: (c) => {
    c.input('input', text)
      .into(MacroStaticEntry)
      .into(MacroStaticDownstream)
      .named('test_kafka')
      .sink('test_kafka', bytes);
  },
});

const MixedMacroEntry = passThrough('mixed_macro_entry_procedure');
const MixedHandDownstream = passThrough('mixed_hand_downstream');

export const mixedOperative = defineOperative({
  factoryId: 'mixed_operative',
  channels: (c) => {
    c.input('input', text)
      .into(MixedMacroEntry)
      .named('hop')
      .into(MixedHandDownstream)
      .named('kafka')
      .sink('test_kafka', bytes);
  },
});

// -- sugar_do_nothing / sugar_store_input -----------------------------------

const SugarDoNothing = passThrough('sugar_do_nothing');

export const sugarDoNothing = defineOperative({
  factoryId: 'sugar_do_nothing',
  channels: (c) => {
    c.input('input', text)
      .into(SugarDoNothing)
      .named('sugar_do_nothing_echo')
      .sink('sugar_do_nothing_echo', bytes);
  },
});

const SugarStoreInput = defineProcedure({
  name: 'sugar_store_input',
  initialState: () => ({ text_inputs: [] }),
  applyChange: (state, change) => {
    state.text_inputs.push(change.push);
  },
  handle: (input, ctx) => {
    ctx.change({ push: input });
    return input;
  },
});

export const sugarStoreInput = defineOperative({
  factoryId: 'sugar_store_input',
  channels: (c) => {
    c.input('input', text)
      .into(SugarStoreInput)
      .named('sugar_store_sink')
      .sink('sugar_store_sink', bytes);
  },
});

// -- hybrid_channel ---------------------------------------------------------
//
// One emit reaching two destinations: an inter-procedure consumer and a plugin
// wire with its own serializer. The serializer refuses a sentinel value, so an
// operator can observe end to end that a refusal is isolated to the emit rather
// than failing the item.

const HYBRID_SENTINEL = 0xdeadbeef;

const HybridProducer = defineProcedure({
  name: 'hybrid_producer_procedure',
  handle: (input, ctx) => {
    const parsed = Number.parseInt(String(input), 10);
    const value = Number.isSafeInteger(parsed) && parsed >= 0 && parsed <= 0xffffffff ? parsed : 0;
    try {
      ctx.emit(value);
    } catch (error) {
      console.error(`hybrid_producer_procedure: serializer refused an emit: ${error.message}`);
    }
  },
});

const HybridConsumer = defineProcedure({
  name: 'hybrid_consumer_procedure',
  handle: (input) => {
    console.log(`hybrid_consumer_procedure: received ${input}`);
  },
});

export const hybridChannel = defineOperative({
  factoryId: 'hybrid_channel',
  channels: (c) => {
    c.input('input', text)
      .into(HybridProducer)
      .branch([
        // The consumer edge first, so a refused plugin serialization leaves the
        // in-process fan-out already recorded — the reference's isolation.
        (edge) => edge.named('hybrid_indicator').into(HybridConsumer),
        (edge) =>
          edge.sink('hybrid_indicators', (value) => {
            if (value === HYBRID_SENTINEL) {
              throw new Error('hybrid_indicators: serializer rejected sentinel 0xDEADBEEF');
            }
            const out = Buffer.alloc(4);
            out.writeUInt32BE(value);
            return out;
          }),
      ]);
  },
});

// -- entity_stress ----------------------------------------------------------

const STRESS_KEYS = ['p0', 'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'];

const stress = (name, entitiesFor) =>
  defineProcedure({
    name,
    entities: { keys: STRESS_KEYS, entitiesFor },
    handle: (input) => input,
  });

const StressA = stress('stress_a', () => STRESS_KEYS);
const StressB = stress('stress_b', () => ['p0']);
const StressC = stress('stress_c', () => ['p0']);
const StressD = stress('stress_d', () => ['p0']);

export const entityStress = defineOperative({
  factoryId: 'entity_stress',
  channels: (c) => {
    c.input('input', text)
      .into(StressA)
      .named('a_to_b')
      .into(StressB)
      .named('b_to_c')
      .into(StressC)
      .named('c_to_d')
      .into(StressD)
      .named('echo')
      .sink('entity_stress_echo', bytes);
  },
});

// -- poison_probe / wedge_probe ---------------------------------------------

let antidoteTaken = false;

const PoisonProbe = defineProcedure({
  name: 'poison_probe_procedure',
  handle: (input) => {
    if (input === 'antidote') antidoteTaken = true;
    if (input === 'poison' && !antidoteTaken) throw new Error('poison payload');
    return `ok:${input}`;
  },
});

export const poisonProbe = defineOperative({
  factoryId: 'poison_probe',
  channels: (c) => {
    c.input('input', text)
      .retryBackoffMs(25)
      .into(PoisonProbe)
      .named('out')
      .sink('probe_sink', bytes);
  },
});

let wedgeAborted = false;

const WedgeProbe = defineProcedure({
  name: 'wedge_probe_procedure',
  handle: async (input) => {
    // Holds the dispatch loop forever: the runner stays connected and
    // heartbeating while its dispatch counter freezes — the wedge signature.
    // Synchronous on purpose; an await would free the loop, which is the whole
    // thing this probe must not do.
    if (input === 'wedge') {
      for (;;) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 3_600_000);
    }
    // Ends the activation as violently as this host allows, once per runner,
    // WITHOUT emitting.
    if (input === 'abort' && !wedgeAborted) {
      wedgeAborted = true;
      throw new Error('wedge_probe: killing the dispatch holding this activation');
    }
    return `ok:${input}`;
  },
});

export const wedgeProbe = defineOperative({
  factoryId: 'wedge_probe',
  channels: (c) => {
    c.input('input', text).into(WedgeProbe).named('out').sink('wedge_sink', bytes);
  },
});

// -- candle -----------------------------------------------------------------
//
// Late-input correction is about fold ORDER, and the only way to prove that is
// to look at the order. Send 100, 300, then the late 200. Without correction
// the fold order is [100, 300, 200] and the candle is permanently wrong. With
// it, the engine rewinds behind 200, replays the retained input log sorted by
// event time, and the fold order becomes [100, 200, 300].

const Candle = defineProcedure({
  name: 'candle_procedure',
  clock: 'event_time',
  eventTime: (input) => Number(String(input).split(':')[0]) || 0,
  // Event times in fold order: the assertion surface.
  initialState: () => ({ folded: [] }),
  applyChange: (state, change) => {
    state.folded.push(change.fold);
  },
  handle: (input, ctx) => {
    // `ctx.now()` IS the extracted event time under an event-time clock.
    const eventTime = Number(ctx.now());
    ctx.change({ fold: eventTime });
    // One row per candle bucket, so a corrected replay overwrites by key
    // rather than appending a second row.
    const bucket = Math.floor(eventTime / 1000);
    ctx.emit(`${bucket}=${input}`, { key: `candle/${bucket}` });
  },
});

export const candle = defineOperative({
  factoryId: 'candle',
  channels: (c) => {
    c.effectivelyOnce().correctionWindow(600_000);
    c.input('input', text).into(Candle).named('sink').keyed().sink('tier_sink', bytes);
  },
});

// -- session_timeout --------------------------------------------------------
//
// The worked session-timeout shape: each input pushes the idle deadline out,
// and the firing closes the session. Single-entity so the e2e's crash-resume
// half exercises a durable Entity. The timeout is read from
// `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).

const SessionTimeout = defineProcedure({
  name: 'session_timeout_procedure',
  initialState: () => ({ touched: [], closed_at: [] }),
  applyChange: (state, change) => {
    if ('touched' in change) state.touched.push(change.touched);
    else state.closed_at.push(change.closed);
  },
  handle: (input, ctx) => {
    ctx.change({ touched: input });
    // A processing-time timer, because the answer to "nothing has arrived"
    // cannot be an event-time one: nothing would advance the clock.
    ctx.timerAfter(Number(process.env.TIMER_TEST_TIMEOUT_MS) || 1000);
  },
  onTimer: (firing, ctx) => {
    const firedAt = Number(firing.scheduledAt);
    ctx.change({ closed: firedAt });
    ctx.emit(`closed@${firedAt}`);
  },
});

export const sessionTimeout = defineOperative({
  factoryId: 'session_timeout',
  channels: (c) => {
    c.input('input', text).into(SessionTimeout).named('kafka').sink('test_kafka', bytes);
  },
});

// -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} -----------------

const boundedMailboxProcedure = (name) =>
  defineProcedure({
    name,
    mailboxCapacity: 4,
    initialState: () => ({ processed: 0 }),
    applyChange: (state) => {
      state.processed += 1;
    },
    handle: (input, ctx) => {
      ctx.change({ processed: true });
      return input;
    },
  });

const boundedMailbox = (policy) =>
  defineOperative({
    factoryId: `bounded_mailbox_${policy}`,
    channels: (c) => {
      c.input(`${policy}_input`, text)
        .onFull(policy)
        .into(boundedMailboxProcedure(`${policy}_test_procedure`))
        .named(`${policy}_echo`)
        .sink('bounded_mailbox_echo', bytes);
    },
  });

export const boundedMailboxDropNothing = boundedMailbox('drop_nothing');
export const boundedMailboxDropNewest = boundedMailbox('drop_newest');
export const boundedMailboxDropOldest = boundedMailbox('drop_oldest');

// -- partition_relay --------------------------------------------------------
//
// The partitioned-operatives upstream: keyed on the wiring so it can be
// partitioned at all, and emitting keyed so a partitioned forward target can
// resolve the owning member. The plugin channel is named `input` because a
// forwarded frame keeps its producing channel's name, and that is what selects
// the receiving operative's entry channel — so the entry here is `in`.

const PartitionRelay = defineProcedure({
  name: 'partition_relay_procedure',
  dynamicEntities: {},
  initialState: () => ({ seen: [] }),
  applyChange: (state, change) => {
    state.seen.push(change.push);
  },
  handle: (input, ctx) => {
    ctx.change({ push: input });
    ctx.emit(input, { key: prefix(input) });
  },
});

export const partitionRelay = defineOperative({
  factoryId: 'partition_relay',
  channels: (c) => {
    c.input('in', text)
      .keyBy(prefix)
      .into(PartitionRelay)
      .named('input')
      .keyed()
      .sink('relay_out', bytes);
  },
});

// -- dynamic_entity ---------------------------------------------------------

const DynamicEntity = defineProcedure({
  name: 'dynamic_entity_procedure',
  dynamicEntities: {
    idleTtlMs: Number(process.env.DYN_TEST_TTL_MS) || undefined,
  },
  initialState: () => ({ seen: [] }),
  applyChange: (state, change) => {
    state.seen.push(change.push);
  },
  handle: (input, ctx) => {
    ctx.change({ push: input });
    return input;
  },
});

export const dynamicEntity = defineOperative({
  factoryId: 'dynamic_entity',
  channels: (c) => {
    c.input('input', text)
      .onFull('drop_nothing')
      // Filtered before keying: a dropped value answers no key, so the core
      // never mints an entity for it.
      .filter((input) => input !== 'drop')
      .keyBy(prefix)
      .into(DynamicEntity)
      .named('kafka')
      .sink('test_kafka', bytes);
  },
});
