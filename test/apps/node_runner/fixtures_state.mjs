// Fixtures whose subject is persisted Entity state: the keyed upsert the bench
// shares, and the counter and resume twins — the same partition as
// test/apps/python_runner/fixtures_state.py and the reference's state.rs.

import { decodeValue, defineOperative, defineProcedure } from '@flezha/sdk';

import { bytes, text } from './fixture_support.mjs';

/** The reference parses `i64` and reads an unparsable input as 0. */
const asInt = (input) => (/^-?\d+$/.test(String(input)) ? Number(input) : 0);

// -- bench_keyed_nopersist / bench_keyed / the tier stores -------------------
//
// The reference's `TierStoreState` and its upsert fold: `tier_store`,
// `checkpoint_store` and `bench_checkpoint` are the same keyed upsert under
// three acknowledgement postures; only the handler differs.

const keyedUpsertProcedure = (name, handle) =>
  defineProcedure({
    name,
    initialState: () => ({ values: {} }),
    applyChange: (state, change) => {
      state.values[change.key] = change.value;
    },
    handle,
  });

/** A `key:value` input as its two halves; the value keeps any later colon. */
const splitKeyed = (input) => {
  const [key, ...rest] = String(input).split(':');
  return [key, rest.join(':')];
};

/** Record and emit one `key:value` input under its natural key. */
const upsert = (input, ctx) => {
  const [key, value] = splitKeyed(input);
  ctx.change({ key, value });
  ctx.emit(`${key}=${value}`, { key });
};

const TierStore = keyedUpsertProcedure('tier_store_procedure', (input, ctx) => {
  // Dead-letter e2e: a "poison" value fails every attempt, exhausting the
  // retry budget so the tier-on atomic park advances the entity past it.
  if (splitKeyed(input)[1] === 'poison') throw new Error('poison value');
  upsert(input, ctx);
});

// `tier_store`'s twin, whose interval closes on a `commit:` input.
const CheckpointStore = keyedUpsertProcedure('checkpoint_store_procedure', (input, ctx) => {
  if (splitKeyed(input)[0] === 'commit') {
    ctx.checkpoint();
    return;
  }
  upsert(input, ctx);
});

// `checkpoint_store` driven by a DECLARED cadence rather than an input.
const BenchCheckpoint = keyedUpsertProcedure('bench_checkpoint_procedure', upsert);

// The same keyed upsert under both durability postures, so the round-trip
// bench can isolate the cost of per-Entity persistence.
const keyedUpsert = (factoryId, options = {}) =>
  defineOperative({
    factoryId,
    ...options,
    channels: (c) => {
      c.input('input', text)
        .onFull('drop_nothing')
        .into(TierStore)
        .named('sink')
        .keyed()
        .sink('tier_sink', bytes);
    },
  });

// Per-entity persistence off: this is the heap-only posture the bench isolates
// against the durable one.
export const benchKeyedNoPersist = keyedUpsert('bench_keyed_nopersist', { persistent: false });
export const benchKeyed = keyedUpsert('bench_keyed');

// The effectively-once reference: a keyed upsert under the tier. A 300 ms
// backoff base keeps the min-watermark freeze observable in the DLQ e2e.
// Declared DropNothing: a durable-input-log operative that shed at its mailbox
// would hold the min-watermark on the shed offset forever while the log behind
// it kept growing.
export const tierStore = defineOperative({
  factoryId: 'tier_store',
  channels: (c) => {
    c.effectivelyOnce();
    c.input('input', text)
      .onFull('drop_nothing')
      .retryBackoffMs(300)
      .into(TierStore)
      .named('sink')
      .keyed()
      .sink('tier_sink', bytes);
  },
});

export const checkpointStore = defineOperative({
  factoryId: 'checkpoint_store',
  channels: (c) => {
    c.effectivelyOnce().checkpointAck();
    c.input('input', text).into(CheckpointStore).named('sink').keyed().sink('tier_sink', bytes);
  },
});

// The same procedure declared `outputsAtCheckpoint`: the interval's outputs
// are held with its changes and ship inside its checkpoint record.
// `checkpointEvery` is deliberately absent so the test drives the close itself
// — with `commit:` for the batch case, and with the delay alone for the
// quiet-Entity case.
export const checkpointBatchedStore = defineOperative({
  factoryId: 'checkpoint_batched_store',
  channels: (c) => {
    c.effectivelyOnce().checkpointAck().outputsAtCheckpoint(1_500);
    c.input('input', text).into(CheckpointStore).named('sink').keyed().sink('tier_sink', bytes);
  },
});

const envInt = (key, fallback) => Number(process.env[key]) || fallback;

// `checkpoint_store` driven by a DECLARED cadence instead of a `commit:` input:
// acknowledging every `FLEZHA_BENCH_CHECKPOINT_EVERY` inputs (default 500) per
// Entity. `FLEZHA_BENCH_CHECKPOINT_OUTPUTS=at_checkpoint` (with
// `FLEZHA_BENCH_CHECKPOINT_DELAY_MS`) additionally holds the interval's outputs
// until it closes.
export const benchCheckpoint = defineOperative({
  factoryId: 'bench_checkpoint',
  channels: (c) => {
    c.effectivelyOnce().checkpointEvery(envInt('FLEZHA_BENCH_CHECKPOINT_EVERY', 500));
    if (process.env.FLEZHA_BENCH_CHECKPOINT_OUTPUTS === 'at_checkpoint') {
      c.outputsAtCheckpoint(envInt('FLEZHA_BENCH_CHECKPOINT_DELAY_MS', 50));
    }
    c.input('input', text)
      .onFull('drop_nothing')
      .into(BenchCheckpoint)
      .named('sink')
      .keyed()
      .sink('tier_sink', bytes);
  },
});

// -- store_input ------------------------------------------------------------

// The durable-state reference: every input is appended through a change.
const StoreInput = defineProcedure({
  name: 'store_input_procedure',
  initialState: () => ({ binary_inputs: [], text_inputs: [] }),
  applyChange: (state, change) => {
    state.text_inputs.push(change.push_text);
  },
  handle: (input, ctx) => {
    ctx.change({ push_text: input });
    return input;
  },
});

export const storeInput = defineOperative({
  factoryId: 'store_input',
  channels: (c) => {
    c.input('input', text).into(StoreInput).named('sink').sink('store_input_sink', bytes);
  },
});

// -- stateful_counter / persistence_off_counter -----------------------------

const CounterEntry = defineProcedure({
  name: 'counter_entry_procedure',
  handle: (input) => asInt(input),
});

const Counter = defineProcedure({
  name: 'counter_procedure',
  initialState: () => ({ value: 0 }),
  applyChange: (state, change) => {
    state.value += change.delta;
  },
  handle: (input, ctx) => {
    ctx.change({ delta: input });
  },
});

export const statefulCounter = defineOperative({
  factoryId: 'stateful_counter',
  channels: (c) => {
    c.input('input', text).into(CounterEntry).named('ticks').into(Counter);
  },
});

// The same topology with per-Entity persistence off, under the reference's
// node names: the toggle is a class property, not a different computation.
export const persistenceOffCounter = defineOperative({
  factoryId: 'persistence_off_counter',
  persistent: false,
  channels: (c) => {
    c.input('input', text)
      .into(CounterEntry, 'persistence_off_entry_procedure')
      .named('ticks')
      .into(Counter, 'persistence_off_counter_procedure');
  },
});

// -- coalescing_counter -----------------------------------------------------

// Wire format: `<interval>:<ts>:<delta>`; a missing or unparsable part reads 0.
const TickEntry = defineProcedure({
  name: 'tick_entry_procedure',
  handle: (input) => {
    const [interval, ts, ...rest] = String(input).split(':');
    return { interval: asInt(interval), ts: asInt(ts), delta: asInt(rest.join(':')) };
  },
});

// The reference coalesces this change on `(interval, ts)` through a `Change`
// trait method. The SDK carries no coalesce-key seam, so the change ships
// uncoalesced — a throughput property, not a semantic one: the fold is
// idempotent per `(interval, ts)` either way, so replay produces the same state.
const Tick = defineProcedure({
  name: 'tick_procedure',
  initialState: () => ({ last: 0 }),
  applyChange: (state, change) => {
    state.last = change.delta;
  },
  handle: (input, ctx) => {
    ctx.change({ ...input });
  },
});

export const coalescingCounter = defineOperative({
  factoryId: 'coalescing_counter',
  channels: (c) => {
    c.input('input', text).into(TickEntry).named('ticks').into(Tick);
  },
});

// -- resume_probe -----------------------------------------------------------

// Emits the running total, so a resume is observable on the wire. Default +
// change replay rebuilds the total on restore; no snapshot fast path needed.
const ResumeProbe = defineProcedure({
  name: 'resume_probe_procedure',
  initialState: () => ({ total: 0 }),
  applyChange: (state, change) => {
    state.total += change.add;
  },
  handle: (input, ctx) => {
    const delta = asInt(input);
    const total = ctx.state.total + delta;
    ctx.change({ add: delta });
    return String(total);
  },
});

export const resumeProbe = defineOperative({
  factoryId: 'resume_probe',
  channels: (c) => {
    c.input('input', text).into(ResumeProbe).named('total').sink('resume_probe_out', bytes);
  },
});

// -- schema_evolution_v1 / schema_evolution_v2 ------------------------------
//
// The same class either side of a deploy that edits its own state type. Both
// halves declare the SAME procedure name, so an operative started under v1 and
// restarted under v2 restores from the very WAL v1 wrote — which is what a
// deploy does, with the two binaries collapsed into one fixture file.
//
// v1: `{total}`. v2: `{total, currency}` at schema version 2, reading down to 1
// through a declared migration. Output is `total:currency` so the e2e can read
// the migrated field off the wire.

const EvolvingCartV1 = defineProcedure({
  name: 'evolving_cart',
  initialState: () => ({ total: 0 }),
  applyChange: (state, change) => {
    state.total += change.add;
  },
  handle: (input, ctx) => {
    const delta = asInt(input);
    const total = ctx.state.total + delta;
    ctx.change({ add: delta });
    return `${total}:`;
  },
});

const EvolvingCartV2 = defineProcedure({
  name: 'evolving_cart',
  schemaVersion: 2,
  minReadableSchemaVersion: 1,
  initialState: () => ({ total: 0, currency: '' }),
  applyChange: (state, change) => {
    state.total += change.add;
  },
  migrateState: (fromVersion, body) => {
    if (fromVersion !== 1) throw new Error(`no migration from ${fromVersion}`);
    const old = decodeValue(body) ?? {};
    return { total: old.total ?? 0, currency: 'EUR' };
  },
  migrateChange: (fromVersion, body) => {
    if (fromVersion !== 1) throw new Error(`no migration from ${fromVersion}`);
    const old = decodeValue(body) ?? {};
    return { add: old.add ?? 0, note: 'migrated' };
  },
  handle: (input, ctx) => {
    const delta = asInt(input);
    const total = ctx.state.total + delta;
    const currency = ctx.state.currency || 'EUR';
    ctx.change({ add: delta, note: 'live' });
    return `${total}:${currency}`;
  },
});

const evolvingCart = (factoryId, procedure) =>
  defineOperative({
    factoryId,
    channels: (c) => {
      c.input('input', text).into(procedure).named('total').sink('schema_evolution_out', bytes);
    },
  });

export const schemaEvolutionV1 = evolvingCart('schema_evolution_v1', EvolvingCartV1);
export const schemaEvolutionV2 = evolvingCart('schema_evolution_v2', EvolvingCartV2);
