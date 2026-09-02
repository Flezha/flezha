# SDK State Persistence

For operative authors and SDK implementers: how a Procedure's per-Entity state becomes
durable in the Rust, Java and Elixir SDKs — the shared model (State, Change, snapshot +
change-log restore, compaction, the persistence toggle, payload ids, the record header), then
each SDK's authoring surface and test harness. The controller's on-disk layout, fsync
batching, retention and the `flz state` CLI are in
[`../internals/state_persistence.md`](../internals/state_persistence.md); frame layouts in
[`protocol_spec.md`](protocol_spec.md#11-per-entity-state-persistence). The Elixir SDK is
its own core and carries the same rules re-expressed in Elixir
([`elixir_sdk.md`](elixir_sdk.md#persistence)); the FFI veneers carry them through the core.

## Shared concepts

### State, Change and the fold

A Procedure declares a per-Entity `State` and a `Change` type — one logical mutation record
(`delta: i64`, `Push(input)`). The author folds it with `apply_change(state, change)`; the
runtime persists the record through the per-Entity flush buffer. Two contracts hold in both
SDKs: `apply_change` MUST be a pure, deterministic fold — replay is the recovery primitive
(crash-resume, retry, rewind, effectively-once), so ambient time, randomness and external
reads inside `run` / `apply_change` violate the contract; time comes from `ctx.now()` (the
declared `Clock`), identifiers from `ctx.stable_id()`, and the harness gate `replay_equals`
/ `replayEquals()` catches a divergence. And `state.apply(change)` is the only recorded
mutation path: one fold, one push.

`NoChange` (uninhabited in both SDKs) opts a Procedure out: the runtime detects it (`TypeId`
in Rust, `changeType() == NoChange.class` in Java) and registers nothing with the flusher.
`Clock` is sealed in both SDKs: `WallClock` (epoch ms, read once per invocation),
`EventTimeFrom` (extracted from the input), `LogicalClock` (the durable change seq — needs a
real Change type). `coalesce_key()` (default none) is opt-in batching: Changes sharing a key
within one linger window collapse to the most recent record at drain time; keyless Changes
are kept verbatim — right for "current value of X", wrong for "+1" deltas. The sugared tier
(`#[derive(flezha::State)]`, Java `@State`) generates the field-granular Change type, its
fold and (Java) the Kryo type list; see
[`authoring_contract.md`](authoring_contract.md#3-state--change-derivation-and-the-manual-boundary).

Timers are folded state: registrations, cancellations and firings are records in the
Entity's own change log (`format_id = 3` timer-ops records, or the `timers` field of an
effect record); a restore rebuilds the pending set by the same walk that folds Changes, and
a compaction re-states the full set after the snapshot seq so truncation cannot eat it
([`entity_timers.md`](entity_timers.md)).

### The flusher lives in the core

`flz_core` owns one flush thread per process, started lazily on the first registration of a
non-`NoChange` Procedure. Every linger tick (`FLEZHA_FLUSH_LINGER_MS`, default 5) it drains
each registered Entity's buffer, coalesces, stamps the 16-byte header and ships the frames.
Rust pushes typed Changes into the buffer directly; Java has no flusher of its own — its
Kryo-encoded change bytes ride the activation result into the core, so a long invocation (a
GC pause included) delays its records: the durability bound is a guideline, not a hard cap.
With persistence off the drain still runs (buffer emptied, seq advanced) but nothing is sent.
The Elixir SDK has no flusher: its Entity process ships each dispatch's records itself, at
the end of the dispatch and before the dispatch's outputs, and advances the seq silently when
the class's persistence is off. Under the effectively-once tier it ships one effect record in
their place, carrying the outputs rather than preceding them.

### Persistence toggle

| Surface | Rust | Java | Elixir |
|---|---|---|---|
| Stream class toggle | `StreamOperative::PERSISTENCE_ENABLED` const, default `true`; frozen on the instance at start, a change applies on the next stop + start | `StreamOperative.persistenceEnabled()` default-method exists but the runtime does not read it; the host declaration ships `persistent = true` iff some Procedure declares a Change other than `NoChange`, so the Java opt-out is per-Procedure via `NoChange` | `StreamOperative.persistence_enabled/0`, default `true`, read when an Entity starts; a Procedure without `apply_change/2` is `NoChange` |
| Runner override | `StreamInstance::new` (`flz_core/src/operative/instance.rs`, `operator_override.unwrap_or(class_const)`) accepts an `operative_persistence.<OperativeClass>` override, but the runner's `start_operative` passes none — the class const decides | same core seam, same unfed override | none — the class callback decides |
| Workflow class toggle | `WorkflowOperative::PERSISTENCE_ENABLED`, default `true` | `WorkflowOperative.persistenceEnabled()`, default `true` | the workflow kind is deferred |
| Controller side | `:operative_persistence` (`%{class => bool}`) resolves `persistent_state?` for workflow classes; a stream class is always durable on the controller side | same | same |

Durable-vs-opted-out workflow guarantees: [`../resilience/workflow_operative.md`](../resilience/workflow_operative.md).

### Restoration flow (warm restart)

On a rebind the controller ships `RestoreInitState { entity_id, snapshot_bytes,
change_log_bytes, terminal_seq, has_compensation_intent, has_compensation_done }`:
`snapshot_bytes` is the raw encoded `State` (the controller strips the snapshot file's
header before shipping), `change_log_bytes` the WAL tail in the controller's on-disk record
framing. Under the Entity's exclusivity gate the runtime calls the author's
`init_state(entity_key, Some(payload))` / `initState(entityKey, Optional.of(payload))` (the
author may inspect byte sizes but never decodes them), replaces the state with the decoded
snapshot when present, folds every record in seq order (plain Changes, the Changes enclosed
in effect records, timer ops into the pending-timer set), re-arms the change seq to
`terminal_seq + 1` (terminal = highest folded seq, never below the command's
`terminal_seq`) and answers `RestoreInitStateResponse { entity_id, terminal_seq }`. It also
discards any open checkpoint interval, half-filled replay slice and queued inputs (the
controller re-forwards them from the durable input log), restores the consumed-offset floor
and event-time high-water from the effect records, and re-arms processing-time timers.

The Elixir SDK folds in its own core (`Flezha.SDK.Record.fold/6`), in the Entity's mailbox
order: the on-disk framing's CRC over header and body, the language before the format before
the schema with the core's refusal wordings, then `apply_change/2` per record; one walk with
an arm per format, so an effect record is unwrapped in place — its enclosed change bodies
fold under the enclosing record's version decision, its timer ops join the pending set, and
its consumed offset and event time become the Entity's floor and high-water — while a
snapshot header carrying that format is malformed. A refused fold leaves the Entity as it
was and the controller parks the operative.
Rust folds in-process with postcard and verifies each record's header (`sdk_lang`,
`schema_version`, `format_id`) and trailing CRC32, failing loud with
`RestoreError::{LangMismatch, SchemaMismatch, HeaderInvalid, CrcMismatch, SnapshotDecode,
ChangeDecode}`. Java folds through a `RestoreFold` activation: the core splits the log on
`format_id`, folds timer ops and cursors itself and hands the host only the snapshot and
change bodies to Kryo-decode and `applyChange`; the host fold performs no header or CRC
check. A restore naming an Entity with no live slot validates the log and acks the terminal
seq; a dynamic Entity the controller has state for is materialised so it survives a rebind;
an evicted Entity is never restored — eviction deletes its files.

### Compaction (`CompactEntity`)

Compaction is controller-driven and real on all three SDKs (the Elixir Entity has nothing
pending when it answers — its records ship at dispatch end — so its snapshot is the state at
`change_seq - 1`, framed behind the records on the same stream). On `CompactEntity { entity_id }` the
core, under the Entity's exclusivity gate, drains the pending flush buffer first
(`flush_one`) so in-memory state equals the shipped WAL, then reads
`snapshot_seq = change_seq - 1` — the highest seq the snapshot already folds. The controller
truncates records with `seq > snapshot_seq` and seeds its baseline there, so stamping
`change_seq` (the next seq to assign) would dedup-drop the next real record. The
pending-timer set is re-stated as a fresh timer-ops record after the seq is read, landing at
`snapshot_seq + 1` where truncation keeps it. `State` is serialised with the change codec —
postcard (Rust, in-process) or Kryo (Java, via a `SnapshotRequest` activation), never the
JSON migration serializer, or restore cannot decode it — shipped as an
`entity_state_snapshot` data frame, and acknowledged with
`CompactEntityResponse { entity_id, snapshot_seq, snapshot_bytes_size }`. An `entity_id`
matching no live Entity is answered as an error with `{0, 0}`; the controller treats
`{0, 0}` against a non-empty WAL as a failed handshake. Size/age triggers, timeout, backoff
and give-up are the controller's.

### Wire payload ids and the record header

| Channel | Name | Carries |
|---|---|---|
| data, SDK → controller | `entity_state_change` | one change record (`format_id` 1 postcard / 4 Kryo / 7 ETF) or timer-ops record (3) |
| data, SDK → controller | `entity_effect_record` | effectively-once effect record (`format_id` 2: changes — enclosed in the writing SDK's own codec, postcard / Kryo / ETF — outputs, consumed offset, timers, dead-letter entry) |
| data, SDK → controller | `entity_state_snapshot` | compaction snapshot body |
| data, SDK → controller | `delete_entity_state` | dynamic-Entity eviction |
| data, SDK → controller | `park_payload`, `compensation_intent`, `compensation_done` | DLQ park; workflow saga markers |
| control, controller → SDK | `RestoreInitState` / `RestoreInitStateResponse` | warm restart (reply from the SDK) |
| control, controller → SDK | `CompactEntity` / `CompactEntityResponse` | compaction (reply from the SDK) |
| control, controller → SDK | `EntityMailboxStatus` | legacy `full` / `open` back-pressure (pre-credit sessions) |

Every durable record carries a 16-byte big-endian header stamped by the core's
`HeaderTemplate` — by `Flezha.SDK.Record.header/2` in the Elixir SDK; effect records and
timer-ops records use `format_id` 2 and 3 on the SDKs that write them, and every SDK's
restore rejects any `sdk_lang` but its own before it reads the format byte:

| Bytes | Field | Rust | Java | Elixir |
|---|---|---|---|---|
| 0 | `sdk_lang` | 1 | 2 | 3 |
| 1..5 | `sdk_version` | `(major << 16) \| (minor << 8) \| patch` of the core crate version | same (stamped by the core) | the same packing of the `flezha` application version |
| 5..9 | `schema_version` | the Procedure's `SCHEMA_VERSION` | the class-wide maximum of its Procedures' `schemaVersion()` | the Procedure's `schema_version/0` |
| 9 | `format_id` | 1 postcard | 4 Kryo | 7 External Term Format |
| 10..12 | reserved | 0 | 0 | 0 |
| 12..16 | `crc32` | over the body | over the body | over the body |

### Per-Procedure knobs

| Knob | Rust | Java | Elixir | Default |
|---|---|---|---|---|
| Schema version — bump on a non-additive Change-schema edit; a restore refuses a version above the running one or below the readable floor by name | `Procedure::SCHEMA_VERSION` | `Procedure.schemaVersion()` | `schema_version/0`, with `min_readable_schema_version/0` the floor | 1 |
| Per-Entity WAL size at which the controller solicits a snapshot; `flz_core::persistence::DEFAULT_COMPACT_AFTER_BYTES`, in lockstep with the controller's `:default_compact_after_bytes`; every SDK ships the smallest value across the class's Procedures in `AddOperative` metadata (`compact_after_bytes`) only when it differs from the default | `Procedure::compact_after_bytes()` | `Procedure.compactAfterBytes()` | `compact_after_bytes/0` | 8 MiB |
| Pending-timer cap; past it registration returns / throws `TimerCapExceeded` | `Procedure::max_timers_per_entity()` | `Procedure.maxTimersPerEntity()` | `max_timers_per_entity/0` | 10 000 |
| Flusher linger between drains | `FLEZHA_FLUSH_LINGER_MS` env (read once by the core) | same | none — records ship at dispatch end | 5 ms |

## Rust SDK

### Trait surface

Everything an author needs is re-exported from `flezha_runner::state`.

```rust
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{Change, RestoreInitPayload, StateHandle, WallClock};

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct CounterState { value: i64 }
#[derive(serde::Serialize, serde::Deserialize)]
struct CounterChange { delta: i64 }
impl Change for CounterChange {}   // override `coalesce_key` to opt into coalescing

#[derive(Default)]
struct CounterProcedure;
impl Procedure for CounterProcedure {
    type Input = i64;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;   // `NoChange` opts out
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "counter_procedure" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> CounterState { CounterState::default() }
    fn apply_change(s: &mut CounterState, c: &CounterChange) { s.value = s.value.saturating_add(c.delta); }
    fn run(&mut self, input: i64, mut state: StateHandle<'_, Self>, _ctx: &mut ProcedureCtx<'_>)
        -> Result<(), ProcedureError> {
        state.apply(CounterChange { delta: input });
        Ok(())
    }
}
```

Stable Rust has no associated-type defaults, so every impl writes `type Change` and
`type Clock` explicitly. `Change` is `Serialize + DeserializeOwned + Send + Sync + 'static`;
`State` is `Serialize + DeserializeOwned` so the runtime can also serialise it for
`EntityState` / migration. `EventTimeFrom<F>` requires `F: TimeExtractor<Input>` — an
unservable clock is a compile error. A non-`NoChange` Change without an `apply_change`
override panics at the first fold with a message naming the fix. `StateHandle<'_, S>`
implements `Deref<Target = S::State>` and one mutation method, `apply(change)`; it
deliberately does not implement `DerefMut`, so a direct field write does not compile — the
structural form of the "no forgotten Change" invariant. The buffer behind the handle is
shared so an emit can drain just-applied Changes onto the durable path before the matching
output frame (flush-before-emit).

### ProcedureTestHarness

Gated by `cfg(any(test, feature = "testing"))`; downstream crates enable
`flezha_runner = { ..., features = ["testing"] }`.

```rust
let mut harness = flezha_runner::testing::ProcedureTestHarness::<CounterProcedure>::new();
harness.apply_inputs(vec![1i64, 2, 3, 4]);
assert_eq!(harness.state().value, 10);
harness.replay_equals().expect("deterministic replay");
harness.replay_emits_equal(&[1, 2, 3, 4]).expect("deterministic emits");
```

| Group | API |
|---|---|
| Construct | `new()` (`init_state(None, None)`); `with_initial_state(state)`; `with_payload(key, payload) -> Result<Self, RestoreError>` — runs the real snapshot + change-log fold |
| Configure | `with_keyed_channel()`, `with_entity_key(key)`, `with_wall_clock(ms)`, `with_wall_clock_script(vec)` |
| Drive | `apply_inputs(iter)` — the declared Clock is evaluated as the dispatcher does; a `run` error panics the test. Timers: `fire_timer(kind, at)`, `advance_event_time(high_water)`, `pending_timers(kind)`, `recorded_timer_ops()` |
| Assert | `expect_changes`, `expect_emits::<C>`, `expect_state`, `recorded_changes()`, `emitted_meta()`, `state()`, static `replay(initial, log)` |
| Gates | `replay_equals()` folds the recorded Changes and timer ops against the captured initial state and compares canonical JSON of state and pending timers (map key order is not significant; `Vec` order is); `Err(ReplayMismatch { expected_serialized, actual_serialized })` renders a hex dump. `replay_emits_equal(inputs)`: two passes from the same baseline must produce byte-identical emits and metadata (`flz.*` stamps in keyed mode) |

## Java SDK

### Procedure shape

```java
public static final class Counter { public long count; }
public record Increment(long atMs) implements Change {}

public static final class CountProcedure
        implements Procedure<String, Counter, String, Increment, WallClock, Void> {
    @Override public String name() { return "count"; }
    /** Stable Kryo ids, positional. Append only. */
    @Override public List<Class<?>> kryoTypes() { return List.of(Increment.class, Counter.class); }
    @Override public void applyChange(Counter state, Increment change) { state.count++; }
    @Override public Counter initState(Optional<String> key, Optional<RestoreInitPayload> payload) { return new Counter(); }
    @Override public void run(String input, StateHandle<Counter, Increment> state, ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(new Increment(ctx.now()));
        ctx.emit("count=" + state.get().count);
    }
}
```

`Procedure<Input, State, Output, ChangeT, ClockT, ConfigT>` carries six type parameters
because erasure leaves no associated types to infer. `inputType()`, `outputType()`,
`changeType()`, `clockType()` and `configType()` default to reading the `implements` clause
(or a concrete subclass of a generic intermediate); an argument still generic at the
concrete class must be overridden or registration fails naming the class and parameter. A
Procedure needs a public no-arg constructor. A non-`NoChange` Change without an
`applyChange` override throws `UnsupportedOperationException` at the first fold.
`Optional<RestoreInitPayload>` allocates once per Entity restart where Rust borrows;
negligible.

### Kryo registration

Each Procedure gets its own `KryoCodec` built from its descriptor's `kryoTypes()`;
`kryo.setRegistrationRequired(true)` makes an unregistered type fail loud at first
serialisation. Built-in registrations (`NoChange`, `RestoreInitPayload`, `CoalesceKey`,
primitive arrays, boxed numbers, `String`, `Optional`, the common `java.util` collections,
`Instant`, `ArrayDeque`) take the low ids; user types follow in `kryoTypes()` order, which
defaults to `[changeType, State]` (empty for a `NoChange` Procedure). The list is
append-only: a new Change variant goes at the end so earlier ids survive across versions;
reordering breaks every persisted record. Wiring may add a type only it knows (a
`routeLatest` document class) to the descriptor's copy of the list.

### StateHandle

`StateHandle<State, ChangeT>` replaces the raw state parameter on `run`: reads go through
`get()`, the journaled mutation path is `apply(change)`. Enforcement is by convention, not
structure: `get()` returns the live object, so `state.get().field = x` compiles and runs but
is never journaled. Such a write survives graceful shutdown and live migration (both ship
the live object) and is lost by every restore, which rebuilds from snapshot + change log —
it disappears exactly under an unplanned crash. Java adds no runtime guard (a defensive
copy or proxy on every `get()` would tax every read); the detection offered instead is the
opt-in `ProcedureTestHarness.replayEquals()`, run in CI (`HarnessBehaviourTest` is the
reference case).

### Change, Clock and registration refusals

`Change` is a marker interface with `Optional<CoalesceKey> coalesceKey()` (default empty);
`CoalesceKey.fromHash(Object...)` hashes arbitrary values. `NoChange` cannot be
instantiated. `Clock` permits `WallClock`, `LogicalClock` and `EventTimeFrom`; all three
are wired. What Rust rejects at compile time is rejected at registration here
(`StreamOperativeConfigurationException`): an `EventTimeFrom` Procedure that does not
override `eventTime(Input)` (`eventTimeExtractorMissing`), and a `LogicalClock` Procedure
with `Change = NoChange` (`unservableClock`).

### Test harness

`flezha-sdk-testing` ships `ProcedureTestHarness<Input, State, Output, ChangeT>`:

```java
assertThat(ProcedureTestHarness.of(new CountProcedure()).withWallClock(t0)
    .applyInputs(List.of("a", "b"))
    .expectChanges(List.of(new Increment(t0), new Increment(t0)))
    .replayEquals()).isEmpty();
```

| Group | API |
|---|---|
| Construct | `of(procedure)` (`initState(empty, empty)`); `withInitialState(state)`; `withPayload(entityKey, payload)` — only hands the payload to `initState`; unlike Rust's `with_payload` it does not run the runtime's snapshot + change-log fold |
| Configure | `withKeyedChannel()`, `withEntityKey(key)`, `withWallClock(ms)`, `withWallClockScript(ms...)` |
| Drive / assert | `applyInputs(iterable)`; `expectChanges`, `expectEmits`, `expectState`, `recordedChanges()`, `recordedEmits()`, `emitRecords()`, `state()`, `replay(initial, log)`. No timer methods — Java timer coverage lives in `ActivationDispatcherTest` |
| Gates | `replayEquals()` returns `Optional.empty()` when the Kryo bytes of the live state equal those of the replayed state (vacuously for a `NoChange` Procedure), else `ReplayMismatch(expectedBytes, actualBytes)` whose `toString()` is a hex dump; it compares state only. `replayEmitsEqual(inputs)` returns an empty `Optional` when a replay produces the same emit sequence, else a diagnostic string |

## Elixir SDK

The authoring surface is the behaviour pair: `apply_change/2` on a `Procedure` declares it
changeful, `Flezha.SDK.Ctx.apply/3` is the recorded mutation path, `state_from_json/1` turns
a JSON object read back from the controller into the state term, `compact_after_bytes/0` is
the compaction threshold, and `StreamOperative.persistence_enabled/0` is the class toggle;
`schema_version/0` and `min_readable_schema_version/0` declare the version and the readable
floor, and `migrate_state/2` / `migrate_change/2` read an older version's DECODED term into
the running one — the body codec is the language's own, so there is no second codec to choose.
The class-level `Flezha.SDK.Channels.effectively_once/1` puts the class on the
effectively-once tier, and `checkpoint_ack/1` / `checkpoint_every/2` /
`outputs_at_checkpoint/2` each turn the checkpoint granularity on: the Entity then ships one
`entity_effect_record` per effectful invocation or per closed interval in place of change
records, and `Flezha.SDK.Ctx.checkpoint/1` closes an interval from inside `run/3`.
The record, the fold, the slot, the commands and the shutdown are described in
[`elixir_sdk.md`](elixir_sdk.md#persistence), the tier in
[`elixir_sdk.md`](elixir_sdk.md#effectively-once-tier); the tests are `test/flezha/sdk/persistence_test.exs`
and `commands_test.exs` over the fixtures in `test/helpers/sdk_persistence_fixtures.ex`.
There is no replay harness yet: the contract that `run/3`'s state equals the fold of its
recorded changes is stated, not machine-checked.

## See also

- [`protocol_spec.md`](protocol_spec.md#11-per-entity-state-persistence) — frame layouts for every payload id and control command
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — controller WAL, snapshots, retention, compaction triggers, live read
- [`poll_core.md`](poll_core.md) — the activation model the Java change bytes ride
- [`entity_timers.md`](entity_timers.md) — timers as folded state
- [`authoring_contract.md`](authoring_contract.md) — tiers, derived Change types, determinism
- [`../resilience/workflow_operative.md`](../resilience/workflow_operative.md) — durable-by-default workflow classes
