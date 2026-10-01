# SDK State Persistence

For operator authors and SDK implementers: how a Tasklet's per-Cell state becomes
durable in the Rust, Java and Elixir SDKs — the shared model (State, Change, snapshot +
change-log restore, compaction, the persistence toggle, payload ids, the record header), then
each SDK's authoring surface and test harness. The controller's on-disk layout, fsync
batching, retention and the `flz state` CLI are in
[`../internals/state_persistence.md`](../internals/state_persistence.md); frame layouts in
[`protocol_spec.md`](protocol_spec.md#11-per-cell-state-persistence). The Elixir SDK is
its own core and carries the same rules re-expressed in Elixir
([`elixir_sdk.md`](elixir_sdk.md#persistence)); the FFI veneers carry them through the core.

## Shared concepts

### State, Change and the fold

A Tasklet declares a per-Cell `State` and a `Change` type — one logical mutation record
(`delta: i64`, `Push(input)`). The change folds itself with `apply_to(state)`; the
runtime persists the record through the per-Cell flush buffer. Two contracts hold in both
SDKs: `apply_to` MUST be a pure, deterministic fold — replay is the recovery primitive
(crash-resume, retry, rewind, effectively-once), so ambient time, randomness and external
reads inside `run` / `apply_to` violate the contract; time comes from `ctx.now()` (the
declared clock), identifiers from `ctx.stable_id()`, and the harness gate `replay_equals`
/ `replayEquals()` catches a divergence. And `ctx.change(change)` (Java: `ctx.change(change)`)
is the only recorded mutation path: one fold, one push.

`NoChange` (Rust, uninhabited) and a `Void` state (Java) opt a Tasklet out: the runtime
detects it and registers nothing with the flusher. Three clocks serve `ctx.now()`: the wall
clock (epoch ms, read once per invocation), event time (extracted from the input) and the
logical clock (the durable change seq — needs a real Change type). Rust reads them off the
tasklet, a written `event_time` or `logical_clock` on `#[flezha::tasklet]`; Java reads them off the
class: a written `eventTime(input)`, or `@Tasklet.Spec(logicalClock = true)`. `coalesce_key()` (default none) is opt-in batching: Changes sharing a key
within one linger window collapse to the most recent record at drain time; keyless Changes
are kept verbatim — right for "current value of X", wrong for "+1" deltas. The shapes for a
keyed or collection change are in
[`authoring_contract.md`](authoring_contract.md#31-keyed-and-collection-changes).

Timers are folded state: registrations, cancellations and firings are records in the
Cell's own change log (`format_id = 3` timer-ops records, or the `timers` field of an
effect record); a restore rebuilds the pending set by the same walk that folds Changes, and
a compaction re-states the full set after the snapshot seq so truncation cannot eat it
([`cell_timers.md`](cell_timers.md)).

### The flusher lives in the core

`flz_core` owns one flush thread per process, started lazily on the first registration of a
Tasklet that keeps state. Every linger tick (`FLEZHA_FLUSH_LINGER_MS`, default 5) it drains
each registered Cell's buffer, coalesces, stamps the 16-byte header and ships the frames.
Rust pushes typed Changes into the buffer directly; Java has no flusher of its own — its
Kryo-encoded change bytes ride the activation result into the core, so a long invocation (a
GC pause included) delays its records: the durability bound is a guideline, not a hard cap.
With persistence off the drain still runs (buffer emptied, seq advanced) but nothing is sent.
The Elixir SDK has no flusher: its Cell process ships each dispatch's records itself, at
the end of the dispatch and before the dispatch's outputs, and advances the seq silently when
the class's persistence is off. Under the effectively-once tier it ships one effect record in
their place, carrying the outputs rather than preceding them.

### Persistence toggle

One setting per class, `durable`, for a flow operator and a steps operator alike, frozen on
the instance at start (a change applies on the next stop + start). The runner announces it
(`durable` = `memory`, `local` or `remote` in the `AddOperator` response, with `sleep_after_ms`
when the class sleeps), so the controller reads the same value: a class kept in memory
(`durable: false`) keeps no WAL or snapshot, is given no restore on a rebind, and answers a live
Cell read with `{:error, :persistence_off}`. Under `local` and `remote` sits `sleep_after`, how
long an idle instance waits before it sleeps: by default never for a flow class and six hours for a
steps class, `false` never.

| Surface | Rust | Java | Elixir |
|---|---|---|---|
| Where work is kept | `OperatorSettings::durable`: `Durable::Memory`, `Durable::Local` (default) or `Durable::Remote` | `durable()`: `Durable.Memory`, `Durable.Local` (default) or `Durable.Remote` | `durable:` on the `use`: `false`, `:local` (default) or `:remote` |
| Sleep interval | `sleep_after: Option<Duration>` on `Local` / `Remote` | `Optional<Duration> sleepAfter` on `Local` / `Remote` | `{:local, sleep_after: ms \| false}` |
| Controller side | `:operator_persistence` (`%{class => bool}`) overrides the announced setting per class | same | same |

The Node, Python, Ruby and PHP SDKs carry the same tree under their own spellings; each SDK
page lists them.

Durable-vs-opted-out steps operator guarantees: [`../resilience/steps_operator.md`](../resilience/steps_operator.md).

### Restoration flow (warm restart)

On a rebind the controller ships `RestoreInitState { cell_id, snapshot_bytes,
change_log_bytes, terminal_seq, has_compensation_intent, has_compensation_done }`:
`snapshot_bytes` is the raw encoded `State` (the controller strips the snapshot file's
header before shipping), `change_log_bytes` the WAL tail in the controller's on-disk record
framing. Under the Cell's exclusivity gate the runtime calls the author's
`init_state(cell_key)` (a Java cell's state comes from its state class or its written
`initState(cellKey)`), replaces the state with the decoded
snapshot when present, folds every record in seq order (plain Changes, the Changes enclosed
in effect records, timer ops into the pending-timer set), re-arms the change seq to
`terminal_seq + 1` (terminal = highest folded seq, never below the command's
`terminal_seq`) and answers `RestoreInitStateResponse { cell_id, terminal_seq }`. It also
discards any open checkpoint interval, half-filled replay slice and queued inputs (the
controller re-forwards them from the durable input log), restores the consumed-offset floor
and event-time high-water from the effect records, and re-arms processing-time timers.

The Elixir SDK folds in its own core (`Flezha.SDK.Record.fold/6`), in the Cell's mailbox
order: the on-disk framing's CRC over header and body, the language before the format before
the schema with the core's refusal wordings, then `apply_change/2` per record; one walk with
an arm per format, so an effect record is unwrapped in place — its enclosed change bodies
fold under the enclosing record's version decision, its timer ops join the pending set, and
its consumed offset and event time become the Cell's floor and high-water — while a
snapshot header carrying that format is malformed. A refused fold leaves the Cell as it
was and the controller parks the operator.
Rust folds in-process with postcard and verifies each record's header (`sdk_lang`,
`schema_version`, `format_id`) and trailing CRC32, failing loud with
`RestoreError::{LangMismatch, SchemaTooOld, SchemaFromTheFuture, HeaderInvalid,
CrcMismatch, SnapshotDecode, ChangeDecode}`. Java folds through a `RestoreFold`
activation: the core splits the log on `format_id`, folds timer ops and cursors itself and
hands the host only the snapshot and change bodies to Kryo-decode and fold through each
change's `applyTo`; the
host fold performs no header or CRC check. A restore naming a Cell with no live slot
validates the log and acks the terminal seq; a dynamic Cell the controller has state for
is materialised so it survives a rebind; an evicted Cell is never restored — eviction
deletes its files.

### Compaction (`CompactCell`)

Compaction is controller-driven and real on all three SDKs (the Elixir Cell has nothing
pending when it answers — its records ship at dispatch end — so its snapshot is the state at
`change_seq - 1`, framed behind the records on the same stream). On `CompactCell { cell_id }` the
core, under the Cell's exclusivity gate, drains the pending flush buffer first
(`flush_one`) so in-memory state equals the shipped WAL, then reads
`snapshot_seq = change_seq - 1` — the highest seq the snapshot already folds. The controller
truncates records with `seq > snapshot_seq` and seeds its baseline there, so stamping
`change_seq` (the next seq to assign) would dedup-drop the next real record. The
pending-timer set is re-stated as a fresh timer-ops record after the seq is read, landing at
`snapshot_seq + 1` where truncation keeps it. `State` is serialised with the change codec —
postcard (Rust, in-process) or Kryo (Java, via a `SnapshotRequest` activation), never the
JSON migration serializer, or restore cannot decode it — shipped as a
`cell_state_snapshot` data frame, and acknowledged with
`CompactCellResponse { cell_id, snapshot_seq, snapshot_bytes_size }`. A `cell_id`
matching no live Cell is answered as an error with `{0, 0}`; the controller treats
`{0, 0}` against a non-empty WAL as a failed handshake. Size/age triggers, timeout, backoff
and give-up are the controller's.

### Wire payload ids and the record header

| Channel | Name | Carries |
|---|---|---|
| data, SDK → controller | `cell_state_change` | one change record (`format_id` 1 postcard / 4 Kryo / 7 ETF) or timer-ops record (3) |
| data, SDK → controller | `cell_effect_record` | effectively-once effect record (`format_id` 2: changes — enclosed in the writing SDK's own codec, postcard / Kryo / ETF — outputs, consumed offset, timers, dead-letter entry) |
| data, SDK → controller | `cell_state_snapshot` | compaction snapshot body |
| data, SDK → controller | `delete_cell_state` | dynamic-Cell eviction |
| data, SDK → controller | `park_payload`, `compensation_intent`, `compensation_done` | DLQ park; steps saga markers |
| control, controller → SDK | `RestoreInitState` / `RestoreInitStateResponse` | warm restart (reply from the SDK) |
| control, controller → SDK | `CompactCell` / `CompactCellResponse` | compaction (reply from the SDK) |
| control, controller → SDK | `CellMailboxStatus` | legacy `full` / `open` back-pressure (pre-credit sessions) |

Every durable record carries a 16-byte big-endian header stamped by the core's
`HeaderTemplate` — by `Flezha.SDK.Record.header/2` in the Elixir SDK; effect records and
timer-ops records use `format_id` 2 and 3 on the SDKs that write them, and every SDK's
restore rejects any `sdk_lang` but its own before it reads the format byte:

| Bytes | Field | Rust | Java | Elixir |
|---|---|---|---|---|
| 0 | `sdk_lang` | 1 | 2 | 3 |
| 1..5 | `sdk_version` | `(major << 16) \| (minor << 8) \| patch` of the writing SDK's own released version | the same packing of the writing SDK's own released version | the same packing of the writing SDK's own released version |
| 5..9 | `schema_version` | the Tasklet's `SCHEMA_VERSION` | the class-wide maximum of its Tasklets' `schemaVersion` | the Tasklet's `schema_version/0` |
| 9 | `format_id` | 1 postcard | 4 Kryo | 7 External Term Format |
| 10..12 | reserved | 0 | 0 | 0 |
| 12..16 | `crc32` | over the body | over the body | over the body |

### Per-Tasklet knobs

| Knob | Rust | Java | Elixir | Default |
|---|---|---|---|---|
| Schema version — bump on a non-additive Change-schema edit; a restore refuses a version above the running one or below the readable floor by name | `Tasklet::SCHEMA_VERSION` | `@Tasklet.Spec(schemaVersion = n)` | `schema_version/0`, with `min_readable_schema_version/0` the floor | 1 |
| Per-Cell WAL size at which the controller solicits a snapshot; `flz_core::persistence::DEFAULT_COMPACT_AFTER_BYTES`, in lockstep with the controller's `:default_compact_after_bytes`; every SDK ships the smallest value across the class's Tasklets in `AddOperator` metadata (`compact_after_bytes`) only when it differs from the default | `Tasklet::compact_after_bytes()` | `@Tasklet.Spec(compactAfterBytes = n)` | `compact_after_bytes/0` | 8 MiB |
| Pending-timer cap; past it registration returns / throws `runtime.timer_cap_exceeded` | `Tasklet::max_timers_per_cell()` | `@Tasklet.Spec(maxTimersPerCell = n)` | `max_timers_per_cell/0` | 10 000 |
| Flusher linger between drains | `FLEZHA_FLUSH_LINGER_MS` env (read once by the core) | same | none — records ship at dispatch end | 5 ms |

## Rust SDK

### Trait surface

The tasklet is your own type; `#[flezha::tasklet]` completes its `impl Tasklet` block
([`authoring_contract.md`](authoring_contract.md#22-one-form-completed-by-what-it-declares)).

```rust
use flezha_runner::{tasklet, Change, Ctx, Tasklet, TaskletError};

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct CounterState { value: i64 }

#[derive(serde::Serialize, serde::Deserialize)]
struct Added { delta: i64 }

impl Change<CounterState> for Added {   // override `coalesce_key` to opt into coalescing
    fn apply_to(&self, s: &mut CounterState) { s.value = s.value.saturating_add(self.delta); }
}

#[derive(Default)]
struct CounterTasklet;

#[tasklet(name = "counter_tasklet")]
impl Tasklet for CounterTasklet {
    type Input = i64;
    type State = CounterState;
    type Change = Added;   // left out, it is `NoChange` and nothing is journaled

    fn run(&mut self, input: i64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Added { delta: input });
        Ok(())
    }
}
```

`Change<S>` is `Serialize + DeserializeOwned + Send + Sync + 'static`; `State` is
`Serialize + DeserializeOwned` so the runtime can also serialise it for `CellState` /
migration. `ctx.state()` hands out `&State` and `ctx.change(c)` is the one mutation path,
which applies `c.apply_to` and records the change; there is no `&mut State` anywhere, so a
direct field write does not compile — the structural form of the "no forgotten Change"
invariant. The buffer behind the context is shared so an emit can drain just-applied
Changes onto the durable path before the matching output frame (flush-before-emit). A
change enum is encoded by variant index, so a new variant goes at the end.

### TaskletTestHarness

Gated by `cfg(any(test, feature = "testing"))`; downstream crates enable
`flezha_runner = { ..., features = ["testing"] }`.

```rust
let mut harness = flezha_runner::testing::TaskletTestHarness::<CounterTasklet>::new();
harness.apply_inputs(vec![1i64, 2, 3, 4]);
assert_eq!(harness.state().value, 10);
harness.replay_equals().expect("deterministic replay");
harness.replay_emits_equal(&[1, 2, 3, 4]).expect("deterministic emits");
```

| Group | API |
|---|---|
| Construct | `new()` (`init_state(None)`); `with_initial_state(state)`; `with_payload(key, payload) -> Result<Self, RestoreError>` — runs the real snapshot + change-log fold |
| Configure | `with_keyed_channel()`, `with_cell_key(key)`, `with_wall_clock(ms)`, `with_wall_clock_script(vec)` |
| Drive | `apply_inputs(iter)` — the declared clock is evaluated as the dispatcher does; a `run` error panics the test. Timers: `fire_timer(kind, at)`, `advance_event_time(high_water)`, `pending_timers(kind)`, `recorded_timer_ops()` |
| Assert | `expect_changes`, `expect_emits` (typed as the tasklet's `Output`), `expect_state`, `recorded_changes()`, `emitted_meta()`, `state()`, static `replay(initial, log)` |
| Gates | `replay_equals()` folds the recorded Changes (each encoded and decoded as the runtime persists it, so a change that does not survive the round trip fails here) and timer ops against the captured initial state and compares canonical JSON of state and pending timers (map key order is not significant; `Vec` order is); `Err(ReplayMismatch { expected_serialized, actual_serialized })` renders a hex dump. `replay_emits_equal(inputs)`: two passes from the same baseline must produce byte-identical emits and metadata (`flz.*` stamps in keyed mode) |

## Java SDK

### Tasklet shape

```java
@Tasklet.Spec(name = "count")
public static final class CountTasklet implements Tasklet<String, CountTasklet.Counter, String> {

    public static final class Counter { public long count; }

    public record Increment(long atMs) implements Change<Counter> {
        @Override public void applyTo(Counter state) { state.count++; }
    }

    @Override
    public void run(String input, Ctx<Counter, String> ctx) throws TaskletException {
        ctx.change(new Increment(ctx.now()));
        ctx.emit("count=" + ctx.state().count);
    }
}
```

`Tasklet<Input, State, Output>` is the one form. The three types are read off the
`implements` clause (or a concrete subclass of a generic intermediate); an argument still
generic at the concrete class fails registration naming the class and parameter. The name
and settings come from `@Tasklet.Spec`, which every tasklet class carries. A new cell's
state is the state class's no-arg constructor unless the class writes
`initState(Optional<String> cellKey)`; a state class with neither fails registration. A
change is a record implementing `Change<State>`, and its `applyTo` is the only method that
writes a state field. `Void` as the state keeps nothing. A Tasklet needs a public no-arg
constructor. [`java_sdk.md`](java_sdk.md#tasklets) lists the `@Tasklet.Spec` members.

### Kryo registration

Each Tasklet gets its own `KryoCodec`; `kryo.setRegistrationRequired(true)` makes an
unregistered type fail loud at the first flush, as `StateSerializeException.UnregisteredClass`
naming the class and both fixes. Built-in registrations (`RestoreInitPayload`,
`CoalesceKey`, primitive arrays, boxed numbers, `String`, `Optional`, the common
`java.util` collections, `Instant`, `ArrayDeque`) take fixed low ids. By default the user
types are discovered: the state class, every member class (declared in a class body, at any
depth) of the tasklet class, a class above it, or the state class that implements `Change`,
and every non-JDK class their fields reach; a local or anonymous class is not found. Each takes an id
derived from its name, in a band above the built-ins, so adding a change record moves no
other id; two classes deriving one id fail registration naming both. A type nested in the
tasklet class is named by the tasklet's `@Tasklet.Spec` name and its path inside the class,
so renaming or moving the tasklet class keeps every id; any other type is named by its
binary class name. A tasklet that writes `kryoTypes()` owns its ids from the first save:
the list replaces discovery, ids follow list order after the built-ins, and only appending
is safe. Switching an existing tasklet between discovery and a list moves every id, so it
needs a `schemaVersion` bump and `migrateState` / `migrateChange`. Wiring may add a type only it
knows (a `routeLatest` document class) to a node's list.

### State and changes

`ctx.state()` reads the cell's state, and `ctx.change(change)` is the journaled mutation
path: it calls the change's `applyTo` and records it. Enforcement is by convention, not
structure: `ctx.state()` returns the live object, so `ctx.state().field = x` compiles and
runs but is never journaled. Such a write survives graceful shutdown and live migration
(both ship the live object) and is lost by every restore, which rebuilds from snapshot +
change log — it disappears exactly under an unplanned crash. Java adds no runtime guard (a
defensive copy or proxy on every read would tax every read); the detection offered instead
is the opt-in `TaskletTestHarness.replayEquals()`, run in CI (`HarnessBehaviourTest` is the
reference case).

### Change, clock and registration refusals

`Change<S>` declares `applyTo(S)` and `Optional<CoalesceKey> coalesceKey()` (default
empty); `CoalesceKey.fromHash(Object...)` hashes arbitrary values. What Rust rejects at
compile time is rejected at registration here (a `FlezhaException`):
a tasklet that writes `eventTime` and declares `logicalClock = true`, and a logical clock
over a `Void` state (`topology.unservable_clock`).

### Test harness

`flezha-sdk-testing` ships `TaskletTestHarness<Input, State, Output>`, which reads the class
exactly as registration does:

```java
assertThat(TaskletTestHarness.of(new CountTasklet()).withWallClock(t0)
    .applyInputs(List.of("a", "b"))
    .expectChanges(List.of(new Increment(t0), new Increment(t0)))
    .replayEquals()).isEmpty();
```

| Group | API |
|---|---|
| Construct | `of(tasklet)` (the initial-state rule for no cell key); `withInitialState(state)`; `withPayload(cellKey, payload)` — the initial state, then the payload's snapshot, then its change log folded through `applyTo` |
| Configure | `withKeyedChannel()`, `withCellKey(key)`, `withWallClock(ms)`, `withWallClockScript(ms...)`; a tasklet writing `eventTime` reads its time from each input, and a logical-clock one the invocation's change seq |
| Drive / assert | `applyInputs(iterable)`; `expectChanges`, `expectEmits`, `expectState` (Kryo bytes), `recordedChanges()`, `recordedEmits()`, `emitRecords()`, `state()`, `replay(initial, log)`. No timer methods — Java timer coverage lives in `ActivationDispatcherTest` |
| Gates | `replayEquals()` returns `Optional.empty()` when the Kryo bytes of the live state equal those of the replayed state (vacuously for a `Void` state), else `ReplayMismatch(expectedBytes, actualBytes)` whose `toString()` is a hex dump; it compares state only. `replayEmitsEqual(inputs)` returns an empty `Optional` when a replay produces the same emit sequence, else a diagnostic string |

## Elixir SDK

The authoring surface is `use Flezha.SDK.Tasklet` with a `state do field … end` block and
`defchange`: the block generates `state_from_json/1`, which turns a JSON object read back
from the controller into the state term, and each `defchange` generates an `apply_change/2`
clause, which declares the tasklet changeful, and its recorder, journaling through
`Flezha.SDK.Ctx.apply/3`, the recorded mutation path. `compact_after_bytes` is the compaction threshold, and
the `durable:` option of `use Flezha.SDK.Operator` is the class setting;
`schema_version/0` and `min_readable_schema_version/0` declare the version and the readable
floor, and `migrate_state/2` / `migrate_change/2` read an older version's DECODED term into
the running one — the body codec is the language's own, so there is no second codec to choose.
`run: {:flow, exactly: :effectively_once}` puts the class on the effectively-once
tier, and `ack: :checkpoint` (or `{:checkpoint, every: n, outputs: :at_checkpoint,
max_delay_ms: ms}`) under it turns the checkpoint granularity on: the Cell then ships one
`cell_effect_record` per effectful invocation or per closed interval in place of change
records, and `Flezha.SDK.Ctx.checkpoint/1` closes an interval from inside `run/3`.
The record, the fold, the slot, the commands and the shutdown are described in
[`elixir_sdk.md`](elixir_sdk.md#persistence), the tier in
[`elixir_sdk.md`](elixir_sdk.md#effectively-once-tier); the tests are `test/flezha/sdk/persistence_test.exs`
and `commands_test.exs` over the fixtures in `test/helpers/sdk_persistence_fixtures.ex`.
There is no replay harness yet: the contract that `run/3`'s state equals the fold of its
recorded changes is stated, not machine-checked.

## See also

- [`protocol_spec.md`](protocol_spec.md#11-per-cell-state-persistence) — frame layouts for every payload id and control command
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — controller WAL, snapshots, retention, compaction triggers, live read
- [`poll_core.md`](poll_core.md) — the activation model the Java change bytes ride
- [`cell_timers.md`](cell_timers.md) — timers as folded state
- [`authoring_contract.md`](authoring_contract.md) — tiers, derived Change types, determinism
- [`../resilience/steps_operator.md`](../resilience/steps_operator.md) — durable-by-default steps classes
