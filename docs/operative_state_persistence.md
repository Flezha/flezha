# Rust SDK Operative State Persistence

This doc covers the state-persistence trait surface a Rust author writes
against. The controller-side WAL writer, fsync batcher, snapshot lifecycle,
retention sweeper and time-travel CLI are described in
`docs/resilience/state_persistence.md`; the runner-side flush cadence, effect
record and restore fold live in the shared runner core
(`docs/sdk_poll_core.md`).

For the architecture rationale and the Tier-1 commitments behind it, see
`openspec/specs/operative-state-persistence/`.

## Concepts

- **Change** — one logical state-mutation record. Implementors describe a
  semantic edit ("`delta: i64`", "`Push(input)`"), declare it as
  `Procedure::Change`, and the runtime persists it via the per-Entity
  flusher. Default is `NoChange` (uninhabited) which opts the Procedure out
  of persistence machinery entirely.

- **`state.apply(change)`** — the only mutation chokepoint. Replaces
  `state.field = value` and `state.field.push(item)` in v1 Procedures.
  Routes the mutation through `Procedure::apply_change` (the user's
  deterministic fold) and pushes the Change onto the per-Entity
  flush buffer for the per-process flusher to drain. Hot path is
  exactly one `Procedure::apply_change` call plus one `Vec::push`.

- **`apply_change`** — the deterministic fold. MUST be pure: same
  state + same Change ⇒ same resulting state. Determinism is convention-
  enforced; the type system does not prevent
  `state.seed = SystemTime::now().subsec_nanos()`. Tests catch
  divergence via `ProcedureTestHarness::replay_equals`.

- **`coalesce_key`** — opt-in batching. Records sharing a non-`None`
  key within one linger window collapse to the most recent record.
  Records returning `None` are retained verbatim.

- **`UnboundedOperative::PERSISTENCE_ENABLED`** — class-level toggle.
  Default `true`. Runner override via operative-start metadata key
  `operative_persistence.<OperativeClass>` wins. Frozen at instance start;
  changes only take effect on the next `stop_operative` + `start_operative`
  cycle.

- **`EntityFlusher`** — the runner core's flush thread (`flz_core`), single
  per process, lazy-initialized on first non-`NoChange` registration. Drains
  every registered Entity's buffer on the linger cadence
  (`FLEZHA_FLUSH_LINGER_MS`, default 5 ms). Toggle-aware: when
  `persistence_enabled == false`, drains-and-discards.

## Trait surface

```rust
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{
    Change, NoChange, RestoreInitPayload, StateHandle, WallClock,
};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
struct CounterState { value: i64 }

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct CounterChange { delta: i64 }
impl Change for CounterChange {}

#[derive(Default)]
struct CounterProcedure;
impl Procedure for CounterProcedure {
    type Input = i64;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str { "counter_procedure" }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }

    fn apply_change(state: &mut CounterState, change: &CounterChange) {
        state.value = state.value.saturating_add(change.delta);
    }

    fn run(
        &mut self,
        input: i64,
        mut state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(CounterChange { delta: input });
        Ok(())
    }
}
```

`StateHandle<'_, S>` implements `Deref<Target = S::State>` for read
access and exposes one mutation method `apply(change: S::Change)`. It
intentionally does NOT implement `DerefMut` — direct field mutation is
structurally impossible. This is the umbrella's "no forgotten Change"
invariant: every state mutation flows through `apply_change` and is
recorded for replay.

### Coalescing example

```rust
use flezha_runner::state::{Change, CoalesceKey};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct TickChange { interval: u8, ts: u64, delta: i64 }
impl Change for TickChange {
    fn coalesce_key(&self) -> Option<CoalesceKey> {
        Some(CoalesceKey::from_hash(&(self.interval, self.ts)))
    }
}
```

Inside the linger window, 50 `TickChange { interval: 1, ts: 100, .. }`
records collapse to the single 50th record at flusher-drain time.
`coalesce_key() == None` records are retained verbatim regardless.

## Persistence toggle

```rust
struct MarketOperative;
impl UnboundedOperative for MarketOperative {
    fn factory_id() -> &'static str { "market" }
    fn channels(c: &ChannelBuilder) { /* ... */ }
    const PERSISTENCE_ENABLED: bool = false;
}
```

When `PERSISTENCE_ENABLED == false` (or the runner override sets it
to `false` at start time), the flusher still drains the buffer on each
linger tick — the buffer is cleared, coalescing runs, seq counters
advance — but nothing is written to the state sink.

## ProcedureTestHarness

```rust
use flezha_runner::testing::ProcedureTestHarness;

#[test]
fn counter_replays_deterministically() {
    let mut harness = ProcedureTestHarness::<CounterProcedure>::new();
    harness.apply_inputs(vec![1i64, 2, 3, 4]);
    assert_eq!(harness.state().value, 10);
    harness.replay_equals().expect("deterministic replay");
}
```

`replay_equals` is the determinism gate: it folds the recorded change
log against a re-constructed initial state and asserts byte-equality
with the harness's terminal state via `serde::Serialize` round-trip.
Returns `Err(ReplayMismatch { expected_serialized, actual_serialized })`
on divergence; `Display` renders a hex-diff for visual inspection.

`ProcedureTestHarness` is gated behind `cfg(any(test, feature = "testing"))`.
Downstream crates that want the harness in their non-test code paths
add `flezha_runner = { ..., features = ["testing"] }`.

## Restoration flow (warm restart)

When an Entity reconnects with persisted state, the runtime invokes
`Procedure::init_state(entity_key, Some(payload))`. The user MAY inspect
`payload.snapshot_bytes` and `payload.change_log_bytes` for
instrumentation but does not have to decode them. After `init_state`
returns, the runtime:

1. Decodes `payload.snapshot_bytes` (16-byte header + postcard-encoded
   `S::State`), validates the header, and replaces the state with the
   decoded snapshot.
2. Iterates `payload.change_log_bytes` records, validates each header
   + CRC, deserializes the Change body, and folds via
   `Procedure::apply_change`.

The end state is identical to the controller-side state at the time of the
last persisted record (modulo CRC failures, which fail loud).

## Wire format

Per record — the authoritative encoder is `flz_core/src/flusher.rs`, and the
full framing is in `docs/sdk_protocol_spec.md`:

```
[entity_id:      u32 BE]
[seq:             u64 BE]
[header:          16 B  ]   <- HeaderTemplate::stamp output
[change_bytes_len:u32 BE]
[change_bytes        ...]
[crc32:           u32 BE]
```

The 16-byte header carries `sdk_lang = 1` (Rust), the SDK version, the
Procedure's `SCHEMA_VERSION`, the `format_id = 1` (postcard), and a CRC32
over the change bytes. The trailing CRC32 is the same value, repeated
on the wire so the controller's WAL reader can validate framing before
decoding the header.

## What lives where

| Concern | Owner |
|---|---|
| `Change` trait, `apply_change`, `StateHandle`, `ProcedureTestHarness` | the Rust SDK (`sdk/rust`) |
| `S::Change` / `S::State` serialization (postcard) | the Rust SDK — the core never decodes a change body |
| Flusher, coalescing, effect record, 16-byte header, CRC32 | the runner core (`flz_core`) |
| `UnboundedOperative::PERSISTENCE_ENABLED` resolution | the SDK declares, the runner override at start wins |
| Per-Procedure `compact_after_bytes` reported at bind | declared by the SDK, consumed by the controller |
| WAL writer, fsync batcher, snapshots, retention, time travel | the controller |

## Tunables

- `FLEZHA_FLUSH_LINGER_MS` — flusher linger between drains. Default 5.
- `Procedure::compact_after_bytes()` — per-Entity WAL size threshold
  reported to the controller at operative start. Default 8 MiB, in lockstep
  with the controller's `:default_compact_after_bytes`.
- `Procedure::SCHEMA_VERSION` — bump on non-additive Change-schema edits;
  the runtime fails loud on mismatch. Registered migrators are reserved and
  not implemented.

## See also

- Umbrella spec: `openspec/specs/operative-state-persistence/`
- Inside a runner: `docs/architecture/runner.md`, `docs/sdk_poll_core.md`
- Wire-format hex: `docs/sdk_protocol_spec.md`
- Bench baselines: `docs/benchmarking.md`
