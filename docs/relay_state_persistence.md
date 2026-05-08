# Rust SDK Relay State Persistence

This doc covers the state-persistence trait surface added by the
`relay-state-persistence-rust` change. The controller-side WAL writer,
fsync batcher, snapshot lifecycle, retention sweeper, and time-travel
CLI ship in `relay-state-persistence-controller`; this doc describes the
SDK-side contract and what runs where today.

For the architecture rationale and the eight Tier-1 commitments that
this change implements, see the umbrella in
`openspec/changes/relay-state-persistence/`.

## Concepts

- **Change** — one logical state-mutation record. Implementors describe a
  semantic edit ("`delta: i64`", "`Push(input)`"), declare it as
  `Shard::Change`, and the runtime persists it via the per-Partition
  flusher. Default is `NoChange` (uninhabited) which opts the Shard out
  of persistence machinery entirely.

- **`state.apply(change)`** — the only mutation chokepoint. Replaces
  `state.field = value` and `state.field.push(item)` in v1 Shards.
  Routes the mutation through `Shard::apply_change` (the user's
  deterministic fold) and pushes the Change onto the per-Partition
  flush buffer for the per-process flusher to drain. Hot path is
  exactly one `Shard::apply_change` call plus one `Vec::push`.

- **`apply_change`** — the deterministic fold. MUST be pure: same
  state + same Change ⇒ same resulting state. Determinism is convention-
  enforced; the type system does not prevent
  `state.seed = SystemTime::now().subsec_nanos()`. Tests catch
  divergence via `ShardTestHarness::replay_equals`.

- **`coalesce_key`** — opt-in batching. Records sharing a non-`None`
  key within one linger window collapse to the most recent record.
  Records returning `None` are retained verbatim.

- **`Relay::PERSISTENCE_ENABLED`** — class-level toggle. Default `true`.
  Executor override via relay-start metadata key
  `relay_persistence.<RelayClass>` wins. Frozen at instance start;
  changes only take effect on the next `stop_relay` + `start_relay`
  cycle.

- **`PartitionFlusher`** — single OS thread per process, lazy-
  initialized on first non-`NoChange` registration. Drains every
  registered Partition's buffer on the linger cadence
  (`FLEZHA_FLUSH_LINGER_MS`, default 5 ms). Toggle-aware: when
  `persistence_enabled == false`, drains-and-discards.

## Trait surface

```rust
use flezha_executor::shard::{Shard, ShardCtx, ShardError};
use flezha_executor::state::{
    Change, NoChange, RestoreInitPayload, StateHandle, WallClock,
};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
struct CounterState { value: i64 }

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct CounterChange { delta: i64 }
impl Change for CounterChange {}

#[derive(Default)]
struct CounterShard;
impl Shard for CounterShard {
    type Input = i64;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str { "counter_shard" }

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
        _ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
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
use flezha_executor::state::{Change, CoalesceKey};

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
struct MarketRelay;
impl Relay for MarketRelay {
    fn factory_id() -> &'static str { "market" }
    fn channels(c: &mut ChannelBuilder) { /* ... */ }
    const PERSISTENCE_ENABLED: bool = false;
}
```

When `PERSISTENCE_ENABLED == false` (or the executor override sets it
to `false` at start time), the per-Partition flusher still drains the
buffer on each linger tick — the buffer is cleared, coalescing runs,
seq counters advance — but `send_frame` is never invoked. Tests can
assert the wire stub was not called.

## ShardTestHarness

```rust
use flezha_executor::testing::ShardTestHarness;

#[test]
fn counter_replays_deterministically() {
    let mut harness = ShardTestHarness::<CounterShard>::new();
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

`ShardTestHarness` is gated behind `cfg(any(test, feature = "testing"))`.
Downstream crates that want the harness in their non-test code paths
add `flezha_executor = { ..., features = ["testing"] }`.

## Restoration flow (warm restart)

When a Partition reconnects with persisted state, the runtime invokes
`Shard::init_state(shard_key, Some(payload))`. The user MAY inspect
`payload.snapshot_bytes` and `payload.change_log_bytes` for
instrumentation but does not have to decode them. After `init_state`
returns, the runtime:

1. Decodes `payload.snapshot_bytes` (16-byte header + postcard-encoded
   `S::State`), validates the header, and replaces the state with the
   decoded snapshot.
2. Iterates `payload.change_log_bytes` records, validates each header
   + CRC, deserializes the Change body, and folds via
   `Shard::apply_change`.

The end state is identical to the controller-side state at the time of the
last persisted record (modulo CRC failures, which fail loud).

## Wire format

Per record (16-byte header per umbrella spec; full frame per
`rust-sdk-dispatch` delta):

```
[partition_id:    u32 BE]
[seq:             u64 BE]
[header:          16 B  ]   <- HeaderTemplate::stamp output
[change_bytes_len:u32 BE]
[change_bytes        ...]
[crc32:           u32 BE]
```

The 16-byte header carries `sdk_lang = 1` (Rust), the SDK version, the
Shard's `SCHEMA_VERSION`, the `format_id = 1` (postcard), and a CRC32
over the change bytes. The trailing CRC32 is the same value, repeated
on the wire so the controller's WAL reader can validate framing before
decoding the header.

## What ships where

| Concern | This change | `relay-state-persistence-controller` |
|---|---|---|
| `Change` trait, `apply_change`, `StateHandle` | ✅ | — |
| Per-Partition flusher (single OS thread) | ✅ | — |
| Wire-frame builder | ✅ (stub `send_frame`) | replaces with NIF |
| `ShardTestHarness::<S>::replay_equals` | ✅ | — |
| 16-byte header, CRC32 validation | ✅ | — |
| `Relay::PERSISTENCE_ENABLED` resolution | ✅ | executor override wired |
| Per-Shard `compact_after_bytes` reported to controller | ✅ | consumes value |
| Controller-side WAL writer + fsync batcher | — | ✅ |
| Snapshot lifecycle + retention | — | ✅ |
| Time-travel CLI | — | ✅ |

Until both changes merge, the SDK is correct in isolation: drained
frames are tracing-logged and discarded by the stub. End-to-end
durability lands when the controller change wires the real `send_frame`.

## Tunables

- `FLEZHA_FLUSH_LINGER_MS` — flusher linger between drains. Default 5.
- `Shard::compact_after_bytes()` — per-Partition WAL size threshold
  reported to the controller at relay start. Default 64 MiB.
- `Shard::SCHEMA_VERSION` — bump on non-additive Change-schema edits;
  the runtime fails loud on mismatch (registered migrators are
  reserved by the umbrella but not implemented in this change).

## See also

- Umbrella spec: `openspec/changes/relay-state-persistence/`
- Rust trait deltas: `openspec/changes/relay-state-persistence-rust/`
- Wire-format hex: `docs/sdk_protocol_spec.md`
- Bench baselines: `docs/benchmarking.md`
