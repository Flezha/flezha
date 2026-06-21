# Flezha SDK Authoring Contract

**Status:** Normative for every SDK binding (Rust, Java today; Python, Elixir next).

This document is the single source of truth for the *authoring sugar* every
SDK offers on top of the `Relay` / `Shard` / `Change` / `Channel` trait (or
interface) surface. The sugar is **pure expansion**: a macro (Rust) or an
annotation processor (Java) generates exactly the trait impls a user would
write by hand, against the unchanged runtime. There is no parallel runtime
path, no new protobuf message, and no WAL-format change.

The trait surface itself is specified elsewhere (`relay-shard-partition-model`,
`rust-sdk-dispatch`, `java-sdk`, `relay-state-persistence`). This contract
governs only how a sugared declaration maps onto it.

## 1. Escape-hatch equivalence (non-negotiable)

- Generated code is equivalent to a valid hand-written `Shard` / `Relay`
  implementation. It compiles and runs identically; no protocol, controller,
  or WAL change is required for it to run.
- A relay MAY mix sugared shards and hand-written shards wired together.
- "Ejecting" a sugared shard to a full impl is a paste of its expansion — the
  relay behaves identically afterward with no other change.

## 2. Signature → associated types (the inference table)

Inference is driven by the `run` declaration. `S` below is the optional
mutable-state parameter.

| Source in the `run` declaration              | Associated type / item        |
| --------------------------------------------- | ------------------------------ |
| first non-state value param of type `I`       | `type Input = I`               |
| return type `O` (or `Result<O, _>`)           | `type Output = O` (always emit) |
| return type `Option<O>` / `Optional<O>`       | `type Output = O`, emit on present |
| return type unit (`()` / `void`)              | no output channel              |
| a mutable-state param of type `S`             | `type State = S` (stateful)    |
| no mutable-state param                        | `type State = ()`, `Change = NoChange` |
| `State` opts into change derivation           | `type Change = <derived enum>` |
| unspecified                                   | `type Clock = WallClock`       |
| unspecified                                   | `type Config = ()`             |
| `id = "…"` else the declaration name          | `name` / `factory_id`          |

A mutable-state parameter is **the single signal** that a shard is stateful.
Its presence sets `State` and (unless overridden) selects the change type;
its absence means `State = ()` and `Change = NoChange`.

## 3. State → Change derivation, and the manual boundary

Opting a state type into change derivation (`#[derive(flezha::State)]` in Rust,
`@State` in Java) generates:

- a `Change` enum / type with **one variant per field**: `SetField(T)` for
  every field, and additionally `PushField(U)` for any `Vec<U>` / `List<U>`
  field;
- `apply_change` folding each variant into the state;
- field-granular mutators on the state handle (`state.set_field(v)`,
  `state.push_field(v)`) plus the explicit `state.apply(change)` path. Both
  journal the change to the existing WAL.

**The manual boundary.** Only field-granular mutations are auto-derivable.
A mutation that is conditional, spans multiple fields atomically, or is a
computed function of the prior state is **not** expressible as a single
generated variant. In that case the SDK requires a hand-written `Change`
(authored exactly as today) and reports a clear error rather than journaling
incorrectly — it never silently produces wrong journaling. Every field type
must be cloneable/serializable for a `Set`/`Push` variant to be generated; a
field that is not triggers a compile-time diagnostic directing the author to a
hand-written `Change`.

### 3.1 Keyed and collection state journaling

Real compute shards keep their state as a per-key map and mutate a collection
or computed value inside each entry. A map field annotated `#[journal(...)]`
(Rust) / `@Journal(...)` (Java) opts that field into per-key journaling: the
SDK generates named per-key mutators, the matching `Change` variants, and the
`apply_change` arms that replay them. The annotation is **required** on a map
field — an unannotated map field is a compile error (there is no single per-key
op to record), directing the author to a hand-written `Change`. The field type
must match the declared mode, or the SDK reports an error.

| Mode (Rust / Java)                | Field type                  | Generated mutators                                                        | Change variants                                              | `apply_change` |
| --------------------------------- | --------------------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------- | -------------- |
| `map_set` / `MapSet`              | `HashMap<K, V>`             | `set_<field>(key, V)`                                                      | `Set<Field>(K, V)`                                          | `map.insert(key, value)` |
| `map_vec` / `MapVec`              | `HashMap<K, Vec<U>>`        | `push_<field>(key, U)`, `drain_<field>_front(key, n)`, `replace_last_<field>(key, U)` | `Push<Field>(K, U)`, `Drain<Field>Front(K, usize)`, `ReplaceLast<Field>(K, U)` | insert-default-then-op on the keyed vector |
| `map_deque` / `MapDeque`          | `HashMap<K, VecDeque<U>>`   | `push_back_<field>(key, U)`, `pop_front_<field>(key, n)`                   | `PushBack<Field>(K, U)`, `PopFront<Field>(K, usize)`        | insert-default-then-op on the keyed deque |

Java names follow the language's casing (`setByProduct`, `pushCloses`,
`drainClosesFront`, `pushBackWindow`, `popFrontWindow`) and use
`Map<K, V>` / `Map<K, List<U>>` / `Map<K, Deque<U>>`. The collection modes use
**insert-default-then-op** semantics: the keyed collection is created on first
touch, so `push`/`drain`/`pop` never need a guard for an absent key.

**`map_set` is the universal fallback.** When a per-entry value is computed
(an EMA, an RSI, a whole candle), compute it in the logic function and call
`set_<field>(key, value)`; the change records the per-key value and replay
re-applies it by insertion. Prefer `map_vec` / `map_deque` only for **growing
collections** (a rolling window, an appended bar history): journaling the whole
collection each tick via `map_set` would grow every WAL record to the
collection's size, whereas the collection modes keep each record a single delta.
A per-key *struct* that merely contains a collection (e.g. a struct holding both
a deque and a scalar) is not journaled sub-field-incrementally — split it into
parallel map fields, or use `map_set` to record the whole per-key value.

Flat fields and `#[journal(...)]` map fields on the same struct compose into a
single generated `Change` enum and a single `apply_change`.

### 3.2 Emit-or-skip output return

A logic function returning an optional output — `Option<O>` (Rust) /
`Optional<O>` (Java) — emits when present and emits nothing when absent. This
replaces the explicit warmup gate (`if series.len() < n { return } …
ctx.emit(v)`): `None` / `empty` skips, `Some(v)` / present emits. The SDK
generates the emit call, so the author never touches the shard context. The
unit return (`()` / `void`, no output channel) and the unconditional `O`
return (always emit) are unchanged.

### 3.3 Single hand-written path determinism

A shard that mutates state **only** through generated journaling mutators has
exactly one hand-written path — the logic function — and one generated replay
path — the generated `apply_change`. The engine journals the recorded `Change`s
before emit (unchanged flush-before-emit ordering) and, on warm restart,
rebuilds state by folding those same `Change`s through the **generated**
`apply_change`. Because that `apply_change` is generated from the same mutator
definitions the mutators use, it cannot drift from the live path — the
live-vs-replay divergence that `ShardTestHarness::replay_equals` /
`replayEquals()` exists to catch is structurally unrepresentable for sugared
shards. The replay-determinism harness remains the conformance gate for **all**
shards (sugared and hand-written).

A consequence: shards that hand-wrote an idempotency guard for replay safety
(e.g. a `last_open_us` dedup that stops a re-folded input from double-counting)
no longer need it. Journaling an idempotent `set` op means replay re-assigns the
computed value rather than re-folding an input, so removing the guard does not
change replay equality.

## 4. Partitioning mode is an explicit opt-in, never inferred

The mode changes lifecycle and on-disk state layout, so it is always an
explicit author choice — never inferred from the `run` body. Each opt-in maps
1:1 onto the existing trait surface:

| Opt-in                                         | Generated trait surface |
| ---------------------------------------------- | ----------------------- |
| none                                           | single-partition (empty `shard_keys()`) |
| `keys = [..]` + `partition_for` (static)       | `shard_keys()` = keys, `PARTITION_FOR_OVERRIDDEN = true`, `partition_for` delegates |
| `shard_key` (+ `idle_ttl`, `max_partitions`)   | `SHARD_KEY_OVERRIDDEN = true`, `shard_key` delegates, `shard_keys()` empty, TTL / cap overrides |

Static and dynamic opt-ins are **mutually exclusive** — declaring both is a
compile-time error (composite static×dynamic keys are out of scope). The
dynamic opt-ins target the trait surface introduced by
`relay-dynamic-partitioning` (`SHARD_KEY_OVERRIDDEN`, `partition_idle_ttl`,
`max_partitions`).

**Per-partition state init (`init`).** By default a stateful shard's
`init_state` returns `State::default()`. When a partition's starting state is a
function of its key — e.g. a statically-partitioned shard whose window length or
bucket span is encoded in the shard key — declare an `init` opt-in naming a
function `fn(Option<&str>) -> State` (Rust: `#[flezha::shard(init = fn)]`; Java:
an `@InitState State initState(Optional<String> shardKey)` method on the
authoring class). The generated `init_state` delegates to it, passing the shard
key (absent in single-partition mode and the test-harness default). `init`
requires a state parameter — declaring it on a stateless shard is a compile-time
error. The restore payload stays runtime-applied after `init_state` returns, so
the `init` function never sees it.

## 5. Single-shard auto-wiring and registration

Exactly **one** topology may be auto-wired: a single shard from a controller
input to one named sink (`input → shard → plugin(sink)`). A sugared shard that
declares both an `id` and a `sink`, and is the sole shard, generates that relay
and registers it automatically — the user writes no registration call.

- **Multi-shard topology stays explicit.** More than one shard, or any
  fan-out / multi-input edge, requires an explicit topology declaration
  (`channels()` in Rust/Java, or the linear `relay!`/pipeline sugar for a
  strictly linear chain). The SDK refuses (compile error) rather than guessing
  a graph.
- **Auto-registration** uses a compile-time-collected registry (Rust:
  `inventory`; Java: the processor emits the `registerRelay` call). Manual
  `register_relay` / `RelayRegistry.registerRelay` stays valid for
  hand-written relays.
- **Duplicate factory id fails fast.** When auto-registration and manual
  registration would register two relays with the same `factory_id`, startup
  fails with an error naming the colliding id.

## 6. Canonical reference relay (cross-SDK conformance anchor)

Every SDK's conformance test asserts that this one relay — the existing
`StaticPartitionRelay` shape — expands to the same topology and associated-type
mapping. It is a two-shard, statically-partitioned, linear relay.

**Authored (Rust sugar):**

```rust
#[flezha::shard(id = "static_entry_shard", keys = ["a", "b", "c", "d"], partition_for = static_part)]
fn static_entry(input: String, state: &mut StaticEntryState) -> String {
    state.push_seen(input.clone());
    input
}

#[flezha::shard]
fn static_downstream(input: String) -> Vec<u8> {
    input.into_bytes()
}

flezha::relay! {
    id: "static_partition",
    pipeline: input::<String> -> StaticEntry -> StaticDownstream -> plugin::<Vec<u8>>("test_kafka")
}
```

**Expected expanded topology** (identical across SDKs):

| Channel        | Kind          | Payload   | Producer            | Consumer / sink      |
| -------------- | ------------- | --------- | ------------------- | -------------------- |
| `input`        | entry         | `String`  | controller          | `StaticEntry`        |
| (hop)          | inter-shard   | `String`  | `StaticEntry`       | `StaticDownstream`   |
| (sink)         | plugin-bound  | `Vec<u8>` | `StaticDownstream`  | plugin `test_kafka`  |

**Expected associated-type mapping:**

| Shard              | Input    | Output    | State              | Change              | Mode   |
| ------------------ | -------- | --------- | ------------------ | ------------------- | ------ |
| `StaticEntry`      | `String` | `String`  | `StaticEntryState` | derived (`Push`/`Set`) | static (`a`,`b`,`c`,`d`) |
| `StaticDownstream` | `String` | `Vec<u8>` | `()`               | `NoChange`          | single |

`factory_id` = `static_partition`. `Clock = WallClock`, `Config = ()` for both.

### 6.1 Canonical keyed-collection reference shard

Every SDK's conformance test also implements this one shard — a **rolling
per-key window** — and asserts its generated journaling matches a hand-written
twin and survives the replay-determinism harness. It is the reference for the
§3.1 keyed/collection rules and the §3.2 emit-or-skip return.

**Before (hand-written `Shard` + `Change`):** a `Change` enum with
`PushBack(key, value)` / `PopFront(key, n)`, an `apply_change` that folds each
onto the keyed deque, and a `run` that re-derives the same two mutations before
the warmup check and `ctx.emit`.

**After (Rust sugar):**

```rust
#[derive(flezha::State)]
struct RollingWindowState {
    #[journal(map_deque)]
    window: HashMap<String, VecDeque<f64>>,
}

#[flezha::shard(id = "rolling_window")]
fn rolling_window(input: (String, f64), state: &mut RollingWindowState) -> Option<f64> {
    let (key, value) = input;
    state.push_back_window(key.clone(), value);          // journaled
    let len = state.window.get(&key).map(|w| w.len()).unwrap_or(0);
    if len > N {
        state.pop_front_window(key.clone(), len - N);    // journaled
    }
    let w = state.window.get(&key)?;
    if w.len() < N { return None; }                      // warmup → skip
    Some(w.iter().sum::<f64>() / N as f64)               // emit on present
}
```

**Java sugar** is the same shape: `@Journal(MapDeque) Map<String, Deque<Double>>
window`, `pushBackWindow` / `popFrontWindow` mutators, and a `run` returning
`Optional<Double>`.

**Expected expanded `Change` + `apply_change`** (identical across SDKs, up to
variant naming):

```rust
enum RollingWindowStateChange {
    PushBackWindow(String, f64),
    PopFrontWindow(String, usize),
}

fn apply_change(state: &mut RollingWindowState, change: &RollingWindowStateChange) {
    match change {
        PushBackWindow(k, v) => { state.window.entry(k.clone()).or_default().push_back(*v); }
        PopFrontWindow(k, n) => {
            let e = state.window.entry(k.clone()).or_default();
            for _ in 0..(*n).min(e.len()) { e.pop_front(); }
        }
    }
}
```

The sugared and hand-written forms produce byte-identical emits and
logically-equal state, and the sugared form passes `replay_equals` /
`replayEquals()` — the conformance assertion each SDK makes.

## 7. Forward-looking sketches (non-normative)

These bind the future Python and Elixir bindings to the same inference rules.
They are targets, not built here, and adopting them must not change this
contract.

**Python (`@flezha.shard`):**

```python
@flezha.shard(id="do_nothing", sink="echo")
def do_nothing(input: str) -> bytes:
    return input.encode()

@flezha.shard(id="store_input", sink="sink")
def store_input(input: str, state: StoreInputState) -> str:   # state param ⇒ stateful
    state.push_text_inputs(input)                              # journaled field mutator
    return input
```

The decorator inspects the function's type hints: first param ⇒ `Input`, the
return annotation ⇒ `Output`, a parameter typed as a `@flezha.state` class ⇒
`State`. `sink` triggers single-shard auto-wiring + auto-registration; multi
shard relays use an explicit `flezha.relay([...])` topology.

**Elixir (`use Flezha.Shard`):**

```elixir
defmodule DoNothing do
  use Flezha.Shard, id: "do_nothing", sink: "echo"

  @impl true
  def run(input, _state), do: {:emit, input}
end

defmodule StoreInput do
  use Flezha.State, fields: [text_inputs: []]   # derives Change + apply_change
end
```

`use Flezha.Shard` injects the six type declarations and the `run` wrapper;
`run/2` returning `{:emit, value}` maps to `Output`, `:noemit` to a unit
output. `use Flezha.State` generates the `Change` variants and `apply_change`.
Partitioning opt-ins (`keys:` + `partition_for:`, or `shard_key:`) and the
single-shard `sink:` auto-wire follow §4 and §5 unchanged.
