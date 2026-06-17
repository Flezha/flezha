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
| return type `O` (or `Result<O, _>`)           | `type Output = O`              |
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
