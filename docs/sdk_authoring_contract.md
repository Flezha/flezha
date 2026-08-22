# Flezha SDK Authoring Contract

**Status:** Normative for every SDK binding (Rust, Java and the Elixir v1 subset today;
Python next). A surface one binding gains, every binding gains in the same change or defers
by name — [`architecture/runner.md` → "One change, every SDK"](architecture/runner.md#one-change-every-sdk).

This document is the single source of truth for the *authoring sugar* every
SDK offers on top of the `StreamOperative` / `Procedure` / `Change` / `Channel`
trait (or interface) surface. The sugar is **pure expansion**: a macro (Rust) or an
annotation processor (Java) generates exactly the trait impls a user would
write by hand, against the unchanged runtime. There is no parallel runtime
path, no new protobuf message, and no WAL-format change.

The trait surface itself is specified elsewhere (`operative-procedure-entity-model`,
`rust-sdk-dispatch`, `java-sdk`, `operative-state-persistence`). This contract
governs only how a sugared declaration maps onto it.

**Before writing a new SDK, read
[`sdk_poll_core.md` → "The core decides, the SDK brings the threads"](sdk_poll_core.md#the-core-decides-the-sdk-brings-the-threads).**
It states which side owns ordering, exclusivity and the limit on work
underway, and which side owns the threading model — the choice a new binding
makes first and can least afford to get wrong.

## 1. Escape-hatch equivalence (non-negotiable)

- Generated code is equivalent to a valid hand-written `Procedure` / `StreamOperative`
  implementation. It compiles and runs identically; no protocol, controller,
  or WAL change is required for it to run.
- An operative MAY mix sugared procedures and hand-written procedures wired together.
- "Ejecting" a sugared procedure to a full impl is a paste of its expansion — the
  operative behaves identically afterward with no other change.

## 2. Signature → associated types (the inference table)

Inference is driven by the `run` declaration. `S` below is the optional
mutable-state parameter.

| Source in the `run` declaration              | Associated type / item        |
| --------------------------------------------- | ------------------------------ |
| first non-state value param of type `I`       | `type Input = I`               |
| return type `O` (or `Result<O, _>`)           | `type Output = O` (always emit) |
| return type `Option<O>` / `Optional<O>`       | `type Output = O`, emit on present |
| return type unit (`()` / `void`)              | no output channel              |
| a trailing procedure-context param + unit return | the body emits; `emits = T` sets `type Output = T` |
| a mutable-state param of type `S`             | `type State = S` (stateful)    |
| no mutable-state param                        | `type State = ()`, `Change = NoChange` |
| `State` opts into change derivation           | `type Change = <derived enum>` |
| unspecified                                   | `type Clock = WallClock`       |
| unspecified                                   | `type Config = ()`             |
| `id = "…"` else the declaration name          | `name` / `factory_id`          |

A mutable-state parameter is **the single signal** that a procedure is stateful.
Its presence sets `State` and (unless overridden) selects the change type;
its absence means `State = ()` and `Change = NoChange`.

### 2.1 Multi-emit: the context parameter

A `run` that must emit more than once per input declares a trailing context
parameter and returns unit:

```rust
#[flezha::procedure(id = "ohlc", emits = Candle)]
fn ohlc(trade: Trade, s: &mut Candles, ctx: &mut ProcedureCtx) -> Result<(), ProcedureError> {
    ctx.emit_keyed(closed.symbol.clone(), closed)?;   // the candle that just closed
    ctx.emit_keyed(opened.symbol.clone(), opened)     // and the one that opened
}
```

```java
@Procedure(id = "ohlc", emits = Candle.class)
public void run(Trade trade, CandlesHandle s, ProcedureCtx ctx) { ctx.emitKeyed(…); ctx.emitKeyed(…); }
```

The generated implementation passes the invocation's context straight through,
so the expansion is the same code a hand-written implementation would contain —
§1 unchanged. Everything else about the declaration infers as above.

Because the body owns every emit, the return type has no output left to declare
and `emits` declares it instead (absent = no output channel). Two rules follow,
both enforced with a named error rather than silently:

- a value return alongside a context parameter is refused — one procedure, one
  emit path;
- `key = …` alongside a context parameter is refused — the body keys its own
  emits — and `emits` without a context parameter is refused, since there the
  return type already answers.

### 2.2 Three tiers, each a superset of the one above

Sugar is the top tier. Below it, a hand-written implementation is charged only
for what its declaration does not already state:

- **Java** — `inputType()`, `outputType()`, `changeType()`, `clockType()` and
  `configType()` default to the type arguments read off the implementing
  class's generic supertype chain: a direct `implements Procedure<…>` with
  concrete arguments, or a concrete subclass of a generic intermediate class
  (type variables are substituted at every hop). `kryoTypes()` defaults to the
  resolved Change and State classes, and to the empty list for a `NoChange`
  procedure, which persists nothing. When an argument is still generic at the
  concrete class it is **never guessed**: `defineStream` / `defineWorkflow`
  fails naming the class and the parameter, and nothing is registered. That is
  a startup error, not a dispatch error.
- **Rust** — `SimpleProcedure` declares `Input`, `Output`, `State`, `name`,
  `init_state` and `run`; a blanket impl lifts it to `Procedure` with
  `Clock = WallClock`, `Config = ()` and the `State` type's derived change
  type. `type State = ()` is the stateless spelling. Stable Rust has no
  associated-type defaults, hence a tier rather than defaults on the trait
  itself.

**Explicit always wins.** A Java override is authoritative over the default it
replaces; a Rust type implements `Procedure` directly to declare a non-default
clock, a typed config or a hand-written change — and a type cannot sit in both
Rust tiers at once, because the blanket impl and a direct impl overlap and the
compiler refuses it.

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

Real compute procedures keep their state as a per-key map and mutate a collection
or computed value inside each entry. A map field annotated `#[journal(...)]`
(Rust) / `@Journal(...)` (Java) opts that field into per-key journaling: the
SDK generates named per-key mutators, the matching `Change` variants, and the
`apply_change` arms that replay them. The annotation is **required** on a map
field — an unannotated map field is a compile error (there is no single per-key
proc to record), directing the author to a hand-written `Change`. The field type
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

**Flat collection fields.** Once a procedure is keyed by the thing a map would
have keyed by, its per-entity collections stop being maps — the Entity *is* the
key. A flat field therefore gets delta mutators too, with no annotation and no
map layer:

| Field type      | Generated mutators (Rust / Java)                                        | Change variants                          |
| --------------- | ----------------------------------------------------------------------- | ---------------------------------------- |
| any `T`         | `set_<field>(T)` / `set<Field>(T)`                                       | `Set<Field>(T)`                          |
| `Vec<U>` / `List<U>`      | additionally `push_<field>(U)` / `push<Field>(U)`             | `Push<Field>(U)`                         |
| `VecDeque<U>` / `Deque<U>` | additionally `push_back_<field>(U)`, `pop_front_<field>(n)` / `pushBack<Field>(U)`, `popFront<Field>(int n)` | `PushBack<Field>(U)`, `PopFront<Field>(usize)` |

`pop_front` is saturating: popping more than the collection holds empties it
rather than failing, so a delta recorded against a longer window replays safely.
Reach for `set_<field>` on a growing collection only when the whole value
genuinely changes — otherwise every WAL record grows to the collection's size.

### 3.2 Emit-or-skip output return

A logic function returning an optional output — `Option<O>` (Rust) /
`Optional<O>` (Java) — emits when present and emits nothing when absent. This
replaces the explicit warmup gate (`if series.len() < n { return } …
ctx.emit(v)`): `None` / `empty` skips, `Some(v)` / present emits. The SDK
generates the emit call, so the author never touches the procedure context. The
unit return (`()` / `void`, no output channel) and the unconditional `O`
return (always emit) are unchanged.

**Keyed output (`key`).** On a `.keyed()` output channel the plain emit
carries the emitting Entity's **own key** — the key its input routed by, so
the stamped `flz.key` cannot drift from it and a keyed procedure never
re-supplies what the runtime already knows. A sugared procedure therefore
needs `key` only where its output's natural key *differs* from its Entity's,
or where the procedure is single-entity and so has no key at all (there, the
plain emit is refused). `key` names a function from the output to that key
(Rust: `#[flezha::procedure(key = fn)]` with `fn(&Output) -> String`; Java:
`key = "methodName"` naming a `static String methodName(Output out)` on the
authoring class). Declaring `key` on a procedure with no output is an error.

### 3.3 Single hand-written path determinism

A procedure that mutates state **only** through generated journaling mutators has
exactly one hand-written path — the logic function — and one generated replay
path — the generated `apply_change`. The engine journals the recorded `Change`s
before emit (unchanged flush-before-emit ordering) and, on warm restart,
rebuilds state by folding those same `Change`s through the **generated**
`apply_change`. Because that `apply_change` is generated from the same mutator
definitions the mutators use, it cannot drift from the live path — the
live-vs-replay divergence that `ProcedureTestHarness::replay_equals` /
`replayEquals()` exists to catch is structurally unrepresentable for sugared
procedures. The replay-determinism harness remains the conformance gate for **all**
procedures (sugared and hand-written).

A consequence: procedures that hand-wrote an idempotency guard for replay safety
(e.g. a `last_open_us` dedup that stops a re-folded input from double-counting)
no longer need it. Journaling an idempotent `set` proc means replay re-assigns the
computed value rather than re-folding an input, so removing the guard does not
change replay equality.

### 3.4 The `on_timer` contract

A Procedure can act because time passed, not only because an input arrived:
`ctx.timer_at_event_time(ts)` / `ctx.timer_after(d)` (Rust) —
`ctx.timerAtEventTime(ts)` / `ctx.timerAfter(d)` (Java) — register a per-Entity
deadline, and `Procedure::on_timer` / `Procedure.onTimer` handles the firing
with the same `StateHandle` and `ProcedureCtx` `run` gets. Registration returns
the absolute timestamp it is scheduled at; that value is the handle a
cancellation takes.

Three rules make the callback part of the determinism contract rather than a
hole in it:

1. **`fired_at` is the SCHEDULED timestamp**, never the wall clock at firing. A
   processing-time timer set for T that fires at T+δ still reports T; passing
   T+δ would make a replay observe a different value, exactly as
   `SystemTime::now()` inside `run` does. `ctx.now()` equals `fired_at` inside
   the callback, so a chained `ctx.timer_after(..)` schedules off the timer that
   woke it.
2. **`timer_after(d)` is anchored on `ctx.now()`**, not on an ambient read —
   which is what makes a replayed invocation register the same absolute
   timestamp the live one did.
3. **A registration made during a replay IS registered.** `ctx.spawn_workflow`
   answers `SkippedOnReplay` because it is an external side effect that must not
   happen twice; a timer is internal state, and skipping it on replay would
   rebuild a pending set the live run never had. The two calls sit on the same
   `ProcedureCtx`, so the contrast is the trap to remember.

`on_timer`'s default panics (Rust) / throws (Java) naming the Procedure:
registration is a runtime call, so a Procedure that registers a timer without
handling it cannot be refused at build time. Full contract, including which kind
to reach for, in [`entity_timers.md`](entity_timers.md).

## 4. Entity mode is an explicit opt-in, never inferred

The mode changes lifecycle and on-disk state layout, so it is always an
explicit author choice — never inferred from the `run` body. Each opt-in maps
1:1 onto the existing trait surface:

| Opt-in                                         | Generated trait surface |
| ---------------------------------------------- | ----------------------- |
| none                                           | single-entity (empty `entity_keys()`) |
| `keys = [..]` + `entities_for` (static)         | `entity_keys()` = keys, `ENTITIES_FOR_OVERRIDDEN = true`, `entities_for` delegates |
| `idle_ttl` / `max_entities` (dynamic knobs) | `entity_idle_ttl()` / `max_entities()` overrides; the KEY is not here |

Static keying is declared **on the procedure**; dynamic keying is declared
**on the wiring** — `.key_by(f)` / `.keyBy(f)` on the stream that feeds it
(`docs/stream_transforms.md`). The sugar therefore offers no `entity_key`
hook at all: declaring one is a compile error (Rust) / annotation-processing
error (Java) naming `key_by` as its replacement. The two modes stay mutually
exclusive — a keyed edge into a procedure declaring `entity_keys()` fails the
build with `KeyByOnUnkeyableTarget`.

**Per-entity state init (`init`).** By default a stateful procedure's
`init_state` returns `State::default()`. When an entity's starting state is a
function of its key — e.g. a statically-entity procedure whose window length or
bucket span is encoded in the procedure key — declare an `init` opt-in naming a
function `fn(Option<&str>) -> State` (Rust: `#[flezha::procedure(init = fn)]`; Java:
an `@InitState State initState(Optional<String> entityKey)` method on the
authoring class). The generated `init_state` delegates to it, passing the procedure
key (absent in single-entity mode and the test-harness default). `init`
requires a state parameter — declaring it on a stateless procedure is a compile-time
error. The restore payload stays runtime-applied after `init_state` returns, so
the `init` function never sees it.

### 4.1 Reaching every keyed Entity: the `route_latest` building block

A keyed procedure cannot receive a broadcast. Both keying declarations are
total — `entities_for` names the Entities an input belongs to, `.key_by(f)`
names exactly one — and inside a partitioned member the computed key is
admitted against that member's key-group range. So "one operative-wide
document that every keyed Entity must eventually see" (a threshold table, a
feature flag, a config document) has no direct spelling, and every application
that hits the wall invents the same bridge.

The bridge is packaged as one builder method:

```rust
let routed = c.route_latest(thresholds, trades, |t: &Trade| t.symbol.clone());
```

```java
Stream<RoutedDocument<Thresholds>> routed =
        c.routeLatest(Thresholds.class, thresholds, trades, Trade::symbol);
```

It expands to a merge into ONE SDK-packaged single-entity procedure (node
identity `route_latest`) whose journaled state is
`{document, version, delivered: key → version}`, and it returns that node's
output stream: a `(key, document)` pair in Rust, a `RoutedDocument<D>` record in
Java (no tuples, and the runtime needs a `Class` token for the routed payload).
The consumer keys by that key, which is what makes each delivery reach one
Entity. Pure expansion, per §1: merge, single-entity procedure, journaled state
— no new runtime semantics, no wire change, no new replay path.

**Deliver on the key's own traffic — never a remembered-set fan-out.** This is
the load-bearing rule, and the reason the block exists rather than a
documentation note. A router that remembered the keys it had seen and fanned a
new document out over them would emit keys the member may no longer own: a
rescale moves keys away, and a single out-of-range item seen during a reconcile
window would poison the set permanently. Every such delivery then parks against
the partition guard. Delivering only on a key's own next traffic item makes
over-delivery impossible *by construction* — the only keys emitted are keys
whose traffic just proved this member owns them.

**Latency is one tick per key**, and that is the price of the construction: a
key learns of a new document on its next traffic item, not when the document
arrives. A key whose traffic has stopped is never delivered to — the same
property stated from the other side, and the reason there is no push to idle
keys, no retained broadcast to late-born Entities and no rescale-time replay to
specify.

**The document is opaque.** It is retained and compared (`Eq` / `equals`) and
never parsed, so parsing and its failure mode stay with the consumer exactly as
when the consumer read the document stream directly. A document equal to the
retained one is inert, which is what absorbs the re-poll a document source
performs after every reconnect; a differing one bumps the version, so every key
is owed one delivery again. A document type without value equality therefore
makes every arrival a new version.

Two routers in one operative need the node-named spelling
(`route_latest_named(.., "primary")` / `routeLatest(.., "primary")`) — by §10 of
`sdk_naming_conventions.md` two unnamed mentions of one class are one node, and
here that means one retained document shared between them.

## 5. Single-procedure auto-wiring and registration

Exactly **one** topology may be auto-wired: a single procedure from a controller
input to one named sink (`input → procedure → plugin(sink)`). A sugared procedure that
declares both an `id` and a `sink`, and is the sole procedure, generates that operative
and registers it automatically — the user writes no registration call.

- **Multi-procedure topology stays explicit.** More than one procedure, or any
  fan-out / multi-input edge, requires an explicit topology declaration
  (`channels()` in Rust/Java, or the linear `operative!`/pipeline sugar for a
  strictly linear chain). The SDK refuses (compile error) rather than guessing
  a graph.
- **Auto-registration** uses a compile-time-collected registry (Rust:
  `inventory`, merged into the runner's declared set at every
  `FlezhaRunner::start()`; Java: the processor emits a
  `GeneratedStreamOperatives.defineAll(FlezhaRunner)` aggregator). Explicit
  declaration — `FlezhaRunner::define_stream::<R>()` /
  `FlezhaRunner.defineStream(Class)`, legal before and after `start()`,
  with `remove_stream`/`removeStream` as the inverse — stays valid for
  hand-written operatives.
- **Duplicate factory id fails fast.** Declaring two operatives with the same
  `factory_id` on one runner fails at the second `define*` call with an error
  naming the colliding id.

## 6. Canonical reference operative (cross-SDK conformance anchor)

Every SDK's conformance test asserts that this one operative — the existing
`StaticEntityOperative` shape — expands to the same topology and associated-type
mapping. It is a two-procedure, statically-entity, linear operative.

**Authored (Rust sugar):**

```rust
#[flezha::procedure(id = "static_entry_procedure", keys = ["a", "b", "c", "d"], entities_for = static_seg)]
fn static_entry(input: String, state: &mut StaticEntryState) -> String {
    state.push_seen(input.clone());
    input
}

#[flezha::procedure]
fn static_downstream(input: String) -> Vec<u8> {
    input.into_bytes()
}

flezha::operative! {
    id: "static_entity",
    pipeline: input::<String> -> StaticEntry -> StaticDownstream -> plugin::<Vec<u8>>("test_kafka")
}
```

**Expected expanded topology** (identical across SDKs):

| Channel        | Kind          | Payload   | Producer            | Consumer / sink      |
| -------------- | ------------- | --------- | ------------------- | -------------------- |
| `input`        | entry         | `String`  | controller          | `StaticEntry`        |
| (hop)          | inter-procedure | `String`  | `StaticEntry`       | `StaticDownstream`   |
| (sink)         | plugin-bound  | `Vec<u8>` | `StaticDownstream`  | plugin `test_kafka`  |

**Expected associated-type mapping:**

| Procedure          | Input    | Output    | State              | Change              | Mode   |
| ------------------ | -------- | --------- | ------------------ | ------------------- | ------ |
| `StaticEntry`      | `String` | `String`  | `StaticEntryState` | derived (`Push`/`Set`) | static (`a`,`b`,`c`,`d`) |
| `StaticDownstream` | `String` | `Vec<u8>` | `()`               | `NoChange`          | single |

`factory_id` = `static_entity`. `Clock = WallClock`, `Config = ()` for both.

### 6.1 Canonical keyed-collection reference procedure

Every SDK's conformance test also implements this one procedure — a **rolling
per-key window** — and asserts its generated journaling matches a hand-written
twin and survives the replay-determinism harness. It is the reference for the
§3.1 keyed/collection rules and the §3.2 emit-or-skip return.

**Before (hand-written `Procedure` + `Change`):** a `Change` enum with
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

#[flezha::procedure(id = "rolling_window")]
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

**Python (`@flezha.procedure`):**

```python
@flezha.procedure(id="do_nothing", sink="echo")
def do_nothing(input: str) -> bytes:
    return input.encode()

@flezha.procedure(id="store_input", sink="sink")
def store_input(input: str, state: StoreInputState) -> str:   # state param ⇒ stateful
    state.push_text_inputs(input)                              # journaled field mutator
    return input
```

The decorator inspects the function's type hints: first param ⇒ `Input`, the
return annotation ⇒ `Output`, a parameter typed as a `@flezha.state` class ⇒
`State`. `sink` triggers single-procedure auto-wiring + auto-registration; multi
procedure operatives use an explicit `flezha.operative([...])` topology.

**Elixir (`use Flezha.Procedure`):**

```elixir
defmodule DoNothing do
  use Flezha.Procedure, id: "do_nothing", sink: "echo"

  @impl true
  def run(input, _state), do: {:emit, input}
end

defmodule StoreInput do
  use Flezha.State, fields: [text_inputs: []]   # derives Change + apply_change
end
```

`use Flezha.Procedure` injects the six type declarations and the `run` wrapper;
`run/2` returning `{:emit, value}` maps to `Output`, `:noemit` to a unit
output. `use Flezha.State` generates the `Change` variants and `apply_change`.
Entity opt-ins (`keys:` + `entities_for:` on the procedure; `key_by` on the
wiring) and the single-procedure `sink:` auto-wire follow §4 and §5 unchanged.
