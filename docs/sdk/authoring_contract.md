# SDK Authoring Contract

For operative authors who use the attribute/annotation sugar and for SDK
implementers who must keep it identical across bindings. It specifies how a
sugared declaration — `#[flezha::procedure]` / `#[derive(flezha::State)]` /
`flezha::operative!` in Rust, `@Procedure` / `@State` / `@StreamOperative` in
Java — maps onto the `StreamOperative` / `Procedure` / `Change` / `Channel`
trait (interface) surface. The sugar is **pure expansion**: a macro (Rust,
`flezha-macros/`) or an annotation processor (Java, `sdk/java/flezha-sdk-processor/`)
generates exactly the impls a user would write by hand, against the unchanged
runtime. There is no parallel runtime path, no new protobuf message and no
WAL-format change.

The sugar exists in the Rust and Java SDKs. The Elixir SDK exposes the
`Flezha.SDK.Procedure` behaviour directly (`name/0`, `init_state/1`, `run/3`,
`entity_keys/0`, `entities_for/1`, …) and has no derive layer — see
[`elixir_sdk.md`](elixir_sdk.md). The Node SDK exposes factory functions —
`defineProcedure`, `defineOperative`, `defineWorkflow`, wired by a fluent
channel builder — and no decorators, because a decorator needs a build step,
does not apply to a plain function, and cannot reshape the decorated type,
which is what a typed graph needs ([`node_sdk.md`](node_sdk.md)). The Ruby SDK
exposes classes — `Flezha::Procedure`, `Flezha::StreamOperative`,
`Flezha::WorkflowOperative`, wired by the same fluent channel builder — and
reads which optional hooks a procedure declares from the methods it defines, so
a declaration and its implementation cannot disagree
([`ruby_sdk.md`](ruby_sdk.md)). The PHP SDK exposes classes too —
`Flezha\Procedure`, `Flezha\StreamOperative`, `Flezha\WorkflowOperative`, wired
by the same fluent channel builder — with the scalar declarations in a
`#[Proc]` attribute and the optional hooks read from the methods a procedure
**overrides**, which is the closest PHP comes to Java's annotations without a
build step ([`php_sdk.md`](php_sdk.md)). The Python SDK exposes decorators —
`@flezha.procedure`, `@flezha.state`, `@flezha.stream_operative`,
`@flezha.workflow_operative`, wired by the same fluent channel builder — over a
handler that is a plain function when it is a transform and a class when it has
hooks, with the input, output, state and change types all read from the type
hints, which is what makes a wrong edge a type-checker error before it is a
registration one ([`python_sdk.md`](python_sdk.md)). A surface one
binding gains, every binding gains in the same change or defers by name:
[`../architecture/runner.md` → "One change, every SDK"](../architecture/runner.md#one-change-every-sdk).

**This contract binds every SDK** — the declaration surface, the invocation
context, the reserved metadata namespace, the deterministic effect rules, the
registration-time refusal set, and the state-mutation discipline. Where a
language allows a stronger enforcement than the contract's minimum, that SDK
takes it, and the two tables below record the level each one actually supplies
rather than one it intends to.

### State-mutation enforcement, per SDK

Declared state is mutated only through declared changes, because the change log
IS how an entity is rebuilt: a field mutated outside one is live now and gone
after a restart, and nothing reports the difference until the restart.

| SDK | Mechanism it ships |
| --- | --- |
| Rust | The borrow checker. `StateHandle` hands out the generated mutators and `apply`; there is no `&mut S` to assign through, so an unrecorded mutation does not compile. |
| Java | The generated `<State>Handle`. The state object is reachable, so an unrecorded mutation is possible in principle and prevented by the generated surface being the only one an author is given. |
| Elixir | Immutability. `run/3` returns the next state, and the SDK journals what it was handed — an unrecorded mutation has no way to exist. |
| Node | Runtime rejection. `ctx.state` is a deep read-only view; an assignment throws naming the field and pointing at `ctx.change`. `applyChange` and the restore fold hold the value itself. |
| Ruby | Documented rule, no runtime check. Node's guard is a `Proxy`; Ruby has no counterpart that does not cost an allocation per field read, so `ctx.change` is the path and the rule is stated rather than paid for on every access. |
| PHP | Documented rule, no runtime check, for Ruby's reason. `$ctx->state` is writable, `$ctx->change()` is the path, and `Harness::assertReplayEquals()` is what makes the rule safe — it catches an unrecorded mutation and an impure fold alike. |
| Python | Runtime rejection. A class declared `@flezha.state` refuses direct attribute assignment, naming the field and the change-based path; the runtime's own path — applying a change and folding a restore — is the one exception, opened per call on the calling thread. Mutation *through* a container a state field holds is not covered, exactly as Rust's interior mutability is not. |

`replay_equals` / `replayEquals()` is the gate for all seven: it folds the
recorded change log against a freshly constructed initial state and requires
the result to equal the live one.

### Compile-time refusal of a wrong edge

Where an SDK's language can reject a mismatched channel wiring before the
program runs, it does, and pins the refusal with fixtures expected to fail
compilation — so a refusal that stops working fails the build rather than
silently becoming a runtime check. Registration-time refusals remain the
backstop for what typing cannot see.

| SDK | Compile-time edge typing | Where the refusals are pinned |
| --- | --- | --- |
| Rust | yes | 25 compile-fail tests in `sdk/rust/tests/ui/` |
| Java | yes, through the generic `Procedure<…>` arguments | `sdk/java/.../StreamOperativeConfigurationException` cases |
| Node | yes, through the channel builder's type parameters | `@ts-expect-error` fixtures in `sdk/node/types-tests/`, compiled under `tsc --noEmit`, plus a negative control that strips the directives and requires each pinned line to still produce a diagnostic |
| Elixir | no — the language cannot | registration-time refusals only |
| Ruby | no — the language cannot | registration-time refusals only, in `sdk/ruby/test/authoring_test.rb` |
| PHP | no — the language cannot | registration-time refusals only, in `sdk/php/tests/AuthoringTest.php` |
| Python | yes, through the generic `Edge[T]` / `Procedure[I, O]` parameters over a `py.typed` package | `sdk/python/tests/typing_fixtures/wrong_edge.py`, which `sdk/python/tests/test_typing.py` requires pyright to reject, against `well_typed.py` as the negative control |

Before writing a new SDK, read
[`poll_core.md` → "The core decides, the SDK brings the threads"](poll_core.md#the-core-decides-the-sdk-brings-the-threads):
it states which side owns ordering, exclusivity and the limit on work underway,
and which side owns the threading model.

## 1. Escape-hatch equivalence (non-negotiable)

- Generated code is equivalent to a valid hand-written `Procedure` /
  `StreamOperative` implementation. It compiles and runs identically; no
  protocol, controller or WAL change is required for it to run.
- An operative MAY mix sugared and hand-written procedures wired together.
- "Ejecting" a sugared procedure to a full impl is a paste of its expansion —
  the operative behaves identically afterwards with no other change.

The replay-determinism harness (`ProcedureTestHarness::replay_equals` in Rust,
`ProcedureTestHarness.replayEquals()` in Java) is the conformance gate for
**all** procedures, sugared and hand-written.

## 2. Signature → associated types (the inference table)

Inference is driven by the `run` declaration. In Rust the declaration is a free
function carrying `#[flezha::procedure(...)]`; in Java `@Procedure` sits on a
class that has exactly one public method named `run`. `S` below is the optional
mutable-state parameter: `&mut S` in Rust; in Java either the `@State`-generated
`<S>Handle` or a raw `StateHandle<S, Change>`.

| Source in the `run` declaration                     | Associated type / item                              |
| --------------------------------------------------- | --------------------------------------------------- |
| first by-value param of type `I`                    | `type Input = I`                                    |
| return type `O` (or `Result<O, _>`)                 | `type Output = O` (always emit)                     |
| return type `Option<O>` / `Optional<O>`             | `type Output = O`, emit on present                  |
| return type unit (`()` / `void`)                    | no output channel                                   |
| a trailing procedure-context param + unit return    | the body emits; `emits = T` sets `type Output = T`  |
| a mutable-state param of type `S`                   | `type State = S` (stateful)                         |
| no mutable-state param                              | `type State = ()` / `Stateless`, `Change = NoChange` |
| `State` opts into change derivation                 | `type Change = <S>Change` (derived)                 |
| `clock = T` (Rust) else unspecified                 | `type Clock = T`, default `WallClock`               |
| unspecified                                         | `type Config = ()` / `Void`                         |
| `id = "…"` else the declaration name                | `name()` — Rust and Python: the function name; Java, Ruby and PHP: the class name in snake_case |
| `capacity = N` / `capacity = N`                     | `mailbox_capacity()` / `mailboxCapacity()` override |
| `on_full = …` / `onFull = "…"`                      | overflow policy of the auto-wired entry channel (§5): `drop_nothing`, `drop_newest`, `drop_oldest` |

A mutable-state parameter is **the single signal** that a procedure is
stateful. Its presence sets `State` and selects the derived change type; its
absence means `State = ()` and `Change = NoChange`. Any other attribute key is
refused by name.

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
public class Ohlc {
    public void run(Trade trade, CandlesHandle s, ProcedureCtx ctx) {
        ctx.emitKeyed(closed.symbol(), closed);
        ctx.emitKeyed(opened.symbol(), opened);
    }
}
```

The generated implementation passes the invocation's context straight through,
so the expansion is the code a hand-written implementation would contain (§1).
Because the body owns every emit, the return type has no output left to declare
and `emits` declares it instead (absent = no output channel). Each of the
following is refused with a named error, never silently:

- a value return alongside a context parameter — one procedure, one emit path;
- `key = …` alongside a context parameter — the body keys its own emits;
- `emits` without a context parameter — the return type already answers.

### 2.2 Three tiers, each a superset of the one above

Sugar is the top tier. Below it, a hand-written implementation is charged only
for what its declaration does not already state:

- **Java** — `inputType()`, `outputType()`, `changeType()`, `clockType()` and
  `configType()` default to the type arguments read off the implementing
  class's generic supertype chain: a direct `implements Procedure<…>` with
  concrete arguments, or a concrete subclass of a generic intermediate class
  (type variables are substituted at every hop). `kryoTypes()` defaults to the
  resolved Change and State classes, and to the empty list for a `NoChange`
  procedure, which persists nothing. An argument still generic at the concrete
  class is **never guessed**: `defineStream` / `defineWorkflow` fails naming the
  class and the parameter, and nothing is registered — a startup error, not a
  dispatch error.
- **Rust** — `SimpleProcedure` declares `Input`, `Output`, `State`, `name`,
  `init_state` and `run`; a blanket impl lifts it to `Procedure` with
  `Clock = WallClock`, `Config = ()` and the `State` type's derived change type.
  `type State = ()` is the stateless spelling. Stable Rust has no
  associated-type defaults, hence a tier rather than defaults on the trait.

**Explicit always wins.** A Java override is authoritative over the default it
replaces; a Rust type implements `Procedure` directly to declare a non-default
clock, a typed config or a hand-written change — and a type cannot sit in both
Rust tiers at once, because the blanket impl and a direct impl overlap and the
compiler refuses it.

## 3. State → Change derivation, and the manual boundary

Opting a state type into change derivation (`#[derive(flezha::State)]` in
Rust, `@State` in Java) generates:

- a `<State>Change` enum (Rust) / sealed class (Java) with **one variant per
  field**: `Set<Field>(T)` for every field, plus collection deltas for
  `Vec`/`List` and `VecDeque`/`Deque` fields (table below);
- `apply_change` folding each variant into the state;
- field-granular mutators on the state handle (`state.set_field(v)`,
  `state.push_field(v)`; Rust exposes them through a generated `<State>Mut`
  trait, Java through a generated `<State>Handle` class) plus the explicit
  `state.apply(change)` path. Both journal the change to the WAL.

**The manual boundary.** Only field-granular mutations are derivable. A
mutation that is conditional, spans multiple fields atomically, or is a
computed function of the prior state is **not** expressible as a single
generated variant: the SDK requires a hand-written `Change` and reports an
error rather than journaling incorrectly. The derive requires a struct with
named fields and at least one field; every field type must be
cloneable/serializable (Rust derives `Clone` + serde on the generated enum), so
a field that is not fails to compile and the author hand-writes a `Change`.

### 3.0 Changing a State or Change type after it has been deployed

> **A factory id is single-language.** Because the persisted encodings below are
> per-SDK, an operative is placed only on a runner of its factory's language,
> and a restore refuses a record another SDK stamped. Re-authoring a factory in
> another language is therefore not an upgrade — it is drain-and-cutover under a
> new factory id (stop, compact, `flz state get`, start the new factory), or
> clearing the old factory's durable state so it re-establishes.

The persisted encodings are **positional and not self-describing** (postcard in
Rust, Kryo in Java). Reordering fields, removing one, or changing a field's
type is not a compatibility question the codec can answer: last version's bytes
either run out mid-decode or land in the wrong fields. So the rule is:

> **Bump `SCHEMA_VERSION` on any non-additive edit to the Procedure's `State`
> or its `Change` type** — not to the `Change` type alone.

The version is stamped in every record's 16-byte header, and every restore
resolves it before decoding anything (`docs/sdk/protocol_spec.md` → "Schema
evolution"). A bump alone is fail-closed: the old records are refused, loudly,
by name. To *read* them, declare how far back you read and how:

| | Rust | Java |
|---|---|---|
| version | `const SCHEMA_VERSION: u32` | `int schemaVersion()` |
| readable floor | `const MIN_READABLE_SCHEMA_VERSION: u32` | `int minReadableSchemaVersion()` |
| state migration | `fn migrate_state(from_version, bytes)` | `State migrateState(int, byte[])` |
| change migration | `fn migrate_change(from_version, bytes)` | `ChangeT migrateChange(int, byte[])` |

Both migrations default to refusing, so declaring a floor without implementing
them fails at the first affected record rather than guessing. Keep the old type
in your own source and decode it inside the migration — the runtime cannot know
a type that no longer exists in this build. Rust re-exports the codec it wrote
the bytes with as `flezha_runner::state::postcard`; Java constructs a
`KryoCodec` over **the old version's type list, in the old order** (Kryo ids are
positional).

```rust
const SCHEMA_VERSION: u32 = 2;
const MIN_READABLE_SCHEMA_VERSION: u32 = 1;

fn migrate_state(from_version: u32, bytes: &[u8]) -> Result<Cart, String> {
    match from_version {
        1 => {
            #[derive(serde::Deserialize)]
            struct CartV1 { items: u32 }
            let old: CartV1 = postcard::from_bytes(bytes).map_err(|e| e.to_string())?;
            Ok(Cart { items: old.items, currency: "EUR".into() })
        }
        v => Err(format!("no migration from {v}")),
    }
}
```

A migration is subject to the determinism contract (§3.3): crash-resume,
retry-replay and rewind all re-enter the restore fold, so the same bytes must
produce the same value every time. Prove it without a controller — the test
harness restores from a payload stamped at the older version
(`ProcedureTestHarness::with_payload`, `sdk/rust/tests/state_evolution.rs`).

Two refusals are not migrations and cannot be made into them: a record older
than your declared floor, and a record from a version **newer** than the one
running (a downgrade — older code meeting newer state). Rolling a deploy back
past a schema bump means restoring the pre-deploy snapshot.

The reference fixtures are `schema_evolution_v1` / `schema_evolution_v2` in the
Rust and Java test apps, driven by `test/system/schema_evolution_e2e_test.exs`.

### 3.1 Keyed and collection state journaling

A map field annotated `#[journal(...)]` (Rust) / `@Journal(...)` (Java) opts
that field into per-key journaling: the SDK generates named per-key mutators,
the matching `Change` variants and the `apply_change` arms that replay them.
The annotation is **required** on a map field — an unannotated map field is a
compile error (there is no single per-key op to record). The field type must
match the declared mode, or the SDK reports an error.

| Mode (Rust / Java)             | Field type                 | Generated mutators (Rust)                                                              | Change variants                                                                   | `apply_change`                                  |
| ------------------------------ | -------------------------- | -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- | ----------------------------------------------- |
| `map_set` / `MAP_SET`          | `HashMap<K, V>`            | `set_<field>(key, V)`                                                                  | `Set<Field>(K, V)`                                                                | `map.insert(key, value)`                        |
| `map_vec` / `MAP_VEC`          | `HashMap<K, Vec<U>>`       | `push_<field>(key, U)`, `drain_<field>_front(key, n)`, `replace_last_<field>(key, U)` | `Push<Field>(K, U)`, `Drain<Field>Front(K, usize)`, `ReplaceLast<Field>(K, U)`   | insert-default-then-op on the keyed vector      |
| `map_deque` / `MAP_DEQUE`      | `HashMap<K, VecDeque<U>>`  | `push_back_<field>(key, U)`, `pop_front_<field>(key, n)`                              | `PushBack<Field>(K, U)`, `PopFront<Field>(K, usize)`                              | insert-default-then-op on the keyed deque       |

Java spells the mode as `@Journal(Journal.Mode.MAP_DEQUE)` (or `MAP_SET` /
`MAP_VEC`), the mutators in camelCase (`setByProduct`, `pushCloses`,
`drainClosesFront`, `replaceLastCloses`, `pushBackWindow`, `popFrontWindow`)
and the field types as `Map<K, V>` / `Map<K, List<U>>` / `Map<K, Deque<U>>`.
The collection modes use **insert-default-then-op** semantics: the keyed
collection is created on first touch, so `push`/`drain`/`pop` never need a
guard for an absent key.

**`map_set` is the universal fallback.** When a per-entry value is computed
(an EMA, an RSI, a whole candle), compute it in the logic function and call
`set_<field>(key, value)`; replay re-applies it by insertion. Prefer
`map_vec` / `map_deque` only for **growing collections** (a rolling window, an
appended bar history): journaling the whole collection each tick via `map_set`
grows every WAL record to the collection's size, whereas the collection modes
keep each record a single delta. A per-key *struct* that merely contains a
collection is not journaled sub-field-incrementally — split it into parallel
map fields, or use `map_set` to record the whole per-key value.

Flat fields and `#[journal(...)]` map fields on the same struct compose into
one generated `Change` and one `apply_change`.

**Flat collection fields.** Once a procedure is keyed by the thing a map would
have keyed by, the Entity *is* the key and its collections stop being maps. A
flat field therefore gets delta mutators too, with no annotation:

| Field type                 | Generated mutators (Rust / Java)                                                                              | Change variants                                |
| -------------------------- | ------------------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| any `T`                    | `set_<field>(T)` / `set<Field>(T)`                                                                            | `Set<Field>(T)`                                |
| `Vec<U>` / `List<U>`       | additionally `push_<field>(U)` / `push<Field>(U)`                                                             | `Push<Field>(U)`                               |
| `VecDeque<U>` / `Deque<U>` | additionally `push_back_<field>(U)`, `pop_front_<field>(n)` / `pushBack<Field>(U)`, `popFront<Field>(int n)` | `PushBack<Field>(U)`, `PopFront<Field>(usize)` |

`pop_front` / `drain_front` are saturating: popping more than the collection
holds empties it rather than failing, so a delta recorded against a longer
window replays safely.

### 3.2 Emit-or-skip output return

A logic function returning `Option<O>` (Rust) / `Optional<O>` (Java) emits
when present and emits nothing when absent — the warmup gate without touching
the procedure context. The unit return (no output channel) and the
unconditional `O` return (always emit) are the other two shapes.

**Keyed output (`key`).** On a `.keyed()` output channel the plain emit carries
the emitting Entity's **own key** — the key its input routed by — so a keyed
procedure never re-supplies what the runtime already knows. A sugared procedure
needs `key` only where its output's natural key *differs* from its Entity's, or
where the procedure is single-entity and so has no key at all (there, the plain
emit is refused). `key` names a function from the output to that key (Rust:
`#[flezha::procedure(key = f)]` with `f(&Output) -> String`; Java:
`key = "methodName"` naming a static method on the authoring class). `key` on a
procedure with a unit return is an error.

### 3.3 Single hand-written path determinism

A procedure that mutates state **only** through generated mutators has exactly
one hand-written path — the logic function — and one generated replay path —
the generated `apply_change`. The engine journals the recorded `Change`s before
emit and, on warm restart, rebuilds state by folding those same `Change`s
through the **generated** `apply_change`. Because that `apply_change` is
generated from the same definitions the mutators use, it cannot drift from the
live path: the live-vs-replay divergence `replay_equals` / `replayEquals()`
exists to catch is structurally unrepresentable for sugared procedures.

### 3.4 The `on_timer` contract

A Procedure can act because time passed, not only because an input arrived:
`ctx.timer_at_event_time(ts)` / `ctx.timer_after(d)` (Rust) —
`ctx.timerAtEventTime(ts)` / `ctx.timerAfter(d)` (Java) — register a
per-Entity deadline, and `Procedure::on_timer` / `Procedure.onTimer` handles
the firing with the same `StateHandle` and `ProcedureCtx` `run` gets.
Registration returns the absolute timestamp it is scheduled at; that value is
the handle a cancellation takes.

Three rules make the callback part of the determinism contract:

1. **`fired_at` is the SCHEDULED timestamp**, never the wall clock at firing.
   `ctx.now()` equals `fired_at` inside the callback, so a chained
   `ctx.timer_after(..)` schedules off the timer that woke it.
2. **`timer_after(d)` is anchored on `ctx.now()`**, not on an ambient read —
   which is what makes a replayed invocation register the same absolute
   timestamp the live one did.
3. **A registration made during a replay IS registered.** `ctx.spawn_workflow`
   answers `SkippedOnReplay` because it is an external side effect that must
   not happen twice; a timer is internal state, and skipping it on replay
   would rebuild a pending set the live run never had.

`on_timer`'s default panics (Rust) / throws (Java) naming the Procedure:
registration is a runtime call, so a Procedure that registers a timer without
handling it cannot be refused at build time. Full contract, including which
kind to reach for, in [`entity_timers.md`](entity_timers.md).

## 4. Entity mode is an explicit opt-in, never inferred

The mode changes lifecycle and on-disk state layout, so it is always an
explicit author choice — never inferred from the `run` body. Each opt-in maps
1:1 onto the trait surface:

| Opt-in (Rust / Java)                                                         | Generated trait surface                                                               |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| none                                                                         | single-entity (empty `entity_keys()`)                                                 |
| `keys = [..]` + `entities_for = f` / `keys = {..}` + an `@EntitiesFor` method | `entity_keys()` = keys, `ENTITIES_FOR_OVERRIDDEN = true` / `entitiesForOverridden()`, `entities_for` delegates |
| `idle_ttl = ms` / `max_entities = n` (Rust; dynamic knobs)                   | `entity_idle_ttl()` / `max_entities()` overrides — the KEY is not here. Java overrides `entityIdleTtl()` on a hand-written Procedure |

Declaring only one of `keys` / `entities_for` is refused. Static keying is
declared **on the procedure**; dynamic keying is declared **on the wiring** —
`.key_by(f)` / `.keyBy(f)` on the stream that feeds it
([`stream_transforms.md`](stream_transforms.md)). The sugar therefore offers no
`entity_key` hook: `entity_key = …` (Rust) / `@EntityKey` (Java) is a
compile / annotation-processing error naming `key_by` as its replacement. The
two modes stay mutually exclusive — a keyed edge into a procedure declaring
`entity_keys()` fails the build with `KeyByOnUnkeyableTarget`.

**Per-entity state init (`init`).** By default a stateful procedure's
`init_state` returns `State::default()` / `new State()`. When an entity's
starting state is a function of its key, declare `init` naming a function
`fn(Option<&str>) -> State` (Rust: `#[flezha::procedure(init = f)]`; Java: an
`@InitState State initState(Optional<String> entityKey)` method on the
authoring class). The generated `init_state` delegates to it, passing the
procedure key (absent in single-entity mode and the test-harness default).
`init` on a stateless procedure is a compile-time error. The restore payload
stays runtime-applied after `init_state` returns, so the `init` function never
sees it.

### 4.1 Reaching every keyed Entity: the `route_latest` building block

A keyed procedure cannot receive a broadcast: both keying declarations are
total — `entities_for` names the Entities an input belongs to, `.key_by(f)`
names exactly one — and inside a partitioned member the computed key is
admitted against that member's key-group range. "One operative-wide document
that every keyed Entity must eventually see" (a threshold table, a feature
flag, a config document) is therefore packaged as one builder method:

```rust
let routed = c.route_latest(thresholds, trades, |t: &Trade| t.symbol.clone());
```

```java
Stream<RoutedDocument<Thresholds>> routed =
        c.routeLatest(Thresholds.class, thresholds, trades, Trade::symbol);
```

It expands to a merge into ONE SDK-packaged single-entity procedure (node
identity `route_latest`) whose journaled state is
`{document, version, delivered: key → version}`, and returns that node's output
stream: a `(key, document)` pair in Rust, a `RoutedDocument<D>` record in Java
(the runtime needs a `Class` token for the routed payload). The consumer keys
by that key, which is what makes each delivery reach one Entity. Pure
expansion per §1: merge, single-entity procedure, journaled state — no new
runtime semantics, no wire change, no new replay path.

**Deliver on the key's own traffic — never a remembered-set fan-out.** A router
that remembered the keys it had seen and fanned a new document out over them
would emit keys the member may no longer own (a rescale moves keys away; one
out-of-range item seen during a reconcile window would poison the set), and
every such delivery would park against the partition guard. Delivering only on
a key's own next traffic item makes over-delivery impossible by construction.

**Latency is one tick per key**: a key learns of a new document on its next
traffic item, not when the document arrives. A key whose traffic has stopped is
never delivered to — so there is no push to idle keys, no retained broadcast to
late-born Entities and no rescale-time replay.

**The document is opaque.** It is retained and compared (`Eq` / `equals`) and
never parsed. A document equal to the retained one is inert (which absorbs the
re-poll a document source performs after every reconnect); a differing one
bumps the version, so every key is owed one delivery again. A document type
without value equality makes every arrival a new version.

Two routers in one operative need the node-named spelling
(`route_latest_named(.., "primary")` / the `routeLatest` overload taking a node
name) — by [`naming_conventions.md`](naming_conventions.md) rule 10 two unnamed
mentions of one class are one node, and here that means one retained document
shared between them.

## 5. Single-procedure auto-wiring and registration

Exactly **one** topology may be auto-wired: a single procedure from a
controller input to one named sink (`input → procedure → plugin(sink)`). A
sugared procedure that declares both an `id` and a `sink` generates that
operative (entry channel `"input"`, the sink channel named after the plugin)
and registers it automatically — the user writes no registration call. `sink`
without `id`, or `sink` on a procedure with no output, is refused.

- **Multi-procedure topology stays explicit.** More than one procedure, or any
  fan-out / multi-input edge, requires an explicit `channels()` body (Rust and
  Java) or, in Rust, the linear `flezha::operative! { id: "…", pipeline:
  input::<T> -> A -> B -> plugin::<U>("name") }` sugar for a strictly linear
  chain. The SDK refuses (compile error) rather than guessing a graph.
- **Auto-registration** uses a compile-time-collected registry. Rust:
  `inventory`, merged into the runner's declared set at every
  `FlezhaRunner::start()`. Java: the processor emits a
  `com.flezha.generated.GeneratedStreamOperatives.defineAll(FlezhaRunner)`
  aggregator covering every generated single-procedure operative and every
  hand-written `@StreamOperative` class (which must implement
  `com.flezha.sdk.operative.StreamOperative`). Explicit declaration —
  `FlezhaRunner::define_stream::<R>()` / `FlezhaRunner.defineStream(Class)`,
  legal before and after `start()`, with `remove_stream` / `removeStream` as
  the inverse — stays valid for hand-written operatives.
- **Duplicate factory id fails fast.** Declaring two operatives with the same
  `factory_id` on one runner fails at the second `define*` call with an error
  naming the colliding id (Rust: `FlezhaError::DuplicateFactoryId`).
- **A declaration that contradicts a durability contract is refused, naming
  both halves.** A channel may not declare a dropping overflow policy
  (`DropNewest` / `DropOldest`) on a class that declares `effectively_once` — a
  shed input's offset is still advanced past by the tier's watermark, so the
  loss is permanent under a contract that promises nothing is lost — nor on one
  that declares a correction window, whose answer may not depend on arrival
  timing. Both facts are frozen at registration, so the contradiction is
  decidable before any data moves, which is where an author can still see it.
  Shedding stays fully available to a class that promises neither. Refused in
  every SDK's builder and again in `flz_core`'s declaration validation, so a
  hand-written host declaration cannot slip past.

## 6. Canonical reference operative (cross-SDK conformance anchor)

Every SDK's conformance test asserts that this one operative — the
`StaticEntityOperative` shape — expands to the same topology and
associated-type mapping. It is a two-procedure, statically-entity, linear
operative.

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

| Channel | Kind            | Payload   | Producer           | Consumer / sink     |
| ------- | --------------- | --------- | ------------------ | ------------------- |
| `input` | entry           | `String`  | controller         | `StaticEntry`       |
| (hop)   | inter-procedure | `String`  | `StaticEntry`      | `StaticDownstream`  |
| (sink)  | plugin-bound    | `Vec<u8>` | `StaticDownstream` | plugin `test_kafka` |

**Expected associated-type mapping:**

| Procedure          | Input    | Output    | State              | Change                 | Mode                     |
| ------------------ | -------- | --------- | ------------------ | ---------------------- | ------------------------ |
| `StaticEntry`      | `String` | `String`  | `StaticEntryState` | derived (`Push`/`Set`) | static (`a`,`b`,`c`,`d`) |
| `StaticDownstream` | `String` | `Vec<u8>` | `()`               | `NoChange`             | single                   |

`Clock = WallClock`, `Config = ()` for both.

Where the fixtures live: the hand-written `StaticEntityOperative`
(`factory_id` `static_entity`) and its `operative!` twin `sugar_static_entity`
are in `test/apps/rust_runner/src/main.rs`; the Java twin is
`test/apps/java_runner/src/main/java/com/flezha/test/SugarStaticEntityStreamOperative.java`
(`sugar_static_entity`, `SugarEntry` with `@EntitiesFor`), asserted by
`SugarTwinTest.referenceOperativeMatchesContract`. Java has no pipeline macro,
so its two-procedure twin is an explicit `@StreamOperative` class with a
`channels()` body.

### 6.1 Canonical keyed-collection reference procedure

Every SDK's conformance test also implements this one procedure — a **rolling
per-key window** — and asserts its generated journaling matches a hand-written
twin and survives the replay-determinism harness. It is the reference for the
§3.1 keyed/collection rules and the §3.2 emit-or-skip return.

**Hand-written twin:** a `Change` with `PushBack(key, value)` /
`PopFront(key, n)`, an `apply_change` that folds each onto the keyed deque, and
a `run` that applies the same two mutations before the warmup check and
`ctx.emit`.

**Rust sugar:**

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

**Java sugar** is the same shape: `@Journal(Journal.Mode.MAP_DEQUE)
Map<String, Deque<Double>> window` on a `@State` class, `pushBackWindow` /
`popFrontWindow` on the generated `RollingWindowStateHandle`, and a `run`
returning `Optional<Double>`.

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
`replayEquals()`. Fixtures (`N = 3`): Rust `rolling_window` +
`RollingWindowTwin` in `test/apps/rust_runner/src/main.rs`; Java
`RollingWindow` (`java_rolling_window`) + `RollingWindowTwin`, asserted by
`SugarTwinTest.rollingWindowMatchesHandWrittenTwin`.

## See also

- [`naming_conventions.md`](naming_conventions.md) — method and node naming every SDK mirrors
- [`poll_core.md`](poll_core.md) — the kernel the sugar's expansion runs on
- [`stream_transforms.md`](stream_transforms.md) — `key_by` and the edge transforms
- [`entity_timers.md`](entity_timers.md) — the timer contract behind §3.4
- [`elixir_sdk.md`](elixir_sdk.md) — the behaviour-only authoring surface
- [`../architecture/runner.md`](../architecture/runner.md) — "One change, every SDK"
