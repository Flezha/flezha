# SDK Authoring Contract

For operator authors who use the attribute/annotation sugar and for SDK
implementers who must keep it identical across bindings. It specifies how a
declaration (`#[flezha::tasklet]`, the short-form attributes and
`flezha::operator!` in Rust) maps onto the `UnboundedOperator` / `Tasklet` / `Change` / `Channel`
trait (interface) surface. The sugar is **pure expansion**: a macro
(`sdk/rust/macros/`) writes exactly the items a user would write by hand, against the unchanged
runtime. There is no parallel runtime path, no new protobuf message and no
WAL-format change.

The Rust SDK has one tasklet form: the author's own type, `impl Tasklet for T`
under `#[flezha::tasklet(name = "...")]`, whose changes are types implementing
`Change<State>` applied through `ctx.change(...)`. The attribute generates no
type; it only completes the block ([§2.2](#22-one-form-completed-by-what-it-declares)).
The Java SDK has one form and no sugar: the
author's class implements `Tasklet<Input, State, Output>` and carries
`@Tasklet.Spec`, its changes are records applied through `ctx.change(...)`, and
nothing is generated ([`java_sdk.md`](java_sdk.md#tasklets)). The Elixir SDK expands the same
declarations through `use` macros — `use Flezha.SDK.Tasklet`,
`use Flezha.SDK.UnboundedOperator` and `use Flezha.SDK.BoundedOperator` take
the scalar settings as options named after the behaviour's own callbacks and
define only the callbacks written, so every default stays where
`function_exported?` resolves it; a `state do field … end` block is its §3
derive layer and `defchange` its manual boundary. The plain `@behaviour` form
keeps working unchanged — see [`elixir_sdk.md`](elixir_sdk.md). The Node SDK exposes factory functions —
`defineTasklet`, `defineUnbounded`, `defineBounded`, `registerFactory`, wired by a fluent
channel builder — and no decorators, because a decorator needs a build step,
does not apply to a plain function, and cannot reshape the decorated type,
which is what a typed graph needs ([`node_sdk.md`](node_sdk.md)). The Ruby SDK
exposes classes — `Flezha::Tasklet`, `Flezha::UnboundedOperator`,
`Flezha::BoundedOperator`, wired by the same fluent channel builder — and
reads which optional hooks a tasklet declares from the methods it defines, so
a declaration and its implementation cannot disagree
([`ruby_sdk.md`](ruby_sdk.md)). The PHP SDK exposes classes too —
`Flezha\Tasklet`, `Flezha\UnboundedOperator`, `Flezha\BoundedOperator`, wired
by the same fluent channel builder — with the scalar declarations in a
`#[Task]` attribute and the optional hooks read from the methods a tasklet
**overrides**, which is the closest PHP comes to Java's annotations without a
build step ([`php_sdk.md`](php_sdk.md)). The Python SDK exposes decorators —
`@flezha.tasklet`, `@flezha.state`, `@flezha.unbounded_operator`,
`@flezha.bounded_operator`, wired by the same fluent channel builder — over a
named tasklet class whose `run` takes the value and a `Ctx[State, Output]` and
whose other methods are the hooks, with the input, output, state and change
types all read from the type hints, which is what makes a wrong edge a
type-checker error before it is a registration one ([`python_sdk.md`](python_sdk.md)). A surface one
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
IS how a cell is rebuilt: a field mutated outside one is live now and gone
after a restart, and nothing reports the difference until the restart.

| SDK | Mechanism it ships |
| --- | --- |
| Rust | The borrow checker. `ctx.state()` hands out `&State` and `ctx.change(..)` is the only path to a mutation; there is no `&mut State` to assign through, so an unrecorded mutation does not compile. |
| Java | One recorded path. `ctx.change(...)` applies a change through its `applyTo` and records it; `ctx.state()` hands out the live object, so an unrecorded mutation compiles and runs, and `TaskletTestHarness.replayEquals()` is what catches it. |
| Elixir | Immutability plus one recorded path. `Flezha.SDK.Ctx.apply/3` folds a change through `apply_change/2` and records it; `run/3` returns the state it produced, and the contract is that the two agree — the recorded changes are what a restore folds. A `state do field … end` block generates that path: `set_<field>` / `push_<field>` and `defchange`'s named recorder each journal exactly one change through `Ctx.apply/3`, so the hand-written fold is only for what the block does not cover. Deterministic effects come from the same ctx: `Ctx.stable_id/1` answers the frozen FNV-1a-128 over the Cell's identity, the invocation's starting seq and a per-invocation ordinal, and `Ctx.checkpoint/1` closes an effectively-once interval. There is no replay harness yet, so the contract is stated rather than checked. |
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
| Rust | yes | 29 compile-fail tests in `sdk/rust/tests/ui/` |
| Java | yes, through the generic `Tasklet<…>` arguments | `sdk/java/.../UnboundedOperatorConfigurationException` cases |
| Node | yes, through the channel builder's type parameters | `@ts-expect-error` fixtures in `sdk/node/types-tests/`, compiled under `tsc --noEmit`, plus a negative control that strips the directives and requires each pinned line to still produce a diagnostic |
| Elixir | no — the language cannot | registration-time refusals only |
| Ruby | no — the language cannot | registration-time refusals only, in `sdk/ruby/test/authoring_test.rb` |
| PHP | no — the language cannot | registration-time refusals only, in `sdk/php/tests/AuthoringTest.php` |
| Python | yes, through the generic `Edge[T]` / `Tasklet[I, O]` parameters over a `py.typed` package | `sdk/python/tests/typing_fixtures/wrong_edge.py`, which `sdk/python/tests/test_typing.py` requires pyright to reject, against `well_typed.py` as the negative control |

Before writing a new SDK, read
[`poll_core.md` → "The core decides, the SDK brings the threads"](poll_core.md#the-core-decides-the-sdk-brings-the-threads):
it states which side owns ordering, exclusivity and the limit on work underway,
and which side owns the threading model.

## 1. Escape-hatch equivalence (non-negotiable)

- Generated code is equivalent to a valid hand-written `Tasklet` /
  `UnboundedOperator` implementation. It compiles and runs identically; no
  protocol, controller or WAL change is required for it to run.
- An operator MAY mix sugared and hand-written tasklets wired together.
- "Ejecting" a sugared tasklet to a full impl is a paste of its expansion —
  the operator behaves identically afterwards with no other change.

The replay-determinism harness (`TaskletTestHarness::replay_equals` in Rust,
`TaskletTestHarness.replayEquals()` in Java) is the conformance gate for
**all** tasklets, sugared and hand-written.

## 2. Declaration → trait items

A Rust tasklet is an `impl Tasklet for T` block under `#[flezha::tasklet(...)]`.
The block writes what only the author knows; the attribute writes the rest.
Java infers nothing: its `Tasklet<Input, State, Output>` names the
three types, and §2.2 says where the rest comes from.

| Written in the block, or the attribute key         | Trait item                                          |
| --------------------------------------------------- | --------------------------------------------------- |
| `type Input = I`                                    | `Input` (required)                                  |
| `type Output = O`, else nothing                     | `Output`, default `()`: no output channel           |
| `type State = S`, else nothing                      | `State`, default `()`: stateless                    |
| `type Change = C` (a `Change<S>`), else nothing     | `Change`, default `NoChange`: nothing journaled     |
| `name = "…"` (required)                             | `NAME` — Rust: the key; Python: the `name` of `@flezha.tasklet(name=…)`; Ruby: `tasklet_name "…"` in the class body; Java names every tasklet in `@Tasklet.Spec(name = …)`; PHP in `#[Task(name: …)]` |
| `fn event_time`, else `logical_clock`, else nothing | `CLOCK`: `EventTime`, `Logical`, else `Wall`        |
| `fn init_state`, else nothing                       | `init_state` returning `State::default()`           |
| `fn compensate` / `fn cells_for` / `fn max_cells` / `fn mailbox_capacity` | the matching `*_OVERRIDDEN` flag set to `true` |
| `capacity = N`                                      | `mailbox_capacity()` and its flag                   |
| `max_cells = N`, `idle_ttl_seconds = N`             | `max_cells()` and its flag, `cell_idle_ttl()`       |
| `keys = [..]` with a written `fn cells_for`         | `cell_keys()`, `CELLS_FOR_OVERRIDDEN`               |
| `schema_version`, `min_readable_schema_version`, `compact_after_bytes`, `max_timers_per_cell` | the item of that name |
| `on_full = …` / `onFull = "…"` (short form only)    | overflow policy of the generated entry channel (§5): `drop_nothing`, `drop_newest`, `drop_oldest` |
| `input = "…"` (short form only)                     | name of the generated entry channel (§5); absent, that channel is named `input` |

An item the block writes that the attribute would also write is refused
naming it, as are an unknown key, `logical_clock` beside a written
`event_time`, and `keys` without `cells_for` (or the reverse).

### 2.1 Emitting: the context

`run` returns `Result<(), TaskletError>` and emits through its context, any
number of times per input, each emit typed as the tasklet's `Output`:

```rust
fn run(&mut self, trade: Trade, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
    ctx.emit_keyed(closed.symbol.clone(), closed)?;   // the candle that just closed
    ctx.emit_keyed(opened.symbol.clone(), opened)     // and the one that opened
}
```

A value of any other type does not compile. Only a function short form (§5)
returns its output instead.

### 2.2 One form, completed by what it declares

- **Java** — one tier, the author's own class. `Input`, `State` and `Output` are
  read off the implementing class's generic supertype chain: a direct
  `implements Tasklet<…>` with concrete arguments, or a concrete subclass of a
  generic intermediate class (type variables are substituted at every hop). An
  argument still generic at the concrete class is **never guessed**:
  `registerFactory` fails naming the class and the parameter, and nothing is
  registered — a startup error, not a dispatch error. The name and scalar
  settings come from `@Tasklet.Spec`, the initial state from the state class's
  no-arg constructor, and the encoded types from discovery (the state class and
  the change records nested in the tasklet or its state), each unless the class
  writes the matching hook (`initState`, `kryoTypes`). The optional hooks
  `onTimer`, `eventTime`, `compensate`, `cellsFor`, `initState`, `migrateState`,
  `migrateChange` and `kryoTypes` are declared by writing them: registration
  reads which ones the class overrides, itself or through a class above it, as
  Ruby reads the methods a tasklet defines and PHP the methods it overrides, so
  a hook cannot be written and left inactive. Writing `eventTime` is what makes
  the clock event time, and `@Tasklet.Spec(logicalClock = true)` the logical one.
- **Rust** has one form too, the author's own type. `#[flezha::tasklet]` reads the
  `impl Tasklet` block at compile time, which is Java's registration-time
  reading done by the compiler: it writes `NAME`, the settings given as keys,
  the `*_OVERRIDDEN` flags from the hooks the block writes, `CLOCK`, a default
  `init_state`, and `State = ()`, `Change = NoChange`, `Output = ()` where the
  block writes none. Stable Rust has no associated-type defaults and cannot
  tell an overridden method from an inherited one, which is why a macro
  completes the block rather than defaults on the trait. The same block
  without the attribute writes every one of those items by hand; that
  reference form is documented in
  [`rust_operator_architecture.md`](rust_operator_architecture.md#the-reference-form)
  and composes with the everyday form in one operator.

**Explicit always wins.** A written Java hook (`initState`, `kryoTypes`) is
authoritative over the default it replaces, and a written Rust `init_state`
replaces the default the attribute would write. A hook cannot be written and
left unclaimed in either SDK: the Rust flag follows the written `fn`.

## 3. State → Change derivation, and the manual boundary

Rust declares its change types by hand, as its one form: a struct or an enum
implementing `Change<State>` whose `apply_to` performs the mutation, recorded
with `ctx.change(...)`. A change enum is encoded by variant index, so a new
variant goes at the end.

**Elixir derives field-granular changes from a block.** `use Flezha.SDK.Tasklet`
makes `state do field :qty, :integer, default: 0 end` available. A `field`
takes a name, a type (`:integer`, `:float`, `:string`, `:boolean`, `:list`,
`:map`, `:term`) and a `default`, and the block generates `init_state/1`
returning the map of defaults, `state_from_json/1` reading each field's string
key, `apply_change/2` clauses for `{:set, field, value}` on every field and
`{:push, field, item}` on every `:list` field, and the recorders
`set_<field>(state, ctx, value)` and `push_<field>(state, ctx, item)`, each
journaling through `Flezha.SDK.Ctx.apply/3`. A module with a `state` block that
also writes `apply_change/2` or `state_from_json/1` by hand fails to compile,
naming both: the block owns them. §3.1's keyed-map and deque modes are
**deferred for Elixir** — its block carries the field and `:list` modes only.
A field-granular derivation is likewise **deferred for Python, Node, Ruby and
PHP**, which declare their change types by hand. Java declares its change types
by hand too, as its one form: a record implementing `Change<State>` whose
`applyTo` performs the mutation.

**The manual boundary.** Only field-granular mutations are derivable. A
mutation that is conditional, spans multiple fields atomically, or is a
computed function of the prior state is **not** expressible as a single
generated variant: the SDK requires a hand-written `Change` and reports an
error rather than journaling incorrectly. In
Elixir, `defchange name(state, args…) do body end` is that hand-written change:
it generates the `apply_change(state, {:name, args…})` clause and the recorder
`name(state, ctx, args…)`, so a mutation spanning several fields is one journal
record.

### 3.0 Changing a State or Change type after it has been deployed

> **A factory id is single-language.** Because the persisted encodings below are
> per-SDK, an operator is placed only on a runner of its factory's language,
> and a restore refuses a record another SDK stamped. Re-authoring a factory in
> another language is therefore not an upgrade — it is drain-and-cutover under a
> new factory id (stop, compact, `flz state get`, start the new factory), or
> clearing the old factory's durable state so it re-establishes.

The persisted encodings are **positional and not self-describing** (postcard in
Rust, Kryo in Java). Reordering fields, removing one, or changing a field's
type is not a compatibility question the codec can answer: last version's bytes
either run out mid-decode or land in the wrong fields. So the rule is:

> **Bump `SCHEMA_VERSION` on any non-additive edit to the Tasklet's `State`
> or its `Change` type** — not to the `Change` type alone.

The version is stamped in every record's 16-byte header, and every restore
resolves it before decoding anything (`docs/sdk/protocol_spec.md` → "Schema
evolution"). A bump alone is fail-closed: the old records are refused, loudly,
by name. To *read* them, declare how far back you read and how:

| | Rust | Java | Elixir |
|---|---|---|---|
| version | `const SCHEMA_VERSION: u32` | `@Tasklet.Spec(schemaVersion = n)` | `schema_version/0` |
| readable floor | `const MIN_READABLE_SCHEMA_VERSION: u32` | `@Tasklet.Spec(minReadableSchemaVersion = n)` | `min_readable_schema_version/0` |
| state migration | `fn migrate_state(from_version, bytes)` | `State migrateState(int, byte[])` | `migrate_state(from_version, old_term) :: {:ok, state} \| {:error, reason}` |
| change migration | `fn migrate_change(from_version, bytes)` | `Change<State> migrateChange(int, byte[])` | `migrate_change(from_version, old_term)` |

Both migrations default to refusing, so declaring a floor without implementing
them fails at the first affected record rather than guessing. Keep the old type
in your own source and decode it inside the migration — the runtime cannot know
a type that no longer exists in this build. Rust re-exports the codec it wrote
the bytes with as `flezha_runner::state::postcard`; Java decodes with a
`KryoCodec` that registers the old types under the ids the old version gave
them (a listed `kryoTypes()` in its old order, or the ids derived from the old
class names); Elixir hands the migration the decoded term, its body codec being
`:erlang.term_to_binary/2` with no second codec to choose.

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
(`TaskletTestHarness::with_payload`, `sdk/rust/tests/state_evolution.rs`).

Two refusals are not migrations and cannot be made into them: a record older
than your declared floor, and a record from a version **newer** than the one
running (a downgrade — older code meeting newer state). Rolling a deploy back
past a schema bump means restoring the pre-deploy snapshot.

The reference fixtures are `schema_evolution_v1` / `schema_evolution_v2` in the
Rust and Java test apps, driven by `test/system/schema_evolution_e2e_test.exs`.

### 3.1 Keyed and collection changes

A change to one entry of a map, or to one end of a collection, is written as a
delta rather than as the whole value, so a WAL record stays one entry long. The
shapes every SDK's conformance twin uses:

| State field                               | Change                            | Its fold                                             |
| ----------------------------------------- | --------------------------------- | ---------------------------------------------------- |
| any `T`                                   | `Set(T)`                          | replace the field                                    |
| `HashMap<K, V>`                           | `Set(K, V)`                       | `map.insert(key, value)`                             |
| `Vec<U>` / `HashMap<K, Vec<U>>`           | `Push(U)` / `Push(K, U)`          | append, creating the keyed vector on first touch     |
| `VecDeque<U>` / `HashMap<K, VecDeque<U>>` | `PushBack(..)`, `PopFront(.., n)` | push or pop, creating the keyed deque on first touch |

**`Set(K, V)` is the universal fallback.** When a per-entry value is computed
(an EMA, an RSI, a whole candle), compute it in `run` and record the entry;
replay re-applies it by insertion. Prefer the collection deltas only for
**growing collections** (a rolling window, an appended bar history):
recording the whole collection each tick grows every WAL record to the
collection's size, whereas a delta keeps each record one entry. Once a tasklet
is keyed by what a map would have keyed by, the Cell *is* the key and its
collections stop being maps.

A pop is saturating: popping more than the collection holds empties it rather
than failing, so a delta recorded against a longer window replays safely.

### 3.2 Emit-or-skip output return

A function short form (§5) returning `Option<O>` (Rust) emits when present and
emits nothing when absent, without the author touching the context. The unit
return (no output channel) and the unconditional `O` return (always emit) are
the other two shapes. A Rust tasklet's `run` returns no output and emits
through its typed context, so emitting nothing is not calling `emit`. A Java `run`
returns nothing and emits through `ctx.emit`, so emitting nothing is not
calling it. A PHP `run`, a tasklet's or a short form's, is declared `: void`
and emits through `$ctx->emit`, so emitting nothing is not calling it either.
A Python function short form emits what it returns, `None` emitting nothing. A
Python tasklet class's `run`, and a short form declared on a class, returns
`None` and emits through `ctx.emit`, and a `run` that returns a value fails
the input.

**Keyed output.** On a `.keyed()` output channel the plain emit carries
the emitting Cell's **own key** (the key its input routed by), so a keyed
tasklet never re-supplies what the runtime already knows. A tasklet passes a
key itself only where its output's natural key *differs* from its Cell's, or
where the tasklet is single-cell and so has no key at all (there, the plain
emit is refused). A Rust body passes the key itself,
`ctx.emit_keyed(key, value)`; a Java body passes
the key itself, `ctx.emitKeyed(key, value)`.

### 3.3 Single hand-written path determinism

A Rust change's `apply_to` is the one place a mutation is written.
`ctx.change(..)` runs it live and records the change; a warm restart folds the
recorded changes through the same `apply_to`. So the live-vs-replay divergence
`replay_equals` / `replayEquals()` exists to catch comes only from a fold that
reads something besides the state and the change (ambient time, a counter, an
external read), or from a change that does not survive being written down. The
Rust harness replays each change encoded and decoded as the runtime persists
it, so it catches both.

### 3.4 The `on_timer` contract

A Tasklet can act because time passed, not only because an input arrived:
`ctx.timer_at_event_time(ts)` / `ctx.timer_after(d)` (Rust) —
`ctx.timerAtEventTime(ts)` / `ctx.timerAfter(d)` (Java) —
`timer_at_event_time(ctx, ts)` / `timer_after(ctx, ms)` (Elixir, imported by `use`) —
register a per-Cell deadline, and `Tasklet::on_timer` /
`Tasklet.onTimer` / `on_timer/4` handles the firing with the same state and
context `run` gets.
Registration returns the absolute timestamp it is scheduled at; that value is
the handle a cancellation takes.

Three rules make the callback part of the determinism contract:

1. **`fired_at` is the SCHEDULED timestamp**, never the wall clock at firing.
   `ctx.now()` equals `fired_at` inside the callback, so a chained
   `ctx.timer_after(..)` schedules off the timer that woke it.
2. **`timer_after(d)` is anchored on `ctx.now()`**, not on an ambient read —
   which is what makes a replayed invocation register the same absolute
   timestamp the live one did.
3. **A registration made during a replay IS registered.** `ctx.spawn_bounded`
   answers the outcome `skipped_on_replay` because it is an external side effect that must
   not happen twice; a timer is internal state, and skipping it on replay
   would rebuild a pending set the live run never had.

`on_timer`'s default panics (Rust) / throws (Java) naming the Tasklet, and
a firing on an Elixir Tasklet without `on_timer/4` parks non-retryably
naming the Tasklet and the callback: registration is a runtime call, so a
Tasklet that registers a timer without handling it cannot be refused at
build time. Full contract, including which
kind to reach for, in [`cell_timers.md`](cell_timers.md).

The contract holds in both kinds. In a bounded operator, a firing is one more thing
that starts a pass: it opens a pass at the tasklet that set the timer, or
joins that tasklet's stage while it runs, or waits its turn behind the pass in
flight, and its timer leaves the pending set when that stage commits. Only a
`compensate` hook has no timers; there every timer call answers
`TimersUnavailable`.

## 4. Cell mode is an explicit opt-in, never inferred

The mode changes lifecycle and on-disk state layout, so it is always an
explicit author choice — never inferred from the `run` body. Each opt-in maps
1:1 onto the trait surface:

| Opt-in (Rust / Java)                                                         | Generated trait surface                                                               |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| none                                                                         | single-cell (empty `cell_keys()`)                                                 |
| `keys = [..]` + a written `cells_for` / `@Tasklet.Spec(keys = {..})` + a written `cellsFor` | `cell_keys()` = keys, `CELLS_FOR_OVERRIDDEN = true` (Rust), the written `cells_for` routes |
| `idle_ttl_seconds = s` / `max_cells = n` (Rust; dynamic knobs)            | `cell_idle_ttl()` / `max_cells()` overrides — the KEY is not here. Java writes `@Tasklet.Spec(idleTtlSeconds = s, maxCells = n)`; Elixir declares `cell_idle_ttl_ms/0` / `max_cells/0` |

Declaring only one of `keys` / `cells_for` is refused. Static keying is
declared **on the tasklet**; dynamic keying is declared **on the wiring** —
`.key_by(f)` / `.keyBy(f)` on the stream that feeds it
([`stream_transforms.md`](stream_transforms.md)). The sugar therefore offers no
`cell_key` hook: `cell_key = …` (Rust) is a compile error naming `key_by` as
its replacement, and a Java tasklet has no member that could spell one. The
two modes stay mutually exclusive — a keyed edge into a tasklet declaring
`cell_keys()` fails the build with `KeyByOnUnkeyableTarget`.

**Per-cell state init.** By default a stateful tasklet's
`init_state` returns `State::default()` / `new State()`. When a cell's
starting state is a function of its key, write the hook
(Rust: `fn init_state(cell_key: Option<&str>) -> Self::State`; Java: a
written `State initState(Optional<String> cellKey)` override). It receives the cell key (absent
in single-cell mode and the test-harness default). The restore payload stays
runtime-applied after `init_state` returns, so the hook never sees it.

### 4.1 Reaching every keyed Cell: the `route_latest` building block

A keyed tasklet cannot receive a broadcast: both keying declarations are
total — `cells_for` names the Cells an input belongs to, `.key_by(f)`
names exactly one — and inside a partitioned member the computed key is
admitted against that member's key-group range. "One operator-wide document
that every keyed Cell must eventually see" (a threshold table, a feature
flag, a config document) is therefore packaged as one builder method:

```rust
let routed = c.route_latest(thresholds, trades, |t: &Trade| t.symbol.clone());
```

```java
Stream<RoutedDocument<Thresholds>> routed =
        c.routeLatest(Thresholds.class, thresholds, trades, Trade::symbol);
```

It expands to a merge into ONE SDK-packaged single-cell tasklet (node
identity `route_latest`) whose journaled state is
`{document, version, delivered: key → version}`, and returns that node's output
stream: a `(key, document)` pair in Rust, a `RoutedDocument<D>` record in Java
(the runtime needs a `Class` token for the routed payload). The consumer keys
by that key, which is what makes each delivery reach one Cell. Pure
expansion per §1: merge, single-cell tasklet, journaled state — no new
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
late-born Cells and no rescale-time replay.

**The document is opaque.** It is retained and compared (`Eq` / `equals`) and
never parsed. A document equal to the retained one is inert (which absorbs the
re-poll a document source performs after every reconnect); a differing one
bumps the version, so every key is owed one delivery again. A document type
without value equality makes every arrival a new version.

Two routers in one operator need the node-named spelling
(`route_latest_named(.., "primary")` / the `routeLatest` overload taking a node
name) — by [`naming_conventions.md`](naming_conventions.md) rule 10 two unnamed
mentions of one class are one node, and here that means one retained document
shared between them.

## 5. The single-tasklet short form, and registration

Exactly **one** topology may be written short: a single tasklet from a
controller input to at most one named sink (`input → tasklet → plugin(sink)`,
or `input → tasklet` with no sink), as an operator of either kind. **Every
SDK carries it**: Rust, Java, Python, Node, Ruby, PHP and Elixir.

**The short form is an operator declaration**, written at the SDK's
kind-named operator entry point, the one a full-form operator already uses.
An operator declaration is a short form exactly when it carries its one
tasklet's body **instead of** a topology body, `channels`, the one name both
kinds give it in every SDK.
The body is `run`, the name every SDK gives a tasklet's body. The kind is the
entry point's own name (in PHP, the base class the `#[Operator]` class
extends), never a key. The identity is the operator's factory id, spelled as
that SDK's full form spells it, and is **written by the author**: an SDK that
otherwise derives a name does not derive a factory id, because the factory id
is an address a user types and a rename must not move it.

| SDK | Unbounded short form | Bounded short form |
| --- | --- | --- |
| Rust | `#[flezha::unbounded_operator(id = "echo", sink = "out", input = "in")] fn echo(s: String) -> String`, or the same attribute on an `impl Tasklet for Echo` block | `#[flezha::bounded_operator(...)]` |
| Java | `@UnboundedOperator(id = "echo", sink = "out", input = "in") class Echo implements Tasklet<String, Void, String> { void run(String s, Ctx<Void, String> ctx) }` | `@BoundedOperator(...)` |
| Python | `@flezha.unbounded_operator(id="echo", sink="out", input="in", codec="text") def echo(s: str) -> str`, or the same decorator on a class whose `run(self, s, ctx)` emits through `ctx` | `@flezha.bounded_operator(...)` |
| Node | `defineUnbounded({ factoryId: 'echo', sink: 'out', input: 'in', codec: 'text', run })` | `defineBounded({ ... })` |
| Ruby | `class Echo < Flezha::UnboundedOperator; factory_id "echo"; sink "out"; input "in"; codec :text; def run(s, ctx) = ctx.emit(s); end` | `< Flezha::BoundedOperator` |
| PHP | `#[Operator(factoryId: 'echo', sink: 'out', input: 'in', codec: ChannelCodec::Text)] final class Echo extends UnboundedOperator { public function run(mixed $s, Ctx $ctx): void }` | `extends BoundedOperator` |
| Elixir | `use Flezha.SDK.UnboundedOperator, factory_id: "echo", sink: "out", input: "in"` with `run/3` | `use Flezha.SDK.BoundedOperator, ...` |

**`sink` is optional.** With a `sink`, the generated operator declares an
entry channel, the tasklet, and a plugin sink of that name. With no `sink`,
it declares the entry channel and the tasklet and no plugin channel, which is
the topology a full-form operator declares when its channel ends at a
tasklet: the operator that only keeps state is one declaration. `input` and
`codec` are accepted either way, because an entry channel exists either way.

**The generated operator is the hand-written one, nothing more.** One entry
channel named by `input` (or `input` when none is written) and decoded by
`codec` where one is written (Python, Node, Ruby, PHP); the tasklet; the sink
when one is written; a factory id equal to the identity; and every class-level
setting at that SDK's default for that kind, with one exception: a bounded operator
short form may declare its loop mode. An unbounded short form is a default
stream. A bounded short form is a `once` bounded operator unless it declares another
loop mode: its entry channel receives the instance's first message, from a
spawn, `flz operator send` or `flz operator start --text`, the tasklet runs
once, its output reaches the sink after the commit, and the instance
completes; `--init-values` is the tasklet's initial state, as on an unbounded operator.
With `per_input` the tasklet runs once per message and the instance stays.
The loop mode is written in the spelling the SDK's full form gives it:
`loop_mode = "per_input"` on the Rust attribute, `loopMode = LoopMode.PER_INPUT`
on `@BoundedOperator`, `loopMode: 'per_input'` on the Node definition,
`loop_mode="per_input"` on the Python decorator, `loop_mode :per_input` in the
Ruby class body, `#[Bounded(loopMode: LoopMode::PerInput)]` beside
`#[Operator]` in PHP, `loop_mode: :per_input` on the Elixir `use`. A loop mode
or rollback mode outside `once` / `per_input` / `chained` and
`all_or_nothing` / `keep_progress` is refused where it is written, naming the
value.

**The expansion is an ordinary tasklet and an ordinary operator, and the
tasklet is named after the factory id, verbatim.** A cell lives at
`<state_root>/<namespace>/<class>/<tasklet_id>/<cell_key>`, so a
tasklet's name is a durable address; pinning it to the factory id is what
makes growing out of the short form keep its state. Where an SDK generates
code, the two are separate types and the author-facing name is the
operator's: Rust emits the operator `Echo` and the tasklet `EchoTasklet`;
Elixir generates the nested module `Echo.Tasklet`. Java generates nothing: the
class the author wrote is the tasklet, and registering it builds the operator
at registration.
Rust's short form on an `impl Tasklet for Echo` block generates no type: it
completes the block as `#[flezha::tasklet]` does, names the tasklet after the
`id`, accepts the tasklet setting keys beside the operator keys, and implements
the operator trait on `Echo` itself. That is the form for a one-tasklet
operator with state or hooks; a function short form takes no state and
refuses a `&mut` parameter with `ShortFormStateOnFunction`.
The generated tasklet composes in another operator's topology body like any
tasklet.

Growing out of the short form is a move, not a rewrite. With the unbounded short
form `echo` above, the full form that keeps every cell where it was is:

```rust
#[derive(Default)]
struct EchoTasklet;

#[flezha::tasklet(name = "echo")]
impl Tasklet for EchoTasklet {
    type Input = String;
    type Output = String;
    fn run(&mut self, s: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(s)
    }
}

struct Echo;
impl UnboundedOperator for Echo {
    fn factory_id() -> &'static str { "echo" }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<EchoTasklet>().sink_named("out");
    }
}
```

The tasklet keeps the name `echo` and the operator keeps the name `Echo`,
so `register_factory(Echo)` is unchanged and the cells stay at
`<class>/echo/<cell_key>`.

Per-tasklet settings the declaration already carries keep working (cell
keys and `cells_for`, mailbox capacity, clock, schema version), because they
belong to the tasklet, not to the class. Where an SDK names an entry channel's
overflow policy on the declaration (`on_full` / `onFull` in Rust and Java), it is
the short form's, describing the entry channel it generates.

- **The entry channel takes the name the author writes, and `input` when the
  author writes none.** That name is the address a user injects on and the
  name a forwarded frame has to match, so growing out of the short form keeps
  it. In Rust it is also an optional name on the `flezha::operator!` pipeline
  head, `input::<String>("prices")`, in the same position the `plugin`
  terminator takes one.

- **Multi-tasklet topology stays explicit.** More than one tasklet, any
  fan-out or multi-input edge, a keyed edge, or any class-level setting other
  than a bounded operator's loop mode (a duration, persistence off, the
  effectively-once tier, a correction window, a rollback mode, synchronous
  durability) requires the full form: an explicit `channels()` body, or, in
  Rust, the linear `flezha::operator! { pub struct Name; id: "…", pipeline:
  input::<T> -> A -> B -> plugin::<U>("name") }` sugar for a strictly linear
  chain. The SDK refuses rather than guessing a graph. A retry ladder is not a
  class setting: it is the edge's `retry_max` / `retry_backoff_ms`, for both
  kinds.

- **Registration is written by the author, in every SDK.** The author hands the
  short form to the runner's `register_factory` like any operator, naming the
  declaration they wrote and never a generated type; nothing is harvested,
  scanned or collected. A program that declares a short form and never
  registers it declares no operator.

### 5.1 One `register_factory`, one `unregister_factory`

Both authoring surfaces are rooted in a shared `Operator` base: in Java an
interface `UnboundedOperator` and `BoundedOperator` both extend, and in Rust a
sealed `Operator<K>` trait the SDK implements for every `UnboundedOperator` (as
`Operator<UnboundedKind>`) and every `BoundedOperator` (as
`Operator<BoundedKind>`), so an author implements only the kind trait. The
base's shared members are `factory_id()` and, in Java, `channels(ChannelBuilder)`:
both kinds declare their graph in a body named `channels`, on the same builder,
in every SDK (in Rust each kind trait declares it, because an author cannot
implement the sealed base). The kind-divergent surface (the persistence opt-in
on one side; loop mode, rollback mode, maximum duration, self-termination and
synchronous durability on the other) stays on the per-kind traits.

An operator declares its kind where it is written, so a runner call that names
the kind again says nothing new and can disagree with the declaration. Every
runner has **one** registration method and **one** inverse, both taking the
same argument:

| SDK | Register | Unregister | How the kind is read |
| --- | --- | --- | --- |
| Rust | `runner.register_factory(EchoOperator)?` | `runner.unregister_factory(EchoOperator)?` | the sealed `Operator<K>` impl; `K` is inferred from the value |
| Java | `runner.registerFactory(EchoOperator.class)` | `runner.unregisterFactory(EchoOperator.class)` | the kind interface the class implements |
| Python | `runner.register_factory(echo)`, `runner.register_modules("app.ops")` | `runner.unregister_factory(echo)` | read from the operator |
| Node | `await runner.registerFactory('./echo.mjs')` | `await runner.unregisterFactory('./echo.mjs')` | read from the module's exports |
| Ruby | `runner.register_factory(Echo)` | `runner.unregister_factory(Echo)` | read from the class |
| PHP | `$runner->registerFactory(Echo::class)` | `$runner->unregisterFactory(Echo::class)` | read from the class |
| Elixir | `Flezha.SDK.register_factory(runner, Echo)`, child spec `operators: [Echo]` | `Flezha.SDK.unregister_factory(runner, Echo)` | the behaviour the module's `use` or `@behaviour` names |

Both are methods on the concrete runner handle (functions over the handle in
Elixir), legal before and after `start()`, and generic over the shared base
where the host language expresses such a bound. Rust passes the operator as a
value because a generic function over two unrelated traits cannot be called
with a type argument alone without naming the marker; operators are unit
structs, so the value costs nothing.

`unregister_factory` undoes a registration made before the runner starts in
every SDK, and one made after start in every SDK whose runner can stop serving
a started declaration. A runner that cannot **rejects** a post-start call with
an error that says so, and never ignores it.

An operator whose kind cannot be read — it declares both kinds, or neither —
is refused at registration with an error beginning `AmbiguousOperatorKind` or
`UnknownOperatorKind` naming the class or module. In Rust such a type cannot
be passed to `register_factory` at all: the kind parameter is unresolvable, so
it is a compile error. In Java a class implementing both kind interfaces does
not compile unless it resolves the `persistenceEnabled()` default both declare,
and one that does is refused at registration.

**The runner announces the kind it read.** Every factory entry a runner ships,
in its connect frame and in each `UpdateFactories`, carries `kind`, exactly
`"unbounded"` or `"bounded"`: Rust and Elixir fill it from the registered class,
and the shared core fills it for Java, Node, Python, Ruby and PHP from the
registry the class declaration landed in. The controller establishes a
factory's kind from the first announcement, as it establishes its language,
and a runner announcing the other kind, or none, is never a placement
candidate for it. So a start names only the factory: `flz operator start` and
`flz schedule add` take no `--kind`, a schedule of a factory announced as
unbounded is refused at `add`, and a tasklet's spawn naming one answers
`factory_not_bounded`. Adding a future operator kind means implementing the
base and teaching `register_factory` / `unregister_factory` the kind, without
editing either existing authoring surface and without adding a runner method.

Node's registration imports a module, which is asynchronous, so its pair is
awaited: before `run()` it imports, reads the exports and records the
specifier, so a module that fails to load or exports no operator rejects at
the line that registers it; after `run()` it also declares the operator
against the running core and every worker.

### 5.2 The three refusal names

Each name appears **verbatim** in the error the author reads: the compile
error text in Rust, Java and an Elixir `use` declaration, the declaration
error's refusal list and message in Python, Node, Ruby and PHP, and the printed
form of the registration error in Elixir. These are the names the conformance
refusal table's short-form rows are checked against:

| Name | Condition |
| --- | --- |
| `ShortFormWithTopology` | an operator declaration carries a tasklet body and also a `channels` body |
| `SinkWithoutOutput` | a `sink` on a body the SDK can see declares no output |
| `DuplicateFactoryId` | a second operator, of either kind, under a factory id the runner holds |

They are not `TopologyIssue` variants: the core never sees a short form. An SDK
whose tasklet bodies declare no output type the SDK can read (Node, Ruby,
PHP, Elixir) owes no `SinkWithoutOutput` and says so in its conformance
manifest. In Ruby no `run` answers output either: what it answers is ignored and
a value leaves only through `ctx.emit`, so nothing in a class says whether it
emits. A declaration with neither body, or with no written identity, is
refused by the rule every operator already obeys, not by a short-form name.
There is no other short-form refusal: an operator declaration always has a
kind, and `input` and `codec` need no sink.

**Duplicate factory id fails fast across kinds.** Declaring a second operator
under a factory id the runner already holds fails at that call, whichever kind
either one is, with an error naming `DuplicateFactoryId` and the colliding id
(Rust: `FlezhaError::DuplicateFactoryId`, whose display begins with the name;
Elixir: the tuple `{:duplicate_factory_id, id}`, whose printed form carries
it).

- **A declaration that contradicts a durability contract is refused, naming
  both halves.** A channel may not declare a dropping overflow policy
  (`DropNewest` / `DropOldest`) on a class that declares `effectively_once` — a
  shed input's offset is still advanced past by the tier's watermark, so the
  loss is permanent under a contract that promises nothing is lost — nor on one
  that declares a correction window, whose answer may not depend on arrival
  timing. Both facts are frozen at registration, so the contradiction is
  decidable before any data moves, which is where an author can still see it.
  Shedding stays fully available to a class that promises neither. Refused in
  every SDK's builder — `DroppingPolicyUnderEffectivelyOnce` names the channel
  and the policy in the Elixir SDK, which is its own core — and again in
  `flz_core`'s declaration validation for the six hosts on it, so a
  hand-written host declaration cannot slip past.

### 5.3 A setting of one kind is refused on the other, by name

What an author writes reaches the judgement of its class's kind; no SDK drops
a setting because the kind does not carry it. An unbounded-only setting on a
bounded operator is refused at registration as `EffectivelyOnceOnBounded` (the
effectively-once tier), `CheckpointOnBounded` (any checkpoint setting) or
`CorrectionWindowOnBounded` (the correction window); a bounded-only setting
on an unbounded operator as `SyncDurabilityOnUnbounded`, and in PHP `#[Bounded operator]` on a class
extending `Flezha\UnboundedOperator` is refused naming the attribute. The shared
core judges the five host SDKs' declarations; Rust and Elixir judge their own
under the same names. Synchronous durability has one spelling per SDK, the
class-level one; no channel builder carries it. Timers belong to a tasklet
and work in either kind; `compact_after_bytes` belongs to a tasklet and is accepted on
either kind.

## 6. Canonical reference operator (cross-SDK conformance anchor)

Every SDK's conformance test asserts that this one operator — the
`StaticCellOperator` shape — expands to the same topology and
associated-type mapping. It is a two-tasklet, static-mode, linear
operator.

**Authored (Rust):**

```rust
#[flezha::tasklet(name = "static_entry_tasklet", keys = ["a", "b", "c", "d"])]
impl Tasklet for StaticEntry {
    type Input = String;
    type Output = String;
    type State = StaticEntryState;
    type Change = EntrySeen;

    fn cells_for(input: &String) -> Vec<&'static str> { static_seg(input) }

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(EntrySeen(input.clone()));
        ctx.emit(input)
    }
}

#[flezha::tasklet(name = "static_downstream")]
impl Tasklet for StaticDownstream {
    type Input = String;
    type Output = Vec<u8>;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.into_bytes())
    }
}

flezha::operator! {
    pub struct StaticCell;
    id: "static_cell",
    pipeline: input::<String> -> StaticEntry -> StaticDownstream -> plugin::<Vec<u8>>("test_kafka")
}
```

**Expected expanded topology** (identical across SDKs):

| Channel | Kind            | Payload   | Producer           | Consumer / sink     |
| ------- | --------------- | --------- | ------------------ | ------------------- |
| `input` | entry           | `String`  | controller         | `StaticEntry`       |
| (hop)   | inter-tasklet | `String`  | `StaticEntry`      | `StaticDownstream`  |
| (sink)  | plugin-bound    | `Vec<u8>` | `StaticDownstream` | plugin `test_kafka` |

**Expected associated-type mapping:**

| Tasklet          | Input    | Output    | State              | Change                 | Mode                     |
| ------------------ | -------- | --------- | ------------------ | ---------------------- | ------------------------ |
| `StaticEntry`      | `String` | `String`  | `StaticEntryState` | `EntrySeen`            | static (`a`,`b`,`c`,`d`) |
| `StaticDownstream` | `String` | `Vec<u8>` | `()`               | `NoChange`             | single                   |

The wall clock for both.

Where the fixtures live: the hand-written `StaticCellOperator`
(`factory_id` `static_cell`) is in `test/apps/rust_runner/src/cell.rs` and
its `operator!` twin `sugar_static_cell` in `test/apps/rust_runner/src/sugar.rs`; the Java twin is
`test/apps/java_runner/src/main/java/com/flezha/test/SugarStaticCellUnboundedOperator.java`
(`sugar_static_cell`, `SugarEntry` writing `cellsFor`), asserted by
`SugarTwinTest.referenceOperatorMatchesContract`. Java has no pipeline macro,
so its two-tasklet twin is an explicit `UnboundedOperator` class with a
`channels()` body.

### 6.1 Canonical keyed-collection reference tasklet

Every SDK's conformance test also implements this one tasklet — a **rolling
per-key window** — and asserts that its changes survive the
replay-determinism harness and that it emits nothing until a key's window is
full. It is the reference for the §3.1 keyed and collection changes.

**The changes:** `PushBack(key, value)` / `PopFront(key, n)`, whose fold pushes
onto or pops from the keyed deque (§3.1), and a `run` that records both before
the warmup check and `ctx.emit`.

**Rust:**

```rust
#[derive(Serialize, Deserialize)]
enum RollingWindowChange {
    PushBack(String, f64),
    PopFront(String, usize),
}

impl Change<RollingWindowState> for RollingWindowChange {
    fn apply_to(&self, state: &mut RollingWindowState) {
        match self {
            RollingWindowChange::PushBack(key, value) => {
                state.window.entry(key.clone()).or_default().push_back(*value);
            }
            RollingWindowChange::PopFront(key, n) => {
                if let Some(w) = state.window.get_mut(key) {
                    let n = (*n).min(w.len());
                    w.drain(..n);
                }
            }
        }
    }
}

#[flezha::tasklet(name = "rolling_window")]
impl Tasklet for RollingWindow {
    type Input = (String, f64);
    type Output = f64;
    type State = RollingWindowState;
    type Change = RollingWindowChange;

    fn run(&mut self, input: (String, f64), ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let (key, value) = input;
        ctx.change(RollingWindowChange::PushBack(key.clone(), value));
        let len = ctx.state().window.get(&key).map(|w| w.len()).unwrap_or(0);
        if len > N {
            ctx.change(RollingWindowChange::PopFront(key.clone(), len - N));
        }
        let Some(w) = ctx.state().window.get(&key) else { return Ok(()) };
        if w.len() < N { return Ok(()); }                  // warmup: emit nothing
        ctx.emit(w.iter().sum::<f64>() / N as f64)
    }
}
```

**Java** writes the two changes by hand, as its one form: records `Pushed` and
`Popped` implementing `Change<RollingWindowState>`, applied with `ctx.change`,
and a `run` that emits the average only once a key's window is full.

Both emit only once a key's window is full, and both pass `replay_equals` /
`replayEquals()`. Fixtures (`N = 3`): Rust `rolling_window` in
`test/apps/rust_runner/src/sugar.rs`; Java
`RollingWindow` (`java_rolling_window`), asserted by
`SugarTwinTest.rollingWindowGatesWarmupAndReplays`.

## See also

- [`naming_conventions.md`](naming_conventions.md) — method and node naming every SDK mirrors
- [`poll_core.md`](poll_core.md) — the kernel the sugar's expansion runs on
- [`stream_transforms.md`](stream_transforms.md) — `key_by` and the edge transforms
- [`cell_timers.md`](cell_timers.md) — the timer contract behind §3.4
- [`elixir_sdk.md`](elixir_sdk.md) — the `use` macros, the `state` block and the behaviour beneath them
- [`../architecture/runner.md`](../architecture/runner.md) — "One change, every SDK"
