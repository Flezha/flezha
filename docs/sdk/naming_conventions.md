# SDK Naming Conventions

For SDK implementers adding or reviewing public surface. These twelve rules
name every public SDK method, error and callback, mirrored 1:1 across the Rust
(`sdk/rust/`) and Java (`sdk/java/`) SDKs, so a new name is decided by one
standard rather than per change. Code comments cite the rules by number; the
numbering is stable.

## The rules

### 1. `Async` is reserved for methods that return a future

The `Async` suffix (Java) is used only on methods returning a
`Future`/`CompletionStage` — the convention Java itself follows
(`CompletableFuture.runAsync`). A method that merely *does not block for the
lifetime of something* is not "async": `start()` returns immediately after the
connect handshake with a plain handle, so it is `start()`, never `runAsync()`
or `startAsync()`.

### 2. `registerFactory` declares a factory; `unregisterFactory` is its inverse

Declaring an operative class tells the runtime what it *may spawn instances of
later* (via `AddOperative`). One method does it, whichever kind the operative
is: `registerFactory(Class)` (Java, Node, PHP), `register_factory(operative)`
(Rust, Python, Ruby, Elixir), legal before and after `start()`. Its one inverse
is `unregisterFactory` / `unregister_factory`, taking exactly what the
registration took — never an `undefine*` / `drop*` synonym.

What is registered is a *factory*: the class the controller places by factory
id. That is why the name says so, and why no call names the kind — an operative
already declares its own, and a call that named it again could disagree with the
declaration.

### 3. The lifecycle verb set is `start()` / `run()` / `awaitTermination()` / `close()`

Identical names in both SDKs (`await_termination` in Rust snake_case; Rust's
`close()` is `Drop`). gRPC's `Server.start()` / `Server.awaitTermination()` is
the direct precedent. `run()` is pure sugar (`start()` then
`awaitTermination()`), never an independent implementation, so the two can
never drift.

### 4. A kind an operative already declares is never a second method

Two per-kind methods were the earlier reading of this rule, and they were
wrong: the kind is a fact of the operative, so a second method for it is a
second place for one fact to be stated, and a call that can disagree with a
declaration. One `registerFactory` reads the kind from what it is handed — a
Rust trait impl, a Java interface, an exported callback — and refuses what it
cannot read, by name.

Where a language genuinely cannot express one method over both kinds, that is
what earns a second one. Rust can: a sealed `Operative<K>` trait over the two
kind markers, with `K` inferred from the value.

### 5. Accessors return `Optional`/`Option`, never throw for "nothing is there yet"

"Is anything running?" is a legitimate question from code that does not
control startup ordering. `Flezha.currentRunner()` returns
`Optional<FlezhaRunner>`; `Flezha::current_runner()` returns
`Option<Arc<FlezhaRunner>>` — empty when no runner started or it already shut
down. An accessor that throws for an ordinary "not yet" state is a bug.

### 6. Authoring mistakes and connection failures are named, catchable errors at the call site that caused them

A structural authoring mistake (invalid topology) or a connection failure is
always a named, catchable error, surfaced at the exact call that caused it,
never a bare panic or an unchecked runtime crash, and never deferred to a later
call (no "poisoned builder"):

| Failure                         | Java                                      | Rust (`Err(FlezhaError::…)`)                 |
| ------------------------------- | ----------------------------------------- | -------------------------------------------- |
| invalid topology                | `StreamOperativeConfigurationException`, `WorkflowOperativeConfigurationException` | `InvalidTopology(TopologyBuildError)` |
| duplicate `factory_id`          | `IllegalArgumentException` at the second `registerFactory` | `DuplicateFactoryId { factory_id }` |
| runner lifecycle misuse         | `IllegalStateException`                   | `AlreadyRunning { namespace }`, `InvalidNamespace` |
| connection failure at `start()` | a named exception (the native binding raises `FlezhaNativeException`) | `ConnectFailed(String)` |
| removing a type with live instances | `OperativeTypeInUseException`         | `OperativeTypeInUse`                         |

The two SDKs report the identical failure class with the same error name: what
Java throws at `registerFactory(...)`, Rust returns as `Err` from
`register_factory(R)`. The five short-form refusals
(`docs/sdk/authoring_contract.md` §5.2) are named the same way in all seven
SDKs, and the name appears verbatim in the error an author reads.

### 7. `create*` constructs an object the caller mutates; `new*` is not used

The entry point is `Flezha.createRunner(ns)` / `Flezha::create_runner(ns)`,
returning the concrete `FlezhaRunner` directly — Beam's `Pipeline.create()`
precedent: one object carries declaration (`registerFactory` /
`unregisterFactory`) and lifecycle (`start` / `run` / `awaitTermination` /
`close`). `new*` names are
not used, and there is no builder type: a separate builder earns its keep only
when construction is invalid until sealed, and a dynamic type set has no seal.

### 8. Prefer established Beam/Spark/Kafka-Streams vocabulary

When an established name exists for a concept in the mainstream dataflow SDKs
(Beam, Spark, Kafka Streams — and gRPC for server lifecycle), prefer it over
inventing one: `create` (Beam `Pipeline.create`), `start` / `awaitTermination`
(gRPC `Server`, Spark `StreamingQuery`), `close` (Kafka Streams). A novel name
is only justified when the concept itself is novel.

### 9. Graph authoring is producer-first: `input` → `into` → `sink`

Topology declaration reads in flow order on a **stream handle**. `input`
declares an entry channel (its name is explicit — an external address of the
operative; an operative may declare several, one per external source, and the
controller routes each inbound frame to its entry by `metadata["channel"]`),
`.into(Proc)` wires a consumer and returns *that procedure's* output stream,
and plugin egress is `sink()` / `sink(name)` / `sink(name, serializer)` (Rust,
which has no overloading: `sink()` / `sink_named(name)` / `sink_with(f)` /
`sink_named_with(name, f)`). The name a sink carries IS the channel's name —
the metadata the controller routes by and a plugin filter matches — so a
channel is named by at most one sink, two channels cannot name one sink, and
naming a channel with both a sink and `named(...)` is refused. The no-name form
leaves the channel named as it would be without a sink. Without a serializer
the defaults are passthrough for bytes, UTF-8 for strings, JSON otherwise.

**There is exactly one egress method.** `sink` always returns the stream
handle, so consumers wire after it; a channel is the plugin-bound terminal by
having no further wiring, not by a second method. `.sealed()` is the optional
terminator: it returns a non-chainable `Channel<T>` so a later consumer or
transform does not compile, and changes nothing about the topology.

Channel config (`keyed`, `onFull` / `on_full`, `retryMax` / `retry_max`,
`retryBackoffMs` / `retry_backoff_ms`, `named`) chains on the handle in any
order relative to consumer wiring. There is no codec method on any handle: the
runtime derives an inter-procedure channel's journal codec. Branching is handle reuse
(hold the handle in a variable, wire it twice) or the equivalent
`branch(arm1, arm2, ...)` sugar; fan-in is two streams naming one consumer, or
`merge` for an explicit multi-input edge.

### 10. A node is named by its class unless `into(Proc, "node_name")` names it

Node identity = the procedure's `name()`, unless an explicit node name is
given at wiring time (`into(Filter.class, "spam_filter")` in Java,
`into_named::<Filter>("spam_filter")` in Rust — rule 4's per-language
spelling). The node name replaces `name()` for everything identity feeds:
per-entity state directories, journal entries, the derived channel name and
diagnostics. Two unnamed mentions of one class are the same node (which is how
fan-in is spelled); node identities are unique per operative. Channel names
auto-derive from the producer node's identity, so renaming a language class
never changes an on-disk or on-wire name — identity strings are hand-written.

### 11. A new diagnostic target ships with a `DEFAULT_DIRECTIVES` entry in the same change

The Rust SDK's diagnostic-target inventory is public API:
`flezha_runner::diag::DEFAULT_DIRECTIVES` is the `EnvFilter` directive string
applications filter with instead of enumerating SDK internals. Emitting on a
tracing target the constant does not pass — a new `flezha::*` family, or a new
crate whose module path untargeted events inherit — silently discards those
events in every application using the exported defaults. So a change
introducing a diagnostic target extends `DEFAULT_DIRECTIVES` in the same
change; the directive-coverage tests in `sdk/rust/tests/diag.rs`
(`default_directives_pass_every_sdk_diagnostic_target`,
`every_source_declared_target_is_enabled_by_the_directives`) enforce it by
sweeping the SDK crates' emit sites and failing on any target the directives
discard at `warn`.

### 12. What happens on an event is spelled `on<Event>`, and a duration is spelled the SDK's own way

Anything an SDK surface says about an event carries the `on<Event>` prefix,
whichever shape it takes: `on_full` / `onFull` declares the policy for a full
mailbox, `on_timer` / `onTimer` is the Procedure hook a timer firing calls, and
`on_sink` / `onSink` registers a local-mode sink callback. `on*` is that prefix
in every SDK, so `addListener`, `subscribe` and `watch` synonyms are not used
for any of those shapes.

Any duration such a surface takes, or reports back, is spelled in the SDK's own
unit: a `Duration` where the language has one (Rust `timer_after(Duration)`,
Java `timerAfter(Duration)`), and milliseconds where the SDK's timers already
speak milliseconds (Node and PHP `timerAfter(ms)`, Ruby, Python and Elixir
`timer_after(ms)`, and `retry_backoff_ms` on every builder). A surface taking
seconds, or a `Duration` in a millisecond SDK, is the inconsistency this rule
refuses.

## See also

- [`authoring_contract.md`](authoring_contract.md) — the sugar these names expand from
- [`stream_transforms.md`](stream_transforms.md) — `map` / `filter` / `flat_map` / `key_by` on the handle
- [`../architecture/runner.md`](../architecture/runner.md) — "One change, every SDK"
