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

### 2. `define*` declares a type; `remove*` is its inverse; `register*` is not public vocabulary

Declaring an operative class tells the runtime what it *may spawn instances of
later* (via `AddOperative`). That is a definition, not a registration of a
live instance — so the runner methods are `defineStream(Class)` /
`defineWorkflow(Class)` (Java), `define_stream::<R>()` /
`define_workflow::<T>()` (Rust), legal before and after `start()`. The inverse
of a definition is a removal: `removeStream` / `removeWorkflow` and
`remove_stream` / `remove_workflow` — every `define*` gets a matching
`remove*`, never an `undefine*` / `unregister*` / `drop*` synonym. `register*`
survives only on internal registries, which do hold registrations.

### 3. The lifecycle verb set is `start()` / `run()` / `awaitTermination()` / `close()`

Identical names in both SDKs (`await_termination` in Rust snake_case; Rust's
`close()` is `Drop`). gRPC's `Server.start()` / `Server.awaitTermination()` is
the direct precedent. `run()` is pure sugar (`start()` then
`awaitTermination()`), never an independent implementation, so the two can
never drift.

### 4. Two explicit per-kind methods beat one overloaded method

Rust has no method overloading, and the two SDKs must keep matching shapes.
When Java could overload (`define(Class)`) but Rust cannot, both SDKs get two
explicit methods (`defineStream` / `defineWorkflow`). Cross-SDK symmetry
outranks Java-side call-site brevity.

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
| duplicate `factory_id`          | `IllegalArgumentException` at the second `define*` | `DuplicateFactoryId { factory_id }`     |
| runner lifecycle misuse         | `IllegalStateException`                   | `AlreadyRunning { namespace }`, `InvalidNamespace` |
| connection failure at `start()` | a named exception (the native binding raises `FlezhaNativeException`) | `ConnectFailed(String)` |
| removing a type with live instances | `OperativeTypeInUseException`         | `OperativeTypeInUse`                         |

The two SDKs report the identical failure class with the same error name: what
Java throws at `defineStream(...)`, Rust returns as `Err` from
`define_stream::<R>()`.

### 7. `create*` constructs an object the caller mutates; `new*` is not used

The entry point is `Flezha.createRunner(ns)` / `Flezha::create_runner(ns)`,
returning the concrete `FlezhaRunner` directly — Beam's `Pipeline.create()`
precedent: one object carries declaration (`define*` / `remove*`) and
lifecycle (`start` / `run` / `awaitTermination` / `close`). `new*` names are
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
and plugin egress is `sink(name)` / `sink(name, serializer)` (Rust:
`sink(name)` / `sink_with(name, f)`) — destination-neutral: the operative
declares a named attachment point, the controller routes to it by channel-name
metadata, and several channels may name one sink. Without a serializer the
defaults are passthrough for bytes, UTF-8 for strings, JSON otherwise.

**There is exactly one egress method.** `sink` always returns the stream
handle, so consumers wire after it; a channel is the plugin-bound terminal by
having no further wiring, not by a second method. `.sealed()` is the optional
terminator: it returns a non-chainable `Channel<T>` so a later consumer or
transform does not compile, and changes nothing about the topology.

Channel config (`codec`, `keyed`, `onFull` / `on_full`, `retryMax` /
`retry_max`, `retryBackoffMs` / `retry_backoff_ms`, `named`) chains on the
handle in any order relative to consumer wiring. Branching is handle reuse
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
discard at `warn`. The Java SDK is exempt: SLF4J logger names inherit the root
level, so there is no enumerate-or-discard filter to keep in sync.

### 12. A runtime fact the SDK alone knows is a plain accessor plus an `on<Event>` registration

The SDK owns the IO threads, so some facts are only observable from inside it.
Each such fact gets two names: the accessor spelling the fact
(`last_entry_delivery(channel)` / `lastEntryDelivery(channel)`, rule 5's
`Option<Instant>` / `Optional<Instant>` for "nothing has happened yet") and,
where polling it in a loop would be the wrong shape, an `on<Event>`
registration taking the caller's own threshold and callback (`on_entry_idle` /
`onEntryIdle`). `on*` is already the SDK's callback prefix (`on_full`,
`on_timer`), so `addListener` / `subscribe` / `watch` synonyms are not used.

The registration never carries a default threshold or a default action. The
*signal* is SDK knowledge; the *policy* — what value is anomalous, and what to
do about it — is the application's domain knowledge, and an SDK-side default
would be a policy the SDK cannot know is right. Contract:
[`entry_input_idle.md`](entry_input_idle.md).

## See also

- [`authoring_contract.md`](authoring_contract.md) — the sugar these names expand from
- [`stream_transforms.md`](stream_transforms.md) — `map` / `filter` / `flat_map` / `key_by` on the handle
- [`entry_input_idle.md`](entry_input_idle.md) — the rule-12 accessor and registration
- [`../architecture/runner.md`](../architecture/runner.md) — "One change, every SDK"
