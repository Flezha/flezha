# SDK naming conventions

Rules for naming public SDK methods, mirrored across the Rust and Java SDKs.
Derived while designing the runner entry-point surface
(`Flezha.createRunner` / `Flezha::create_runner` and the `FlezhaRunner`
lifecycle); they bind every future SDK method name so naming is decided by one
standard, not ad hoc per PR.

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
live instance — so the runner methods are `defineUnbounded(Class)` /
`defineBounded(Class)` (Java), `define_unbounded::<R>()` /
`define_bounded::<T>()` (Rust), legal before and after `start()`. The inverse
of a definition is a removal: `removeUnbounded`/`removeBounded` /
`remove_unbounded`/`remove_bounded` — every `define*` gets a matching
`remove*`, never an `undefine*`/`unregister*`/`drop*` synonym. `register*`
survives only on internal registries, which do hold registrations.

### 3. The lifecycle verb set is `start()` / `run()` / `awaitTermination()` / `close()`

Identical names in both SDKs (`await_termination` in Rust snake_case; Rust's
`close()` is `Drop`). Chosen over Guava's `startAsync`/`awaitTerminated` and
Rust's `spawn` flavor for being the shortest widely-recognized set — gRPC's
`Server.start()` / `Server.awaitTermination()` is the direct precedent.
`run()` is pure sugar (`start()` then `awaitTermination()`), never an
independent implementation, so the two can never drift.

### 4. Two explicit per-kind methods beat one overloaded method

Rust has no method overloading, and the two SDKs must keep matching shapes.
When Java could overload (`define(Class)`) but Rust cannot, both SDKs get two
explicit methods (`defineUnbounded`/`defineBounded`). Cross-SDK symmetry
outranks Java-side call-site brevity.

### 5. Accessors return `Optional`/`Option`, never throw for "nothing is there yet"

"Is anything running?" is a legitimate question from code that does not
control startup ordering. `Flezha.currentRunner()` returns
`Optional<FlezhaRunner>` / `Flezha::current_runner()` returns
`Option<Arc<FlezhaRunner>>` — empty when no runner started or it already shut
down. An accessor that throws for an ordinary "not yet" state is a bug.

### 6. Authoring mistakes and connection failures are named, catchable errors at the call site that caused them

A structural authoring mistake (invalid topology) or a connection failure is
always a named, catchable error — a Java exception
(`OperativeConfigurationException`, `IllegalStateException`) or a Rust
`Result` (`FlezhaError`) — surfaced at the exact
call that caused it, never a bare panic or an unchecked runtime crash, and
never deferred to a later call (no "poisoned builder"). The two SDKs must
report the identical failure class with the same error name: what Java throws
at `defineUnbounded(...)`, Rust returns as `Err` from
`define_unbounded::<R>()`.

### 7. `create*` constructs an object the caller mutates; `new*` is not used

The entry point is `Flezha.createRunner(ns)` / `Flezha::create_runner(ns)`,
returning the concrete `FlezhaRunner` directly — Beam's `Pipeline.create()`
precedent: one object carries declaration (`define*`/`remove*`) and lifecycle
(`start`/`run`/`awaitTermination`/`close`). `new*` names are not used (and
there is no builder type: a separate builder earns its keep only when
construction is invalid until sealed, and a dynamic type set has no seal).

### 8. Prefer established Beam/Spark/Kafka-Streams vocabulary

When an established name exists for a concept in the mainstream dataflow SDKs
(Beam, Spark, Kafka Streams — and gRPC for server lifecycle), prefer it over
inventing one: `create` (Beam `Pipeline.create`), `start`/`awaitTermination`
(gRPC `Server`, Spark `StreamingQuery`), `close` (Kafka Streams). A novel name
is only justified when the concept itself is novel.

### 9. Graph authoring is producer-first: `input` → `into` → `output`

Topology declaration reads in flow order on a **stream handle**
(`sdk-graph-authoring`): `input` declares an entry channel (its name is
explicit — an external address of the operative; an operative may declare
several, one per external source, and the controller routes each inbound frame
to its entry by `metadata["channel"]`), `.into(Proc)` wires a
consumer and returns *that procedure's* output stream, and the plugin-bound
terminal is `output(name)` — destination-neutral (the operative declares a
named exit; the controller decides what happens there). The hybrid tap is
`alsoOutput(name[, serializer])` (Rust: `also_output` / `also_output_with`),
using the same default serialization rules as `output` (passthrough for bytes,
UTF-8 for strings, JSON otherwise). Rename mapping from the retired
channel-centric surface:

| retired                              | current                              |
| ------------------------------------ | ------------------------------------ |
| `channel(T.class, "name")` / `channel::<T>("name")` | *(gone — the producer's `Output` types the stream; `.named("name")` overrides the derived name)* |
| `.from(Proc)` / `.from::<Proc>()`        | *(gone — the stream handle already knows its producer)* |
| `toPlugin(name)` / `to_plugin(name)` | `output(name)`                       |
| `alsoToPluginWith(name, f)` / `also_to_plugin_with(name, f)` | `alsoOutput(name, f)` / `also_output_with(name, f)` |
| —                                    | `alsoOutput(name)` / `also_output(name)` (default serializer) |

Channel config (`codec`, `keyed`, `onFull`, `retryMax`, `retryBackoffMs`,
`named`) chains on the handle in any order relative to consumer wiring.
Branching is handle reuse (hold the handle in a variable, wire it twice) or
the equivalent `branch(arm1, arm2, ...)` sugar; fan-in is two streams naming
one consumer.

### 10. A node is named by its class unless `into(Proc, "node_name")` names it

Node identity = the procedure's `name()`, unless an explicit node name is
given at wiring time (`into(Filter.class, "spam_filter")` in Java,
`into_named::<Filter>("spam_filter")` in Rust — rule 4's per-language
spelling). The node name replaces `name()` for everything identity feeds:
per-entity state directories, journal entries, the derived channel name, and
diagnostics. Two unnamed mentions of one class are the same node (which is how
fan-in is spelled); node identities are unique per operative. Channel names
auto-derive from the producer node's identity, so renaming a language class
never changes an on-disk or on-wire name — identity strings are hand-written.
