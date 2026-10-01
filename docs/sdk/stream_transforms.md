# Stream transforms and edge keying

For operator authors wiring a flow topology and for SDK implementers: the
four edge methods — `map`, `filter`, `flat_map` / `flatMap` and `key_by` /
`keyBy` — where they run, what that obliges, how a failing closure is
contained, the build-time refusals, and where the code lives; and an edge
that ends in an operator class instead of a tasklet.

A stateless reshaping step costs one line, not a `Tasklet`, and keying is
visible at the wiring site instead of inside the consumer class:

```rust
c.input::<Trade>("trades")
    .filter(|t: &Trade| t.qty > 1.0)
    .flat_map(|t: Trade| TIMEFRAMES.iter().map(move |tf| Slot::new(&t, tf)))
    .key_by(|s: &Slot| format!("{}@{}", s.symbol, s.timeframe))
    .into::<CandleTasklet>()
    .sink_named("candles");
```

```java
c.input(Trade.class, "trades")
        .filter(t -> t.qty() > 1.0)
        .flatMap(Slot.class, t -> TIMEFRAMES.stream().map(tf -> new Slot(t, tf)).toList())
        .keyBy(s -> s.symbol() + "@" + s.tf())
        .into(CandleTasklet.class)
        .sink("candles");
```

One `Trade` becomes four `Slot`s in four Cells, and the topology contains
exactly one Tasklet node. The Elixir SDK carries the same four calls
(`Flezha.SDK.Channels.map/2`, `filter/2`, `flat_map/2`, `key_by/2`) with the
same refusal names ([`elixir_sdk.md`](elixir_sdk.md)).

## One closure per edge

A transform is a closure on a `ConsumerEdge` — the same field a merge mapping
uses. Everything declared between one `.into` and the next composes into
**one** stored closure, so the runtime applies at most one transform and one
key closure per edge and never learns a chain existed. Fusion is structural,
not an optimizer pass: `.into(P)` is the boundary, and the value crosses a
mailbox exactly once, at `P`. There is no `disableChaining()` because there is
no chain to break.

## Where a closure runs, and what that obliges

Closures are evaluated at the **routing seam**, the one path both live
delivery and the controller's re-forwarded replay slice travel: in-process
`flz_core::routing::route_to_consumers` for Rust; for a byte-level host, in
the producing activation's emit context (Java's `EmitContext`), **once**, as
that activation completes — the core then routes on `(edge, key)` and
evaluates no host closure, and the consumer applies nothing. That is the
**stream** seam. A **steps stage** has a different one: the core does not
route a stage's output on `(edge, key)` at all — it collects every output as
one released value, and the driver hands that batch to each child itself. A
steps producer therefore releases the value **once, raw**, naming only the
producing channel, and each consumer edge is evaluated at the consumer (Java's
`CellRouting.valuesForCell`), which is also the seam a journal re-seed
crosses. Evaluating producer-side there would multiply the released batch by
the channel's consumer count. A byte-level host's delivery seam therefore
applies an edge only to a payload no producer transformed: an entry payload,
whose producer is the wire, and a steps stage's released value. Two
obligations follow:

- **Pure and total.** A closure re-runs on replay; one that reads the clock or
  a counter rebuilds a state the live run never had.
- **Cheap.** It runs on the routing thread, so an expensive closure stalls
  routing for every consumer of that channel. Real work belongs in a
  `Tasklet` behind `key_by`, which buys per-Cell parallelism. This is an
  authoring obligation, not a refusal.

The key closure sees the **post-transform** value (hence `topology.transform_after_key_by`)
and is total: it names a Cell and never declines — drop inputs with a
`filter` declared before the key. A keyed edge makes its consumer dynamic;
`cell_keys` / `cells_for`, `cell_idle_ttl` and `max_cells` stay on
the Tasklet. `Tasklet` has no key hook of its own, in Java as in Rust.

## Containment

A closure that panics or throws does not take the routing thread with it.

- **Flow operator.** `ConsumerEdge::transform_value` / `key_of` catch the
  unwind once, and the value climbs the channel's retry→DLQ ladder
  (`run_retry_park_ladder`, the one a failing entry parse rides) under its
  `retry_max` / `retry_backoff_ms` budget; the other consumers are still
  served and later inputs keep routing. On Java the closure runs inside an
  activation, so a throw is that activation's failure and rides its ladder.
- **Steps operator.** There is no DLQ. An in-process closure runs on the
  worker that produced the value, inside the routing seam, is contained there
  and fails the producing stage with no retry; the saga compensates and the
  instance still reports `OperatorCompleted` with status `error`. Undo scope:
  [`multi_input_tasklets.md`](multi_input_tasklets.md). An edge **key**
  never reaches a steps operator: a keyed edge makes its consumer dynamic, which a
  steps operator refuses.

## Refusals

Registration-time, and the same in every SDK. A builder records the links it
was given; `flz_core::host::register::check` judges the record and answers with
the refusal's name and sentence. The Elixir SDK is its own core and
re-expresses the same check against the same fixture, so it answers the same
names — it inherits no code.

| Name | Fires when |
| --- | --- |
| `topology.duplicate_key_by` | two `key_by` on one edge (or on one merge) |
| `topology.transform_after_terminal` | a transform or key declared on a handle already closed by a plugin sink |
| `topology.transform_after_key_by` | a transform declared after the key it would feed |
| `topology.dangling_transform_chain` | a transform chain no `.into(...)` ever closed |
| `topology.key_by_on_unkeyable_target` | a `key_by` edge wires a Tasklet declaring a non-empty `cell_keys()` |
| `topology.unkeyed_edge_into_dynamic_tasklet` | a keyless edge feeds a Tasklet another edge keyed into dynamic mode |
| `topology.edge_already_wired` | a consumer edge re-declared where either side carries a transform or a key |

The whole vocabulary is `flz_core/src/host/issue.rs`; the table above is the
transform half of it. `test/fixtures/reference_topologies.txt` carries one row
per name, and `test/sdk_conformance/topology_check_test.exs` fails if a name
has no row or a row has no name.

`topology.transform_after_terminal` makes the declaration order load-bearing where a
channel carries both a sink and a transformed consumer: close the transform on
its `.into(...)` first, then attach the sink to the held handle.

A re-declaration carrying neither transform nor key is the legal fan-in /
repeated-`.into` no-op (one consumer edge, not two) in every SDK. On a steps operator
an **entry** channel's closures run as on a flow operator: the driver applies them when it seeds the
pass on that channel's chain, so a message reaches the first tasklet mapped, filtered or fanned
out as declared.

## An edge into an operator class

`into` also takes an operator class. The class runs by its own `run` setting:

- **A flow class is inlined.** Its tasklets and channels join the outer graph under names
  prefixed with the node's name (`<factory_id>.<node>` by default), and the whole graph is
  judged as one by the rules above. Nothing of the inner class exists at run time.
- **A steps class becomes a stepped scope** per edge key: one inner instance of the class for
  each key the edge mints, inside the outer instance, with its own Cells, pass sheet, task
  journal and input log (an unkeyed edge has one). It runs by its own `each`, `on_failure`
  and `durable`: a `per_input` scope opens a pass per message, a `chained` one loops until a
  batch comes back empty, a `once` one takes one message for its life and dead-letters a
  later one. It sleeps after its own `sleep_after` while the outer instance goes on, an undo
  compensates its own tasklets only, and `flz operator rewind <outer>/<inner>/<key>` rewinds
  it alone. `max_cells` on the edge caps how many scopes it keeps at once.

The edge feeds the inner class's one entry channel, or the one `input` names when the class
declares several. The inner graph's **exit** is its one leaf whose output stays inside it: a
host SDK names it with `.named(...)`, since a host tasklet emits only on a channel it declares.
The handle `into` returns is that exit, so consumers wire after it as after a tasklet: a
`per_input` or `once` scope's output continues at its stage commit, a `chained` scope's last
non-empty batch when the loop converges, and a `complete(result)` carries the result out.

A scope's messages and exits are as durable as the outer instance's own input. Inside a
durable outer instance each message is journaled to the scope's input log before the scope
sees it, once, however often the outer input that sent it is replayed. When the outer
instance also journals its own input (on the effectively-once tier, or one that sleeps), the
exit values of a scope that keeps its work ride the commit that makes them final into the
outer instance's input log, so a runner that dies before the next outer tasklet took them
costs nothing: the outer instance's next bind delivers them. Anywhere else an exit is
delivered at most once.

| SDK | Spelling |
| --- | --- |
| Rust | `.into_operator::<Session, String>()`, `.into_operator_with::<Session, String>(Embedding { input, name, max_cells })` |
| Java | `.into(Session.class, String.class)`, `.into(Session.class, String.class, Embedding.of().maxCells(10_000))` |
| Elixir | `into(SessionOp)`, `into(SessionOp, input: "turns", name: "s", max_cells: 10_000)` |
| Python | `.into(session, name, input="turns", max_cells=10_000)` |
| Node | `.into(session, { name, input: 'turns', maxCells: 10_000 })` |
| Ruby | `.into(SessionSteps, name, input: "turns", max_cells: 10_000)` |
| PHP | `->into(SessionSteps::class, $name, input: 'turns', maxCells: 10_000)` |

An embedded class is not a factory: the runner announces the outer class only, and
`flz operator start session` is refused unless the class is also registered on its own. The
refusals an embedding adds:

| Name | Fires when |
| --- | --- |
| `topology.nested_steps` | a steps class embeds a steps class (a steps class may embed a flow class) |
| `topology.embedded_entry_ambiguous` | the class declares several entry channels and the edge names none with `input` |
| `topology.embedded_exit_ambiguous` | the class's graph ends in more than one candidate exit |
| `topology.invalid_setting` (setting `sink`) | a plugin sink, response or call follows the embedding node directly; put a tasklet between them |

## Two asymmetries between the SDKs

Java `map` / `flatMap` take the post-transform type as an explicit `Class<U>`
— erasure cannot recover it from a lambda, and the runtime compares declared
types when wiring a consumer; `filter` and `keyBy` need none. Rust checks the
`Tasklet<Input = T>` bound on `.into::<P>()` at compile time.

Rust unifies both handles into one `Stream<'b, T, K>`, with
`EntryStream<'b, T>` a type alias (`K = EntryTarget`); Java declares the shared
surface as `EdgeStream<T>`, which `EntryChannel<T>` extends and `Stream<T>`
implements. Both handles carry the transforms, the key and the consumer-side
knobs (`on_full`, `retry_max`, `retry_backoff_ms`); producer-side methods
(`named`, `keyed`, `sink`, `branch`) stay on `Stream` alone — an entry
channel has no producer.

## Where the code lives

| Piece | Rust | Java |
| --- | --- | --- |
| The check and its vocabulary | `flz_core/src/host/{register,issue}.rs` — one implementation, every SDK | shared core |
| Builder surface, fusion (`attach_consumer`, `extend_pending`, `compose`) | `sdk/rust/src/channel/builder/` | `flezha-sdk-api/.../channel/{EdgeStream,EntryChannel,Stream}.java`, `flezha-sdk-runtime/.../operator/builder/OperatorTopologyBuilder.java` |
| `ConsumerEdge`, `EdgeTransformFn`, `EdgeYield`, `transform_value`, `key_of` | `flz_core/src/channel.rs` | `flezha-sdk-runtime/.../operator/{ConsumerEdge,EdgeYield}.java` |
| Routing seam and ladder (`route_to_consumers`, `deliver_edge`, `run_retry_park_ladder`) | `flz_core/src/routing.rs` | `flezha-sdk-runtime/.../core/{EmitContext,CellRouting,EdgeTransforms}.java` |
| Steps operator containment (`extend_mapped`) and refusals | `flz_core/src/steps/{driver,spec}.rs` | shared core |
| Embedding an operator class: the judgment, the scope family, routing in and out | `flz_core/src/operator/scope.rs` | shared core |
| Elixir SDK | `sdk/elixir/lib/flezha/sdk/{channels,topology,scope}.ex` | — |

Tests: `sdk/rust/tests/stream_transforms.rs` (asserts fusion by counting
traversals), `sdk/rust/tests/ui/tasklet_cell_key_removed.rs`, Java
`OperatorTopologyBuilderTest` / `CellRoutingTest` /
`StepsDispatchTest`, and
`test/system/dynamic_cell_e2e_test.exs` on both runtimes;
`test/system/inner_operator_e2e_test.exs` drives a stepped scope on every runtime. Examples:
`sdk/rust/examples/stream_transforms.rs`, Java `E10_StreamTransforms`.

## See also

- [`multi_input_tasklets.md`](multi_input_tasklets.md) — a merge mapping is one link of an edge transform chain; keying is declared once, on the merge.
- [`rust_operator_architecture.md`](rust_operator_architecture.md) — dynamic cell mode and the Cell lifecycle the key selects.
- [`authoring_contract.md`](authoring_contract.md) — how the attribute/annotation sugar maps onto the trait surface.
- [`cell_timers.md`](cell_timers.md) — a keyed Cell that can also act on the passage of time.
