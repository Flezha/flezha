# Stream transforms and edge keying

For operator authors wiring an unbounded topology and for SDK implementers: the
four edge methods — `map`, `filter`, `flat_map` / `flatMap` and `key_by` /
`keyBy` — where they run, what that obliges, how a failing closure is
contained, the build-time refusals, and where the code lives.

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
**stream** seam. A **bounded stage** has a different one: the core does not
route a stage's output on `(edge, key)` at all — it collects every output as
one released value, and the driver hands that batch to each child itself. A
bounded producer therefore releases the value **once, raw**, naming only the
producing channel, and each consumer edge is evaluated at the consumer (Java's
`CellRouting.valuesForCell`), which is also the seam a journal re-seed
crosses. Evaluating producer-side there would multiply the released batch by
the channel's consumer count. A byte-level host's delivery seam therefore
applies an edge only to a payload no producer transformed: an entry payload,
whose producer is the wire, and a bounded stage's released value. Two
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

- **Unbounded operator.** `ConsumerEdge::transform_value` / `key_of` catch the
  unwind once, and the value climbs the channel's retry→DLQ ladder
  (`run_retry_park_ladder`, the one a failing entry parse rides) under its
  `retry_max` / `retry_backoff_ms` budget; the other consumers are still
  served and later inputs keep routing. On Java the closure runs inside an
  activation, so a throw is that activation's failure and rides its ladder.
- **Bounded operator.** There is no DLQ. An in-process closure runs on the
  worker that produced the value, inside the routing seam, is contained there
  and fails the producing stage with no retry; the saga compensates and the
  instance still reports `BoundedCompleted` with status `error`. Undo scope:
  [`multi_input_tasklets.md`](multi_input_tasklets.md). An edge **key**
  never reaches a bounded operator: a keyed edge makes its consumer dynamic, which a
  bounded operator refuses.

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
repeated-`.into` no-op (one consumer edge, not two) in every SDK. On a bounded operator
an **entry** channel's closures run as on an unbounded operator: the driver applies them when it seeds the
pass on that channel's chain, so a message reaches the first tasklet mapped, filtered or fanned
out as declared.

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
| Builder surface, fusion (`attach_consumer`, `extend_pending`, `compose`) | `sdk/rust/src/channel/builder/` | `flezha-sdk-api/.../channel/{EdgeStream,EntryChannel,Stream}.java`, `flezha-sdk-runtime/.../operator/builder/UnboundedOperatorTopologyBuilder.java` |
| `ConsumerEdge`, `EdgeTransformFn`, `EdgeYield`, `transform_value`, `key_of` | `flz_core/src/channel.rs` | `flezha-sdk-runtime/.../operator/{ConsumerEdge,EdgeYield}.java` |
| Routing seam and ladder (`route_to_consumers`, `deliver_edge`, `run_retry_park_ladder`) | `flz_core/src/routing.rs` | `flezha-sdk-runtime/.../core/{EmitContext,CellRouting,EdgeTransforms}.java` |
| Bounded operator containment (`extend_mapped`) and refusals | `flz_core/src/bounded/{driver,spec}.rs` | shared core |
| Elixir SDK | `sdk/elixir/lib/flezha/sdk/{channels,topology}.ex` | — |

Tests: `sdk/rust/tests/stream_transforms.rs` (asserts fusion by counting
traversals), `sdk/rust/tests/ui/tasklet_cell_key_removed.rs`, Java
`UnboundedOperatorTopologyBuilderTest` / `CellRoutingTest` /
`BoundedDispatchTest`, and
`test/system/dynamic_cell_e2e_test.exs` on both runtimes. Examples:
`sdk/rust/examples/stream_transforms.rs`, Java `E10_StreamTransforms`.

## See also

- [`multi_input_tasklets.md`](multi_input_tasklets.md) — a merge mapping is one link of an edge transform chain; keying is declared once, on the merge.
- [`rust_operator_architecture.md`](rust_operator_architecture.md) — dynamic cell mode and the Cell lifecycle the key selects.
- [`authoring_contract.md`](authoring_contract.md) — how the attribute/annotation sugar maps onto the trait surface.
- [`cell_timers.md`](cell_timers.md) — a keyed Cell that can also act on the passage of time.
