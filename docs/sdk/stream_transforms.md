# Stream transforms and edge keying

For operative authors wiring a stream topology and for SDK implementers: the
four edge methods — `map`, `filter`, `flat_map` / `flatMap` and `key_by` /
`keyBy` — where they run, what that obliges, how a failing closure is
contained, the build-time refusals, and where the code lives.

A stateless reshaping step costs one line, not a `Procedure`, and keying is
visible at the wiring site instead of inside the consumer class:

```rust
c.input::<Trade>("trades")
    .filter(|t: &Trade| t.qty > 1.0)
    .flat_map(|t: Trade| TIMEFRAMES.iter().map(move |tf| Slot::new(&t, tf)))
    .key_by(|s: &Slot| format!("{}@{}", s.symbol, s.timeframe))
    .into::<CandleProcedure>()
    .sink("candles");
```

```java
c.input(Trade.class, "trades")
        .filter(t -> t.qty() > 1.0)
        .flatMap(Slot.class, t -> TIMEFRAMES.stream().map(tf -> new Slot(t, tf)).toList())
        .keyBy(s -> s.symbol() + "@" + s.tf())
        .into(CandleProcedure.class)
        .sink("candles");
```

One `Trade` becomes four `Slot`s in four Entities, and the topology contains
exactly one Procedure node. The Elixir SDK carries the same four calls
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
**stream** seam. A **workflow stage** has a different one: the core does not
route a stage's output on `(edge, key)` at all — it collects every output as
one released value, and the driver hands that batch to each child itself. A
workflow producer therefore releases the value **once, raw**, naming only the
producing channel, and each consumer edge is evaluated at the consumer (Java's
`EntityRouting.valuesForEntity`), which is also the seam a journal re-seed
crosses. Evaluating producer-side there would multiply the released batch by
the channel's consumer count. A byte-level host's delivery seam therefore
applies an edge only to a payload no producer transformed: an entry payload,
whose producer is the wire, and a workflow stage's released value. Two
obligations follow:

- **Pure and total.** A closure re-runs on replay; one that reads the clock or
  a counter rebuilds a state the live run never had.
- **Cheap.** It runs on the routing thread, so an expensive closure stalls
  routing for every consumer of that channel. Real work belongs in a
  `Procedure` behind `key_by`, which buys per-Entity parallelism. This is an
  authoring obligation, not a refusal.

The key closure sees the **post-transform** value (hence `TransformAfterKeyBy`)
and is total: it names an Entity and never declines — drop inputs with a
`filter` declared before the key. A keyed edge makes its consumer dynamic;
`entity_keys` / `entities_for`, `entity_idle_ttl` and `max_entities` stay on
the Procedure. `Procedure` has no key hook of its own, and a Java
`@EntityKey` annotation is a processor error naming `.keyBy(f)`.

## Containment

A closure that panics or throws does not take the routing thread with it.

- **Stream operative.** `ConsumerEdge::transform_value` / `key_of` catch the
  unwind once, and the value climbs the channel's retry→DLQ ladder
  (`run_retry_park_ladder`, the one a failing entry parse rides) under its
  `retry_max` / `retry_backoff_ms` budget; the other consumers are still
  served and later inputs keep routing. On Java the closure runs inside an
  activation, so a throw is that activation's failure and rides its ladder.
- **Workflow operative.** There is no DLQ. An in-process closure runs on the
  driver thread in `extend_mapped`, is contained there and becomes a terminal
  chain error the saga compensates — the instance still reports
  `WorkflowCompleted` with status `error`, with no retry. Undo scope:
  [`multi_input_procedures.md`](multi_input_procedures.md). An edge **key**
  never reaches the driver: a keyed edge makes its consumer dynamic, which a
  workflow operative refuses.

## Refusals

Build-time. **Where** column: *by name* means the SDK raises the reference's own
name and sentence; *own words* means it refuses the same shape without the name.

| Name | Fires when | Where |
| --- | --- | --- |
| `DuplicateKeyBy` | two `key_by` on one edge (or on one merge) | every SDK, by name |
| `TransformAfterTerminal` | a transform or key declared on a handle already closed by a plugin sink | every SDK, by name |
| `TransformAfterKeyBy` | a transform declared after the key it would feed | every SDK, by name |
| `DanglingTransformChain` | a transform chain no `.into(...)` ever closed | every SDK, by name |
| `KeyByOnUnkeyableTarget` | a `key_by` edge wires a Procedure declaring a non-empty `entity_keys()` | Rust, Java, Elixir by name; Node/Python/Ruby/PHP in their own words |
| `UnkeyedEdgeIntoDynamicProcedure` | a keyless edge feeds a Procedure another edge keyed into dynamic mode | Rust, Java, Elixir by name; Node/Python/Ruby/PHP in their own words |
| `EdgeAlreadyWired` | a consumer edge re-declared where either side carries a transform or a key | Rust, Java, Node, Python, Ruby, PHP by name; **not the Elixir SDK** |

`TransformAfterTerminal` makes the declaration order load-bearing where a
channel carries both a sink and a transformed consumer: close the transform on
its `.into(...)` first, then attach the sink to the held handle.

A re-declaration carrying neither transform nor key is the legal fan-in /
repeated-`.into` no-op — one consumer edge, not two — in every SDK but Elixir,
whose `do_into` (`lib/flezha/sdk/channels.ex`) still appends unconditionally, so
both shapes leave the procedure wired twice there. A workflow operative also
refuses any closure on an **entry** channel
(`BuildWorkflowError::MergeFromEntryChannel`): the workflow seed bypasses the
routing seam, so the closure could never be selected.

## Two asymmetries between the SDKs

Java `map` / `flatMap` take the post-transform type as an explicit `Class<U>`
— erasure cannot recover it from a lambda, and the runtime compares declared
types when wiring a consumer; `filter` and `keyBy` need none. Rust checks the
`Procedure<Input = T>` bound on `.into::<P>()` at compile time.

Rust unifies both handles into one `Stream<'b, T, K>`, with
`EntryStream<'b, T>` a type alias (`K = EntryTarget`); Java declares the shared
surface as `EdgeStream<T>`, which `EntryChannel<T>` extends and `Stream<T>`
implements. Both handles carry the transforms, the key and the consumer-side
knobs (`on_full`, `retry_max`, `retry_backoff_ms`); producer-side methods
(`named`, `codec`, `keyed`, `sink`, `sealed`, `branch`) stay on `Stream`
alone — an entry channel has no producer.

## Where the code lives

| Piece | Rust | Java |
| --- | --- | --- |
| Builder surface, fusion (`attach_consumer`, `extend_pending`, `compose`), `TopologyIssue` | `sdk/rust/src/channel/builder/` | `flezha-sdk-api/.../channel/{EdgeStream,EntryChannel,Stream}.java`, `flezha-sdk-runtime/.../operative/builder/StreamOperativeTopologyBuilder.java`, refusals in `flezha-sdk-api/.../operative/StreamOperativeConfigurationException.java` |
| `ConsumerEdge`, `EdgeTransformFn`, `EdgeYield`, `transform_value`, `key_of` | `flz_core/src/channel.rs` | `flezha-sdk-runtime/.../operative/{ConsumerEdge,EdgeYield}.java` |
| Routing seam and ladder (`route_to_consumers`, `deliver_edge`, `run_retry_park_ladder`) | `flz_core/src/routing.rs` | `flezha-sdk-runtime/.../core/{EmitContext,EntityRouting,EdgeTransforms}.java` |
| Workflow containment (`extend_mapped`) and refusals | `flz_core/src/workflow/{driver,spec}.rs` | shared core |
| Elixir SDK | `lib/flezha/sdk/{channels,topology}.ex` | — |

Tests: `sdk/rust/tests/stream_transforms.rs` (asserts fusion by counting
traversals), `sdk/rust/tests/ui/procedure_entity_key_removed.rs`, Java
`StreamOperativeTopologyBuilderTest` / `EntityRoutingTest` /
`WorkflowDispatchTest`, and
`test/system/dynamic_entity_e2e_test.exs` on both runtimes. Examples:
`sdk/rust/examples/stream_transforms.rs`, Java `E10_StreamTransforms`.

## See also

- [`multi_input_procedures.md`](multi_input_procedures.md) — a merge mapping is one link of an edge transform chain; keying is declared once, on the merge.
- [`rust_operative_architecture.md`](rust_operative_architecture.md) — dynamic entity mode and the Entity lifecycle the key selects.
- [`authoring_contract.md`](authoring_contract.md) — how the attribute/annotation sugar maps onto the trait surface.
- [`entity_timers.md`](entity_timers.md) — a keyed Entity that can also act on the passage of time.
