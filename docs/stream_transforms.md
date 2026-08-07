# Stream transforms and edge keying

`stream-transforms` adds four methods to the graph-authoring surface —
`map`, `filter`, `flat_map` / `flatMap`, and `key_by` / `keyBy` — and deletes
the `entity_key` Procedure hook they replace.

Two things change for an author:

- **A stateless reshaping step costs one line, not one class.** A `filter` used
  to mean a whole `Procedure`: six type tokens, an `init_state`, a `name()`, an
  Entity, a mailbox and a dispatch hop, all for one `if`.
- **Keying is visible at the wiring site.** `channels()` used to show
  `.into::<CandleProcedure>()` and say nothing about whether that ran in one
  Entity or ten thousand, because the decision lived in `entity_key()` inside
  the consumer class. It is now `.key_by(Trade::symbol)`, on the edge.

```rust
c.input::<Trade>("trades")
    .filter(|t: &Trade| t.qty > 1.0)
    .flat_map(|t: Trade| TIMEFRAMES.iter().map(move |tf| Slot::new(&t, tf)))
    .key_by(|s: &Slot| format!("{}@{}", s.symbol, s.timeframe))
    .into::<CandleProcedure>()
    .output("candles");
```

```java
c.input(Trade.class, "trades")
        .filter(t -> t.qty() > 1.0)
        .flatMap(Slot.class, t -> TIMEFRAMES.stream().map(tf -> new Slot(t, tf)).toList())
        .keyBy(s -> s.symbol() + "@" + s.tf())
        .into(CandleProcedure.class)
        .output("candles");
```

One `Trade` becomes four `Slot`s in four Entities, and the topology contains
exactly ONE Procedure node.

## What they are, mechanically

A transform is a closure on a `ConsumerEdge` — the same field a merge mapping
has always used. Everything declared between one `.into` and the next composes
into **one** stored closure, and the runtime applies at most one transform
closure and one key closure per edge. It never learns that a chain existed.

That is what makes fusion **structural** rather than an optimizer pass: there
is no chain to break, so there is no `disableChaining()` to write. `.into(P)`
IS the boundary — everything before it on that edge is one function, and the
value crosses a mailbox exactly once, at `P`.

```
.map(f).filter(g).map(h).into::<P>()
        │
        └── one EdgeTransformFn on the entry→P edge; P is one node
```

## Where they run — and what that obliges

Transform and key closures are evaluated in `route_to_consumers` (Rust) /
`ProcedureDispatcher.routeToConsumers` (Java): **the routing seam**, which is
the one path both live delivery and the controller's re-forwarded replay slice
travel. Two consequences follow, and they are the whole contract:

- **They MUST be pure and total.** They re-run on replay. A closure that read
  the clock, a counter, or anything else ambient would rebuild a state the live
  run never had. This is the merge mapping's existing contract, unchanged.
- **They MUST be cheap.** They run on the routing thread, so an expensive
  transform stalls routing for every consumer of that channel. Real work
  belongs in a `Procedure` behind `key_by`, which buys per-Entity parallelism.
  This is not enforceable — user-code cost is unmeasurable at build time — so
  it is a documentation and example-shape obligation, not a refusal.

The key closure is evaluated on the **post-transform** value, which is why a
transform declared after `key_by` is refused: the key was written against the
type it saw at declaration.

## Containment

An edge closure that panics or throws does not take the routing thread with it.
It is contained at the routing seam and the value climbs the channel's
**existing** retry→DLQ ladder — the same ladder a failing entry parse rides,
under the channel's own `retry_max` / `retry_backoff_ms` budget, parked to the
dead-letter queue on exhaustion. Later inputs keep routing.

On a **bounded** chain there is no DLQ; abort-and-compensate is the escape. An
edge closure there runs on the driver thread rather than inside an entity
invocation — the one author closure the existing `catch_unwind`s miss — so
`extend_mapped` / `BoundedOperativeRunner.extendMapped` contains it and turns
it into a terminal chain error the saga compensates, with the instance still
reporting `BoundedCompleted` (status `error`). See
`docs/multi_input_procedures.md` for the per-site undo-scope table; a `map` and
a merge mapping are the same closure on the same seam.

An edge **key** never reaches the bounded driver: a keyed edge makes its
consumer dynamic, and a dynamic procedure is refused on a bounded operative.

## Keying replaces `entity_key`

`Procedure::entity_key` + `ENTITY_KEY_OVERRIDDEN` (Rust) and
`entityKey` + `entityKeyOverridden` (Java) are **deleted**, not deprecated.
There is one way to key, so there is no silent winner when two are declared.

Migrating a dynamic-mode Procedure:

| Before | After |
| --- | --- |
| `fn entity_key(i: &In) -> Option<String> { Some(k(i)) }` | `.key_by(\|i: &In\| k(i))` on the edge |
| a `None` return | `.filter(...)` declared before the keying |
| `entity_idle_ttl()` / `max_entities()` | unchanged — only the KEY moved |
| `entity_keys()` / `entities_for()` (static mode) | unchanged |

The key function is **total**: it names an Entity, it never declines. The
`inputs_dropped_no_key_total` counter is retired with the keyless-drop path it
measured.

## Refusals

All build-time, both SDKs, same names:

| Name | Fires when |
| --- | --- |
| `KeyByOnUnkeyableTarget` | a `key_by` edge wires a Procedure declaring a non-empty `entity_keys()` |
| `DuplicateKeyBy` | two `key_by` on one edge (or on one merge) |
| `TransformAfterTerminal` | a transform or key declared on a handle already closed by a plugin sink |
| `TransformAfterKeyBy` | a transform declared after the key it would feed |
| `DanglingTransformChain` | a transform chain no `.into(...)` ever closed |
| `UnkeyedEdgeIntoDynamicProcedure` | a keyless edge feeds a Procedure another edge keyed into dynamic mode |
| `EdgeAlreadyWired` | a consumer edge re-declared where either side carries a transform or a key |

A re-declaration carrying neither stays the legal fan-in / repeated-`.into`
no-op.

Bounded operatives additionally refuse any edge closure on an **entry**
channel (`BuildBoundedError::MergeFromEntryChannel`): the bounded seed is a
bare payload that bypasses the routing seam, so a closure declared there could
never be selected.

## Java takes a type token; Rust does not

`map` and `flatMap` take the post-transform type as an explicit `Class<?>`,
matching the existing `c.input(String.class, "in")` convention, because erasure
cannot recover it from a lambda and the runtime compares declared types when
wiring a consumer. `filter` and `keyBy` need none — the payload type is
unchanged and a key is a `String`. Rust needs no token anywhere: the
`Procedure<Input = T>` bound on `.into::<P>()` is checked at compile time.

This is the same permanent asymmetry the SDK already documents for
`TimeExtractor` and `UnservableClock`.

## Both handles carry one surface

`EntryStream` (Rust) / `EntryChannel` (Java) and `Stream` expose the *same*
transform, keying and consumer-side configuration methods, defined once:
Rust unifies the two handles into one `Stream<'b, T, K>` parameterized by a
target marker (`EntryStream<'b, T>` is a type alias for it); Java declares the
shared surface as `EdgeStream<T>`, which `EntryChannel` extends and `Stream`
implements. Producer-side methods (`named`, `codec`, `keyed`, `also_output`,
`output`, `branch`) stay on `Stream` alone — an entry channel has no producer.

## See also

- `docs/multi_input_procedures.md` — a merge mapping is one link of an edge
  transform chain; keying is declared once, on the merge.
- `docs/operative_architecture.md` — "Dynamic entity mode", the dispatch-path
  cost model, and the Entity lifecycle the key selects.
- `docs/sdk_authoring_contract.md` — how the attribute/annotation sugar maps
  onto the trait surface, now that it offers no key hook.
- `docs/entity_timers.md` — the other half of a windowing library: a keyed
  Entity that can also act on the passage of time.
