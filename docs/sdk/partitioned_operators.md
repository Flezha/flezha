# Partitioned operators

For operator authors and the users who run them. `--partitions N` materialises one
operator, a flow operator or a `per_input` steps operator, as N ordinary instances (`ohlc#p0..#p{N-1}`),
each owning a contiguous slice of 256 key groups. The model — partition as instance, no stored group record, key groups
instead of `mod N`, a guarded routing contract — lives in
[`../architecture/stream_profile.md`](../architecture/stream_profile.md#partitioned-operators).
This page is the contract: the one rule an author must keep, how sources and forwards split,
the operations surface, converge and rescale semantics, sizing, and the named limits.

```bash
flz operator start candles --id ohlc --partitions 3
```

`--partitions 1` (the default) is an unpartitioned start. `N` must be between 2 and 256. A
partitioned start carries no first message: `--text` names one instance, and a group's message
reaches its member by key, so `--partitions` with `--text` is refused before any member starts.

## Key groups

```text
group(key) = fnv1a128(key_bytes) mod 256
```

The hash is the FNV-1a-128 the SDKs' `StableId` already freezes (offset basis
`0x6c62272e07bb014262b821756295c58d`, prime `2^88 + 0x13b`). Two implementations carry the key
group — `Flezha.SDK.KeyGroup.group_of/1` (controller and Elixir SDK) and
`flz_core::util::key_group` (Rust SDK, and the Java host through the poll core) — pinned
against shared fixture vectors:

| key | key group |
| --- | --- |
| `SOL-USD` | 60 |
| `BTC-USD` | 129 |
| `ETH-USD` | 213 |
| `a` | 100 |
| `""` | 141 |

Assignment is a contiguous even split — `Partition.ranges(3)` is
`[{0,84}, {85,169}, {170,255}]` — but the owned range is **stored in the member's opts and
shipped on `AddOperator`** (`key_group_from` / `key_group_to`), never re-derived from
`(partition, partitions)`.

## The one contract an author must keep

> For a partitioned operator, the routing item — the source's subscription string, an
> emitter's `flz.key` — must equal the value `key_by` extracts.

A plain `emit` on a keyed channel holds it by construction (the stamped key is the Cell's
own). What is left to get wrong is the source's subscription string and an explicit
`emit_keyed` under a key of the author's choosing. With `partition_count >= 2`, dispatch checks
every computed cell key against the member's range; a miss parks **before any state for
that key is created**:

```text
key 'BTC-USD' hashes to key group 129, which partition 0 of 3 does not own
(it owns groups 0..84) — the routing item / flz.key must equal the key_by result
```

Below `partition_count = 2` the check is one integer comparison.

**Keyless topologies are refused.** A class with no keyed entry-fed tasklet — neither
`key_by` nor declared cell keys — cannot be partitioned: the SDK refuses the `AddOperator`
with `PartitionsWithoutKeyedTasklet` (only the SDK can tell; key closures are compiled
code), the controller treats that as a member start failure and rolls the group back. The
class also reports `keyed: false` in its `AddOperator` response metadata, so a later
partitioned start of a known-keyless class is refused before any runner is touched. A
deliberate everything-in-one-place aggregate belongs in the two-stage pattern below. A
**statically** keyed class partitions normally: every member declares every key, and the ones
outside its range receive no input.

**A steps operator that reads one message is refused.** A `once` or `chained` steps operator would hand its
one message to one member while the others waited for nothing, so the SDK refuses the
`AddOperator` with `PartitionsWithoutPerInputLoop`, ahead of the keyless check, and the class
declares `each` in its response metadata so a later partitioned start of it is refused
before any runner is touched. Only a `per_input` steps operator is a group.

## Sources: filter the ask, never the data

| Source | Partition behaviour |
|---|---|
| Websocket subscription | The desired set derived from a poll is filtered to the member's range between decode and reconcile; the applied set, delta frames, the durable `applied_subscriptions` and the connect-time re-assert derive from it. A static `subscription_items` list rides the same filter — mutually exclusive with `subscription_url`. |
| HTTP poll | `{{partition_index}}` / `{{partition_count}}` in `url` are substituted once at `init_provider`; validation runs on the rendered URL; a placeholder in a plugin with no partition identity is refused at init. |

**Fan-out.** A `--plugins` config given at group start is decoded once per member: the
member's `partition_index` / `partition_count` / `key_group_from` / `key_group_to` are merged
into every plugin's opts as integers, and the plugin id gains the member's `#pK` suffix so two
members on one node cannot collide in a provider's worker registry.
`flz plugin bind <name> --operator_id <group>` binds every member with those opts
injected, and `flz plugin unbind` is symmetric; a partial bind reports which members
succeeded and which failed, by name. A plugin that can be shared is not shared with a
partitioned operator: each member needs its own share of the key range, so each runs its own
copy, as it runs a plugin that cannot be shared. An unpartitioned operator bound to the same
plugin keeps the controller's shared copy.

## Forwarding to a partitioned operator

`operator_forward` targeting a group resolves the member set at `init_provider` and keeps
**one worker per target member** (a worker is one queue with one in-flight slot, so a shared
router would head-of-line-block every partition behind the slowest). Each message routes by
`group(flz.key)` to the owning member's worker. A message with **no `flz.key`** (an emit on a
channel not declared `.keyed()`, from a flow or a steps operator) is parked with
`partitioned target requires keyed emit`; it is never round-robined and never payload-hashed.

## The user's view

```text
$ flz operator list
  OPERATOR      HEALTH      PARTS   NODE       RUNNER
  ohlc          healthy     3/3     node@a     p0→r1, p1→r2, p2→r1

$ flz operator info
Partition group: ohlc (3/3)
  p0  ohlc#p0    key groups 0-84     r1  healthy
  p1  ohlc#p1    key groups 85-169   r2  healthy
  p2  ohlc#p2    key groups 170-255  r1  healthy
  coverage: all 256 key groups owned exactly once
```

One `list` row per group, showing the **worst** member's health. A `failed` or `unbound` `list`
row or `info` member line reads `recovering` until the unmet-intent grace `/healthz` uses runs
out (`FLEZHA_CONVERGENCE_UNMET_INTENT_GRACE_MS`), and its own word after that. A group moving
off a dead runner is the usual case. Coverage is recomputed from
the member rows on every read; a missing member or an overlapping range is rendered as a
warning naming the exact gap or the contested key group. Membership is a metadata match over
the registry rows (`ProcessHub.metadata_query/2` on `:partition`), never a registry decode.
`flz operator stop <group>` stops every member. `flz state get <group>/<tasklet>/<key>`
reads from the member owning the key; a tasklet-level read fans out and merges.

## Re-running the start: converge

`flz operator start <factory> --id <id> --partitions N` against a group with live members **converges**: it
starts only the members whose indices are missing and leaves every survivor untouched. A
restarted member restores its Cell state from its WAL at bind, so the cold-start command is
also the heal for an under-membered group.

```text
$ flz operator start candles --id ohlc --partitions 3
Operator ohlc converged: started 1 missing member(s), 2 already running:
  started ohlc#p2
  already running ohlc#p0
  already running ohlc#p1
```

A fully-present group exits 0 with nothing started. Converge is a presence check, not config
convergence: survivors' factory, init values, opts and plugin sets are not compared or
touched, and `--plugins` fans out only to the members this run starts. Converge never changes
N: the requested count must equal the `partitions` every survivor declares and each survivor's
stored range must equal the recomputed slice, otherwise the command refuses before starting
anything and names `flz operator scale`. Also refused: a plain start of a live group's base
id (only member ids are ever claimed), a `--partitions` start of an id held by a live
unpartitioned operator, and a base id that leaves no room for the `#pK` suffix inside the
operator-id byte limit.

## Rescale

```bash
flz operator scale ohlc 5 --timeout 60000     # quiesce timeout in ms, default 30000
```

1. **Quiesce.** Every member's source plugins are disabled; the command refuses while any
   member holds parked DLQ entries (`flz dlq list` — replay or drop first), then waits until
   every member's input-log watermark has reached its last offset. On timeout it aborts with
   **nothing stopped and nothing moved**, naming the lagging member and its offsets; a
   checkpoint-ack class advances its watermark only at a checkpoint close, so the wait rides
   its checkpoint cadence. The group is then untouched except that its sources are off —
   re-run with a longer `--timeout` once the laggard catches up.
2. **Preflight.** Ranges are recomputed for M; the same id-budget and `max_operators` checks
   a partitioned start runs apply to the net member delta. A failure leaves every member
   running.
3. **Stop.** Members stop with state preserved; the command waits up to 15 s for each id to be
   released before reusing it.
4. **Move.** Every Cell whose key group changes owner has its whole durable file set moved
   into the new owner's tree — an atomic rename on the same controller, the `WalTransfer`
   chunked pull (fsync, then rename, then drop the source) across controllers. Cell identity
   contains the key and no runner, so no file format changes. A member on another controller
   is gathered here under its own id, and a steps operator member's journal and input log travel
   with its Cells. The stop closed every member's writers first, on the node that ran it.
5. **Start.** The M members start with the new ranges and restore through the ordinary bind —
   a cold working-set rebuild, the same cost class as crash-resume. A member placed on another
   controller pulls its files before the bind reads its resume state.
6. **Clean up.** A vacated instance tree, now holding only a fully-consumed input log, is
   removed; members past M already left the hub's declared set and the registry at their stop,
   so an absent node can never resurrect one.

A crash mid-rescale is recoverable: before step 4 nothing durable has moved, each move is
atomic per Cell, and re-running `flz operator scale` re-derives the remaining moves from
what is on disk. **One rescale of a group runs at a time, cluster-wide** — a second command
is refused, and the lock is released if the controller holding it dies. **Forwards into the
group follow it**: a forwarding plugin whose resolved members no longer match the group reports
itself unhealthy, and the convergence loop restarts it so it re-reads the member set (a
surviving member keeps its worker and queued payloads, a removed member's queue is re-routed to
the new owner). The rescale command itself reports the move, not the forwards.

## A per-input steps operator as a group

A `per_input` steps operator partitions like a flow operator: `--partitions N` starts N ordinary steps operator
instances, each with its own key-group range, journal, input log and pass sheet, through the
same start, converge and rescale. Its entry edge must be keyed. A message reaches its member by
key, through a keyed forward, or by member id (`flz operator send orders#p1 ...`); a key the
member does not own parks under the entry channel and opens no pass. What a steps operator adds is
only what it already is:

- **Each member ends and compensates itself.** `ctx.complete`, `max_duration`, a stop or a
  terminal failure ends that member alone, with its own completion record; its rollback undoes
  its own passes. There is no group-wide end, no group result and no group rollback. A member
  that ended leaves a coverage gap in `flz operator info` until a converge restarts it.
- **Per member, by member id**: `flz operator rewind`, `flz state journal`,
  `flz state inbox`. `flz operator stop <group>` is one cooperative stop per member.
- **A rescale happens between passes.** The quiesce waits until every member's input log is
  consumed to its floor, which for a steps operator holds when no pass is in flight and no message
  is queued; a pass that outlives `--timeout` refuses the rescale with nothing changed.
- **A rescale seals.** Each surviving member's journal is cut to its latest committed
  iteration before it restarts, whatever the class declares, because a moved Cell's earlier
  passes now belong to another member. A rewind below the seal is refused with the committed
  range, and an all-or-nothing rollback stops at the seal. The scale command names the members
  it sealed. A new member starts at iteration 0 with an empty log.
- **`once` and `chained` are refused** (`PartitionsWithoutPerInputLoop`), and so is `--text`.
- **A spawn and a schedule name one instance.** Neither carries a key: `ctx.spawn_steps`
  naming a group's base id is refused as a plain start of that id is.

## Hot keys: two-stage aggregation

Per-key ordering means one key is served by one Cell on one machine; a key hot enough to
saturate a core cannot be split by partitioning. Split it in the topology instead:

```rust
// Stage 1 — partitioned: fan one symbol across 8 sub-keys, aggregate locally.
c.input::<Trade>("trades")
    .key_by(|t: &Trade| format!("{}#{}", t.symbol, t.trade_id % 8))
    .into::<PartialCandleTasklet>()
    .keyed()
    .sink_named("partials");

// Stage 2 — a single-partition merger, fed by forward: one Cell per symbol.
c.input::<PartialCandle>("partials")
    .key_by(|p: &PartialCandle| p.symbol.clone())
    .into::<CandleTasklet>()
    .sink_named("candles");
```

Stage 1 runs as `partials --partitions 8`; stage 2 runs unpartitioned (or with its own smaller
N) and receives stage 1's keyed output through `operator_forward`. It is the sub-key — not the
symbol — that stage 1's `key_by` must return, or the guard says so on the first frame.

## Sizing and limits

- **More partitions than runners buys nothing.** The extra members share machines with the
  others and add no parallelism, so start the runners before the group.
- **Placement spreads by count, not by anti-affinity.** Candidates are ranked by the ordinary HRW
  hash and narrowed by a fairness cap on operators already bound, so a group of N members across
  N runners normally lands one per runner; nothing pins a *particular* member to a *particular*
  runner, and two members can still share one when the fleet is already uneven —
  `flz operator info` shows where each landed.
- **One partition's whole state lives on one controller node**, which keeps the per-class input
  log correct. N partitions consume N `max_operators` units and open N source connections;
  each partition's sink plugins open their own connections.
- Rescale is stop-the-group with a cold restore: no warm handoff, no live (non-quiesced)
  rescale. A rescale spanning two controllers is covered at the mechanism level (cross-node
  file move, tree adoption); the orchestration above it is exercised single-node.
- Assignment is contiguous and even; the wire and opts carry an explicit range, but there is
  no authoring surface for a weighted assignment.
- A `once` or `chained` steps operator is not partitionable; see above.

## See also

- [`../glossary.md`](../glossary.md) — partition, key group, group
- [`protocol_spec.md`](protocol_spec.md) — the `AddOperator` partition fields
- [`../plugins/websocket_subscription.md`](../plugins/websocket_subscription.md),
  [`../plugins/http_poll.md`](../plugins/http_poll.md) — the partition-aware sources
- [`../plugins/operator_forward.md`](../plugins/operator_forward.md) — forwarding into a group
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — how a rescale
  moves durable state
