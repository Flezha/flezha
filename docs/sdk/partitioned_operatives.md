# Partitioned operatives

For operative authors and the operators who run them. `--partitions N` materialises one
stream operative as N ordinary instances (`ohlc#p0..#p{N-1}`), each owning a contiguous slice
of 256 key groups. The model — partition as instance, no stored group record, key groups
instead of `mod N`, a guarded routing contract — lives in
[`../architecture/stream_profile.md`](../architecture/stream_profile.md#partitioned-operatives).
This page is the contract: the one rule an author must keep, how sources and forwards split,
the operator surface, converge and rescale semantics, sizing, and the named limits.

```bash
flz operative start ohlc candles --partitions 3
```

`--partitions 1` (the default) is an unpartitioned start. `N` must be between 2 and 256;
`--partitions` applies to stream operatives only — a workflow start is refused.

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
shipped on `AddOperative`** (`key_group_from` / `key_group_to`), never re-derived from
`(partition, partitions)`.

## The one contract an author must keep

> For a partitioned operative, the routing item — the source's subscription string, an
> emitter's `flz.key` — must equal the value `key_by` extracts.

A plain `emit` on a keyed channel holds it by construction (the stamped key is the Entity's
own). What is left to get wrong is the source's subscription string and an explicit
`emit_keyed` under a key of the author's choosing. With `partition_count >= 2`, dispatch checks
every computed entity key against the member's range; a miss parks **before any state for
that key is created**:

```text
key 'BTC-USD' hashes to key group 129, which partition 0 of 3 does not own
(it owns groups 0..84) — the routing item / flz.key must equal the key_by result
```

Below `partition_count = 2` the check is one integer comparison.

**Keyless topologies are refused.** A class with no keyed entry-fed procedure — neither
`key_by` nor declared entity keys — cannot be partitioned: the SDK refuses the `AddOperative`
with `PartitionsWithoutKeyedProcedure` (only the SDK can tell; key closures are compiled
code), the controller treats that as a member start failure and rolls the group back. The
class also reports `keyed: false` in its `AddOperative` response metadata, so a later
partitioned start of a known-keyless class is refused before any runner is touched. A
deliberate everything-in-one-place aggregate belongs in the two-stage pattern below. A
**statically** keyed class partitions normally: every member declares every key, and the ones
outside its range receive no input.

## Sources: filter the ask, never the data

| Source | Partition behaviour |
|---|---|
| Websocket subscription | The desired set derived from a poll is filtered to the member's range between decode and reconcile; the applied set, delta frames, the durable `applied_subscriptions` and the connect-time re-assert derive from it. A static `subscription_items` list rides the same filter — mutually exclusive with `subscription_url`. |
| HTTP poll | `{{partition_index}}` / `{{partition_count}}` in `url` are substituted once at `init_provider`; validation runs on the rendered URL; a placeholder in a plugin with no partition identity is refused at init. |

**Fan-out.** A `--plugins` config given at group start is decoded once per member: the
member's `partition_index` / `partition_count` / `key_group_from` / `key_group_to` are merged
into every plugin's opts as integers, and the plugin id gains the member's `#pK` suffix so two
members on one node cannot collide in a provider's worker registry.
`flz plugin enable <group> …` runs the per-operative enable once per member with those opts
injected, and `flz plugin disable <group_plugin_id>` is symmetric; a partial enable reports
which members succeeded and which failed, by name.

## Forwarding to a partitioned operative

`operative_forward` targeting a group resolves the member set at `init_provider` and keeps
**one worker per target member** (a worker is one queue with one in-flight slot, so a shared
router would head-of-line-block every partition behind the slowest). Each message routes by
`group(flz.key)` to the owning member's worker. A message with **no `flz.key`** — an emit on a
channel not declared `.keyed()`, or any workflow-outbox output — is parked with
`partitioned target requires keyed emit`; it is never round-robined and never payload-hashed.

## The operator's view

```text
$ flz operative list
  OPERATIVE      HEALTH    PARTS   NODE       RUNNER
  ohlc           healthy   3/3     node@a     p0→r1, p1→r2, p2→r1

$ flz operative info
Partition group: ohlc (3/3)
  p0  ohlc#p0    key groups 0-84     r1  healthy
  p1  ohlc#p1    key groups 85-169   r2  healthy
  p2  ohlc#p2    key groups 170-255  r1  healthy
  coverage: all 256 key groups owned exactly once
```

One `list` row per group, showing the **worst** member's health. Coverage is recomputed from
the member rows on every read; a missing member or an overlapping range is rendered as a
warning naming the exact gap or the contested key group. Membership is a metadata match over
the registry rows (`ProcessHub.metadata_query/2` on `:partition`), never a registry decode.
`flz operative stop <group>` stops every member. `flz state get <group>/<procedure>/<key>`
reads from the member owning the key; a procedure-level read fans out and merges.

## Re-running the start: converge

`flz operative start <id> --partitions N` against a group with live members **converges**: it
starts only the members whose indices are missing and leaves every survivor untouched. A
restarted member restores its Entity state from its WAL at bind, so the cold-start command is
also the heal for an under-membered group.

```text
$ flz operative start ohlc candles --partitions 3
Operative ohlc converged: started 1 missing member(s), 2 already running:
  started ohlc#p2
  already running ohlc#p0
  already running ohlc#p1
```

A fully-present group exits 0 with nothing started. Converge is a presence check, not config
convergence: survivors' factory, init values, opts and plugin sets are not compared or
touched, and `--plugins` fans out only to the members this run starts. Converge never changes
N: the requested count must equal the `partitions` every survivor declares and each survivor's
stored range must equal the recomputed slice, otherwise the command refuses before starting
anything and names `flz operative scale`. Also refused: a plain start of a live group's base
id (only member ids are ever claimed), a `--partitions` start of an id held by a live
unpartitioned operative, and a base id that leaves no room for the `#pK` suffix inside the
operative-id byte limit.

## Rescale

```bash
flz operative scale ohlc 5 --timeout 60000     # quiesce timeout in ms, default 30000
```

1. **Quiesce.** Every member's source plugins are disabled; the command refuses while any
   member holds parked DLQ entries (`flz dlq list` — replay or drop first), then waits until
   every member's input-log watermark has reached its last offset. On timeout it aborts with
   **nothing stopped and nothing moved**, naming the lagging member and its offsets; a
   checkpoint-ack class advances its watermark only at a checkpoint close, so the wait rides
   its checkpoint cadence. The group is then untouched except that its sources are off —
   re-run with a longer `--timeout` once the laggard catches up.
2. **Preflight.** Ranges are recomputed for M; the same id-budget and `max_operatives` checks
   a partitioned start runs apply to the net member delta. A failure leaves every member
   running.
3. **Stop.** Members stop with state preserved; the command waits up to 15 s for each id to be
   released before reusing it.
4. **Move.** Every Entity whose key group changes owner has its whole durable file set moved
   into the new owner's tree — an atomic rename on the same controller, the `WalTransfer`
   chunked pull (fsync, then rename, then drop the source) across controllers. Entity identity
   contains the key and no runner, so no file format changes.
5. **Start.** The M members start with the new ranges and restore through the ordinary bind —
   a cold working-set rebuild, the same cost class as crash-resume.
6. **Clean up.** A vacated instance tree, now holding only a fully-consumed input log, is
   removed; members past M already left the hub's declared set and the registry at their stop,
   so an absent node can never resurrect one.

A crash mid-rescale is recoverable: before step 4 nothing durable has moved, each move is
atomic per Entity, and re-running `flz operative scale` re-derives the remaining moves from
what is on disk. **One rescale of a group runs at a time, cluster-wide** — a second command
is refused, and the lock is released if the controller holding it dies. **Forwards into the
group follow it**: a forwarding plugin whose resolved members no longer match the group reports
itself unhealthy, and the convergence loop restarts it so it re-reads the member set (a
surviving member keeps its worker and queued payloads, a removed member's queue is re-routed to
the new owner). The rescale command itself reports the move, not the forwards.

## Hot keys: two-stage aggregation

Per-key ordering means one key is served by one Entity on one machine; a key hot enough to
saturate a core cannot be split by partitioning. Split it in the topology instead:

```rust
// Stage 1 — partitioned: fan one symbol across 8 sub-keys, aggregate locally.
c.input::<Trade>("trades")
    .key_by(|t: &Trade| format!("{}#{}", t.symbol, t.trade_id % 8))
    .into::<PartialCandleProcedure>()
    .keyed()
    .sink_named("partials");

// Stage 2 — a single-partition merger, fed by forward: one Entity per symbol.
c.input::<PartialCandle>("partials")
    .key_by(|p: &PartialCandle| p.symbol.clone())
    .into::<CandleProcedure>()
    .sink_named("candles");
```

Stage 1 runs as `partials --partitions 8`; stage 2 runs unpartitioned (or with its own smaller
N) and receives stage 1's keyed output through `operative_forward`. It is the sub-key — not the
symbol — that stage 1's `key_by` must return, or the guard says so on the first frame.

## Sizing and limits

- **More partitions than runners buys nothing.** The start proceeds but warns, naming both
  counts and how many members will share machines.
- **Placement spreads by count, not by anti-affinity.** Candidates are ranked by the ordinary HRW
  hash and narrowed by a fairness cap on operatives already bound, so a group of N members across
  N runners normally lands one per runner; nothing pins a *particular* member to a *particular*
  runner, and two members can still share one when the fleet is already uneven —
  `flz operative info` shows where each landed.
- **One partition's whole state lives on one controller node**, which keeps the per-class input
  log correct. N partitions consume N `max_operatives` units and open N source connections;
  each partition's sink plugins open their own connections.
- Rescale is stop-the-group with a cold restore: no warm handoff, no live (non-quiesced)
  rescale. A rescale spanning two controllers is covered at the mechanism level (cross-node
  file move, tree adoption); the orchestration above it is exercised single-node.
- Assignment is contiguous and even; the wire and opts carry an explicit range, but there is
  no authoring surface for a weighted assignment.
- Workflow operatives are not partitionable.

## See also

- [`../glossary.md`](../glossary.md) — partition, key group, group
- [`protocol_spec.md`](protocol_spec.md) — the `AddOperative` partition fields
- [`../plugins/websocket_subscription.md`](../plugins/websocket_subscription.md),
  [`../plugins/http_poll.md`](../plugins/http_poll.md) — the partition-aware sources
- [`../plugins/operative_forward.md`](../plugins/operative_forward.md) — forwarding into a group
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — how a rescale
  moves durable state
