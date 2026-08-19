# Partitioned operatives

A stream operative binds to exactly one runner. Its Entities fan out across
that runner's cores, and the machine is the ceiling. `--partitions N` lifts it:

```bash
flz operative start ohlc candles --partitions 3
```

That starts **three ordinary operative instances** — `ohlc#p0`, `ohlc#p1`,
`ohlc#p2` — each a full operative with its own Overwatch, ProcessHub child,
registry row, runner binding, input log, state tree, plugins and DLQ identity.
Each owns a contiguous slice of the key space, and the CLI presents the three
as one logical `ohlc`.

`--partitions 1` (the default) is byte-identical to an unpartitioned start.

## The model

**A partition is an ordinary operative instance.** Nothing in placement,
convergence, rebind, migration or recovery knows partitions exist — they act
per member, unchanged. That is the whole design: partitioning buys machine
scale-out without a second lifecycle to keep correct.

**A group is derived, never stored.** Each member's durable cspec opts
self-describe:

```
group: "ohlc", partition: 1, partitions: 3, key_group_from: 85, key_group_to: 169
```

Every group-level view — `flz operative list`'s single row, `info`'s coverage
check, the forward provider's routing table — is recomputed from the member
rows at read time. A stored group record would be a second source of truth that
can drift from the registry, which is the exact failure class the control plane
already has to defend against.

## Key groups

```
group(key) = fnv1a128(key_bytes) mod 256
```

The hash is the one already frozen by the SDKs' `StableId` (offset basis
`0x6c62272e07bb014262b821756295c58d`, prime `2^88 + 0x13b`). Three
implementations carry it — `Flezha.Partition` (controller),
`flz_core::util::key_group` (Rust SDK and, through the poll core, the Java
host), `StableId` (Java) — pinned against **one** shared fixture-vector set:

| key | key group |
| --- | --- |
| `SOL-USD` | 60 |
| `BTC-USD` | 129 |
| `ETH-USD` | 213 |
| `a` | 100 |
| `""` | 141 |

Any constant that drifts in one implementation breaks all three suites at once.

Assignment is a contiguous even split — `ranges(3)` is
`[{0,84}, {85,169}, {170,255}]` — but the owned range is **stored and shipped
explicitly**, never re-derived from `(partition, partitions)`. A rescale's
recomputed ranges and a future weighted assignment (isolating a hot key's
group) therefore need no format change, and every consumer reads one
authoritative shape.

Rehashing on rescale is avoided the same way Flink avoids it: `hash mod N`
would rehome nearly every key when N changes (mod 3 → mod 5); key groups move
whole slices instead.

## The one contract an author must keep

> For a partitioned operative, the routing item — the source's subscription
> string, an emitter's `flz.key` — must equal the value `key_by` extracts.

A procedure's plain `emit` on a keyed channel holds it by construction: the
stamped key IS the Entity's own, which is what `key_by` returned. What is left
to get wrong is the source's subscription string and an explicit `emit_keyed`
under a key of the author's own choosing.

Nothing upstream can enforce that, so the SDK does, at the moment and place it
is violated. When `partition_count ≥ 2`, dispatch checks that each computed
entity key hashes into the member's own range, and a miss takes the park path
**before any state for that key is created**:

```
key 'BTC-USD' hashes to key group 129, which partition 0 of 3 does not own
(it owns groups 0..84) — the routing item / flz.key must equal the key_by result
```

Below `partition_count = 2` the check is one integer comparison, so an
unpartitioned operative pays nothing.

### Keyless topologies are refused

A class with no keyed entry-fed procedure — neither `key_by` nor declared
entity keys — cannot be partitioned: each member would fold its own slice of
the input into a private copy of "the one" state, N divergent partial truths
that all look like the state. The SDK refuses the `AddOperative` with
`PartitionsWithoutKeyedProcedure` (only the SDK can tell: key closures are
compiled code, never on the wire), the controller treats that as a member start
failure and rolls the group back. The class also declares its keylessness in
the `AddOperative` response, so a later partitioned start of a class already
known to be keyless is refused before any runner is touched.

A deliberate everything-in-one-place aggregate belongs in the two-stage pattern
below, where it is explicit.

A **statically** keyed class partitions normally: every member declares every
key, and the ones outside its range simply receive no input.

## Sources: filter the ask, never the data

Each member subscribes upstream to only its own items, so nothing is received
and discarded — total bandwidth equals today's single connection, split.

**Websocket subscription source.** The desired set derived from a poll is
filtered to the partition's range between decode and reconcile. Everything
downstream (the applied set, delta frames, the durable
`applied_subscriptions`, the connect-time re-assert) derives from it, so that
is the only edit. A new static `subscription_items` opt supplies a literal item
list through the same machinery — mutually exclusive with `subscription_url`,
and filtered identically.

**HTTP poll source.** `{{partition_index}}` / `{{partition_count}}`
placeholders in `url` are substituted exactly once, at `init_provider`, and URL
validation runs on the *rendered* URL:

```
url: "https://api.example/trades?shard={{partition_index}}&of={{partition_count}}"
```

A URL carrying a placeholder while the plugin has no partition identity is
refused at init.

**Fan-out.** A `--plugins` config given at group start is decoded once per
member with that member's operative id, `partition_index` / `partition_count` /
`key_group_from` / `key_group_to` merged into each plugin's opts as integers,
and the plugin's own id suffixed with the member's `#pK` so two members landing
on one node cannot collide in a provider's worker registry.

Runtime attach works the same: `flz plugin enable <group> …` runs the ordinary
per-operative enable once per member with those opts injected, and
`flz plugin disable <group_plugin_id>` is symmetric. A partial enable reports
which members succeeded and which failed, by name.

## Forwarding to a partitioned operative

`operative_forward` targeting a group resolves the member set at
`init_provider` and keeps **one worker per target member** — a worker is one
queue with one in-flight slot, so a shared router would head-of-line-block
every partition behind the slowest, and per-key ordering only needs per-member
FIFO. Each message routes by `group(flz.key)` to the owning member's worker.

A message with **no `flz.key`** — an emit on a channel not declared `.keyed()`,
or any workflow-outbox output, which never carries one — is parked with
`partitioned target requires keyed emit`, naming the source, the target and the
contract. It is never round-robined and never payload-hashed: both scatter one
key across partitions and detonate downstream, where the cause is invisible.

Single-member targets behave byte-identically to before.

## The operator's view

```
$ flz operative list
  OPERATIVE      HEALTH    PARTS   NODE       RUNNER
  ohlc           healthy   3/3     node@a     p0→r1, p1→r2, p2→r1
```

One row per group, showing partition count, the worst member's health, and
per-member placement. A group is exactly as healthy as its sickest member — an
aggregate that rounded up would hide the one partition that stopped serving its
keys.

```
$ flz operative info
Partition group: ohlc (3/3)
  p0  ohlc#p0    key groups 0-84     r1  healthy
  p1  ohlc#p1    key groups 85-169   r2  healthy
  p2  ohlc#p2    key groups 170-255  r1  healthy
  coverage: all 256 key groups owned exactly once
```

Coverage is recomputed from the rows on every read, because there is nothing
stored that could be consulted instead. A missing member or an overlapping
range is rendered as a loud warning naming the exact gap or the contested key
group — never a quietly short list.

`flz operative stop <group>` stops every member.
`flz state get <group>/<procedure>/<key>` hashes the key and reads from the
owning member; a procedure-level read fans out and merges.

## Re-running the start: converge

`flz operative start <id> --partitions N` against a group that already has
live members **converges** instead of refusing: it starts only the members
whose indices are missing and leaves every survivor untouched. A restarted
member restores its Entity state from its on-disk WAL at bind, so the same
command that cold-starts a group is also the heal for an under-membered one —
a re-run after a node roll, or the retry loop of a `set -e` start script.

```
$ flz operative start ohlc --partitions 3 ...
Operative ohlc converged: started 1 missing member(s), 2 already running:
  started ohlc#p2
  already running ohlc#p0
  already running ohlc#p1
```

A fully-present group exits 0 with nothing started. Converge is a presence
check, not config convergence: survivors' factory, init values, opts and
plugin sets are not compared or touched, and the `--plugins` config fans out
only to the members this run starts.

Converge never changes N. The requested count must equal the `partitions`
every survivor declares in its cspec, and each survivor's stored range must
equal the recomputed slice — on any mismatch the command refuses before
starting anything and names `flz operative scale`, because changing N moves
Entity state, which a start never does.

Two id collisions are refused rather than silently honoured: a plain
(unpartitioned) start of a live group's base id — the base id itself is never
claimed, only member ids are, so without the guard a rogue singleton would
start beside the group — and a `--partitions` start of an id held by a live
unpartitioned operative.

## Rescale

```bash
flz operative scale ohlc 5
```

1. **Quiesce.** The members' source plugins are disabled; the command waits
   until every member's input-log watermark has reached its last offset, and
   refuses outright while any member has parked DLQ entries. On timeout it
   aborts with **nothing stopped and nothing moved**, naming the lagging member
   and its offsets. A checkpoint-ack class advances its watermark only at a
   checkpoint close, so the wait rides its checkpoint cadence — the refusal
   says so.
2. **Preflight.** Contiguous ranges are recomputed for M, and the same id-budget
   and `max_operatives` checks a partitioned start runs are applied to the net
   member delta. A preflight failure leaves every member running.
3. **Stop.** Members stop with their state preserved: this stop is a move, not a
   teardown.
4. **Move.** Every Entity whose key group changes owner has its whole durable
   file set (WAL, snapshots, outbox dedup sidecar, compensation markers) moved
   into the new owner's tree — an atomic rename on the same controller, the
   `WalTransfer` chunked pull (fsync, then rename, then drop the source) across
   controllers. Entity identity contains the key and no runner, so no file
   format changes.
5. **Start.** The M members start with the new ranges and restore their Entities
   through the ordinary bind path — a cold working-set rebuild, the same cost
   class as crash-resume.
6. **Clean up.** A vacated instance tree, now holding only a fully-consumed
   input log, is removed. The members past M are already gone from the hub's
   declared set and the registry — their stop removed both — so a node that
   was absent for the whole rescale can never resurrect a vacated partition,
   with no expiry window to wait out.

Crash mid-rescale is recoverable by construction: before step 4 nothing durable
has moved, and each move is atomic per Entity, so re-running `flz operative
scale` re-derives the remaining moves from what is on disk and completes them.

**Only one rescale of a group runs at a time, cluster-wide.** Two interleaved
ones would each stop the members the other just started, so a second command is
refused outright rather than queued behind an outcome it did not ask for. The
lock is released if the controller holding it dies, which is exactly the
re-runnable case above.

**Forwards into the group follow it.** Anything forwarding to this group is
nudged to re-read the member set as the rescale finishes — a surviving member
keeps its worker and its queued payloads, a removed member hands its queue back
to be re-routed to the new owner, and the command reports how many plugins were
re-pointed. See [`operative_forward_plugin.md`](operative_forward_plugin.md).

### Runbook

```bash
flz operative info                      # note the current partition count and coverage
flz dlq list                            # replay or drop anything parked first
flz operative scale ohlc 5 --timeout 60000
flz operative info                      # coverage clean, 5/5
```

If the quiesce times out, the group is untouched except that its sources are
off; re-run the command (with a longer `--timeout`) once the laggard catches
up.

## Sizing and placement

- **More partitions than runners buys nothing.** The start proceeds — a fleet
  can grow — but the CLI warns loudly, naming both counts and how many members
  will share machines.
- **Placement is statistical.** Members place through the existing HRW hash like
  any operative, so two members of one group can land on the same runner.
  `flz operative info` shows per-member placement, so co-location is visible.
  Anti-affinity is a named follow-up (`partition-placement-anti-affinity`).
- **One partition's whole state lives on one controller node.** That is a
  property of partition-as-instance, not a rule anything has to enforce, and it
  is what keeps the state layer's per-class input log correct.
- **Quota.** N partitions consume N `max_operatives` units and open N source
  connections.

## The hot-key limit, and the pattern for it

Per-key ordering means one key is served by one Entity on one machine. A key
hot enough to saturate a core cannot be split by partitioning — Flink behaves
identically — so split it in the topology instead, with an explicit two-stage
aggregation.

Take OHLC candles where `BTC-USD` alone carries most of the volume:

```rust
// Stage 1 — partitioned: fan one symbol across 8 sub-keys, aggregate locally.
c.input::<Trade>("trades")
    .key_by(|t: &Trade| format!("{}#{}", t.symbol, t.trade_id % 8))
    .into::<PartialCandleProcedure>()
    .named("partials")
    .keyed()
    .sink("partial_candles");

// Stage 2 — a single-partition merger, fed by forward: one Entity per symbol.
c.input::<PartialCandle>("partial_candles")
    .key_by(|p: &PartialCandle| p.symbol.clone())
    .into::<CandleProcedure>()
    .sink("candles");
```

Stage 1 runs as `partials --partitions 8`; stage 2 runs unpartitioned (or with
its own smaller N) and receives stage 1's keyed output through
`operative_forward`. The sub-key is the author's choice, and it is the sub-key
— not the symbol — that must be what stage 1's `key_by` returns, or the guard
will say so on the first frame.

## Limits, named

- Rescale is stop-the-group with a cold restore. A warm working-set handoff and
  live (non-quiesced) rescale are follow-ups, not silent gaps.
- Assignment is contiguous and even. Weighted assignment has wire and opts
  support but no authoring surface yet.
- Workflow operatives are not partitionable (`--partitions` is refused for
  `kind: workflow`).
- Placement anti-affinity is deferred.
- A rescale spanning two controllers is covered at the mechanism level only —
  the file move between nodes and the new member adopting its tree each have
  tests, but the orchestration above them is exercised single-node.
- Each partition's sink plugins open their own connections; sharing a Postgres
  connection across sink workers is a named follow-up
  (`postgres-sink-connection-sharing`).

## See also

- `docs/glossary.md` — partition, key group, group
- `docs/operative_architecture.md` — where a partition sits in the graph model
- `docs/sdk_protocol_spec.md` — the `AddOperative` partition fields
- `docs/websocket_subscription_source.md` — the filtered subscription source
- `docs/operative_forward_plugin.md` — forwarding to a partitioned target
- `docs/resilience/state_persistence.md` — how a rescale moves durable state
