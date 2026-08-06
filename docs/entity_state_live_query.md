# Reading an Entity's live state

`entity-state-live-query` answers one question the platform could not answer
before: **what is this Entity's value right now?**

```
$ flz state get ohlc/candle/BTCUSD@1m
state get ohlc/candle/BTCUSD@1m:
  change_seq: 412
  state:      {"candle":{"BTCUSD@1m":{"open":64100.0,"high":64380.5,"volume":19.4}}}
```

Until now the only read was `flz state at <entity> <seq|timestamp>` — the
controller's own WAL, at a point in the *past*. There was no "now". Anything
that needed the current value had to wait for it to be sunk to Postgres or
Kafka and read the copy: a second source of truth, always slightly behind, free
to diverge.

This is also the capability Flink gave up on. Queryable State was deprecated
and dropped in 2.0 because Flink's state lives inside a subtask that is
reshuffled on every rescale — there is no stable address to query. A flezha
Entity has a stable name, and the controller knows which runner holds it.

## What an author has to do

Nothing. Any Entity of a running Operative is readable, because the mechanism
is the `EntityState` command the SDK has always answered for migration and
handover — it serializes `Procedure::State` through the `Serialize` bound the
type already carries. Declaring a `Change` type is what makes the value
*durable*; nothing extra makes it *readable*.

| Procedure mode | Readable as |
|---|---|
| single (no `entity_keys`) | `<operative>/<procedure>` — one Entity under the Procedure's name |
| static (`entity_keys() = [...]`) | `<operative>/<procedure>/<declared key>` |
| dynamic (`.key_by` / `.keyBy` on the wiring) | `<operative>/<procedure>/<live key>` — whatever keys the stream actually produced |

Both SDKs sort object keys, so an equivalent state serializes byte-identically
on either runtime; a reader cannot tell Rust from Java by the shape.

## The selector, and what it costs

`EntityState` gained two optional fields. Each form narrows the walk, and the
work is proportional to what was asked for — a keyed lookup is one lookup, not
one walk over every key:

| Command | Serializes |
|---|---|
| `flz state get ohlc` — *not offered*; use the CLI forms below | — |
| `flz state get ohlc/candle` | every live Entity of `candle` |
| `flz state get ohlc/candle/BTCUSD@1m` | exactly that one Entity |

An `entity_key` without a `procedure` is refused: an Entity key is unique only
*within* its Procedure, so a bare key names nothing. (The CLI's path shape makes
this unreachable; the refusal exists because the wire command allows it.)

The selector-less form — every Procedure of the Operative — is the
migration/handover shape and is unchanged, byte for byte. That is deliberate:
an unset proto3 string encodes to nothing, so an old caller's frame is
identical to what it always sent.

## The freshness contract

**A live read returns the freshest value there is, and may be ahead of what is
durable.**

The SDK serializes under the Entity's state lock and does **not** drain pending
changes, so the reply includes changes `run` applied that the flusher has not
yet shipped. That is strictly better than a durable read — provided the caller
is told which version it got, which is what the seq is for:

- The reply frame carries `flz.change_seq`, the seq the state was serialized
  at, whenever the selector names exactly one Entity. It rides the **frame
  metadata**, not the JSON body, so the body keeps the shape the
  migration/handover caller already decodes.
- Two reads separated by work report different seqs.
- The value may never become durable — the process can die between the read and
  the flush. That is inherent to reading the freshest value, and is why the seq
  is reported rather than hidden.

A read triggers **no** compaction, snapshot, or WAL truncation. Compaction must
drain before it snapshots, because its `snapshot_seq` becomes a truncation
point; a read has no such obligation, and taking one would make an operator's
`flz state get` silently rewrite an Entity's durable history.

## A non-resident Entity is refused, never served from disk

There is deliberately no fallback to the durable store. Three reasons, any one
sufficient:

- **Dynamic-mode eviction is destructive** by existing decision — the recreate
  cold-starts, so there is no older value to serve.
- **The controller cannot decode durable state.** WAL bytes are postcard (Rust)
  / Kryo (Java) and it stores them opaquely — the same property that makes
  `EffectRecordBody.changes` SDK-owned. Any readable answer must be produced by
  the SDK.
- A fallback would mean binding a runner and restoring an Entity **as a side
  effect of a read**.

So the read fails, naming which case applies and pointing at the durable path:

| Refusal | Means | Raised by |
|---|---|---|
| `entity_evicted` | the Entity was idle-evicted; eviction is destructive, so there is no durable value either | SDK |
| `entity_not_found` | that Procedure holds no Entity by that key | SDK |
| `procedure_not_found` | the Operative declares no Procedure by that name | SDK |
| `entity_key_without_procedure` | a key was given with no Procedure to scope it | SDK |
| `{:not_resident, :suspended}` | the instance is suspended at a committed boundary and holds no runner resources | controller |
| `{:not_resident, :unbound}` | the Operative is not bound to a runner — refused rather than blocking until a bind occurs | controller |
| `{:unsupported_for_kind, :persistent_state}` | the kind has no per-Entity state | controller |
| `{:oversized_state, bytes}` | the reply exceeds `:wal_high_watermark_bytes` | controller |

Every refusal exits non-zero with the reason on stderr, and the non-resident
family names `flz state at` as the durable-read path.

**`entity_evicted` vs `entity_not_found` is a real distinction, not a guess.**
Eviction leaves nothing behind — the reaper unlinks the Entity's files — so
after the fact the two are indistinguishable unless something remembers. Each
SDK keeps a bounded FIFO (1024 keys) of what its reaper evicted, purely to
answer this. Beyond the cap the answer degrades to `entity_not_found`, which is
the safe direction: it claims no eviction it cannot show.

## Size bound

The reply is bounded by the existing `:wal_high_watermark_bytes` (16 MiB
default) — the same ceiling the bounded handoff uses. No new configuration key.
Over it, the read is refused with the byte count and the selector that would
narrow it:

```
$ flz state get orders/lines
state get: the selected state is 20971520 bytes, over the
:wal_high_watermark_bytes ceiling — narrow the read with
<operative>/<procedure>/<entity_key>
```

## Live read vs time-travel read

| | `flz state get` | `flz state at` |
|---|---|---|
| Question | what is it **now** | what was it **then** |
| Source | the runner's memory | the controller's WAL |
| Freshness | at or **ahead of** durable | exactly as durable at that seq |
| Needs a bound runner | yes | no |
| Version reported | `change_seq` it was read at | the seq/timestamp you asked for |
| Non-resident Entity | refused, naming why | served, if the WAL still holds it |

## Output formats

```
$ flz state get ohlc/candle/BTCUSD@1m --output json
{"change_seq":412,"path":"ohlc/candle/BTCUSD@1m","state":{"candle":{"BTCUSD@1m":{"high":64380.5}}}}
```

A whole-Procedure read names no single version, and prints `change_seq: -`
(JSON `null`) rather than picking one of the Entities' seqs.

## The programmatic seam

The CLI is one caller. The read is available in-process, which is what a later
HTTP surface consumes — building the listener, its authz and its response
contract was deliberately left out of this change rather than bundling two
decisions into one:

```elixir
# The narrowed read: {:ok, json, change_seq} | {:error, reason}
OperativeManager.get_entity_state("trading", "ohlc", %{
  procedure: "candle",
  entity_key: "BTCUSD@1m"
})

# Whole Operative, seq-less — the shape migration/handover has always used.
OperativeManager.get_entity_state("trading", "ohlc")

# The CLI verb itself, including path parsing, authz and rendering.
CliManager.state_get("ohlc/candle/BTCUSD@1m", "json")
```

`state_get/3` resolves the namespace that actually holds the Operative
(`OperativeManager.namespace_of/2`) rather than assuming `default`, and
authorizes as `:"state.get"` — a **viewer**-level action, like `state.list` and
`state.at`.

## Cost, and what this is not

Each read is one control round-trip plus one state-lock acquisition on the
Entity — the same lock wait compaction already takes, bounded by one
invocation. So a read cannot wedge an Entity that is making progress, but it is
a **point read for operators and low-rate readers**, not a serving API. A
high-rate path is still a sink: `.output(...)` into Kafka or Postgres, which is
built for fan-out and costs the Entity nothing per reader. Rate limiting
belongs to the future HTTP surface, not here.

It is also not a subscription: there is no change feed, no watch, no
cross-Operative or wildcard query. A reader that wants a stream already has the
plugin sink.

## Worked examples

- `sdk/rust/examples/live_state_read.rs`
- `sdk/java/flezha-sdk-examples/.../E11_LiveStateRead.java`

Both declare a dynamic-keyed Operative and show the exact `flz state get`
invocations that read one of its Entities, including the refusals.

## See also

- `docs/resilience/state_persistence.md` — "Live read (`flz state get`)": the
  same contract from the persistence side, next to compaction and retention.
- `docs/sdk_protocol_spec.md` — the `EntityState` command: selector fields,
  `flz.change_seq` metadata, and the refusal tokens on the wire.
- `docs/glossary.md` — "Live read vs time-travel read".
- `docs/stream_transforms.md` — `.key_by` / `.keyBy`, which is what creates the
  dynamic Entity keys a live read addresses.
