# Kafka consumer plugin

The Kafka consumer plugin delivers the records of Kafka topics onto a relay's entry
channel. It joins the consumer group `group_id` for its `topics`, injects every record of
the partitions assigned to it as one input, and commits the group's offsets to Kafka as the
relay's input durability says. `FlezhaKafka.Consumer` is its provider, under the
provider id `kafka_consumer`, in the Kafka plugin package beside the producer
([`kafka_producer.md`](kafka_producer.md), which also covers installing the package).

| | |
| --- | --- |
| Type | relay plugin |
| Ships | package `flezha_kafka` |
| Shareable | no: one group member per binding, so a second relay bound to it is refused (`plugin.not_shareable`) |
| Partitioned relays | refused (`plugin.partitioned_host`): Kafka partitions are not Flezha key groups |
| Durability | each fetched batch waits for the relay's verdicts; offsets are committed after every confirmed batch for durable input, otherwise at most every `commit_interval_ms` |

## Options

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `hosts` | any | none | yes | `{host, port}` list, or a comma-separated string |
| `auth` | any | none | no | SASL configuration, as a `KafkaEx.Auth.Config` or a map |
| `sasl_password_env` | string | none | no | Environment variable holding the SASL password, preferred over an inline one |
| `use_ssl` | bool | none | no | TLS to the brokers |
| `ssl_options` | any | none | no | Erlang `:ssl` options for that connection |
| `topics` | names | none | yes | The topics to consume, comma-separated |
| `group_id` | string | none | yes | The consumer group |
| `channel` | names | none | no | The channel this plugin is attached to: the entry channel every record is injected on, and one name. Required, unless `channel_in` names it |
| `channel_in` | names | none | no | Entry channel every record is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it |
| `plugin_id` | string | none | no | Plugin id; defaults to the id it was enabled under |
| `start_at` | enum | `"end"` | no | Where a group with no committed offset starts: the earliest record, or the next one written |
| `max_bytes` | int | `1048576` | no | Bytes one fetch takes from one partition |
| `max_wait_ms` | int | `500` | no | How long a fetch waits for records; below the client's 5 s request timeout |
| `commit_interval_ms` | int | `5000` | no | Longest the offsets of taken records wait to be committed, unless the relay's input is durable |
| `lag_interval_ms` | int | `5000` | no | How often the lag of each assigned partition is read |
| `failure_threshold` | int | `3` | no | Refused batches in a row before the plugin reports unhealthy |
| `idle_after_ms` | int | `0` | no | Age of the last settled lag read past which the source is idle; 0 derives it from `lag_interval_ms` |
<!-- provider-options:end -->

## Example

```sh
$ flz plugin enable trades-in kafka_consumer --relay_id=pricer \
  --Xhosts=kafka1:9092 --Xtopics=trades --Xgroup_id=pricing \
  --Xchannel=trades --Xstart_at=beginning
```

## Contract

| | |
| --- | --- |
| Hooks | none needed: the consumer drives itself, so a binding names no hook (the provider also defines `:on_terminate`) |
| Shared | no: one group member per binding, so a second relay bound to it is refused (`plugin.not_shareable`), and a member of a partition group is refused (`plugin.partitioned_host`), because Kafka partitions are not Flezha key groups |
| Upstream | the topics' partitions Kafka assigns to this member of `group_id` |
| Injection | one record as one input, its value as the payload, on the entry channel `:channel` (or `:channel_in`) names, with `flz.msg_id` set |
| Delivery | each fetched batch waits for the relay's verdicts; a batch with a refusal commits nothing and is fetched again |
| Failure posture | a group that exits is started again after `lag_interval_ms`; a refused batch is fetched again from the last committed offset |

As a plugin spec at relay start (`flz relay start --plugins`):

```json
[
  {
    "name": "trades-in",
    "provider": "kafka_consumer",
    "opts": {
      "hosts": "kafka1:9092,kafka2:9092",
      "topics": "trades",
      "group_id": "pricing",
      "start_at": "beginning",
      "channel": "trades"
    },
    "hooks": ["on_terminate"]
  }
]
```

Enabled and bound in two commands, with the settings written without their dashes:

```sh
$ flz plugin enable trades-in kafka_consumer Xhosts=kafka1:9092 Xtopics=trades \
  Xgroup_id=pricing Xchannel=trades Xstart_at=beginning
$ flz plugin bind trades-in --relay_id pricer --hook=on_terminate
```

## Options in detail

`hosts`, `auth`, `sasl_password_env`, `use_ssl` and `ssl_options` are the producer's, from
one connection module both providers use: a credential is held in memory, the prepared
`auth` carries `**redacted**` in place of the password, and `sasl_password_env` is read again
at every group start. Prefer it: an inline password does not survive a move to another
controller.

`start_at` places a group that has no committed offset for a partition: `beginning` reads
from the earliest record Kafka still holds, `end` from the next one written. A group with a
committed offset resumes from it, whatever `start_at` says.

## A record as an input

The payload is the record's value, an empty binary for a null value. The metadata values are
strings, as every input's are:

| Key | Value |
| --- | --- |
| `channel` | the entry channel |
| `kafka.topic`, `kafka.partition`, `kafka.offset` | where the record is |
| `kafka.timestamp` | the record's timestamp in milliseconds |
| `kafka.key` | the record's key, when it has one |
| `kafka.header.<name>` | each header's value; the last one wins on a repeated name |
| `flz.msg_id` | `<plugin_id>/<topic>/<partition>/<offset>` |

A key or header value that is not UTF-8 text is carried Base64-encoded under its name with
`.b64` appended (`kafka.key.b64`, `kafka.header.<name>.b64`).

## Delivery and commits

Every fetched batch is handed to the relay with `Host.inject_many/4`, up to
`FLEZHA_PLUGIN_INGEST_WINDOW` records in flight, and the consumer waits for the verdicts,
2 s at most, before it fetches more. How the offsets are committed follows the relay's
input:

- **Durable input** (effectively-once, sleeping or durable steps): a verdict means the record
  is in the relay's input log, and the batch's offsets are committed before the next
  fetch. A record fetched again after a crash, a restart or a rebalance is absorbed by its
  `flz.msg_id` while the input log still holds it; with effectively-once the relay's
  outputs are effectively once on top of that.
- **Otherwise**: a verdict means the relay accepted the record, and the offsets are
  committed at most every `commit_interval_ms`, and when the partition consumer stops. A
  crash can lose records taken but not processed, and repeats up to `commit_interval_ms` of
  them.

A batch with any verdict that is not `:ok` (an input log that refuses an append, a relay
that does not answer within the 2 s) commits nothing: its partition consumer logs the
refusal and stops, and is started again at once, fetching from the last committed offset.
The records before the refused one come again, absorbed with durable input and repeated
without it. A Kafka fetch returns whole record batches, so `max_bytes` bounds a fetch only
down to the size of one batch the producer wrote.

## Scaling with a group

Kafka splits a group's partitions between its members, round-robin. Several relays,
each bound to its own `kafka_consumer` with the same `group_id` and `topics`, on one
controller or on several, are several members: each record reaches one of them. A member
with no partition left (more members than partitions) consumes nothing until a rebalance
gives it one. One relay consuming many partitions is limited by that relay; add
members to go wider.

## Rebalances

A member joining or leaving makes Kafka assign the partitions again. A partition taken from
this member gets 2.5 s to finish: its batch either gets every verdict and commits (at once
with durable input, the acknowledged offset at stop without), or ends refused and commits
nothing; one still running past the 2.5 s is stopped and commits nothing. The member that
takes the partition resumes from the last committed offset, so the overlap is absorbed with
durable input and repeated without it. Each change of this member's assignment is the event
`rebalanced`.

## Health

| Verdict | When |
| --- | --- |
| `{:unhealthy, {:read_failed, error}}` | `failure_threshold` batches in a row were refused; `error` names the verdict; the next batch taken clears it |
| `{:unhealthy, {:poll_idle, age_ms}}` | no lag read has settled within `idle_after_ms` (three `lag_interval_ms` when 0): the broker is away, or the group has not joined |
| `{:unhealthy, :no_member}` / `{:unhealthy, :member_dead}` | no group member, or a dead one |
| `:healthy` | otherwise, including a topic nothing is written to |

A lag read settles when the group has answered its assignment and every offset was read. A
group that exits (a failed join, a partition consumer that failed three times in five
seconds) is logged and joined again after `lag_interval_ms`. `flz relay info` shows the
group, the topics, the last assignment and lag, and the verdict.

## Lag

Every `lag_interval_ms` the member reads, for each partition assigned to it, the latest
offset and the group's committed one; the lag is their difference, the consumer-group lag
Kafka's own tools report. A partition with no committed offset counts from where `start_at`
places it. Without durable input the lag trails the records taken by up to
`commit_interval_ms`; with durable input a stalled runner does not raise it, because a
verdict is the log append.

## Telemetry

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:flezha, :plugin, :kafka_consumer, :shipped]` | `count` (records taken in one batch) | `plugin_id`, `topic`, `partition` |
| `[:flezha, :plugin, :kafka_consumer, :failed]` | `count` | `plugin_id`, and `topic`, `partition` for a refused batch or `group_id` for a group that exited; `reason` |
| `[:flezha, :plugin, :kafka_consumer, :lag]` | `lag` | `plugin_id`, `group_id`, `topic`, `partition` |
| `[:flezha, :plugin, :kafka_consumer, :rebalanced]` | `count` (partitions assigned) | `plugin_id`, `group_id`, `partitions` (`topic/partition`, comma-separated) |

They are bridged to the observability bus under the source `:flezha_kafka_consumer`:
`shipped` and `lag` as metrics, `failed` and `rebalanced` as events.

## The client timeout

The `kafka_ex` client bounds every request it makes by its `sync_timeout`, whatever timeout
the caller names. A group's join waits at the broker for the rebalance: 3 s for a new group
(the broker's `group.initial.rebalance.delay.ms`), and at most the rebalance timeout the
member joins with, 4 s. The library's 1 s default covers neither, so the package sets
`sync_timeout` to 5 s, above both and above the longest `max_wait_ms`. The member's other
timings fit inside it: a heartbeat every second, a 10 s session, and a partition consumer
given 2.5 s to stop. The setting is the whole library's: while a broker is away, the
producer's worker can answer a stats call up to 5 s late, and loses nothing.

## Limits

- **Read committed is not offered.** The client library delivers the records of aborted
  transactions, so a topic written transactionally is read as `read_uncommitted`.
- **A partitioned relay is refused.** Bind the consumer to a relay that is not
  partitioned and route from there.
- **No regex subscription, no manual assignment, no seek.** The group decides; a reset of
  the group's offsets is Kafka's own tooling.
- **A broker that holds a connection open without answering** while a group starts keeps the
  member from answering its probe until the request's 5 s run out; the probe then reads as
  uncertain, which counts as healthy.
- **`channel` is not validated at attach.** As for every source, channel names are an SDK-side
  fact the controller never sees.

## See also

- [kafka_producer.md](kafka_producer.md): the producer, its connection options and installing the package
- [postgres_poll.md](postgres_poll.md): the Postgres poll source, whose health verdicts this one shares
- [plugins.md](../topics/plugins/plugins.md): the plugin model and every source and output plugin
