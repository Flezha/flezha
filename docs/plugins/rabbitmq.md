# RabbitMQ plugins

The RabbitMQ plugins deliver the messages of a RabbitMQ queue onto an operator's entry
channel, and publish an operator's outputs to a RabbitMQ exchange. Both providers live in
the RabbitMQ plugin package: `FlezhaRabbitmq.Consumer` under the provider id
`rabbitmq_consumer`, and `FlezhaRabbitmq.Producer` under the provider id
`rabbitmq_producer`. This page covers installing the package, the connection both share,
the consumer's acknowledgements and message ids, the producer's routing, headers and
confirms, recovery, topology declaration, health, telemetry and limits.

| | `rabbitmq_consumer` | `rabbitmq_producer` |
| --- | --- | --- |
| Type | operator plugin | operator plugin |
| Ships | package `flezha_rabbitmq` | package `flezha_rabbitmq` |
| Shareable | no: one consumer per binding (`plugin.not_shareable`) | yes: every operator bound to it on a controller publishes through one worker and one connection |
| Partitioned operators | refused (`plugin.partitioned_host`) | allowed: each member runs its own copy |
| Durability | a delivery is acknowledged only after the operator took it; with durable input `flz.msg_id` absorbs a redelivery | a delivery-guaranteed output is held until the broker confirms it; a plain one is not awaited |

## Options

### Consumer

<!-- provider-options:begin rabbitmq_consumer -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `host` | string | `"localhost"` | no | Broker host |
| `port` | int | &mdash; | no | AMQP port; 5672, or 5671 with `ssl` |
| `virtual_host` | string | `"/"` | no | Virtual host |
| `username` | string | `"guest"` | no | User the plugin connects as |
| `password` | string | &mdash; | no | Password, redacted once the plugin is prepared |
| `password_env` | string | &mdash; | no | Environment variable holding the password instead |
| `ssl` | bool | `false` | no | TLS to the broker, verifying it against the system's CA store |
| `ssl_options` | any | &mdash; | no | Erlang `:ssl` options, passed through |
| `heartbeat_s` | int | `30` | no | AMQP heartbeat; 0 turns it off |
| `reconnect_backoff_ms` | int | `500` | no | First wait before connecting again after a loss |
| `reconnect_max_backoff_ms` | int | `30000` | no | Ceiling for that wait |
| `queue` | string | &mdash; | yes | The queue to consume |
| `channel` | names | &mdash; | no | The channel this plugin is attached to: the entry channel every message is injected on, and one name. Required, unless `channel_in` names it |
| `channel_in` | names | &mdash; | no | Entry channel every message is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it |
| `plugin_id` | string | &mdash; | no | Plugin id; defaults to the id it was enabled under |
| `prefetch` | int | `200` | no | Deliveries the broker sends before the first of them is acknowledged |
| `batch_ms` | int | `10` | no | Longest a delivery waits for others before its batch goes to the operator |
| `msg_id_header` | string | &mdash; | no | A header whose value is the message's id, in place of its `message_id` |
| `declare` | bool | `false` | no | Declare `queue` durable when it does not exist |
| `bind_exchange` | string | &mdash; | no | With `declare`: an exchange the queue is bound to |
| `routing_keys` | names | &mdash; | no | With `bind_exchange`: the binding keys, comma-separated; one empty key when unset |
| `failure_threshold` | int | `3` | no | Refused batches in a row before the plugin reports unhealthy |
<!-- provider-options:end -->

### Producer

<!-- provider-options:begin rabbitmq_producer -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `host` | string | `"localhost"` | no | Broker host |
| `port` | int | &mdash; | no | AMQP port; 5672, or 5671 with `ssl` |
| `virtual_host` | string | `"/"` | no | Virtual host |
| `username` | string | `"guest"` | no | User the plugin connects as |
| `password` | string | &mdash; | no | Password, redacted once the plugin is prepared |
| `password_env` | string | &mdash; | no | Environment variable holding the password instead |
| `ssl` | bool | `false` | no | TLS to the broker, verifying it against the system's CA store |
| `ssl_options` | any | &mdash; | no | Erlang `:ssl` options, passed through |
| `heartbeat_s` | int | `30` | no | AMQP heartbeat; 0 turns it off |
| `reconnect_backoff_ms` | int | `500` | no | First wait before connecting again after a loss |
| `reconnect_max_backoff_ms` | int | `30000` | no | Ceiling for that wait |
| `exchange` | string | `""` | no | The exchange each payload is published to; the default exchange when empty |
| `routing_key` | string | `""` | no | The routing key of every payload, unless `routing_key_from` names another |
| `routing_key_from` | enum | &mdash; | no | `flz_key`: each output's own `flz.key` is its routing key |
| `persistent` | bool | `true` | no | Delivery mode 2: the broker writes it to disk |
| `mandatory` | bool | `false` | no | The broker returns a payload no queue is bound for; a guaranteed one is parked |
| `headers` | json | `%{}` | no | Static message headers, a JSON object; names starting `flz.` are refused |
| `content_type` | string | &mdash; | no | The message's `content_type` property |
| `declare_exchange` | bool | `false` | no | Declare `exchange` durable when it does not exist |
| `exchange_type` | enum | `"direct"` | no | With `declare_exchange`: the exchange's type |
| `channel` | names | &mdash; | no | The channel this plugin is attached to: only payloads naming it are published. Several names, comma-separated, publish them all |
| `channel_out` | names | &mdash; | no | Publish only payloads whose metadata channel matches; the explicit spelling |
| `plugin_id` | string | &mdash; | no | Plugin id; defaults to the id it was enabled under |
| `retries` | int | `5` | no | Publishes again of a guaranteed output the broker refused or a lost connection left unconfirmed |
| `retry_backoff_ms` | int | `250` | no | First delay between retries |
| `retry_max_backoff_ms` | int | `10000` | no | Ceiling for that delay |
| `queue_max` | int | `10000` | no | Payloads waiting while the connection is down |
| `queue_max_bytes` | int | `67108864` | no | Payload bytes waiting while the connection is down |
| `queue_overflow` | enum | `:drop_oldest` | no | What a full queue does with a plain output: keep it past the bound, drop the oldest plain one, or drop the newest; a guaranteed one always waits |
<!-- provider-options:end -->

## Example

```sh
$ flz plugin enable rates-in rabbitmq_consumer --operator_id=pricer \
  --Xhost=broker --Xqueue=rates --Xchannel=rates --Xpassword_env=RABBITMQ_PASSWORD
$ flz plugin enable rates-out rabbitmq_producer --operator_id=pricer --hook=on_output \
  --Xhost=broker --Xexchange=rates --Xrouting_key_from=flz_key \
  --Xpassword_env=RABBITMQ_PASSWORD
```

## Installing

Build the archive from the package (its README: `mix flezha.plugin.package`), then install
it and restart the controller, or name it in `FLEZHA_PLUGINS`:

```sh
$ flz plugin install flezha_rabbitmq-0.1.0.fpz
$ flz controller stop && flz controller start
$ flz plugin list --installed
```

The package bundles the `amqp` client and what it needs that the release does not ship
(`amqp_client`, `rabbit_common`, `credentials_obfuscation`, `thoas`, `ranch`); none of them
is native code, and the controller starts them when it loads the package.

## The connection

Each worker owns one connection and one channel. The connection options are the same rows on
both providers: `host`, `port` (5672, or 5671 with `ssl`), `virtual_host`, `username`,
`password` or `password_env`, `ssl` (verifying the broker against the system's CA store),
`ssl_options` (passed through to Erlang's `:ssl`) and `heartbeat_s`.

A literal `password` is redacted once the plugin is prepared and held in memory only;
`password_env` is read again at every connect. Neither is written to a log line, a telemetry
label or `flz plugin list`. The broker's management API shows each connection as
`flezha <plugin> <node>`.

A lost connection or channel (a broker restart, a network cut, a channel the broker closed)
makes the plugin unhealthy with `rabbitmq.connection_failed` (the host and the reason) or
`rabbitmq.channel_closed` (the broker's reply code and text), and the worker connects again
along its backoff curve, `reconnect_backoff_ms` doubling up to `reconnect_max_backoff_ms`,
then resumes consuming or publishing without the plugin being enabled again.

## The consumer

| | |
| --- | --- |
| Hooks | none needed: the consumer drives itself, so a binding names no hook (the provider also defines `:on_terminate`) |
| Shared | no: one consumer per binding, so a second operator bound to it is refused (`plugin.not_shareable`), and a member of a partition group is refused (`plugin.partitioned_host`) |
| Upstream | one queue, with a prefetch window |
| Injection | one message as one input, its body as the payload, on the entry channel `:channel` (or `:channel_in`) names |
| Delivery | each batch waits for the operator's verdicts; a delivery is acknowledged only after the operator took it |

Enabled and bound in two commands, with the settings written without their dashes:

```sh
$ flz plugin enable rates-in rabbitmq_consumer Xhost=broker Xqueue=rates Xchannel=rates \
  Xpassword_env=RABBITMQ_PASSWORD
$ flz plugin bind rates-in --operator_id pricer --hook=on_terminate
```

Several operators bound to consumers of one queue are competing consumers, as RabbitMQ
intends: each message reaches one of them.

### A message as an input

The payload is the message body. The metadata carries the entry channel and:

| Key | Value |
| --- | --- |
| `rabbitmq.exchange` | the exchange it was published to, empty for the default exchange |
| `rabbitmq.routing_key` | its routing key |
| `rabbitmq.redelivered` | `true` when the broker delivered it before |
| `rabbitmq.headers` | its headers as a JSON object; text that is not UTF-8 is Base64 |
| `rabbitmq.message_id` | its `message_id` property, when set |
| `rabbitmq.timestamp` | its `timestamp` property, when set |
| `flz.msg_id` | `<plugin_id>/<id>`, where the id is the `message_id` property, or the header `msg_id_header` names when it is set; absent when the message has none |

### Acknowledgement

The consumer consumes with manual acknowledgement and a prefetch of `prefetch`. Deliveries
collect until half the prefetch or `batch_ms`, so the broker keeps sending while a batch is
with the operator, then go to the operator in one batch. The longest prefix of the batch the
operator took is acknowledged with one `basic.ack`; the first refused delivery, every one
after it and every one held behind the batch go back to the queue with one `basic.nack`. A
returned delivery comes straight back, so after a refusal the consumer waits along its
backoff curve before it hands the operator anything again.

What a level guarantees:

- **With durable input** (an `effectively_once` operator, or one that journals its input), a
  verdict means the input log holds the delivery. A crash before the acknowledgement
  redelivers it, and its `flz.msg_id` absorbs the copy while the log holds the first one. A
  message with no id is at least once.
- **Without it**, a delivery is acknowledged once the operator accepted it. A crash loses what
  the operator had not processed, and a delivery the broker redelivers (the connection was lost
  before its acknowledgement) reaches the operator twice, because nothing absorbs it.

A delivery tag belongs to its channel: when the connection or the channel is lost, the
deliveries held are forgotten, never acknowledged on the new channel, and the broker delivers
them again.

## The producer

| | |
| --- | --- |
| Hooks | `:on_output` or `:on_input` |
| Shared | yes: every operator bound to one plugin on a controller publishes through one worker and one connection |
| Downstream | one exchange |
| Delivery | a guaranteed output is held until the broker confirms it; a plain one is published without the operator waiting |

```sh
$ flz plugin enable rates-out rabbitmq_producer Xhost=broker Xexchange=rates \
  Xrouting_key_from=flz_key Xpassword_env=RABBITMQ_PASSWORD
$ flz plugin bind rates-out --operator_id pricer --hook=on_output
```

The hook holds a delivery-guaranteed output in the outbox and hands it to the plugin's
worker; nothing else runs per output. The worker builds each message, publishes it on a
channel in confirm mode and settles it.

### Routing and headers

Each output goes to `exchange` (the default exchange when empty, which routes by queue name)
with `routing_key`, or with its own `flz.key` when `routing_key_from` is `flz_key`. A
message carries the headers `flz.key`, `flz.cell`, `flz.seq` and `flz.epoch` from its
output, then the static `headers`; a static header named `flz.*` is refused at enable
(`plugin.headers_invalid`). Messages are persistent (delivery mode 2) unless `persistent` is
false.

### Message ids and confirms

A delivery-guaranteed output is published with `message_id` `<cell>/<seq>/<epoch>`, the
same on every attempt, and is confirmed on the broker's `basic.ack`. A `basic.nack`, or a
lost connection before the ack, publishes it again along the retry curve
(`retry_backoff_ms` up to `retry_max_backoff_ms`) while its attempts stay within
`retries`; then it is parked as a dead letter with `rabbitmq.exhausted`. With
`mandatory`, an output no queue is bound for is returned by the broker and parked at once
with `rabbitmq.unroutable`. A consumer that wants each output once deduplicates on
`message_id`, which this package's consumer does through `flz.msg_id`.

A plain output is never awaited: its ack is counted `shipped`, and a nack, a return or a
lost connection before its ack is counted `failed`.

### The queue

While the connection is down, outputs wait in the worker's queue, bounded by `queue_max`
and `queue_max_bytes`. A guaranteed output always waits, past the bound if it must: the
outbox bounds how many there are. For a plain output `queue_overflow` decides:
`drop_oldest` (the default) drops the oldest plain output, `drop_newest` drops the new one,
and `drop_nothing` keeps it past the bound, since the hook never waits. Every drop is
counted as `overflowed`. A stop parks every guaranteed output the worker still holds, as
`http_push` does.

## Topology declaration

Off by default: topology usually belongs to whoever runs the broker. A consumer with
`declare` declares its queue durable, and with `bind_exchange` binds it to that exchange
under each of `routing_keys`. A producer with `declare_exchange` declares its exchange
durable, of `exchange_type`. RabbitMQ 4 refuses a queue that is neither durable nor
exclusive, so a queue the plugins share is durable.

## Health

| Verdict | When |
| --- | --- |
| `rabbitmq.connection_failed` | the connection is lost, or cannot be opened |
| `rabbitmq.channel_closed` | the broker closed the channel; a missing queue is code 404 |
| `{:read_failed, error}` | the consumer's last `failure_threshold` batches were refused |

The worker is asked for its facts with a bounded probe; one busy handing a batch to a slow
operator does not answer in time, which reads as uncertain and so healthy.

## Telemetry

Under `[:flezha, :plugin, :rabbitmq_consumer, verb]` and
`[:flezha, :plugin, :rabbitmq_producer, verb]`, each with `%{count: n}` and the plugin id:
`shipped`, `retried`, `failed` (with a `reason`) and `overflowed` (with the `policy`), the
verbs every provider shares, and `reconnected` (an event) on both. The consumer also emits
`unacked` (a metric) after each batch: the deliveries it holds that are not settled yet.

## Limits

- One queue per consumer, one exchange per producer. RabbitMQ streams, AMQP 1.0, RPC
  replies and dead-letter exchange configuration are not covered.
- A message without an id is at least once, even with durable input.
- `ssl_options` is passed through as given; client certificates beyond it are not managed.

## See also

- [kafka_consumer.md](kafka_consumer.md) and [kafka_producer.md](kafka_producer.md): the Kafka package, laid out the same way
- [http_push.md](http_push.md): the HTTP sink, whose queue rows the producer shares
- [plugins.md](../topics/plugins/plugins.md): the plugin model
