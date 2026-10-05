# rabbitmq-plugin Specification

## Purpose
RabbitMQ in both directions, as a plugin package outside the core: a consumer that reads one
queue onto a relay's entry channel and acknowledges each message only after the relay
took it, and a producer that publishes each output to an exchange and holds a
delivery-guaranteed one until the broker confirms it.
## Requirements
### Requirement: RabbitMQ SHALL be a plugin package built outside the core

The providers `rabbitmq_consumer` and `rabbitmq_producer` SHALL be the providers of the plugin
package `flezha_rabbitmq`, built from the repository `flezha_rabbitmq_plugin` with
`mix flezha.plugin.package`, carrying no native code and loading in a release of the core at
boot.

#### Scenario: Installed, it round-trips

- **GIVEN** a release with the package installed and the dev broker
- **WHEN** one relay bound to a `rabbitmq_producer` publishes 100 outputs to a queue and a
  second relay bound to a `rabbitmq_consumer` on that queue reads it
- **THEN** the second relay SHALL receive the 100 payloads

### Requirement: Both providers SHALL connect, report and recover the same way

Both providers SHALL take the connection options `host`, `port`, `virtual_host`, `username`,
`password` or `password_env`, `ssl`, `ssl_options` and `heartbeat_s`, SHALL keep the password
out of worker state, logs, telemetry and `flz plugin list`, SHALL report unhealthy with
`rabbitmq.connection_failed` or `rabbitmq.channel_closed` (with the broker's reply code and
text) while their connection or channel is lost, and SHALL reconnect along their backoff curve
and resume consuming or publishing.

#### Scenario: A broker restart

- **GIVEN** a running consumer and producer
- **WHEN** the broker is restarted
- **THEN** both SHALL be unhealthy while it is down and healthy after it is back, and both
  SHALL resume without being enabled again

#### Scenario: A missing queue

- **WHEN** a consumer is enabled on a queue that does not exist, without `declare`
- **THEN** it SHALL be unhealthy with `rabbitmq.channel_closed` and the code 404

### Requirement: The consumer SHALL acknowledge a delivery only after the relay took it

`rabbitmq_consumer` SHALL consume its `queue` with manual acknowledgement and a prefetch of
`prefetch`, SHALL hand deliveries to the relay in batches of at most `prefetch` or
`batch_ms` with `inject_many/4`, SHALL acknowledge the longest prefix of deliveries whose
verdict was `:ok`, and SHALL return every other delivery of the batch to the queue. When the
relay's input is durable a verdict SHALL mean the input log holds the delivery.

#### Scenario: A crash with durable input loses nothing

- **GIVEN** a relay with `effectively_once` bound to a consumer on a queue of 10 000
  messages, each with a `message_id`
- **WHEN** the controller is killed mid-way and started again
- **THEN** every message SHALL have reached the relay, the queue SHALL be empty, and the
  relay SHALL have received each `message_id` once while its input log held the first copy

#### Scenario: A refused delivery goes back to the queue

- **GIVEN** a relay whose input log refuses one append
- **WHEN** that delivery's verdict is the refusal
- **THEN** it and the deliveries after it in the batch SHALL be returned to the queue and
  delivered again

#### Scenario: Competing consumers

- **GIVEN** two relays each bound to a consumer on the same queue
- **WHEN** 1 000 messages are published
- **THEN** each message SHALL reach exactly one of the two relays

### Requirement: A delivery SHALL become an input carrying where it came from

The payload SHALL be the message body. The metadata SHALL carry `rabbitmq.exchange`,
`rabbitmq.routing_key`, `rabbitmq.redelivered`, `rabbitmq.headers`, `rabbitmq.message_id` and
`rabbitmq.timestamp` when set, and `flz.msg_id` `<plugin_id>/<id>`, where the id is the
`message_id` property, or the header `msg_id_header` names when it is set, when the message has
one.

#### Scenario: A message id becomes the duplicate identity

- **GIVEN** a consumer plugin `orders`
- **WHEN** a message with `message_id` `m-1` is consumed
- **THEN** the input's metadata SHALL carry `flz.msg_id` `orders/m-1` and `rabbitmq.message_id`
  `m-1`

### Requirement: The producer SHALL publish each output with its identity

`rabbitmq_producer` SHALL publish each payload dispatched to its hooks to `exchange` with
`routing_key`, or with the output's `flz.key` when `routing_key_from` is `flz_key`,
persistent unless `persistent` is false, with the headers `flz.key`, `flz.cell`, `flz.seq` and
`flz.epoch` from the output and its static `headers`; a `headers` name starting with `flz.`
SHALL be refused at enable with `plugin.headers_invalid`. A delivery-guaranteed output SHALL
carry `message_id` `<cell>/<seq>/<epoch>`.

#### Scenario: Routing by key

- **GIVEN** a topic exchange bound to two queues by `EUR.*` and `USD.*`, and a producer with
  `routing_key_from: flz_key`
- **WHEN** outputs keyed `EUR.USD` and `USD.JPY` are emitted
- **THEN** each SHALL arrive in the queue its key routes to, carrying its `flz.*` headers

### Requirement: A guaranteed output SHALL be held until the broker confirms it

The producer SHALL publish on a channel in confirm mode. A plain output SHALL be published
without the relay waiting. While the connection is down outputs SHALL wait in the plugin's
queue: a full queue SHALL drop and count plain outputs as `queue_overflow` says, and SHALL keep
every guaranteed one. A delivery-guaranteed output SHALL be held in the outbox until the
broker's acknowledgement, SHALL be published again with the same `message_id` after a negative
acknowledgement or a connection loss before its acknowledgement, SHALL be parked with
`rabbitmq.exhausted` when its retries run out, and, with `mandatory`, SHALL be parked with
`rabbitmq.unroutable` when the broker returns it.

#### Scenario: A broker restart during publishing

- **GIVEN** a relay with `effectively_once` emitting 1 000 outputs through the producer to
  a durable queue
- **WHEN** the broker is restarted in the middle
- **THEN** the queue SHALL hold every output at least once, every copy of one output SHALL carry
  the same `message_id`, and the outbox SHALL be empty afterwards

#### Scenario: An unroutable output becomes a dead letter

- **GIVEN** a producer with `mandatory: true` to an exchange with no binding for its routing key
- **WHEN** a relay with `effectively_once` emits an output
- **THEN** the relay's DLQ SHALL hold it with `rabbitmq.unroutable`

### Requirement: The consumer SHALL NOT be shared or bound to a partitioned relay, and the producer SHALL be shareable

`rabbitmq_consumer` SHALL declare itself not shareable and SHALL refuse a partitioned relay
with `plugin.partitioned_host`; `rabbitmq_producer` SHALL declare itself shareable, and relays
bound to one producer on a controller SHALL publish through one connection.

#### Scenario: One producer, two relays

- **GIVEN** two relays on one controller bound to one `rabbitmq_producer`
- **THEN** the broker SHALL show one connection from that plugin on that controller

