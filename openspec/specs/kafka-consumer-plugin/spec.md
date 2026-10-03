# kafka-consumer-plugin Specification

## Purpose

The `kafka_consumer` provider of the Kafka plugin package: one member of a Kafka consumer group
per binding, injecting the records of its share of the partitions into one operator and
committing the group's offsets as the operator's input durability says.

## Requirements
### Requirement: `kafka_consumer` SHALL consume a group's share of its topics into one operator

The provider `kafka_consumer` of the Kafka package SHALL join the consumer group `group_id`
for its `topics` and SHALL inject every record of the partitions assigned to it as one input
on its entry channel, in offset order per partition. A group with no committed offset SHALL
start at the earliest offset with `start_at: beginning` and at the end with `start_at: end`
(the default). It SHALL refuse a partitioned operator with `plugin.partitioned_host` and SHALL
declare itself not shareable.

#### Scenario: Records in order

- **GIVEN** a topic with one partition holding records 0 to 99 and a plugin with
  `start_at: beginning`
- **WHEN** it is bound to an operator
- **THEN** the operator SHALL receive the 100 values in offset order

#### Scenario: Two operators split a topic

- **GIVEN** a topic with 6 partitions and two operators, each bound to a `kafka_consumer`
  with the same `group_id`
- **WHEN** 600 records spread over the partitions are produced
- **THEN** each record SHALL reach exactly one of the two operators, and each operator SHALL
  be assigned at least one partition

#### Scenario: A partitioned operator is refused

- **WHEN** a `kafka_consumer` is enabled for a member of a partition group
- **THEN** it SHALL be refused with `plugin.partitioned_host`

### Requirement: A record SHALL become an input carrying where it came from

The payload SHALL be the record's value (an empty binary for a null value). The metadata SHALL
carry, as strings, `kafka.topic`, `kafka.partition`, `kafka.offset`, `kafka.timestamp` in
milliseconds, `kafka.key` when the record has one, `kafka.header.<name>` for each header (the
last one on a repeated name), a key or header value that is not UTF-8 text Base64-encoded
under its name with `.b64` appended, and `flz.msg_id`
`<plugin_id>/<topic>/<partition>/<offset>`.

#### Scenario: Key and headers arrive

- **WHEN** a record with key `EUR-USD` and header `source: feed` is consumed
- **THEN** the input's metadata SHALL carry `kafka.key` `EUR-USD`, `kafka.header.source`
  `feed` and its topic, partition and offset

#### Scenario: A null value and a binary key

- **WHEN** a record with no value and a key that is not UTF-8 text is consumed
- **THEN** the input's payload SHALL be empty and its metadata SHALL carry the key
  Base64-encoded as `kafka.key.b64`

### Requirement: The consumer SHALL follow the operator's input durability

The consumer SHALL hand each fetched batch to the operator with `inject_many/4` and wait for
the verdicts before fetching more. When every verdict is `:ok` it SHALL commit the batch's
offsets: at once when the operator's input is durable, and at most every
`commit_interval_ms` when it is not. When any verdict is not `:ok` it SHALL commit nothing for
the batch and SHALL fetch again from the last committed offset.

#### Scenario: A crash with durable input loses nothing

- **GIVEN** an operator with `effectively_once` bound to a consumer reading a topic of
  records
- **WHEN** the controller is killed mid-way and started again
- **THEN** every record SHALL have reached the operator at least once by its `flz.msg_id`

#### Scenario: Without durable input commits follow the timer

- **GIVEN** a plain flow operator bound to a consumer with `commit_interval_ms` 5000
- **WHEN** it consumes for two seconds at full speed
- **THEN** the group's committed offset SHALL have moved at most once in those two seconds

#### Scenario: A refused batch is fetched again

- **GIVEN** an operator that refuses the first batch it is handed
- **WHEN** the consumer's batch receives that refusal
- **THEN** it SHALL commit nothing for the batch and the operator SHALL receive every record of
  it after the consumer fetches again

### Requirement: The consumer SHALL report its group, its lag and its stalls

The consumer SHALL read its lag every `lag_interval_ms`, and a read SHALL settle only when the
group answered its assignment and every offset was read. It SHALL be unhealthy with
`{:poll_idle, age_ms}` when no read has settled within `idle_after_ms` (three
`lag_interval_ms` when 0), which covers a broker that is away and a group that cannot join,
and with `{:read_failed, error}` after `failure_threshold` refused batches in a row; healthy
again after the next settled read and the next batch taken. It SHALL emit each change of its
assignment as the event `rebalanced` with the assigned partitions, and the lag of each
assigned partition (latest offset minus the group's committed offset) as the metric `lag`.

#### Scenario: The broker goes away

- **GIVEN** a running consumer with `idle_after_ms` 5000
- **WHEN** the broker is paused for 10 seconds and resumed
- **THEN** the plugin SHALL be unhealthy with `poll_idle` during the pause and healthy after
  it, and no record SHALL be lost for an operator with durable input

#### Scenario: Lag is visible

- **GIVEN** a consumer whose operator refuses every batch while 1 000 records are produced
- **THEN** the `lag` metric for its partition SHALL reach at least 900

