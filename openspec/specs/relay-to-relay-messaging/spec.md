# relay-to-relay-messaging Specification

## Purpose
TBD - created by archiving change relay-to-relay-messaging. Update Purpose after archive.
## Requirements
### Requirement: First-party relay-forward plugin provider

The system SHALL provide a first-party relay plugin provider,
`Flezha.Plugin.Provider.RelayForward`, whose delivery destination is another relay in
the same namespace rather than an external endpoint. The provider SHALL be enabled by name,
bound to one relay and unbound or disabled through `Flezha.Service.PluginManager` using the
same `%Flezha.Plugin{}` hook mechanism as every other relay plugin, SHALL declare itself
not shareable, and SHALL support the hooks `:on_output` and `:on_input`. The provider SHALL
declare no new hooks.

Required opts: `:target_relay_id`. Optional opts: `:queue_max`, `:retry_backoff_ms`,
`:retry_max_backoff_ms`, `:plugin_id`.

#### Scenario: Enabling the provider on a relay
- **WHEN** a `%Flezha.Plugin{}` whose provider is `Flezha.Plugin.Provider.RelayForward` and
  whose opts contain a `:target_relay_id` naming a different relay is bound with the
  hook `{:on_output, _}` to a relay
- **THEN** the bind SHALL succeed and subsequent output from that relay SHALL be
  delivered to the target relay's input stream

#### Scenario: Disabling the provider stops delivery
- **WHEN** an enabled relay-forward plugin is unbound from its relay, or disabled
- **THEN** no further output from the source relay SHALL be delivered to the target, and the
  provider's worker SHALL be stopped

#### Scenario: Missing target is refused at enable time
- **WHEN** a `%Flezha.Plugin{}` for this provider is enabled without a `:target_relay_id` opt
- **THEN** the enable call SHALL return an error naming the missing option and no worker SHALL
  be started

### Requirement: Delivery is asynchronous and never blocks the source relay

The provider's `:on_output` and `:on_input` callbacks SHALL enqueue the payload to a long-lived
worker process and return without waiting for delivery. No extension callback SHALL perform a
synchronous call into another relay's Overwatch. The worker SHALL be supervised through the
shared `Flezha.Plugin.Provider.WorkerSup` core as its own provider family, keyed by
`{{namespace, name}, target_relay_id}`, and a re-prepared plugin SHALL reuse its live worker so
queued items survive source-Overwatch restarts.

#### Scenario: Source Overwatch is not blocked by a slow target
- **WHEN** the target relay's Overwatch is unresponsive and the source relay emits output
- **THEN** the source relay's `:on_output` dispatch SHALL return without waiting, and the
  source relay SHALL continue processing input

#### Scenario: Two relays forwarding to each other do not deadlock
- **WHEN** relay A has a relay-forward plugin targeting relay B, relay B has one
  targeting relay A, and both emit output concurrently
- **THEN** both messages SHALL be delivered and neither Overwatch SHALL block on the other

#### Scenario: Worker is reused across plugin re-preparation
- **WHEN** a relay-forward plugin with queued undelivered items is re-prepared on the same
  relay with the same target
- **THEN** the existing worker SHALL be reused and the queued items SHALL still be delivered

### Requirement: Confirming ingest entry point with typed verdicts

The system SHALL provide a synchronous ingest entry point on `Flezha.Relay.Overwatch` that
runs the same single pre-delivery path as the existing `{:io_in, _, _}` cast — the `:on_input`
extension chain followed by `journal_input/3` — and returns a verdict describing the outcome
instead of discarding it. `Flezha.Service.RelayManager` SHALL expose this entry point as a
confirming counterpart to `send_io/4`, resolving the target by `(namespace, relay_id)`.

The verdicts SHALL be:

- `:ok` — the payload was forwarded to the bound runner, or durably journaled
- `{:ok, :absorbed}` — `ingest/4` answered `nil`: filtered by an `:on_input` plugin, absorbed as
  a duplicate, or refused by the durable log as already consumed
- `{:ok, :suspended}` — the target is suspended; the payload is durable in its inbox and SHALL
  be delivered when the instance rehydrates
- `{:error, :not_ready}` — the target is parked with an `init_error`
- `{:error, :not_found}` — no pid is registered for `(namespace, relay_id)`
- `{:error, :refused}` — the durable log refused the append

The existing `{:io_in, _, _}` cast behaviour SHALL be unchanged, including its silent drop for
an `init_error`-parked relay. The drop SHALL be silent to the sender and never unrecorded:
each dropped cast SHALL increment a per-relay counter, summed into the data-plane aggregates
the transport source publishes, so a cast-based source's loss is measurable from outside the
controller for as long as the relay stays parked.

Under the effectively-once tier the verdict SHALL be answered when the durable log answers the
append — the relay's host does not wait for the log — in call order, so a caller parks
exactly while the log parks and for no other reason; the host keeps running its output chains
meanwhile.

For a source holding many inputs at once, `RelayManager` SHALL expose a many-input form
of the confirming entry point that keeps up to a caller-chosen window (default 64) of ordinary
ingest requests in flight against the one target and answers one verdict per input, in input
order. Every input SHALL run the relay's single ingest path once, in input order — only
the caller's round trips overlap; there SHALL be no second, batched server-side ingest. An
unresolvable target SHALL answer `{:error, :not_found}` for every input of the batch.

#### Scenario: Durable delivery is confirmed
- **WHEN** the confirming entry point is invoked against a healthy bound relay
- **THEN** it SHALL return `:ok` and the payload SHALL have passed through the `:on_input` chain
  and `journal_input/3`

#### Scenario: A suspended target reports success
- **WHEN** the confirming entry point is invoked against a suspended durable steps relay
- **THEN** it SHALL return `{:ok, :suspended}`, the payload SHALL be durable in the relay's
  inbox, and the relay SHALL rehydrate and receive the payload in its undelivered tail

#### Scenario: A parked target reports not-ready rather than dropping
- **WHEN** the confirming entry point is invoked against a relay whose `init_error` is set
- **THEN** it SHALL return `{:error, :not_ready}` and the payload SHALL NOT be discarded by the
  Overwatch

#### Scenario: A filtered payload reports absorbed
- **WHEN** an `:on_input` plugin on the target filters the payload so `ingest/4` answers `nil`
- **THEN** the confirming entry point SHALL return `{:ok, :absorbed}`

#### Scenario: An unresolvable target reports not-found
- **WHEN** the confirming entry point is invoked for a relay id with no registered pid in
  the namespace
- **THEN** it SHALL return `{:error, :not_found}`

#### Scenario: The existing cast keeps its behaviour
- **WHEN** `{:io_in, message, metadata}` is cast to a relay whose `init_error` is set
- **THEN** the message SHALL be silently discarded as before and no verdict SHALL be produced

#### Scenario: A parked log parks the caller and only the caller
- **WHEN** the confirming entry point is invoked against an effectively-once relay whose
  source credit is exhausted
- **THEN** the call stays unanswered until credit mints, while the relay's host keeps
  answering control calls and delivering outputs; the verdict then answers `:ok` in call order

#### Scenario: A batch keeps a window in flight and answers in order
- **WHEN** the many-input form is invoked with 200 inputs and a window of 64 against a healthy
  bound relay
- **THEN** at most 64 ingest requests are outstanding at any time, the relay ingests the
  200 inputs in the order given, and the caller receives 200 verdicts in that same order

#### Scenario: A batched input keeps its per-input semantics
- **WHEN** the many-input form delivers inputs to a relay under the effectively-once tier
- **THEN** each input is journaled before it is forwarded and answers the same verdict a single
  confirming ingest of it would have

#### Scenario: An unresolvable target answers not-found for every input
- **WHEN** the many-input form is invoked for a relay id with no registered pid in the
  namespace
- **THEN** it SHALL return `{:error, :not_found}` once per input and send nothing

#### Scenario: The parked cast's drop is counted

- **WHEN** `{:io_in, message, metadata}` is cast twice to a relay whose `init_error` is set
- **THEN** the relay's parked-drop counter SHALL read `2`, its received-input counter SHALL
  stay at `0`, and the aggregate the transport source publishes SHALL include those two drops

### Requirement: Retry until confirmed, with bounded queue and back-pressure

The worker SHALL treat `:ok`, `{:ok, :absorbed}`, and `{:ok, :suspended}` as terminal success and
remove the item from its queue. It SHALL treat `{:error, :not_ready}`, `{:error, :not_found}`,
`{:error, :refused}`, and a call timeout as retryable, retaining the item and retrying with
exponential backoff capped at `:retry_max_backoff_ms`, reusing the platform's existing capped
backoff rather than introducing a second ladder. The worker SHALL NOT drop a retryable item on
a delivery deadline.

The worker SHALL bound its queue at `:queue_max` and SHALL apply back-pressure to the enqueueing
caller when the bound is reached. Items SHALL NOT be routed to the dead-letter queue: the DLQ
carries poison input, not a temporarily unreachable peer.

#### Scenario: A transiently unavailable target is retried
- **WHEN** the target relay is not bound at the time of delivery and becomes bound afterwards
- **THEN** the worker SHALL retry with backoff and the payload SHALL be delivered once the target
  is bound, without user intervention

#### Scenario: An absorbed payload is not retried
- **WHEN** delivery returns `{:ok, :absorbed}`
- **THEN** the worker SHALL remove the item from its queue and SHALL NOT retry it

#### Scenario: A saturated queue applies back-pressure
- **WHEN** the worker's queue has reached `:queue_max` and a further payload is enqueued
- **THEN** the configured overflow behaviour SHALL be applied as back-pressure and the condition
  SHALL be observable through telemetry

### Requirement: Effectively-once delivery across the hop for durable senders

The system SHALL deliver each emitted payload to the target effectively once when the source
relay has a transactional outbox — an effectively-once flow relay or a durable
steps relay — without introducing any new dedup store or wire-format change. The guarantee
SHALL be composed from the output identity the sender's outbox already stamps (`flz.cell`,
`flz.seq`, `flz.epoch`), which the plugin carries as the payload's `flz.msg_id`
(`<plugin id>/<flz.cell>/<flz.seq>/<flz.epoch>`) unless the payload already has one, the
worker's retry-until-confirmed, and the target input log's existing absorption of a repeated
`flz.msg_id`. The plugin id in the message id keeps two plugins that forward one output to
one target two deliveries.

The delivery guarantee SHALL NOT be configurable on the plugin. A source relay without a
transactional outbox SHALL receive retry-until-confirmed delivery without a de-duplication
guarantee, and this asymmetry SHALL be documented.

#### Scenario: A retried delivery is absorbed rather than duplicated
- **WHEN** a durable steps relay source emits a payload, the first delivery attempt is retried after
  the target has already durably journaled it, and the retry carries the same identity stamps
- **THEN** the target SHALL absorb the retry as a duplicate, the worker SHALL observe
  `{:ok, :absorbed}`, and the target SHALL have journaled the payload exactly once
- **AND** `test/flezha/plugin/provider/relay_forward_test.exs` SHALL drive this against an
  effectively-once target and a durable steps target

#### Scenario: Two plugins forwarding one output stay two deliveries
- **WHEN** two relay-forward plugins forward the same effectively-once output to one target
- **THEN** each delivery SHALL carry a message id naming its own plugin and the target SHALL
  journal both

#### Scenario: Identity stamps survive the hop
- **WHEN** an effectively-once source relay emits a payload through the relay-forward
  plugin
- **THEN** the metadata delivered to the target SHALL carry the sender's `flz.seq`, `flz.key`,
  and `flz.epoch` stamps unmodified

#### Scenario: No delivery-mode option is accepted
- **WHEN** a plugin config supplies an option requesting a specific delivery guarantee or
  acknowledgement mode
- **THEN** the enable call SHALL reject the unknown option rather than accepting a guarantee the
  topology may not be able to honour

### Requirement: Enable-time refusal of self-targeting and cross-namespace targets

The system SHALL refuse, at enable time, a relay-forward plugin whose
`:target_relay_id` equals the relay it is being enabled on, and one whose target resolves
to a different namespace than the source relay. Each refusal SHALL return an error naming the
reason and SHALL leave no worker started. Longer cycles between two or more distinct relays
SHALL NOT be refused.

#### Scenario: Self-targeting is refused
- **WHEN** a relay-forward plugin is enabled on relay `X` with `target_relay_id: "X"`
- **THEN** the enable call SHALL return an error identifying the self-target and no worker SHALL
  be started

#### Scenario: Cross-namespace targeting is refused
- **WHEN** a relay-forward plugin is enabled with a target that resolves to a namespace other
  than the source relay's
- **THEN** the enable call SHALL return an error identifying the namespace boundary and no worker
  SHALL be started

#### Scenario: A two-relay cycle is permitted
- **WHEN** relay A is configured to forward to relay B and relay B is configured to
  forward to relay A
- **THEN** both enable calls SHALL succeed

### Requirement: Telemetry for forwarded delivery

The system SHALL emit telemetry for relay-to-relay delivery covering successful delivery,
retry, and queue saturation, with metadata identifying the source relay, the target relay,
and the plugin id, so a user can observe a saturating cycle without parsing log lines.

#### Scenario: Delivery and retry are observable
- **WHEN** a payload is delivered to a target relay, and when a delivery attempt is retried
- **THEN** a telemetry event SHALL be emitted for each, carrying source relay, target
  relay, and plugin id in its metadata

#### Scenario: Queue depth is observable
- **WHEN** a worker's queue depth grows toward `:queue_max`
- **THEN** the depth SHALL be observable through telemetry so a runaway cycle is visible to a
  user

### Requirement: Forwarding to a partitioned target SHALL route each message by key hash
When the configured `target_relay_id` names a partition group (N ≥ 2 members), the provider SHALL resolve the member set and ranges at init, keep one forward worker per target member (the existing `{{namespace, name}, target}` worker keying), and route each message in `forward/2` to the member whose key-group range contains `fnv1a128(flz.key) mod 256`. Per-partition workers preserve per-member FIFO; a slow member SHALL NOT head-of-line-block deliveries to other members. A target with one member behaves byte-identically to today. A not-yet-started member is tolerated exactly as a not-yet-started target is today (the worker retries).

#### Scenario: Keyed outputs land on the owning partition
- **WHEN** a keyed stream output with `flz.key: "BTC-USD"` is forwarded to a 2-partition target
- **THEN** it is enqueued on the worker of exactly the member whose range contains that key's group, and repeated outputs for the same key always take the same member

#### Scenario: Single-member target unchanged
- **WHEN** the target has one member (or is an ordinary relay)
- **THEN** forwarding behavior, worker count, and delivery semantics are identical to before this change

#### Scenario: One slow partition does not block the others
- **WHEN** one target member is unreachable and its worker is retrying
- **THEN** deliveries to the other members continue unaffected

### Requirement: A message without `flz.key` SHALL be parked, never guessed
When the target is partitioned, a message whose metadata carries no `flz.key` (an unkeyed stream emit, or any steps-outbox output — those never carry a key) SHALL be parked via the DLQ store with the error `partitioned target requires keyed emit`, naming the source, target, and contract. The provider SHALL never round-robin or payload-hash an unkeyed message to a partitioned target.

#### Scenario: Unkeyed emit parks with the named error
- **WHEN** a message without `flz.key` reaches a forward whose target has 2+ members
- **THEN** it is parked with the `partitioned target requires keyed emit` error and no partition receives it

#### Scenario: Unkeyed messages to single-member targets still flow
- **WHEN** a message without `flz.key` is forwarded to a single-member target
- **THEN** it is delivered normally, exactly as today

### Requirement: Fan-out worker lifecycle SHALL be complete
With N workers per plugin, disabling or terminating the plugin SHALL stop all N workers (no worker may outlive its plugin), and `health/1` SHALL aggregate over all workers with the worst verdict winning.

#### Scenario: Disable stops every worker
- **WHEN** a forward plugin with 3 target-member workers is disabled
- **THEN** all 3 workers are stopped and none remains registered

#### Scenario: Health reflects the sickest worker
- **WHEN** one of the 3 workers is commit-stalled
- **THEN** `health/1` reports the stalled verdict, not the healthy majority

