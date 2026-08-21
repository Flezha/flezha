# operative-to-operative-messaging Specification

## Purpose
TBD - created by archiving change operative-to-operative-messaging. Update Purpose after archive.
## Requirements
### Requirement: First-party operative-forward plugin provider

The system SHALL provide a first-party `:io_binding` plugin provider,
`Flezha.Plugin.Provider.OperativeForward`, whose delivery destination is another operative in
the same namespace rather than an external endpoint. The provider SHALL be enabled and disabled
per operative through `Flezha.Service.PluginManager` using the same `%Flezha.Plugin{}`
subscription mechanism as every other `:io_binding` provider, and SHALL support subscribing to
`:on_output` and to `:on_input`. The provider SHALL declare no new extension points.

Required opts: `:target_operative_id`. Optional opts: `:queue_max`, `:retry_backoff_ms`,
`:retry_max_backoff_ms`, `:plugin_id`.

#### Scenario: Enabling the provider on an operative
- **WHEN** a `%Flezha.Plugin{}` whose provider is `Flezha.Plugin.Provider.OperativeForward` and
  whose opts contain a `:target_operative_id` naming a different operative subscribes to
  `{:on_output, _}` on an operative
- **THEN** the enable call SHALL succeed and subsequent output from that operative SHALL be
  delivered to the target operative's input stream

#### Scenario: Disabling the provider stops delivery
- **WHEN** an enabled operative-forward plugin is disabled via `PluginManager.disable_plugin/2`
- **THEN** no further output from the source operative SHALL be delivered to the target, and the
  provider's worker SHALL be stopped

#### Scenario: Missing target is refused at enable time
- **WHEN** a `%Flezha.Plugin{}` for this provider is enabled without a `:target_operative_id` opt
- **THEN** the enable call SHALL return an error naming the missing option and no worker SHALL
  be started

### Requirement: Delivery is asynchronous and never blocks the source operative

The provider's `:on_output` and `:on_input` callbacks SHALL enqueue the payload to a long-lived
worker process and return without waiting for delivery. No extension callback SHALL perform a
synchronous call into another operative's Overwatch. The worker SHALL be supervised through the
shared `Flezha.Plugin.Provider.WorkerSup` core as its own provider family, keyed by
`{plugin_id, target_operative_id}`, and a re-prepared plugin SHALL reuse its live worker so
queued items survive source-Overwatch restarts.

#### Scenario: Source Overwatch is not blocked by a slow target
- **WHEN** the target operative's Overwatch is unresponsive and the source operative emits output
- **THEN** the source operative's `:on_output` dispatch SHALL return without waiting, and the
  source operative SHALL continue processing input

#### Scenario: Two operatives forwarding to each other do not deadlock
- **WHEN** operative A has an operative-forward plugin targeting operative B, operative B has one
  targeting operative A, and both emit output concurrently
- **THEN** both messages SHALL be delivered and neither Overwatch SHALL block on the other

#### Scenario: Worker is reused across plugin re-preparation
- **WHEN** an operative-forward plugin with queued undelivered items is re-prepared on the same
  operative with the same target
- **THEN** the existing worker SHALL be reused and the queued items SHALL still be delivered

### Requirement: Confirming ingest entry point with typed verdicts

The system SHALL provide a synchronous ingest entry point on `Flezha.Operative.Overwatch` that
runs the same single pre-delivery path as the existing `{:io_in, _, _}` cast — the `:on_input`
extension chain followed by `journal_input/3` — and returns a verdict describing the outcome
instead of discarding it. `Flezha.Service.OperativeManager` SHALL expose this entry point as a
confirming counterpart to `send_io/4`, resolving the target by `(namespace, operative_id)`.

The verdicts SHALL be:

- `:ok` — the payload was forwarded to the bound runner, or durably journaled
- `{:ok, :absorbed}` — `ingest/4` answered `nil`: filtered by an `:on_input` plugin, absorbed as
  a duplicate, or refused by the durable log as already consumed
- `{:ok, :suspended}` — the target is suspended; the payload is durable in its inbox and SHALL
  be delivered when the instance rehydrates
- `{:error, :not_ready}` — the target is parked with an `init_error`
- `{:error, :not_found}` — no pid is registered for `(namespace, operative_id)`
- `{:error, :refused}` — the durable log refused the append

The existing `{:io_in, _, _}` cast behaviour SHALL be unchanged, including its silent drop for
an `init_error`-parked operative.

Under the effectively-once tier the verdict SHALL be answered when the durable log answers the
append — the operative's host does not wait for the log — in call order, so a caller parks
exactly while the log parks and for no other reason; the host keeps running its output chains
meanwhile.

For a source holding many inputs at once, `OperativeManager` SHALL expose a many-input form
of the confirming entry point that keeps up to a caller-chosen window (default 64) of ordinary
ingest requests in flight against the one target and answers one verdict per input, in input
order. Every input SHALL run the operative's single ingest path once, in input order — only
the caller's round trips overlap; there SHALL be no second, batched server-side ingest. An
unresolvable target SHALL answer `{:error, :not_found}` for every input of the batch.

#### Scenario: Durable delivery is confirmed
- **WHEN** the confirming entry point is invoked against a healthy bound operative
- **THEN** it SHALL return `:ok` and the payload SHALL have passed through the `:on_input` chain
  and `journal_input/3`

#### Scenario: A suspended target reports success
- **WHEN** the confirming entry point is invoked against a suspended durable workflow operative
- **THEN** it SHALL return `{:ok, :suspended}`, the payload SHALL be durable in the operative's
  inbox, and the operative SHALL rehydrate and receive the payload in its undelivered tail

#### Scenario: A parked target reports not-ready rather than dropping
- **WHEN** the confirming entry point is invoked against an operative whose `init_error` is set
- **THEN** it SHALL return `{:error, :not_ready}` and the payload SHALL NOT be discarded by the
  Overwatch

#### Scenario: A filtered payload reports absorbed
- **WHEN** an `:on_input` plugin on the target filters the payload so `ingest/4` answers `nil`
- **THEN** the confirming entry point SHALL return `{:ok, :absorbed}`

#### Scenario: An unresolvable target reports not-found
- **WHEN** the confirming entry point is invoked for an operative id with no registered pid in
  the namespace
- **THEN** it SHALL return `{:error, :not_found}`

#### Scenario: The existing cast keeps its behaviour
- **WHEN** `{:io_in, message, metadata}` is cast to an operative whose `init_error` is set
- **THEN** the message SHALL be silently discarded as before and no verdict SHALL be produced

#### Scenario: A parked log parks the caller and only the caller
- **WHEN** the confirming entry point is invoked against an effectively-once operative whose
  source credit is exhausted
- **THEN** the call stays unanswered until credit mints, while the operative's host keeps
  answering control calls and delivering outputs; the verdict then answers `:ok` in call order

#### Scenario: A batch keeps a window in flight and answers in order
- **WHEN** the many-input form is invoked with 200 inputs and a window of 64 against a healthy
  bound operative
- **THEN** at most 64 ingest requests are outstanding at any time, the operative ingests the
  200 inputs in the order given, and the caller receives 200 verdicts in that same order

#### Scenario: A batched input keeps its per-input semantics
- **WHEN** the many-input form delivers inputs to an operative under the effectively-once tier
- **THEN** each input is journaled before it is forwarded and answers the same verdict a single
  confirming ingest of it would have

#### Scenario: An unresolvable target answers not-found for every input
- **WHEN** the many-input form is invoked for an operative id with no registered pid in the
  namespace
- **THEN** it SHALL return `{:error, :not_found}` once per input and send nothing

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
- **WHEN** the target operative is not bound at the time of delivery and becomes bound afterwards
- **THEN** the worker SHALL retry with backoff and the payload SHALL be delivered once the target
  is bound, without operator intervention

#### Scenario: An absorbed payload is not retried
- **WHEN** delivery returns `{:ok, :absorbed}`
- **THEN** the worker SHALL remove the item from its queue and SHALL NOT retry it

#### Scenario: A saturated queue applies back-pressure
- **WHEN** the worker's queue has reached `:queue_max` and a further payload is enqueued
- **THEN** the configured overflow behaviour SHALL be applied as back-pressure and the condition
  SHALL be observable through telemetry

### Requirement: Effectively-once delivery across the hop for durable senders

The system SHALL deliver each emitted payload to the target effectively once when the source
operative has a transactional outbox — an effectively-once stream operative or a durable
workflow operative — without introducing any new identity scheme, dedup store, or wire-format
change. The guarantee SHALL be composed from the existing identity stamps the sender's outbox
already applies (`flz.seq`, `flz.key`, `flz.epoch`), the worker's retry-until-confirmed, and the
target's existing duplicate absorption in `journal_input/2`.

The delivery guarantee SHALL NOT be configurable on the plugin. A source operative without a
transactional outbox SHALL receive retry-until-confirmed delivery without a de-duplication
guarantee, and this asymmetry SHALL be documented.

#### Scenario: A retried delivery is absorbed rather than duplicated
- **WHEN** a durable workflow source emits a payload, the first delivery attempt is retried after
  the target has already durably journaled it, and the retry carries the same identity stamps
- **THEN** the target SHALL absorb the retry as a duplicate, the worker SHALL observe
  `{:ok, :absorbed}`, and the target SHALL have journaled the payload exactly once

#### Scenario: Identity stamps survive the hop
- **WHEN** an effectively-once source operative emits a payload through the operative-forward
  plugin
- **THEN** the metadata delivered to the target SHALL carry the sender's `flz.seq`, `flz.key`,
  and `flz.epoch` stamps unmodified

#### Scenario: No delivery-mode option is accepted
- **WHEN** a plugin config supplies an option requesting a specific delivery guarantee or
  acknowledgement mode
- **THEN** the enable call SHALL reject the unknown option rather than accepting a guarantee the
  topology may not be able to honour

### Requirement: Enable-time refusal of self-targeting and cross-namespace targets

The system SHALL refuse, at enable time, an operative-forward plugin whose
`:target_operative_id` equals the operative it is being enabled on, and one whose target resolves
to a different namespace than the source operative. Each refusal SHALL return an error naming the
reason and SHALL leave no worker started. Longer cycles between two or more distinct operatives
SHALL NOT be refused.

#### Scenario: Self-targeting is refused
- **WHEN** an operative-forward plugin is enabled on operative `X` with `target_operative_id: "X"`
- **THEN** the enable call SHALL return an error identifying the self-target and no worker SHALL
  be started

#### Scenario: Cross-namespace targeting is refused
- **WHEN** an operative-forward plugin is enabled with a target that resolves to a namespace other
  than the source operative's
- **THEN** the enable call SHALL return an error identifying the namespace boundary and no worker
  SHALL be started

#### Scenario: A two-operative cycle is permitted
- **WHEN** operative A is configured to forward to operative B and operative B is configured to
  forward to operative A
- **THEN** both enable calls SHALL succeed

### Requirement: Telemetry for forwarded delivery

The system SHALL emit telemetry for operative-to-operative delivery covering successful delivery,
retry, and queue saturation, with metadata identifying the source operative, the target operative,
and the plugin id, so an operator can observe a saturating cycle without parsing log lines.

#### Scenario: Delivery and retry are observable
- **WHEN** a payload is delivered to a target operative, and when a delivery attempt is retried
- **THEN** a telemetry event SHALL be emitted for each, carrying source operative, target
  operative, and plugin id in its metadata

#### Scenario: Queue depth is observable
- **WHEN** a worker's queue depth grows toward `:queue_max`
- **THEN** the depth SHALL be observable through telemetry so a runaway cycle is visible to an
  operator

### Requirement: Forwarding to a partitioned target SHALL route each message by key hash
When the configured `target_operative_id` names a partition group (N ≥ 2 members), the provider SHALL resolve the member set and ranges at init, keep one forward worker per target member (the existing `{plugin_id, target}` worker keying), and route each message in `forward/2` to the member whose key-group range contains `fnv1a128(flz.key) mod 256`. Per-partition workers preserve per-member FIFO; a slow member SHALL NOT head-of-line-block deliveries to other members. A target with one member behaves byte-identically to today. A not-yet-started member is tolerated exactly as a not-yet-started target is today (the worker retries).

#### Scenario: Keyed outputs land on the owning partition
- **WHEN** a keyed stream output with `flz.key: "BTC-USD"` is forwarded to a 2-partition target
- **THEN** it is enqueued on the worker of exactly the member whose range contains that key's group, and repeated outputs for the same key always take the same member

#### Scenario: Single-member target unchanged
- **WHEN** the target has one member (or is an ordinary operative)
- **THEN** forwarding behavior, worker count, and delivery semantics are identical to before this change

#### Scenario: One slow partition does not block the others
- **WHEN** one target member is unreachable and its worker is retrying
- **THEN** deliveries to the other members continue unaffected

### Requirement: A message without `flz.key` SHALL be parked, never guessed
When the target is partitioned, a message whose metadata carries no `flz.key` (an unkeyed stream emit, or any workflow-outbox output — those never carry a key) SHALL be parked via the DLQ store with the error `partitioned target requires keyed emit`, naming the source, target, and contract. The provider SHALL never round-robin or payload-hash an unkeyed message to a partitioned target.

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
