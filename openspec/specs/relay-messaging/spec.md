# relay-messaging Specification

## Purpose

How messages reach a running relay from somewhere other than its own inputs. A relay forwards to
another relay through the first-party forward plugin, asynchronously, retried until confirmed,
effectively once between durable relays and routed by key to a partitioned target. Any
cluster-reachable caller can send a typed request to a relay's message handler plugins and wait for
a reply with its own timeout, and SDK factories may answer through an optional `handle_message`.
Application code on a runner sends to a relay through the controller, or locally to a relay bound on
the same runner with weaker guarantees, and can list the relays bound on that runner.

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
instead of discarding it. `Flezha.Service.RelayManager` SHALL expose this entry point by resolving the target by
`(namespace, relay_id)` and calling `Flezha.Plugin.Host.inject/5` in `:verdict` mode, the one
ingress: a feeding plugin and a forward SHALL read the same verdicts below and the same exit
policy (a target that exits or does not answer reads `{:error, {:unreachable, reason}}`).
`RelayManager` SHALL carry no cast-only `send_io/4`.

The verdicts SHALL be:

- `:ok` — the payload was forwarded to the bound runner, or durably journaled
- `{:ok, :absorbed}` — `ingest/4` answered `nil`: filtered by an `:on_input` plugin, absorbed as
  a duplicate, or refused by the durable log as already consumed
- `{:ok, :suspended}` — the target is suspended; the payload is durable in its inbox and SHALL
  be delivered when the instance rehydrates
- `{:error, {:not_accepting_input, relay_id}}` — the target is parked with an `init_error`; the
  Overwatch names its error verdicts itself, so every caller reads the same tuple
- `{:error, :not_found}` — no pid is registered for `(namespace, relay_id)`
- `{:error, {:input_refused, relay_id}}` — the durable log refused the append

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
- **THEN** it SHALL return `{:error, {:not_accepting_input, relay_id}}` and the payload SHALL NOT be discarded by the
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

#### Scenario: A feeding plugin reads the typed verdict

- **WHEN** a feeding plugin calls `Host.inject/5` in `:verdict` mode and an `:on_input` plugin on its relay filters the payload
- **THEN** it SHALL read `{:ok, :absorbed}`, the verdict the confirming entry point answers for the same payload

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

### Requirement: Per-relay message handler plugin configuration

The system SHALL allow configuring message handlers on a running relay via `Flezha.Service.PluginManager.enable_plugin/2` using `%Flezha.Plugin{}` entries whose `hooks` list contains one `{:on_message, hook_opts}` entry. `hook_opts` MUST contain `key: atom` identifying the request key this handler serves. The plugin's `provider` module MUST implement an `on_message/2` callback receiving `(Flezha.Plugin.Extension.Ctx.Msg.t(), opts :: map())`.

#### Scenario: Enable handler with unique key
- **WHEN** a `%Flezha.Plugin{}` has the hook `{:on_message, [key: :fetch_init_state]}` on a relay where no other plugin uses that key on the `:on_message` hook
- **THEN** the enable call MUST succeed and subsequent requests to the relay with that key MUST be routed to this plugin's `on_message/2` callback

#### Scenario: Enable handler with conflicting key
- **WHEN** a `%Flezha.Plugin{}` has the hook `{:on_message, [key: :fetch_init_state]}` on a relay where another plugin already serves that key on `:on_message`
- **THEN** the enable call MUST return `{:error, {:key_conflict, :on_message, :fetch_init_state}}` and the existing handler MUST remain registered unchanged

### Requirement: Typed request envelope routing

The system SHALL route incoming messages of the shape `{:flezha_request, handler_key, payload, reply_to, ref}` sent to a relay's Overwatch pid to the `MessageHandlerPlugin` whose `handler_key` matches exactly. The payload SHALL be an opaque binary. The reply SHALL be sent to `reply_to` as `{:flezha_response, ref, result}` where `result` is `{:ok, reply_payload}` or `{:error, reason}`.

#### Scenario: Request matches registered handler
- **WHEN** an envelope with `handler_key: :fetch_init_state` is sent to a relay that has a plugin registered for that key, and the plugin's `handle/2` returns `{:ok, payload_bytes}`
- **THEN** the caller MUST receive `{:flezha_response, ref, {:ok, payload_bytes}}`

#### Scenario: Request names an unknown handler
- **WHEN** an envelope with `handler_key: :nope` is sent to a relay with no plugin registered for that key
- **THEN** the caller MUST receive `{:flezha_response, ref, {:error, {:unknown_handler_key, :nope}}}`

#### Scenario: Envelope shape matches but handler_key is unknown
- **WHEN** a message matching `{:flezha_request, handler_key, _, reply_to, ref}` is sent and no plugin is registered for `handler_key`
- **THEN** the Overwatch MUST NOT crash and MUST reply `{:flezha_response, ref, {:error, {:unknown_handler_key, handler_key}}}`

#### Scenario: Message does not match the request envelope shape
- **WHEN** an arbitrary message that does not match `{:flezha_request, _, _, _, _}` is sent to the Overwatch
- **THEN** the behaviour MUST match Overwatch's existing out-of-contract message handling (no permissive catch-all is introduced); any caller with a pending `ref` MUST observe its own `after timeout_ms` deadline and receive `{:error, :timeout}`

### Requirement: Handler plugin invocation and context

The system SHALL invoke the matched subscriber's `on_message/2` callback with a `%Flezha.Plugin.Extension.Ctx.Msg{}` struct containing exactly these fields: `key` (atom), `payload` (binary), `reply_to` (pid), `ref` (reference), `relay_id` (string), `namespace` (string), and `runner_id` (string). The second argument MUST be the subscriber's `opts` map.

#### Scenario: Handler receives full context
- **WHEN** a handler is invoked for a request on relay `"op-1"` in namespace `"tenant-a"` from caller pid `P`
- **THEN** the `on_message/2` callback MUST be invoked with `(ctx, opts)` where `ctx.relay_id == "op-1"`, `ctx.namespace == "tenant-a"`, `ctx.reply_to == P`, and `opts` equals the subscriber's configured `opts` map

#### Scenario: Handler returns an error result
- **WHEN** the handler's `on_message/2` returns `{:error, reason}`
- **THEN** the caller MUST receive `{:flezha_response, ref, {:error, reason}}`

#### Scenario: Handler raises an exception
- **WHEN** the handler's `on_message/2` raises an exception
- **THEN** the Overwatch MUST NOT crash and the caller MUST receive `{:flezha_response, ref, {:error, {:exception, kind, reason}}}`

### Requirement: SDK round-trip via first-party MessageForward provider

The system SHALL ship a first-party provider `Flezha.Plugin.Provider.MessageForward` implementing `on_message/2`. The callback MUST forward the payload to the SDK-side relay by sending a `RunnerCommands.RelayMessage { relay_id, payload }` on the control channel and waiting for a `RunnerCommands.RelayMessageResponse { payload, error }`. When `error` is empty, `on_message/2` MUST return `{:ok, response.payload}`; otherwise it MUST return `{:error, response.error}`.

#### Scenario: Factory returns success payload
- **WHEN** the SDK factory's `handle_message(payload)` returns success bytes `B`
- **THEN** the `MessageForward` provider MUST return `{:ok, B}`

#### Scenario: Factory signals unsupported
- **WHEN** the SDK factory does not implement `handle_message` and returns the default "unsupported" error
- **THEN** the `MessageForward` provider MUST return `{:error, "relay does not support handle_message"}` (or an equivalent SDK-provided error string) without crashing

### Requirement: Sync request/response with caller-side timeout

The system SHALL provide a synchronous request path for callers to send an envelope and wait for a reply bounded by a caller-supplied `timeout_ms`. When the timeout elapses before a reply is received, the caller MUST observe `{:error, :timeout}`. Late replies arriving after the timeout MUST NOT be observed by the caller and MUST NOT crash either party.

#### Scenario: Reply arrives within timeout
- **WHEN** the caller sets `timeout_ms: 1000` and the handler replies in 100ms
- **THEN** the caller MUST observe the handler's reply

#### Scenario: Reply does not arrive within timeout
- **WHEN** the caller sets `timeout_ms: 200` and the handler takes 2s to reply
- **THEN** the caller MUST observe `{:error, :timeout}` at 200ms, and the delayed reply at 2s MUST NOT crash the caller

### Requirement: New control-channel command `RelayMessage`

The system SHALL extend `crates/protobufs/runner_commands.proto` with two messages: `RelayMessage { string relay_id = 1; bytes payload = 2; }` and `RelayMessageResponse { bytes payload = 1; string error = 2; }`. `RelayMessage` MUST be dispatched on the existing control stream via the same send/reply primitives used by `CellState`.

The message is named for the Relay because it addresses one: it carries `relay_id` and no tasklet or cell identifier. The former name asserted a level it never targeted.

#### Scenario: Command travels on control channel
- **WHEN** the Elixir agent sends a `RelayMessage` for relay `"op-1"` with payload `P`
- **THEN** the SDK MUST receive it on the control channel with `relay_id == "op-1"` and `payload == P`

#### Scenario: Response carries empty error on success
- **WHEN** the SDK factory successfully handles the message and returns bytes `B`
- **THEN** the SDK MUST reply with `RelayMessageResponse { payload: B, error: "" }`

#### Scenario: Response carries error string on failure
- **WHEN** the SDK factory returns an error `E` or the factory does not implement `handle_message`
- **THEN** the SDK MUST reply with `RelayMessageResponse { payload: <any>, error: E }` and the agent MUST treat the response as an error regardless of `payload` contents

#### Scenario: No retired spelling is accepted
- **WHEN** either SDK or the Controller references the retired message name
- **THEN** the build MUST fail, since no alias for the former name is retained

### Requirement: SDK factory `handle_message` is optional with default-error behaviour

The system SHALL expose an optional `handle_message` callback on the Rust and Java SDK relay factory interfaces. Factories that do not override it MUST behave as if the default returned an explicit "unsupported" error so the agent observes a well-formed `RelayMessageResponse` with a non-empty `error`. Factories that override it MAY produce arbitrary reply bytes.

#### Scenario: Pre-existing Rust factory continues to work
- **WHEN** an existing Rust relay factory (no `handle_message` implementation) is loaded
- **THEN** receiving a `RelayMessage` MUST result in a `RelayMessageResponse` with a non-empty `error` and the relay MUST remain running and healthy

#### Scenario: Pre-existing Java factory continues to work
- **WHEN** an existing Java relay factory (no `handleMessage` implementation) is loaded
- **THEN** receiving a `RelayMessage` MUST result in a `RelayMessageResponse` with a non-empty `error` and the relay MUST remain running and healthy

#### Scenario: Overriding factory returns custom bytes
- **WHEN** a factory overrides `handle_message` to return bytes `B` for input payload `P`
- **THEN** receiving a `RelayMessage` with payload `P` MUST result in a `RelayMessageResponse { payload: B, error: "" }`

### Requirement: Handler plugins survive relay migration

The system SHALL migrate `:on_message` hooks along with the relay via the existing
ProcessHub handover path. The destination Overwatch SHALL bring up the relay's declared
bindings, so that, after migration, previously registered handlers remain reachable under the
same key and continue to route requests identically.

#### Scenario: Handler remains registered after migration
- **WHEN** a relay with an `:on_message` hook serving `key: :k` is migrated to a new node
- **THEN** sending `{:flezha_request, :k, payload, self(), ref}` to the new Overwatch pid MUST produce a reply from the same plugin code as before migration

### Requirement: Overwatch lifetime isolation from handler failures

The system SHALL isolate the Overwatch GenServer from handler plugin failures. A handler plugin that crashes, raises, or exits during `handle/2` MUST NOT bring down the Overwatch process. The caller MUST observe an error reply; the Overwatch MUST remain available for subsequent requests and other lifecycle commands.

#### Scenario: Handler process crash during dispatch
- **WHEN** a handler plugin's `handle/2` raises an exception
- **THEN** the Overwatch MUST continue running, MUST continue serving other requests on other `handler_key`s, and the caller MUST receive `{:error, {:exception, kind, reason}}`
</content>
</invoke>

### Requirement: The runner sends to a relay through the controller

The runner object of every SDK SHALL send a message to a relay in its namespace, naming
the relay, the message and the entry channel, through the controller, under the same
role check the control surface's calls take. It SHALL answer the controller's verdict:
`delivered`, `absorbed` or `suspended`, with the relay that took it. It SHALL take an
optional message id, stamped on the input as `flz.msg_id`, so a relay with an input log
that still holds an input of that id absorbs the repeat. It SHALL take an optional
partition key that selects the member of a partition group whose range owns it.
`flz relay send` SHALL take `--msg-id <id>` with the same meaning.

#### Scenario: A message reaches a relay

- **WHEN** application code on a runner bound to role `app` sends `hello` to `my_echo` on
  channel `in`
- **THEN** the call SHALL answer `delivered` naming `my_echo`, and the relay SHALL
  handle the message

#### Scenario: A repeated message id is absorbed

- **WHEN** application code sends a message with id `orders/0/41` to a relay with an
  input log, and sends it again with the same id while the log still holds the first
- **THEN** the first call SHALL answer `delivered`, the second SHALL answer `absorbed`,
  and the relay SHALL handle the message once

#### Scenario: A key reaches the owning member

- **WHEN** application code sends a message with key `EURUSD` to a partition group
- **THEN** the member whose range owns `EURUSD` SHALL take it, and the answer SHALL name
  that member

#### Scenario: A group without a key is refused

- **WHEN** application code sends to a partition group without a key
- **THEN** the call SHALL be refused naming the group's members, as the CLI's is

#### Scenario: The CLI passes a message id

- **WHEN** `flz relay send orders --text x --msg-id a1` runs twice against a relay
  with an input log
- **THEN** the second run SHALL report the input as absorbed

### Requirement: A runner lists the relays bound on it

The runner object of every SDK SHALL answer the ids of the relay instances bound on that
runner at the moment of the call, a partition member by its member id, without contacting
the controller. It SHALL answer an empty list when none is bound.

#### Scenario: Two bound relays are listed

- **WHEN** a runner has `orders` and `prices-1` bound and application code asks for its
  relays
- **THEN** the answer SHALL be exactly `orders` and `prices-1`

#### Scenario: A relay that moved away is no longer listed

- **WHEN** a relay is unbound from the runner and application code asks again
- **THEN** that relay SHALL not be in the answer

### Requirement: A local send skips the controller

The send SHALL take a `local` option. A local send SHALL deliver the message to a relay
bound on that runner in-process, without a frame to the controller, entering where a frame
off the wire enters, so the channel's decoder, transforms and keying run as they do for any
input. It SHALL apply the backpressure that runner's own entry path applies: where a frame
off the wire waits while the consumer's mailbox is full, a local send SHALL wait too. It
SHALL return once the message is routed. State changes and outputs it causes SHALL travel to the controller as
any others do.

#### Scenario: A local message is handled

- **WHEN** application code sends `hello` locally to the entry channel `in` of a bound echo
  relay
- **THEN** the relay SHALL handle it and its output SHALL reach its output channel as an output
  from controller-delivered input does

#### Scenario: State changes are kept as usual

- **WHEN** a local message changes a cell's state on a durable relay and the runner
  then stops gracefully
- **THEN** the change SHALL be in the controller's cell log, as any state change is

### Requirement: A local send is refused only for what this runner cannot take

A local send SHALL be refused with `runtime.relay_not_local`, naming the relay, when
the relay is not bound on this runner or names a partition group. A channel the relay
does not declare SHALL be refused with `runtime.channel_undeclared`, and a channel that is
not an entry channel with `runtime.channel_wrong_kind`. A local send given a message id or
a key SHALL be refused before anything is sent. A refused local send SHALL not be forwarded
to the controller.

#### Scenario: A relay on another runner

- **WHEN** application code sends locally to a relay bound on a different runner
- **THEN** the call SHALL be refused with `runtime.relay_not_local` and nothing SHALL be
  sent

#### Scenario: An undeclared channel

- **WHEN** application code sends locally to a channel the relay does not declare
- **THEN** the call SHALL be refused with `runtime.channel_undeclared` listing the declared
  entry channels

#### Scenario: Local with an id

- **WHEN** application code sends locally with a message id
- **THEN** the call SHALL be refused and nothing SHALL be sent

### Requirement: A local input is not durable and is handled at most once

A local input SHALL not be written to the controller's disk and SHALL take no input-log
offset. It SHALL be allowed to every relay, including one with an input log
(`exactly: effectively_once`, a durable steps relay). A crash before it is handled SHALL
lose it, and a replay SHALL never repeat it. On a relay with an input log, the
relay's guarantee SHALL hold for input that came through the controller and SHALL hold
for no local input. Every SDK's reference for the `local` option SHALL say this in one
line.

#### Scenario: A crash loses an unhandled local input

- **WHEN** application code sends locally to an at-least-once flow relay and the runner
  is killed before the input is handled
- **THEN** the relay SHALL rebind and carry on, and the input SHALL not be handled

#### Scenario: An effectively-once relay takes a local input

- **WHEN** application code sends locally to an effectively-once relay, the input is
  handled, and the runner is then killed and the relay replays its input log
- **THEN** the relay SHALL rebind and carry on, the replay SHALL not handle the local
  input again, and controller-delivered input SHALL keep its effectively-once guarantee

#### Scenario: A durable steps relay takes a local input

- **WHEN** application code sends locally to a durable steps relay waiting for input,
  and the runner is killed after the step it started commits
- **THEN** the relay SHALL resume after that committed step

### Requirement: The docs show the new names and warn about the local send

A docs page SHALL show runner code sending to a relay: through the controller with a
message id and a key first, the local send second, preceded by a warning box that says its
input is not on the controller's disk, is lost on a crash, and is not covered by the
guarantee of a relay with an input log. No docs page, SDK example or SDK reference
comment SHALL show `admin()` or a send on the control surface.

#### Scenario: The warning is on the page

- **WHEN** the docs are built
- **THEN** the page SHALL carry a `{: .warning}` box above the local send's first example

#### Scenario: No old name is left

- **WHEN** `docs/`, `README.md`, `CHANGELOG.md`'s new entry and `sdk/` are
  searched for the SDK accessor `admin`
- **THEN** no hit SHALL remain outside the authorization roles and earlier changelog entries

### Requirement: Every SDK SHALL call a relay's channel by value

Every SDK's runner object SHALL offer, beside its send, a call of a relay's channel with a
value (`runner.call(id, channel, value, answerType)` in Java, the language's idiom elsewhere),
carried over the SDK's shared entry as `Relay.call_channel` under the role check its send
takes. The value SHALL be encoded as an output with no codec encodes it, and the answer decoded by
its format: JSON into the requested type or the language's JSON value, text as a string, bytes
as bytes, and `{"accepted": true}` for a send. A refusal SHALL raise the SDK's error type with
the controller's code. The control surface SHALL not offer this call.

#### Scenario: Java calls with a record

- **WHEN** a Java runner calls `runner.call("orders-eu", "place", new PlaceOrder("EUR/USD", 1000), Answer.class)`
- **THEN** it receives the answer as an `Answer` record

#### Scenario: Same in every SDK

- **WHEN** a test in each SDK calls a JSON channel of a test relay through its
  runner object
- **THEN** each receives the same decoded answer

### Requirement: A runner's send SHALL reach the relay byte for byte

The core SHALL carry the payload of every SDK's runner `send` to `Relay.send` as base64 with the
`payload: "base64"` option `Relay.call_channel` already takes, so a send and a call encode a payload
one way and whatever an SDK's `send` accepts reaches the relay byte for byte: a text message as its
UTF-8 bytes, and a byte string from an SDK whose `send` takes one (Ruby, PHP, Elixir, Go) unchanged even
when it is not UTF-8. No SDK's `send` signature changes. `Relay.send`
SHALL read `payload: "text"` (the default, as `flz relay send --text` sends) and `"base64"`, and
refuse any other value with `usage.unknown_word`.

#### Scenario: A binary message reaches the relay

- **WHEN** Elixir application code sends the binary `<<0xFF, 0x00, 0x7F>>` to `my_echo` on channel `in`
- **THEN** the call SHALL answer `delivered` and the relay SHALL receive exactly those three bytes
