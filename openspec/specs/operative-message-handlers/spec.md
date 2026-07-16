# operative-message-handlers Specification

## Purpose

The operative-message-handlers capability enables typed request/reply messaging to a running operative from any cluster-reachable caller — including other operatives — routed by the operative's Overwatch to a per-operative handler plugin selected by an atom `handler_key`. Plugins are represented as `%Flezha.Plugin{}` entries subscribing to the `:on_message` extension point and are enabled per operative via `Flezha.Service.PluginManager.enable_plugin/2`; the first-party `Flezha.Plugin.Provider.MessageForward` provider round-trips the payload to the SDK-side operative factory via a new `OperationMessage` / `OperationMessageResponse` control-channel command pair, mirroring the existing `SegmentState` request/reply path. SDK factories expose an optional `handle_message` callback (Rust and Java) that defaults to an explicit "unsupported" error so existing factories keep working. Requests are synchronous with a caller-side timeout (caller's deadline wins; late replies are discarded), the Overwatch is isolated from handler failures, and handler registrations survive operative migration via the existing ProcessHub handover path. Authorization is open in v1; scoping (namespace/node ACLs), fire-and-forget/streaming replies, fan-out per key, and SDK-side deadline propagation are explicitly deferred.

## Requirements

### Requirement: Per-operative message handler plugin configuration

The system SHALL allow configuring message handlers on a running operative via `Flezha.Service.PluginManager.enable_plugin/2` using `%Flezha.Plugin{}` entries whose `subscriptions` list contains one `{:on_message, sub_opts}` entry. `sub_opts` MUST contain `key: atom` identifying the request key this handler serves. The plugin's `provider` module MUST implement an `on_message/2` callback receiving `(Flezha.Plugin.Extension.Ctx.Msg.t(), opts :: map())`.

#### Scenario: Enable handler with unique key
- **WHEN** a `%Flezha.Plugin{}` subscribes to `{:on_message, [key: :fetch_init_state]}` on an operative where no other plugin uses that key on the `:on_message` extension
- **THEN** the enable call MUST succeed and subsequent requests to the operative with that key MUST be routed to this plugin's `on_message/2` callback

#### Scenario: Enable handler with conflicting key
- **WHEN** a `%Flezha.Plugin{}` subscribes to `{:on_message, [key: :fetch_init_state]}` on an operative where another plugin already serves that key on `:on_message`
- **THEN** the enable call MUST return `{:error, {:key_conflict, :on_message, :fetch_init_state}}` and the existing handler MUST remain registered unchanged

### Requirement: Typed request envelope routing

The system SHALL route incoming messages of the shape `{:flezha_request, handler_key, payload, reply_to, ref}` sent to an operative's Overwatch pid to the `MessageHandlerPlugin` whose `handler_key` matches exactly. The payload SHALL be an opaque binary. The reply SHALL be sent to `reply_to` as `{:flezha_response, ref, result}` where `result` is `{:ok, reply_payload}` or `{:error, reason}`.

#### Scenario: Request matches registered handler
- **WHEN** an envelope with `handler_key: :fetch_init_state` is sent to an operative that has a plugin registered for that key, and the plugin's `handle/2` returns `{:ok, payload_bytes}`
- **THEN** the caller MUST receive `{:flezha_response, ref, {:ok, payload_bytes}}`

#### Scenario: Request names an unknown handler
- **WHEN** an envelope with `handler_key: :nope` is sent to an operative with no plugin registered for that key
- **THEN** the caller MUST receive `{:flezha_response, ref, {:error, {:unknown_handler_key, :nope}}}`

#### Scenario: Envelope shape matches but handler_key is unknown
- **WHEN** a message matching `{:flezha_request, handler_key, _, reply_to, ref}` is sent and no plugin is registered for `handler_key`
- **THEN** the Overwatch MUST NOT crash and MUST reply `{:flezha_response, ref, {:error, {:unknown_handler_key, handler_key}}}`

#### Scenario: Message does not match the request envelope shape
- **WHEN** an arbitrary message that does not match `{:flezha_request, _, _, _, _}` is sent to the Overwatch
- **THEN** the behaviour MUST match Overwatch's existing out-of-contract message handling (no permissive catch-all is introduced); any caller with a pending `ref` MUST observe its own `after timeout_ms` deadline and receive `{:error, :timeout}`

### Requirement: Handler plugin invocation and context

The system SHALL invoke the matched subscriber's `on_message/2` callback with a `%Flezha.Plugin.Extension.Ctx.Msg{}` struct containing exactly these fields: `key` (atom), `payload` (binary), `reply_to` (pid), `ref` (reference), `operative_id` (string), `namespace` (string), and `runner_id` (string). The second argument MUST be the subscriber's `opts` map.

#### Scenario: Handler receives full context
- **WHEN** a handler is invoked for a request on operative `"proc-1"` in namespace `"tenant-a"` from caller pid `P`
- **THEN** the `on_message/2` callback MUST be invoked with `(ctx, opts)` where `ctx.operative_id == "proc-1"`, `ctx.namespace == "tenant-a"`, `ctx.reply_to == P`, and `opts` equals the subscriber's configured `opts` map

#### Scenario: Handler returns an error result
- **WHEN** the handler's `on_message/2` returns `{:error, reason}`
- **THEN** the caller MUST receive `{:flezha_response, ref, {:error, reason}}`

#### Scenario: Handler raises an exception
- **WHEN** the handler's `on_message/2` raises an exception
- **THEN** the Overwatch MUST NOT crash and the caller MUST receive `{:flezha_response, ref, {:error, {:exception, kind, reason}}}`

### Requirement: SDK round-trip via first-party MessageForward provider

The system SHALL ship a first-party provider `Flezha.Plugin.Provider.MessageForward` implementing `on_message/2`. The callback MUST forward the payload to the SDK-side operative by sending a `RunnerCommands.OperationMessage { operative_id, payload }` on the control channel and waiting for a `RunnerCommands.OperationMessageResponse { payload, error }`. When `error` is empty, `on_message/2` MUST return `{:ok, response.payload}`; otherwise it MUST return `{:error, response.error}`.

#### Scenario: Factory returns success payload
- **WHEN** the SDK factory's `handle_message(payload)` returns success bytes `B`
- **THEN** the `MessageForward` provider MUST return `{:ok, B}`

#### Scenario: Factory signals unsupported
- **WHEN** the SDK factory does not implement `handle_message` and returns the default "unsupported" error
- **THEN** the `MessageForward` provider MUST return `{:error, "operative does not support handle_message"}` (or an equivalent SDK-provided error string) without crashing

### Requirement: Sync request/response with caller-side timeout

The system SHALL provide a synchronous request path for callers to send an envelope and wait for a reply bounded by a caller-supplied `timeout_ms`. When the timeout elapses before a reply is received, the caller MUST observe `{:error, :timeout}`. Late replies arriving after the timeout MUST NOT be observed by the caller and MUST NOT crash either party.

#### Scenario: Reply arrives within timeout
- **WHEN** the caller sets `timeout_ms: 1000` and the handler replies in 100ms
- **THEN** the caller MUST observe the handler's reply

#### Scenario: Reply does not arrive within timeout
- **WHEN** the caller sets `timeout_ms: 200` and the handler takes 2s to reply
- **THEN** the caller MUST observe `{:error, :timeout}` at 200ms, and the delayed reply at 2s MUST NOT crash the caller

### Requirement: New control-channel command `OperationMessage`

The system SHALL extend `protobufs/runner_commands.proto` with two messages: `OperationMessage { string operative_id = 1; bytes payload = 2; }` and `OperationMessageResponse { bytes payload = 1; string error = 2; }`. `OperationMessage` MUST be dispatched on the existing control stream via the same send/reply primitives used by `SegmentState`.

#### Scenario: Command travels on control channel
- **WHEN** the Elixir agent sends an `OperationMessage` for operative `"proc-1"` with payload `P`
- **THEN** the SDK MUST receive it on the control channel with `operative_id == "proc-1"` and `payload == P`

#### Scenario: Response carries empty error on success
- **WHEN** the SDK factory successfully handles the message and returns bytes `B`
- **THEN** the SDK MUST reply with `OperationMessageResponse { payload: B, error: "" }`

#### Scenario: Response carries error string on failure
- **WHEN** the SDK factory returns an error `E` or the factory does not implement `handle_message`
- **THEN** the SDK MUST reply with `OperationMessageResponse { payload: <any>, error: E }` and the agent MUST treat the response as an error regardless of `payload` contents

### Requirement: SDK factory `handle_message` is optional with default-error behaviour

The system SHALL expose an optional `handle_message` callback on the Rust and Java SDK operative factory interfaces. Factories that do not override it MUST behave as if the default returned an explicit "unsupported" error so the agent observes a well-formed `OperationMessageResponse` with a non-empty `error`. Factories that override it MAY produce arbitrary reply bytes.

#### Scenario: Pre-existing Rust factory continues to work
- **WHEN** an existing Rust operative factory (no `handle_message` implementation) is loaded
- **THEN** receiving an `OperationMessage` MUST result in an `OperationMessageResponse` with a non-empty `error` and the operative MUST remain running and healthy

#### Scenario: Pre-existing Java factory continues to work
- **WHEN** an existing Java operative factory (no `handleMessage` implementation) is loaded
- **THEN** receiving an `OperationMessage` MUST result in an `OperationMessageResponse` with a non-empty `error` and the operative MUST remain running and healthy

#### Scenario: Overriding factory returns custom bytes
- **WHEN** a factory overrides `handle_message` to return bytes `B` for input payload `P`
- **THEN** receiving an `OperationMessage` with payload `P` MUST result in an `OperationMessageResponse { payload: B, error: "" }`

### Requirement: Handler plugins survive operative migration

The system SHALL migrate `:on_message` subscriptions along with the operative via the existing ProcessHub handover path. `Flezha.Plugin.Extension.Table.rehydrate/2` MUST re-establish the subscription set on the destination node so that, after migration, previously registered handlers remain reachable under the same key and continue to route requests identically.

#### Scenario: Handler remains registered after migration
- **WHEN** an operative with an `:on_message` subscription serving `key: :k` is migrated to a new node
- **THEN** sending `{:flezha_request, :k, payload, self(), ref}` to the new Overwatch pid MUST produce a reply from the same plugin code as before migration

### Requirement: Overwatch lifetime isolation from handler failures

The system SHALL isolate the Overwatch GenServer from handler plugin failures. A handler plugin that crashes, raises, or exits during `handle/2` MUST NOT bring down the Overwatch process. The caller MUST observe an error reply; the Overwatch MUST remain available for subsequent requests and other lifecycle commands.

#### Scenario: Handler process crash during dispatch
- **WHEN** a handler plugin's `handle/2` raises an exception
- **THEN** the Overwatch MUST continue running, MUST continue serving other requests on other `handler_key`s, and the caller MUST receive `{:error, {:exception, kind, reason}}`
</content>
</invoke>