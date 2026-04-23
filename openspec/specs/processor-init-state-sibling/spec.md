# processor-init-state-sibling Specification

## Purpose

The processor-init-state-sibling capability ships a first-party `Flezha.Processor.InitState.Providers.Sibling` init-state provider that seeds a cold-starting processor's initial state from another running processor. It is composed on top of `processor-init-state` (as a provider entry in a processor's `Builder.opts[:init_state_plugins]` chain) and `processor-message-handlers` (as the transport for the typed request envelope). The provider resolves the source processor via `Flezha.Service.ProcessorManager.get_processor_pid/2`, sends a `{:flezha_request, handler_key, payload, self(), ref}` envelope, decodes the reply as a JSON object into a map, and returns it to the enclosing init-state chain. It propagates handler errors verbatim so the outer `%InitStatePlugin{}.on_error` policy decides whether to abort or continue, and it bounds its own `receive` strictly below the enclosing `InitStatePlugin.timeout_ms` so the inner wait times out before `Task.shutdown(:brutal_kill)` fires. The provider performs no hidden state mutation on the source processor — it only sends typed request envelopes via the message-handler surface.

## Requirements

### Requirement: First-party `Sibling` init-state provider

The system SHALL ship a first-party `Flezha.Processor.InitState.Providers.Sibling` module implementing `Flezha.Processor.InitStateProvider`. The provider SHALL be configurable as an entry in a processor's `Builder.opts[:init_state_plugins]` chain via `%Flezha.Processor.InitStatePlugin{}.provider == Flezha.Processor.InitState.Providers.Sibling` with the provider's own `opts` map carrying at least `:source_processor_id` (string), `:source_namespace` (string, defaults to the starting processor's namespace), `:handler_key` (atom), optional `:request_payload` (binary, defaults to `<<>>`), and optional `:request_timeout_ms` (positive integer, defaults to a value below the enclosing `InitStatePlugin.timeout_ms`).

#### Scenario: Provider is configured on a processor's init-state chain
- **WHEN** a processor is started cold with `opts: [init_state_plugins: [%InitStatePlugin{provider: Flezha.Processor.InitState.Providers.Sibling, opts: %{source_processor_id: "src", handler_key: :fetch_init_state}}]]`
- **THEN** the provider's `resolve/1` MUST be invoked during `Overwatch.init/1` exactly once, following the existing cold-start gating rules of `processor-init-state`

### Requirement: Source processor lookup and request dispatch

The provider SHALL resolve the source processor's Overwatch pid via `Flezha.Service.ProcessorManager.get_processor_pid/2` using `:source_namespace` and `:source_processor_id`. The provider SHALL send `{:flezha_request, handler_key, request_payload, self(), ref}` to that pid and block on `{:flezha_response, ref, result}` bounded by `:request_timeout_ms`.

#### Scenario: Source processor is running and registered
- **WHEN** the source processor is running in the specified namespace and the handler responds with JSON-encoded bytes `{"a": 1}`
- **THEN** the provider's `resolve/1` MUST return `{:ok, %{"a" => 1}}`

#### Scenario: Source processor is not running
- **WHEN** `ProcessorManager.get_processor_pid/2` does not find a process for the configured source
- **THEN** the provider's `resolve/1` MUST return `{:error, :source_processor_not_found}`

#### Scenario: Request times out
- **WHEN** the handler does not reply within `:request_timeout_ms`
- **THEN** the provider's `resolve/1` MUST return `{:error, :timeout}` and MUST NOT crash the starting processor's Overwatch

### Requirement: JSON decoding of reply payload into a map

On a successful `{:ok, bytes}` reply, the provider SHALL decode `bytes` as JSON using the existing `Flezha.Service.Payloader.json_to_map/1` decoder. A top-level JSON object MUST be returned as `{:ok, map}`. A non-object JSON value (array, string, number, boolean, null) or an undecodable payload MUST be returned as `{:error, {:invalid_reply_payload, reason}}`.

#### Scenario: Reply is a JSON object
- **WHEN** the reply bytes decode to a JSON object `{"inputs": ["a"]}`
- **THEN** the provider MUST return `{:ok, %{"inputs" => ["a"]}}`

#### Scenario: Reply is a JSON array
- **WHEN** the reply bytes decode to a JSON array `[1, 2, 3]`
- **THEN** the provider MUST return `{:error, {:invalid_reply_payload, :not_a_map}}`

#### Scenario: Reply is not valid JSON
- **WHEN** the reply bytes are not valid JSON
- **THEN** the provider MUST return `{:error, {:invalid_reply_payload, reason}}` where `reason` describes the decoding failure

### Requirement: Handler error propagation

When the handler returns `{:flezha_response, ref, {:error, reason}}`, the provider SHALL propagate that as `{:error, reason}` from `resolve/1` without attempting to decode `reason`. This preserves the caller's ability to dispatch via the outer `%InitStatePlugin{}.on_error` policy.

#### Scenario: Handler reports an error
- **WHEN** the handler replies with `{:error, :snapshot_unavailable}`
- **THEN** the provider MUST return `{:error, :snapshot_unavailable}` and the outer `%InitStatePlugin{}.on_error` policy MUST decide whether to abort or continue the init-state chain

### Requirement: Timeout budget alignment with the enclosing InitStatePlugin

The provider SHALL enforce its `:request_timeout_ms` strictly below the enclosing `%InitStatePlugin{}.timeout_ms` so the inner `receive` times out before the outer `Task.shutdown(:brutal_kill)` fires. When `:request_timeout_ms` is not supplied, the provider SHALL derive a default by subtracting a fixed margin from the enclosing timeout or by using a safe constant (e.g. 4000ms when the enclosing default is 5000ms).

#### Scenario: Default timeout is below outer budget
- **WHEN** the provider is used with the default `InitStatePlugin.timeout_ms: 5_000` and no explicit `:request_timeout_ms`
- **THEN** the provider MUST time out its own `receive` before 5000ms has elapsed, producing `{:error, :timeout}` that the outer `on_error` can dispatch cleanly

### Requirement: No hidden state mutation on the source processor

The provider SHALL only send typed request envelopes to the source processor's Overwatch. It MUST NOT invoke migration handlers, state serialization commands, or any API that mutates the source processor's observable state.

#### Scenario: Source processor state is unchanged after a Sibling request
- **WHEN** a `Sibling` request is served by the source processor
- **THEN** calling `Flezha.Service.ProcessorManager.get_processor_state/2` on the source before and after the request MUST return the same state (up to any mutations the source processor itself performs via normal IO)
