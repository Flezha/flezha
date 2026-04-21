# processor-init-state Specification

## Purpose

The processor-init-state capability lets operators hydrate a processor's initial state at startup via a configured chain of provider plugins, rather than requiring every caller to know the full `init_values` map at Builder time. Providers run synchronously inside `Flezha.Processor.Overwatch.init/1`, before the SDK sees the processor, and feed into the existing `AddProcessor.init_values` field — no transport, protobuf, or SDK change. Plugins are chained with shallow-merge semantics, each is bounded by a per-plugin timeout enforced by a supervised task, and failures dispatch via a per-plugin `on_error` policy that either aborts processor startup or gracefully degrades to the upstream accumulator. The capability runs only on cold start; migration (`compute_engine_migrate`) preserves its existing snapshot-or-inline-`init_values` logic and never invokes providers.

## Requirements

### Requirement: Per-processor init-state plugin configuration

The system SHALL allow configuring an ordered list of init-state plugins per processor via `Flezha.Processor.Builder.opts[:init_state_plugins]`. Each entry MUST be a `%Flezha.Processor.InitStatePlugin{}` struct carrying `provider` (a module implementing the `Flezha.Processor.InitStateProvider` behaviour), `opts` (a map of provider configuration), `on_error` (`:fail` or `:empty`, default `:fail`), and `timeout_ms` (positive integer, default `5000`).

#### Scenario: No init-state plugins configured
- **WHEN** a processor is started whose Builder `opts` does not include `:init_state_plugins` (or sets it to `[]`)
- **THEN** the processor's `init_values` MUST be exactly what the Builder provided, and no provider module is invoked

#### Scenario: Single plugin configured
- **WHEN** a processor is started with one `%InitStatePlugin{}` entry and the Builder's `init_values` is `%{}`
- **THEN** the provider's `resolve/1` callback MUST be invoked exactly once, and its returned map MUST be sent to the SDK as `init_values` in the `AddProcessor` command

#### Scenario: Multiple plugins configured
- **WHEN** a processor is started with an ordered list `[p1, p2, p3]` of init-state plugins and Builder `init_values` is `%{}`
- **THEN** the providers MUST be invoked in the order `p1 → p2 → p3`, each receiving the accumulated map produced by its predecessors

### Requirement: Init-state provider context

The system SHALL invoke each provider's `resolve/1` callback with a context map containing exactly these keys: `processor_id` (string), `namespace` (atom), `factory_id` (string), `accumulated` (map of state accumulated from prior plugins in the chain), and `opts` (the plugin's own configuration map as set on `%InitStatePlugin{}.opts`).

#### Scenario: First plugin in chain receives empty accumulator
- **WHEN** the first plugin's `resolve/1` is invoked
- **THEN** `context.accumulated` MUST equal `%{}`

#### Scenario: Later plugin receives merged predecessors
- **WHEN** a plugin at position N (N ≥ 2) has its `resolve/1` invoked, and all predecessors returned `{:ok, map}`
- **THEN** `context.accumulated` MUST equal the shallow merge (in order) of every predecessor's returned map

#### Scenario: Provider receives processor identity
- **WHEN** a provider's `resolve/1` is invoked for a processor with id `"proc-42"` in namespace `:tenant_a` with factory id `"fact-7"`
- **THEN** `context.processor_id` MUST be `"proc-42"`, `context.namespace` MUST be `:tenant_a`, and `context.factory_id` MUST be `"fact-7"`

### Requirement: Shallow merge combination

The system SHALL combine each provider's `{:ok, map}` return value into the accumulator by shallow merge (`Map.merge/2`), with the provider's returned keys taking precedence over existing accumulator keys for the same key.

#### Scenario: Later plugin overrides earlier plugin's key
- **WHEN** plugin `p1` returns `{:ok, %{"a" => 1, "b" => 2}}` and plugin `p2` returns `{:ok, %{"b" => 99, "c" => 3}}`
- **THEN** the final `init_values` MUST be `%{"a" => 1, "b" => 99, "c" => 3}`

#### Scenario: Plugin returns empty map
- **WHEN** a plugin returns `{:ok, %{}}`
- **THEN** the accumulator MUST pass through unchanged to the next plugin

### Requirement: Cold-start only, snapshot-wins on migration

The system SHALL invoke the init-state plugin chain only when a processor is started cold — that is, when its Builder `init_values` is `%{}` AND there is no incoming migration snapshot. On migration (`handle_call({:compute_engine_migrate, ...})`), the existing snapshot-or-inline-`init_values` logic MUST be preserved unchanged; init-state providers MUST NOT run for a migrated processor.

#### Scenario: Cold start with no inline init_values
- **WHEN** a processor is started and `processor.init_values == %{}`
- **THEN** the init-state chain MUST run

#### Scenario: Cold start with inline init_values
- **WHEN** a processor is started with non-empty Builder `init_values`
- **THEN** the init-state chain MUST NOT run, and the Builder's `init_values` MUST be used as-is

#### Scenario: Processor migration
- **WHEN** an existing processor is migrated to a new compute engine via `compute_engine_migrate`
- **THEN** the init-state chain MUST NOT run, and migration MUST use the existing snapshot-or-inline-`init_values` selection logic

### Requirement: Per-plugin timeout enforcement

The system SHALL enforce each plugin's `timeout_ms` by running its `resolve/1` invocation inside an isolated task. If the task does not return within `timeout_ms` milliseconds, the system MUST forcibly terminate the task and treat the timeout as an error for the plugin's `on_error` handling. A plugin crashing or raising MUST NOT crash the Overwatch process.

#### Scenario: Provider returns within timeout
- **WHEN** a provider's `resolve/1` returns `{:ok, map}` within `timeout_ms`
- **THEN** the chain continues with the returned map merged into the accumulator

#### Scenario: Provider exceeds timeout
- **WHEN** a provider's `resolve/1` does not return within `timeout_ms`
- **THEN** the system MUST terminate the provider task and dispatch an error to the plugin's `on_error` handler (default `:fail`)

#### Scenario: Provider raises an exception
- **WHEN** a provider's `resolve/1` raises an exception
- **THEN** the system MUST capture the exception without crashing Overwatch and dispatch an error to the plugin's `on_error` handler

### Requirement: Configurable failure semantics

The system SHALL, on any plugin error (explicit `{:error, reason}` return, timeout, or exception), dispatch based on the plugin's `on_error` field. When `on_error: :fail` (the default), `Overwatch.init/1` MUST return `{:error, reason}` (the same shape as the existing `start_processor_sdk` error path), and the processor MUST NOT be started. When `on_error: :empty`, the plugin MUST be skipped and the accumulator MUST pass through unchanged to the next plugin.

#### Scenario: Default failure aborts processor start
- **WHEN** a plugin with `on_error: :fail` returns `{:error, :db_unreachable}`
- **THEN** `Overwatch.init/1` MUST return `{:error, :db_unreachable}` and the SDK MUST NOT receive an `AddProcessor` command for this processor

#### Scenario: Degrading failure continues the chain
- **WHEN** a plugin with `on_error: :empty` returns `{:error, any}` or times out
- **THEN** the plugin MUST be skipped, the accumulator MUST pass through unchanged to the next plugin, and the chain MUST continue

#### Scenario: Mid-chain failure with :fail
- **WHEN** a chain `[p1, p2, p3]` is running and `p2` (with `on_error: :fail`) returns `{:error, reason}`
- **THEN** `p3` MUST NOT be invoked and `Overwatch.init/1` MUST return `{:error, reason}`

### Requirement: Plugin configuration validation

The system SHALL validate each entry of `processor.opts[:init_state_plugins]` before invocation. An entry that is not a `%Flezha.Processor.InitStatePlugin{}` struct, or whose `provider` module is not loaded or does not export `resolve/1`, MUST be treated as an error for that plugin's `on_error` handling (or `:fail` if `on_error` cannot be read).

#### Scenario: Non-struct entry
- **WHEN** `processor.opts[:init_state_plugins]` contains an entry that is not a `%InitStatePlugin{}`
- **THEN** the system MUST produce an error of the form `{:invalid_init_state_plugin, reason}` and apply `:fail` semantics

#### Scenario: Provider module does not implement resolve/1
- **WHEN** a `%InitStatePlugin{}` names a provider module that does not export `resolve/1`
- **THEN** the system MUST produce an error of the form `{:invalid_init_state_plugin, reason}` and apply the plugin's `on_error` policy

### Requirement: No SDK or protocol change

The system SHALL feed the resolved map into the existing `init_values` field of the `AddProcessor` protobuf command. No changes to transport, protobuf definitions, SDK-side handlers, or any non-Flezha-agent code are required by this capability.

#### Scenario: SDK receives resolved values identically to inline values
- **WHEN** init-state plugins resolve to a map `M` and `AddProcessor` is sent to the SDK
- **THEN** the SDK MUST observe the same `init_values == M` as if the caller had baked `M` into the Builder directly
