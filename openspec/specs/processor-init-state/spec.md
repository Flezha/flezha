# processor-init-state Specification

## Purpose

The processor-init-state capability lets operators hydrate a processor's initial state at startup via a configured chain of provider plugins, rather than requiring every caller to know the full `init_values` map at Builder time. Providers run synchronously inside `Flezha.Processor.Overwatch.init/1`, before the SDK sees the processor, and feed into the existing `AddProcessor.init_values` field — no transport, protobuf, or SDK change. Plugins are chained with shallow-merge semantics, each is bounded by a per-plugin timeout enforced by a supervised task, and failures dispatch via a per-plugin `on_error` policy that either aborts processor startup or gracefully degrades to the upstream accumulator. The capability runs only on cold start; migration (`compute_engine_migrate`) preserves its existing snapshot-or-inline-`init_values` logic and never invokes providers.

## Requirements

### Requirement: Per-processor init-state plugin configuration

The system SHALL allow configuring init-state plugins per processor via `Flezha.Processor.Builder.opts[:plugins]`. Each entry MUST be a `%Flezha.Plugin{}` struct whose `subscriptions` list contains one `{:on_processor_init, sub_opts}` entry. `sub_opts` is a keyword list supporting `position` (non-negative integer, default assigned by insertion order), `on_error` (`:fail` or `:empty`, default `:fail`), and `timeout_ms` (positive integer, default `5000`). The plugin's `provider` module MUST implement an `on_processor_init/2` callback receiving `(Flezha.Plugin.Extension.Ctx.Init.t(), opts :: map())`.

#### Scenario: No init-state subscribers configured
- **WHEN** a processor is started whose Builder `opts` contains no `%Flezha.Plugin{}` subscribing to `:on_processor_init` (either because `opts[:plugins]` is empty, absent, or none of the listed plugins subscribe to that extension)
- **THEN** the processor's `init_values` MUST be exactly what the Builder provided, no provider callback is invoked, and the active-mask bit for `:on_processor_init` MUST remain clear

#### Scenario: Single subscriber configured
- **WHEN** a processor is started with one `%Flezha.Plugin{subscriptions: [{:on_processor_init, _}]}` entry and the Builder's `init_values` is `%{}`
- **THEN** the provider module's `on_processor_init/2` callback MUST be invoked exactly once, and the resulting `accumulated` map MUST be sent to the SDK as `init_values` in the `AddProcessor` command

#### Scenario: Multiple subscribers configured
- **WHEN** a processor is started with three plugins `[p1, p2, p3]` each subscribing to `:on_processor_init` with positions 0, 1, 2, and the Builder `init_values` is `%{}`
- **THEN** the subscribers MUST be invoked in order of their `position` (0 → 1 → 2), each receiving the `Flezha.Plugin.Extension.Ctx.Init` context whose `accumulated` field holds the shallow-merged output of all predecessors

### Requirement: Init-state provider context

The system SHALL invoke each `:on_processor_init` subscriber's `on_processor_init/2` callback with a `%Flezha.Plugin.Extension.Ctx.Init{}` struct containing exactly these fields: `processor_id` (string), `namespace` (atom), `factory_id` (string), and `accumulated` (map of state accumulated from prior subscribers in the fold). The second argument MUST be the subscriber's `opts` map (the `%Flezha.Plugin{}.opts` for this plugin).

#### Scenario: First subscriber in chain receives empty accumulator
- **WHEN** the first subscriber's `on_processor_init/2` is invoked
- **THEN** the ctx's `accumulated` field MUST equal the Builder's `init_values` (typically `%{}` for cold start)

#### Scenario: Later subscriber receives merged predecessors
- **WHEN** a subscriber at position N (N ≥ 1) has its `on_processor_init/2` invoked, and all predecessors returned `{:ok, ctx_with_merged_accumulator}`
- **THEN** the ctx's `accumulated` field MUST equal the shallow merge (in order) of every predecessor's returned map

#### Scenario: Subscriber receives processor identity
- **WHEN** a subscriber's `on_processor_init/2` is invoked for a processor with id `"proc-42"` in namespace `:tenant_a` with factory id `"fact-7"`
- **THEN** the ctx's `processor_id` MUST be `"proc-42"`, `namespace` MUST be `:tenant_a`, and `factory_id` MUST be `"fact-7"`

### Requirement: Shallow merge combination

The system SHALL combine each subscriber's `{:ok, ctx}` return value into the accumulator by shallow-merging `ctx.accumulated` into the chain-level accumulator via `Map.merge/2`, with the subscriber's returned keys taking precedence over existing accumulator keys for the same key.

#### Scenario: Later subscriber overrides earlier subscriber's key
- **WHEN** subscriber `p1` returns `{:ok, %Ctx.Init{accumulated: %{"a" => 1, "b" => 2}}}` and subscriber `p2` returns `{:ok, %Ctx.Init{accumulated: %{"b" => 99, "c" => 3}}}`
- **THEN** the final `init_values` MUST be `%{"a" => 1, "b" => 99, "c" => 3}`

#### Scenario: Subscriber returns empty accumulator
- **WHEN** a subscriber returns `{:ok, %Ctx.Init{accumulated: %{}}}`
- **THEN** the accumulator MUST pass through unchanged to the next subscriber

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

The system SHALL enforce each `:on_processor_init` subscriber's `timeout_ms` by running its `on_processor_init/2` invocation inside an isolated task. If the task does not return within `timeout_ms` milliseconds, the system MUST forcibly terminate the task and treat the timeout as an error for the subscriber's `on_error` handling. A subscriber crashing or raising MUST NOT crash the Overwatch process. Timeout and exception handling MUST be driven by the fold-kind dispatch semantics defined in the `processor-extensions` capability.

#### Scenario: Subscriber returns within timeout
- **WHEN** a subscriber's `on_processor_init/2` returns `{:ok, ctx}` within `timeout_ms`
- **THEN** the chain continues with the returned ctx merged into the accumulator

#### Scenario: Subscriber exceeds timeout
- **WHEN** a subscriber's `on_processor_init/2` does not return within `timeout_ms`
- **THEN** the system MUST terminate the subscriber task and dispatch an error to the subscriber's `on_error` handler (default `:fail`)

#### Scenario: Subscriber raises an exception
- **WHEN** a subscriber's `on_processor_init/2` raises an exception
- **THEN** the system MUST capture the exception without crashing Overwatch and dispatch an error to the subscriber's `on_error` handler

### Requirement: Configurable failure semantics

The system SHALL, on any `:on_processor_init` subscriber error (explicit `{:error, reason}` return, timeout, or exception), dispatch based on the subscription's `on_error` option. When `on_error: :fail` (the default), `Overwatch.init/1` MUST return `{:error, reason}` and the processor MUST NOT be started. When `on_error: :empty`, the failed subscriber MUST be skipped and the accumulator MUST pass through unchanged to the next subscriber in the fold.

#### Scenario: Default failure aborts processor start
- **WHEN** a subscriber with `on_error: :fail` returns `{:error, :db_unreachable}`
- **THEN** `Overwatch.init/1` MUST return `{:error, :db_unreachable}` and the SDK MUST NOT receive an `AddProcessor` command for this processor

#### Scenario: Degrading failure continues the chain
- **WHEN** a subscriber with `on_error: :empty` returns `{:error, any}` or times out
- **THEN** the subscriber MUST be skipped, the accumulator MUST pass through unchanged to the next subscriber, and the fold MUST continue

#### Scenario: Mid-chain failure with :fail
- **WHEN** a chain `[p1, p2, p3]` is running and `p2` (with `on_error: :fail`) returns `{:error, reason}`
- **THEN** `p3` MUST NOT be invoked and `Overwatch.init/1` MUST return `{:error, reason}`

### Requirement: Plugin configuration validation

The system SHALL validate each `%Flezha.Plugin{}` subscribed to `:on_processor_init` at subscribe time via `Flezha.Plugin.Extension.Table.subscribe/3`. An entry whose `provider` module is not loaded, does not export `on_processor_init/2`, or whose subscription opts contain invalid values (e.g. non-integer `timeout_ms`, unknown `on_error`) MUST cause `subscribe/3` to return `{:error, reason}` without mutating table state. When validation fails for a plugin provided via `Builder.opts[:plugins]`, the failure MUST be handled by the plugin's `on_error` policy (or `:fail` if `on_error` cannot be read).

#### Scenario: Provider module does not implement on_processor_init/2
- **WHEN** a `%Flezha.Plugin{}` subscribes to `:on_processor_init` but its `provider` module does not export `on_processor_init/2`
- **THEN** `subscribe/3` MUST return `{:error, {:missing_callback, :on_processor_init}}` and the plugin's `on_error` policy MUST be applied when encountered via Builder opts

#### Scenario: Subscription opts are invalid
- **WHEN** a plugin subscribes to `:on_processor_init` with a non-positive `timeout_ms` or an `on_error` value other than `:fail` or `:empty`
- **THEN** `subscribe/3` MUST return `{:error, {:invalid_subscription_opts, reason}}` and the plugin's `on_error` policy MUST be applied when encountered via Builder opts

### Requirement: No SDK or protocol change

The system SHALL feed the resolved map into the existing `init_values` field of the `AddProcessor` protobuf command. No changes to transport, protobuf definitions, SDK-side handlers, or any non-Flezha-agent code are required by this capability.

#### Scenario: SDK receives resolved values identically to inline values
- **WHEN** init-state plugins resolve to a map `M` and `AddProcessor` is sent to the SDK
- **THEN** the SDK MUST observe the same `init_values == M` as if the caller had baked `M` into the Builder directly
</content>
</invoke>