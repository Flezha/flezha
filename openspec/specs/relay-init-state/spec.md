# relay-init-state Specification

## Purpose

The relay-init-state capability lets executors hydrate a relay's initial state at startup via a configured chain of provider plugins, rather than requiring every caller to know the full `init_values` map at Builder time. Providers run synchronously inside `Flezha.Relay.Overwatch.init/1`, before the SDK sees the relay, and feed into the existing `AddRelay.init_values` field — no transport, protobuf, or SDK change. Plugins are chained with shallow-merge semantics, each is bounded by a per-plugin timeout enforced by a supervised task, and failures dispatch via a per-plugin `on_error` policy that either aborts relay startup or gracefully degrades to the upstream accumulator. The capability runs only on cold start; migration (`executor_migrate`) preserves its existing snapshot-or-inline-`init_values` logic and never invokes providers.

## Requirements

### Requirement: Per-relay init-state plugin configuration

The system SHALL allow configuring init-state plugins per relay via `Flezha.Relay.Builder.opts[:plugins]`. Each entry MUST be a `%Flezha.Plugin{}` struct whose `subscriptions` list contains one `{:on_relay_init, sub_opts}` entry. `sub_opts` is a keyword list supporting `position` (non-negative integer, default assigned by insertion order), `on_error` (`:fail` or `:empty`, default `:fail`), and `timeout_ms` (positive integer, default `5000`). The plugin's `provider` module MUST implement an `on_relay_init/2` callback receiving `(Flezha.Plugin.Extension.Ctx.Init.t(), opts :: map())`.

#### Scenario: No init-state subscribers configured
- **WHEN** a relay is started whose Builder `opts` contains no `%Flezha.Plugin{}` subscribing to `:on_relay_init` (either because `opts[:plugins]` is empty, absent, or none of the listed plugins subscribe to that extension)
- **THEN** the relay's `init_values` MUST be exactly what the Builder provided, no provider callback is invoked, and the active-mask bit for `:on_relay_init` MUST remain clear

#### Scenario: Single subscriber configured
- **WHEN** a relay is started with one `%Flezha.Plugin{subscriptions: [{:on_relay_init, _}]}` entry and the Builder's `init_values` is `%{}`
- **THEN** the provider module's `on_relay_init/2` callback MUST be invoked exactly once, and the resulting `accumulated` map MUST be sent to the SDK as `init_values` in the `AddRelay` command

#### Scenario: Multiple subscribers configured
- **WHEN** a relay is started with three plugins `[p1, p2, p3]` each subscribing to `:on_relay_init` with positions 0, 1, 2, and the Builder `init_values` is `%{}`
- **THEN** the subscribers MUST be invoked in order of their `position` (0 → 1 → 2), each receiving the `Flezha.Plugin.Extension.Ctx.Init` context whose `accumulated` field holds the shallow-merged output of all predecessors

### Requirement: Init-state provider context

The system SHALL invoke each `:on_relay_init` subscriber's `on_relay_init/2` callback with a `%Flezha.Plugin.Extension.Ctx.Init{}` struct containing exactly these fields: `relay_id` (string), `namespace` (atom), `factory_id` (string), and `accumulated` (map of state accumulated from prior subscribers in the fold). The second argument MUST be the subscriber's `opts` map (the `%Flezha.Plugin{}.opts` for this plugin).

#### Scenario: First subscriber in chain receives empty accumulator
- **WHEN** the first subscriber's `on_relay_init/2` is invoked
- **THEN** the ctx's `accumulated` field MUST equal the Builder's `init_values` (typically `%{}` for cold start)

#### Scenario: Later subscriber receives merged predecessors
- **WHEN** a subscriber at position N (N ≥ 1) has its `on_relay_init/2` invoked, and all predecessors returned `{:ok, ctx_with_merged_accumulator}`
- **THEN** the ctx's `accumulated` field MUST equal the shallow merge (in order) of every predecessor's returned map

#### Scenario: Subscriber receives relay identity
- **WHEN** a subscriber's `on_relay_init/2` is invoked for a relay with id `"proc-42"` in namespace `:tenant_a` with factory id `"fact-7"`
- **THEN** the ctx's `relay_id` MUST be `"proc-42"`, `namespace` MUST be `:tenant_a`, and `factory_id` MUST be `"fact-7"`

### Requirement: Shallow merge combination

The system SHALL combine each subscriber's `{:ok, ctx}` return value into the accumulator by shallow-merging `ctx.accumulated` into the chain-level accumulator via `Map.merge/2`, with the subscriber's returned keys taking precedence over existing accumulator keys for the same key.

#### Scenario: Later subscriber overrides earlier subscriber's key
- **WHEN** subscriber `p1` returns `{:ok, %Ctx.Init{accumulated: %{"a" => 1, "b" => 2}}}` and subscriber `p2` returns `{:ok, %Ctx.Init{accumulated: %{"b" => 99, "c" => 3}}}`
- **THEN** the final `init_values` MUST be `%{"a" => 1, "b" => 99, "c" => 3}`

#### Scenario: Subscriber returns empty accumulator
- **WHEN** a subscriber returns `{:ok, %Ctx.Init{accumulated: %{}}}`
- **THEN** the accumulator MUST pass through unchanged to the next subscriber

### Requirement: Cold-start only, snapshot-wins on migration

The system SHALL invoke the init-state plugin chain only when a relay is started cold — that is, when its Builder `init_values` is `%{}` AND there is no incoming migration snapshot AND there is no incoming `RestoreInitState` changelog payload from the `relay-state-persistence` capability. On migration (`handle_call({:executor_migrate, ...})`), the existing snapshot-or-inline-`init_values` logic MUST be preserved unchanged; init-state providers MUST NOT run for a migrated relay. On reconnect of a Partition whose Relay has `persistence_enabled = true` AND whose agent-side state is non-empty (snapshot file or WAL records exist), the agent SHALL deliver state via `RestoreInitState{partition_id, snapshot_bytes, change_log_bytes}` (per `relay-state-persistence`) instead of via `init_values`; init-state providers MUST NOT run for a Partition restored via `RestoreInitState`.

For Partitions whose Relay has `persistence_enabled = false`, OR whose agent-side state is empty (no snapshot, no WAL records), the existing init-state capability flow runs unchanged: the init-state plugin chain runs (cold start), or the inline Builder `init_values` is used (cold start with non-empty inline values), or the migration snapshot is used (migrated relay).

`RestoreInitState` and `executor_migrate` SHALL be mutually exclusive per Partition: when `executor_migrate` is invoked for a Partition with persisted state, the migration path's snapshot bytes SHALL be the restoration source AND `RestoreInitState` SHALL NOT be issued for that Partition. The agent's `Overwatch.init/1` SHALL enforce this exclusion.

#### Scenario: Cold start with no inline init_values and no persisted state

- **WHEN** a relay is started and `relay.init_values == %{}` AND no `RestoreInitState` payload is present (either because `persistence_enabled = false` OR no agent-side state exists for any Partition)
- **THEN** the init-state chain MUST run

#### Scenario: Cold start with inline init_values

- **WHEN** a relay is started with non-empty Builder `init_values`
- **THEN** the init-state chain MUST NOT run, and the Builder's `init_values` MUST be used as-is (regardless of `persistence_enabled`)

#### Scenario: Relay migration

- **WHEN** an existing relay is migrated to a new compute engine via `executor_migrate`
- **THEN** the init-state chain MUST NOT run, and migration MUST use the existing snapshot-or-inline-`init_values` selection logic; `RestoreInitState` SHALL NOT be issued for any Partition of the migrated relay (even Partitions with persisted state — the migration snapshot is authoritative)

#### Scenario: Partition restart via changelog

- **WHEN** a Partition reconnects AND its Relay has `persistence_enabled = true` AND the agent has a non-empty WAL or snapshot under the `relay-state-persistence` capability
- **THEN** the agent SHALL deliver state via `RestoreInitState{partition_id, snapshot_bytes, change_log_bytes}`; the init-state plugin chain MUST NOT run for that Partition; the SDK SHALL restore via `init_state(shard_key, Some(payload))` + runtime auto-snapshot-decode + change-log fold per `relay-state-persistence`

#### Scenario: Partition restart with persistence disabled at Relay class

- **WHEN** a Partition reconnects AND its Relay has `persistence_enabled = false`
- **THEN** the agent SHALL NOT issue `RestoreInitState` for that Partition (regardless of any stale agent-side state from a prior `persistence_enabled = true` deploy of the same Relay class); the init-state chain MUST run if `relay.init_values == %{}`

#### Scenario: Partition restart with persistence enabled but no persisted state

- **WHEN** a Partition reconnects AND its Relay has `persistence_enabled = true` AND no WAL or snapshot file exists for this Partition
- **THEN** the agent SHALL NOT issue `RestoreInitState` (there is nothing to restore); the init-state chain MUST run if `relay.init_values == %{}`

### Requirement: Per-plugin timeout enforcement

The system SHALL enforce each `:on_relay_init` subscriber's `timeout_ms` by running its `on_relay_init/2` invocation inside an isolated task. If the task does not return within `timeout_ms` milliseconds, the system MUST forcibly terminate the task and treat the timeout as an error for the subscriber's `on_error` handling. A subscriber crashing or raising MUST NOT crash the Overwatch process. Timeout and exception handling MUST be driven by the fold-kind dispatch semantics defined in the `relay-extensions` capability.

#### Scenario: Subscriber returns within timeout
- **WHEN** a subscriber's `on_relay_init/2` returns `{:ok, ctx}` within `timeout_ms`
- **THEN** the chain continues with the returned ctx merged into the accumulator

#### Scenario: Subscriber exceeds timeout
- **WHEN** a subscriber's `on_relay_init/2` does not return within `timeout_ms`
- **THEN** the system MUST terminate the subscriber task and dispatch an error to the subscriber's `on_error` handler (default `:fail`)

#### Scenario: Subscriber raises an exception
- **WHEN** a subscriber's `on_relay_init/2` raises an exception
- **THEN** the system MUST capture the exception without crashing Overwatch and dispatch an error to the subscriber's `on_error` handler

### Requirement: Configurable failure semantics

The system SHALL, on any `:on_relay_init` subscriber error (explicit `{:error, reason}` return, timeout, or exception), dispatch based on the subscription's `on_error` option. When `on_error: :fail` (the default), `Overwatch.init/1` MUST return `{:error, reason}` and the relay MUST NOT be started. When `on_error: :empty`, the failed subscriber MUST be skipped and the accumulator MUST pass through unchanged to the next subscriber in the fold.

#### Scenario: Default failure aborts relay start
- **WHEN** a subscriber with `on_error: :fail` returns `{:error, :db_unreachable}`
- **THEN** `Overwatch.init/1` MUST return `{:error, :db_unreachable}` and the SDK MUST NOT receive an `AddRelay` command for this relay

#### Scenario: Degrading failure continues the chain
- **WHEN** a subscriber with `on_error: :empty` returns `{:error, any}` or times out
- **THEN** the subscriber MUST be skipped, the accumulator MUST pass through unchanged to the next subscriber, and the fold MUST continue

#### Scenario: Mid-chain failure with :fail
- **WHEN** a chain `[p1, p2, p3]` is running and `p2` (with `on_error: :fail`) returns `{:error, reason}`
- **THEN** `p3` MUST NOT be invoked and `Overwatch.init/1` MUST return `{:error, reason}`

### Requirement: Plugin configuration validation

The system SHALL validate each `%Flezha.Plugin{}` subscribed to `:on_relay_init` at subscribe time via `Flezha.Plugin.Extension.Table.subscribe/3`. An entry whose `provider` module is not loaded, does not export `on_relay_init/2`, or whose subscription opts contain invalid values (e.g. non-integer `timeout_ms`, unknown `on_error`) MUST cause `subscribe/3` to return `{:error, reason}` without mutating table state. When validation fails for a plugin provided via `Builder.opts[:plugins]`, the failure MUST be handled by the plugin's `on_error` policy (or `:fail` if `on_error` cannot be read).

#### Scenario: Provider module does not implement on_relay_init/2
- **WHEN** a `%Flezha.Plugin{}` subscribes to `:on_relay_init` but its `provider` module does not export `on_relay_init/2`
- **THEN** `subscribe/3` MUST return `{:error, {:missing_callback, :on_relay_init}}` and the plugin's `on_error` policy MUST be applied when encountered via Builder opts

#### Scenario: Subscription opts are invalid
- **WHEN** a plugin subscribes to `:on_relay_init` with a non-positive `timeout_ms` or an `on_error` value other than `:fail` or `:empty`
- **THEN** `subscribe/3` MUST return `{:error, {:invalid_subscription_opts, reason}}` and the plugin's `on_error` policy MUST be applied when encountered via Builder opts

### Requirement: No SDK or protocol change

The system SHALL feed the resolved map into the existing `init_values` field of the `AddRelay` protobuf command. No changes to transport, protobuf definitions, SDK-side handlers, or any non-Flezha-agent code are required by this capability.

#### Scenario: SDK receives resolved values identically to inline values
- **WHEN** init-state plugins resolve to a map `M` and `AddRelay` is sent to the SDK
- **THEN** the SDK MUST observe the same `init_values == M` as if the caller had baked `M` into the Builder directly