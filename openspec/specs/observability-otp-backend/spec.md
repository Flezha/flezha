# observability-otp-backend Specification

## Purpose

The observability-otp-backend capability provides the content side of the observability contract by shipping a library of built-in process-backed `Source` implementations registered through the foundation's `Flezha.Observability.Source` behaviour. It covers the BEAM VM (`:beam_vm`), the OS process via Linux `/proc` (`:host`), every active ProcessHub instance (`:processhub`), each configured transport (`:flezha_transport`), and the bounded-mailbox subsystem (`:flezha_bounded_mailbox`). The sources run under a dedicated `Flezha.Observability.OtpBackend.Supervisor` (`:one_for_one`) inserted into the foundation supervision tree after `DefaultSinks`, each honouring per-source cardinality budgets and an `:otp_backend_enabled` runtime flag for wholesale opt-out. The capability also ships the `flz observe` CLI verb family (`sources`, `sinks`, `tail`, `enable`, `disable`) that exposes the observability service API to operators with text and `--json` output modes, every verb routing through `Flezha.Service.CliManager.observability_*` wrappers with a `caller_context` of `source: :cli`.

## Requirements

### Requirement: Built-in BEAM VM source

The system SHALL provide a built-in process-backed source named `:beam_vm` that polls the running Erlang VM and emits `:metric` signals covering memory, schedulers, run queues, and garbage collection. The source SHALL register with the foundation Registry at supervisor start and SHALL be visible in `Flezha.Observability.Registry.list_sources/0`. The source SHALL respect a per-source cardinality budget appropriate to a single-node BEAM (a small constant — no per-process or per-scheduler labels at value granularity).

#### Scenario: BEAM VM source registers at boot
- **WHEN** the application boots with `:otp_backend_enabled: true`
- **THEN** `Flezha.Observability.Registry.list_sources/0` SHALL include a source with `name: :beam_vm`, `flavour: :process_backed`, and `status: :active`

#### Scenario: BEAM VM source emits memory signals
- **WHEN** the source has been running for at least one poll interval
- **THEN** `:metric` signals on source `:beam_vm` SHALL appear on the bus carrying at minimum `name`s for `total memory`, `processes memory`, `atom memory`, `binary memory`, and `ets memory`, each with a `measurements: %{value: integer()}` map

#### Scenario: BEAM VM source emits scheduler utilisation
- **WHEN** the source has emitted at least two scheduler polls (so a delta is available)
- **THEN** the source SHALL emit a `:scheduler_utilisation` `:metric` signal with `measurements: %{value: float()}` between `0.0` and `1.0`

#### Scenario: BEAM VM source poll interval is configurable
- **WHEN** the application boots with `:observability` → `:otp_backend` → `:poll_intervals` containing `beam_vm: 250`
- **THEN** the source SHALL poll every `250` milliseconds rather than the default

### Requirement: Built-in host source with platform degradation

The system SHALL provide a built-in process-backed source named `:host` that polls Linux `/proc/self/{stat,status}` and `/proc/loadavg` and emits `:metric` signals for OS process RSS, CPU user/system time, open file descriptor count, and load average. On non-Linux platforms the source SHALL emit a single `:event` signal with `name: :host_source_degraded` on its first poll and SHALL produce no further signals.

#### Scenario: Host source emits RSS on Linux
- **WHEN** the source polls on a Linux host
- **THEN** a `:metric` signal on source `:host` with `name: :rss_bytes` and `measurements: %{value: pos_integer()}` SHALL appear on the bus

#### Scenario: Host source degrades on non-Linux platforms
- **WHEN** the source polls on a platform where `/proc/self/stat` is not readable
- **THEN** a single `:event` signal with `source: :host`, `name: :host_source_degraded` SHALL be emitted, and no subsequent `:metric` signals SHALL be emitted from the `:host` source

#### Scenario: Host source emits CPU utilisation after two samples
- **WHEN** the source has completed at least two polls
- **THEN** a `:metric` signal with `name: :cpu_utilisation` and `measurements: %{value: float()}` SHALL appear on the bus, computed from the delta of `/proc/self/stat`'s utime + stime fields

### Requirement: Built-in ProcessHub source

The system SHALL provide a built-in process-backed source named `:processhub` that polls every active ProcessHub instance (via `ProcessHub.process_list/2` and related public APIs) and emits `:metric` signals covering per-hub registered process counts and pending recoveries. The source SHALL emit one signal per hub per poll and SHALL handle the no-hubs case as a no-op (no signals, no error).

#### Scenario: Per-hub registered process count
- **WHEN** a ProcessHub instance `:hub_default` is up with `7` registered processes
- **THEN** a `:metric` signal on source `:processhub` with `name: :registered_processes`, `measurements: %{value: 7}`, and `metadata: %{hub: :hub_default}` SHALL appear on the bus

#### Scenario: No-hubs case
- **WHEN** no ProcessHub instances are active
- **THEN** the source SHALL emit no signals and SHALL NOT raise

### Requirement: Built-in transport source

The system SHALL provide a built-in process-backed source named `:flezha_transport` that polls each configured transport (`:uds`, `:quic`) and emits `:metric` signals for per-transport open connection count, lifetime bytes in, and lifetime bytes out. The source SHALL produce one signal per active transport per poll.

#### Scenario: Per-transport open-connection count
- **WHEN** a UDS transport has `4` open runner connections
- **THEN** a `:metric` signal on source `:flezha_transport` with `name: :open_connections`, `measurements: %{value: 4}`, and `metadata: %{transport: :uds}` SHALL appear on the bus

#### Scenario: Transport-absent case
- **WHEN** a transport is not enabled in `:flezha, :transports` config
- **THEN** the source SHALL NOT emit signals for that transport and SHALL NOT raise

### Requirement: Built-in bounded-mailbox source

The system SHALL provide a built-in process-backed source named `:flezha_bounded_mailbox` that polls the bounded-mailbox subsystem and emits `:metric` signals for per-Operative mailbox high-water marks and drop counters.

#### Scenario: Per-Operative mailbox high-water mark
- **WHEN** a Operative named `"orders"` in namespace `"default"` has a mailbox high-water mark of `120`
- **THEN** a `:metric` signal on source `:flezha_bounded_mailbox` with `name: :mailbox_high_water`, `measurements: %{value: 120}`, and `metadata: %{namespace: "default", operative: "orders"}` SHALL appear on the bus

#### Scenario: Drop counter exposed
- **WHEN** a Operative has dropped `5` messages due to bounded-mailbox overflow
- **THEN** a `:metric` signal with `name: :messages_dropped`, `measurements: %{count: 5}` SHALL appear on the bus with the same `namespace` / `operative` metadata shape

### Requirement: Cardinality budget enforcement on built-in sources

Every built-in source SHALL declare its cardinality budget at registration time and SHALL be designed so that the cluster-scale label cardinality (operatives × operations × segments × runners × hubs) fits within the declared budget. Where the natural cardinality exceeds the foundation's default `1_000` budget, the source SHALL raise its budget at registration time and document the rationale in the source module's `@moduledoc`.

#### Scenario: Source declares a budget at registration
- **WHEN** the `:flezha_bounded_mailbox` source registers
- **THEN** its `cardinality_budget` value SHALL be at least equal to the expected count of Operatives × Namespaces in a typical cluster, and the value SHALL be visible in `Flezha.Observability.Registry.list_sources/0`

#### Scenario: Over-budget emit triggers the foundation's budget-exceeded event
- **WHEN** a built-in source emits `:metric` signals with more distinct metadata shapes than its declared cardinality budget allows
- **THEN** the foundation's `:cardinality_budget_exceeded` event SHALL fire (per the foundation's contract) and subsequent over-budget shapes SHALL be dropped at the bus

### Requirement: OTP backend supervisor and enable flag

The system SHALL host all built-in sources under a dedicated supervisor `Flezha.Observability.OtpBackend.Supervisor` (`:one_for_one`) added as a child of `Flezha.Observability.Supervisor` AFTER the foundation's `DefaultSinks` worker. When the runtime config flag `:observability, :otp_backend_enabled` is `false`, the OTP backend supervisor SHALL start with no children and no source SHALL register.

#### Scenario: OTP backend enabled (default)
- **WHEN** the application boots with default `:observability` config
- **THEN** `Flezha.Observability.OtpBackend.Supervisor` SHALL be running and `which_children/1` SHALL list one child per built-in source

#### Scenario: OTP backend disabled by config
- **WHEN** the application boots with `:observability, :otp_backend_enabled: false`
- **THEN** `Flezha.Observability.OtpBackend.Supervisor` SHALL be running with zero children, and `Flezha.Observability.Registry.list_sources/0` SHALL NOT include any of `:beam_vm`, `:host`, `:processhub`, `:flezha_transport`, `:flezha_bounded_mailbox`

#### Scenario: Per-source crash does not affect peers
- **WHEN** the `:host` source process exits abnormally
- **THEN** the `:one_for_one` strategy SHALL restart only the `:host` source and the other built-in sources SHALL continue emitting

### Requirement: `flz observe` CLI verb family

The system SHALL provide a `flz observe` CLI command tree exposing the observability service API to operators. At minimum the verbs `sources`, `sinks`, `tail <source>`, `enable <source>`, and `disable <source>` SHALL be implemented. Every verb SHALL build a `caller_context` with `source: :cli` and route through `Flezha.Service.CliManager.observability_*` wrappers. Text output SHALL be the default; `--json` SHALL render machine-parseable output.

#### Scenario: `flz observe sources` lists registered sources
- **WHEN** the operator runs `flz observe sources` against a running Controller
- **THEN** the command SHALL print one row per registered source with at minimum its name, status (`active` / `disabled`), declared kinds, and cardinality budget

#### Scenario: `flz observe tail` streams live signals from a source
- **WHEN** the operator runs `flz observe tail beam_vm` and a `:metric` signal is emitted on `:beam_vm`
- **THEN** the command SHALL print one line per received signal (including signal `name`, monotonic `ts`, and selected `measurements` / `metadata` fields) within the same scheduler tick the signal was emitted

#### Scenario: `flz observe disable` disables the source via the service API
- **WHEN** the operator runs `flz observe disable beam_vm`
- **THEN** `Flezha.Service.Observability.disable_source/2` SHALL be called with a `caller_context` of `source: :cli`, the `:beam_vm` source SHALL transition to `status: :disabled`, and an audit `:event` SHALL be emitted on `:observability_service` per the foundation's audit-seam contract

#### Scenario: `--json` flag emits machine-parseable output
- **WHEN** the operator runs `flz observe sources --json`
- **THEN** the command SHALL print a single JSON document with an array of source records, each containing the same fields as the text output

#### Scenario: `flz observe tail` when PubSub sink is disabled
- **WHEN** the operator runs `flz observe tail beam_vm` against a Controller whose `:pubsub_sink_enabled` is `false`
- **THEN** the command SHALL exit with a non-zero status and a clear error message indicating the PubSub sink must be enabled, and SHALL NOT block
