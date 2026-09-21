# observability-otp-backend Specification

## Purpose

The observability-otp-backend capability provides the content side of the observability contract by shipping a library of built-in process-backed sources, each built on `Flezha.Observability.OtpBackend.Poller` — the macro that declares the foundation's `Flezha.Observability.Source` behaviour and registers the source through `Flezha.Observability.Registry.register_source/1`. It covers the BEAM VM (`:beam_vm`), the OS process via Linux `/proc` (`:host`), every active ProcessHub instance (`:processhub`), each configured transport (`:flezha_transport`), and the bounded-mailbox subsystem (`:flezha_bounded_mailbox`). The sources run under a dedicated `Flezha.Observability.OtpBackend.Supervisor` (`:one_for_one`) inserted into the foundation supervision tree after `DefaultSinks`, each honouring per-source cardinality budgets and an `:otp_backend_enabled` runtime flag for wholesale opt-out. The capability also ships the `flz observe` CLI verb family (`sources`, `sinks`, `tail`, `enable`, `disable`) that exposes the observability service API to operators with text and `--json` output modes, every verb routing through `Flezha.Service.CliManager.observability_*` wrappers with a `caller_context` of `source: :cli`.
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

The system SHALL provide a built-in process-backed source named
`:flezha_transport` that polls each configured transport (`:uds`, `:quic`) and
emits `:metric` signals for per-transport open connection count, lifetime bytes
in, and lifetime bytes out. The source SHALL produce one signal per active
transport per poll.

Every counter a runner's data heartbeat carries SHALL be mirrored into a
liveness slot, and every liveness slot SHALL declare the surface that reads it —
a metric this source emits, a convergence verdict that consumes it, a bridged
telemetry event, or an explicit internal-only marker carrying its reason. A
counter the runner pays for on the wire and the controller discards is invisible
to every operator surface, and no reader can tell a key that is absent from one
whose value is genuinely zero. The heartbeat's key set is a cross-SDK contract,
so a key added to it SHALL be given a slot and a surface in the same change that
adds it.

The slot layout SHALL be the single declaration from which this source's metric
rows are derived. A liveness slot SHALL NOT require a hand-written emit line of
its own: this source SHALL iterate the layout and emit one signal per slot whose
declared surface is a metric, so a slot cannot reach an exporter in one release
and not the next because a line was forgotten. Emitting a metric outside that
iteration SHALL be a defect, whether or not the metric is correct.

A heartbeat counter is global to the runner that sends it, and the runner sends
the same heartbeat on every secondary stream it holds, so the controller mirrors
one value into every channel row of that runner. A surface SHALL count such a
slot once per runner, never once per channel: the collapse SHALL live with the
slot layout and every reader SHALL use it, so that a metric's value does not
change with the size of a runner's stream pool. A slot that is genuinely per
channel — a frame count, a credit session, a send-gate hold — is summed across
channels as before. Whether a slot is summed, collapsed or reduced by maximum
SHALL be read from the slot's own declaration rather than inferred from its
name, and a slot holding an instant or a gauge SHALL declare the reduction that
makes its cross-runner value meaningful.

#### Scenario: Per-transport open-connection count
- **WHEN** a UDS transport has `4` open runner connections
- **THEN** a `:metric` signal on source `:flezha_transport` with `name: :open_connections`, `measurements: %{value: 4}`, and `metadata: %{transport: :uds}` SHALL appear on the bus

#### Scenario: Transport-absent case
- **WHEN** a transport is not enabled in `:flezha, :transports` config
- **THEN** the source SHALL NOT emit signals for that transport and SHALL NOT raise

#### Scenario: A rebind's discarded inputs reach an operator surface
- **WHEN** a runner's heartbeat carries `restore_discarded_inputs` after a rebind threw inputs away
- **THEN** the count SHALL be readable on a controller surface rather than dropped at the channel

#### Scenario: Armed timers that stop firing are visible
- **WHEN** a runner's heartbeats carry a climbing `timers_armed` and a frozen `timers_fired`
- **THEN** both counters SHALL be emitted by this source, so wedged promotion is distinguishable from an idle Operative

#### Scenario: Every heartbeat key has a slot
- **WHEN** the controller's mirrored key set is compared with the key set the shared core's heartbeat builds
- **THEN** the two SHALL name the same keys

#### Scenario: A slot declared without a surface is refused
- **WHEN** a liveness slot is added to the layout and declares no surface
- **THEN** the declaration test SHALL fail and name the slot, rather than the slot compiling and reaching nothing

#### Scenario: A metric row is derived, not written
- **WHEN** a liveness slot declaring a metric surface is added to the layout and this source is left unedited
- **THEN** the metric SHALL be emitted on the next poll, and no hand-written emit line SHALL exist for it

#### Scenario: A runner-global counter is counted once per runner
- **WHEN** one runner holds three secondary streams and its heartbeat carries `timers_fired: 1` on each
- **THEN** this source SHALL emit `timers_fired` with count 1, and the same value whether the runner holds one stream or eight

#### Scenario: A gauge is reduced as its row declares
- **WHEN** two runners report `inflight_oldest_ms` of `900` and `4_100` on the same poll
- **THEN** the emitted value SHALL be `4_100`, the reduction the row declares, and SHALL NOT be their sum

#### Scenario: Both readers collapse the same way
- **WHEN** the stall classifier and this source read one runner's channel rows
- **THEN** both SHALL reach the same per-runner value through one shared function, so a slot cannot be collapsed in one reader and summed in the other

#### Scenario: The derived rows stay inside the declared cardinality budget
- **WHEN** the metric rows are derived from the slot layout
- **THEN** their count SHALL be within this source's declared `cardinality_budget`, and a layout that would exceed it SHALL fail the declaration test rather than the budget check at runtime

### Requirement: Built-in bounded-mailbox source

The system SHALL provide a built-in process-backed source named `:flezha_bounded_mailbox` that polls the bounded-mailbox subsystem and emits `:metric` signals for per-Operative mailbox high-water marks and drop counters.

#### Scenario: Per-Operative mailbox high-water mark
- **WHEN** a Operative named `"orders"` in namespace `"default"` has a mailbox high-water mark of `120`
- **THEN** a `:metric` signal on source `:flezha_bounded_mailbox` with `name: :mailbox_high_water`, `measurements: %{value: 120}`, and `metadata: %{namespace: "default", operative: "orders"}` SHALL appear on the bus

#### Scenario: Drop counter exposed
- **WHEN** a Operative has dropped `5` messages due to bounded-mailbox overflow
- **THEN** a `:metric` signal with `name: :messages_dropped`, `measurements: %{count: 5}` SHALL appear on the bus with the same `namespace` / `operative` metadata shape

### Requirement: Cardinality budget enforcement on built-in sources

Every built-in source SHALL declare its cardinality budget at registration time and SHALL be designed so that the cluster-scale label cardinality (operatives × tasklets × cells × runners × hubs) fits within the declared budget. Where the natural cardinality exceeds the foundation's default `1_000` budget, the source SHALL raise its budget at registration time and document the rationale in the source module's `@moduledoc`.

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

### Requirement: Built-in sources share one poller template that declares the source contract

Every built-in process-backed source SHALL be built with `Flezha.Observability.OtpBackend.Poller`, which SHALL inject the whole poll lifecycle — `start_link/1`, an `init/1` that registers the source and arms the first tick, `handle_info(:poll, …)` with a catch-all, and `poll_interval_ms/0` — and SHALL declare `@behaviour Flezha.Observability.Source` on every module it builds, supplying an overridable `config_schema/0`. A source SHALL supply only what varies: `init_state/1`, `poll/1`, and `config_schema/0` when it has configuration to describe.

The template SHALL be named for what it is. It is poll plumbing, not a source: the modules it builds are the sources, and naming the template `Source` too leaves the subsystem with two different contracts under one name, which is what left the foundation's own `Source` behaviour with no declared implementers.

The list of built-in sources SHALL be one declaration, read by the supervisor that starts them and by the test that checks them against the tree, so a module cannot be started without declaring the contract and a module cannot declare the contract without being started.

The template's periodic half SHALL be a separate extension point from its polling half. A process that needs only a repeating tick — a ring-buffer sweep, an export flush, a log rotation, a retention pass — SHALL declare `tick_interval_ms/1` and `handle_tick/1` against that extension point rather than arming its own timer and writing its own re-arm clause. A deadline armed per event rather than on a period, such as an fsync deadline armed by the write that made data undurable, is not a tick and SHALL keep its own timer.

#### Scenario: Every built-in source declares the foundation's source contract
- **WHEN** each module in `Flezha.Observability.OtpBackend.Supervisor.source_modules/0` is loaded
- **THEN** its declared behaviours SHALL include `Flezha.Observability.Source`
- **AND** `config_schema/0` SHALL be exported and SHALL answer `%{}` where the source declares no configuration

#### Scenario: A source overrides only what varies
- **WHEN** a source is built with the template and defines neither `init_state/1` nor `poll/1`
- **THEN** it SHALL still compile, register at `init/1`, arm its tick at its configured interval, and answer each `:poll` without emitting

#### Scenario: A source in the list that declares nothing is refused
- **WHEN** a module that implements neither the source contract nor the template is added to the declared source list
- **THEN** the conformance test SHALL fail and name the module

#### Scenario: A periodic process re-arms through the declared tick
- **WHEN** a process declares `tick_interval_ms/1` and `handle_tick/1` and its tick fires
- **THEN** the next tick SHALL be armed by the extension point, with no `handle_info` clause or re-arm helper of the process's own

### Requirement: A stamp SHALL be collapsed within a runner to the latest reading a channel took

The per-runner collapse SHALL reduce a slot declared `kind: :instant` to the latest
stamp among the runner's channels that wrote one. A channel that never wrote a slot
holds `0`, and for a stamp that `0` is absent rather than a reading: it SHALL NOT
displace a stamp another channel of the same runner wrote, and a silence read from a
stamp that no channel of the runner wrote SHALL be reported as not measured rather than
as a value. The rule lives in the one per-runner collapse every reader of the channel
rows uses, beside the rules the built-in transport source's requirement states for a
mirrored runner fact and for a per-channel count.

#### Scenario: A stamp one channel wrote survives the collapse
- **GIVEN** a runner whose control channel wrote `last_control_frame_ms`, one of whose data channels wrote `last_send_ms`, and whose other channels wrote neither
- **WHEN** the runner's liveness is read from its collapsed row
- **THEN** `control_silent_ms` and `data_silent_ms` SHALL each be measured from the stamp its channel wrote, and neither SHALL read as not measured

#### Scenario: A stamp no channel wrote reads as not measured
- **GIVEN** a runner none of whose channels wrote `last_control_frame_ms` or `last_send_ms`
- **WHEN** the runner's liveness is read from its collapsed row
- **THEN** `control_silent_ms` and `data_silent_ms` SHALL both be absent, not zero

#### Scenario: Two stamps keep the later one
- **WHEN** two data channels of one runner wrote `last_send_ms` of `100` and `200`
- **THEN** the collapsed `last_send_ms` SHALL be `200`, neither their sum nor the earlier stamp

