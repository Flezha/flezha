# controller-graceful-shutdown Specification

## Purpose

The controller-graceful-shutdown capability defines how the Flezha controller (BEAM agent) reacts to user-initiated and orchestrator-initiated termination signals so that locally hosted Operators migrate cleanly to peers (or flush WAL state for restart survival), in-flight work is allowed to drain, and the BEAM exits within the orchestrator's grace window. The capability owns a three-phase shutdown coordinator (quiesce → drain → finalise), OS signal trapping wired into `Flezha.Application`, the `flz controller stop` CLI surface, the `Application.prep_stop/1` integration, the configuration knobs that let users tune or disable the mechanism, the structured-log + telemetry observability surface, and the docker-compose / kubernetes packaging that wires orchestrator grace periods to the BEAM-side timeout.
## Requirements
### Requirement: Controller SHALL run a three-phase graceful shutdown coordinated by `Flezha.Shutdown.Coordinator`

`Flezha.Shutdown.Coordinator` is a GenServer started by `Flezha.Application` after `Flezha.Service.HubManager`. On `Coordinator.shutdown(reason, opts)` it SHALL execute three phases in order: **quiesce → drain → finalise** — the `:node`-scope walk of the shutdown plan. Each phase SHALL emit one structured log line at start and one telemetry event under `[:flezha, :shutdown, :phase_started | :phase_completed]` with metadata `%{phase: :quiesce | :drain | :finalise, reason: atom()}`.

**Quiesce** SHALL set a process-tree-visible flag such that `Flezha.Service.OperatorManager.start_operator/2` returns `{:error, :shutting_down}` and `Flezha.Runner.FleetManager` rejects new runner connect attempts with a structured error, and SHALL declare `{:node_draining, deadline}` to the convergence loop. Already-running Operators and already-connected runners SHALL NOT be affected by this phase.

**Drain** SHALL migrate locally hosted Operators to peers **while they are still alive** by calling `ProcessHub.Service.Migration.drain(hub_id, timeout: remaining_budget)` for every running namespace's hub through the one seam `Flezha.Service.HubManager.drain_hub/2` — reachable from `lib/flezha/shutdown/` only — concurrently via a bounded operator `Task.async_stream` over the namespace list, where `remaining_budget` is derived downward from the configured shutdown timeout (timeout minus elapsed minus slack, floored at a small positive minimum; per hub minus the handover grace). `drain/2` redistributes each local child through the migration-consent gate (live HotSwap handover) and force-migrates whatever is still deferred at its deadline. The drain is the one remedy-shaped action outside the convergence primitives: an administration verb the loop is told about and never chooses. While the `{:node_draining, _}` fact stands the convergence Planner SHALL plan no op that places onto this node and no `publish_binding` for a local row, so the loop cannot fight the drain. Per-hub outcomes SHALL be folded as follows:

- `{:ok, %{migrated: m, forced: f}}` — counted into the drain outcome; `f > 0` SHALL be logged at WARNING but does not change the overall result.
- `{:error, :no_target_nodes}` — the solo-node path: log `[shutdown] drain skipped (no peers); state will be restored on next boot via on-disk WAL/snapshot` and proceed (local Operators terminate when the hub supervisor stops; WAL fsync flushes in finalise).
- `{:error, :partitioned | :locked | :draining}` — log at WARNING with the reason and degrade that hub to the no-peer behaviour; shutdown SHALL NOT wedge on a drain precondition.

Only **after** every hub's drain call has returned (or the shutdown timeout fired) SHALL the coordinator call `Flezha.Service.HubManager.stop_hub_manager()` to stop the hub supervisor. The coordinator SHALL emit `[:flezha, :shutdown, :drain_progress]` once per completed hub with `%{migrated: integer(), forced: integer(), remaining_hubs: integer(), elapsed_ms: integer()}`.

**Finalise** SHALL synchronously complete all pending I/O steps: flush `Flezha.State.FsyncBatcher`, close the `Flezha.Observability.AuditLog.Writer` (if running), and stop transports via `Flezha.Transport.Server`; it SHALL clear the `{:node_draining, _}` fact. Each step SHALL be wrapped in a shared `safe_finalise_step(label, fun, timeout_ms)` private helper that catches `:exit` and logs a warning with `label` rather than crashing the coordinator. After finalise returns, normal supervision-tree shutdown completes the rest.

The whole sequence SHALL be bounded by `:flezha, :graceful_shutdown_timeout_ms` (default `60_000`, env `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`), the one budget every lower scope derives from. On timeout the coordinator SHALL log `[shutdown] timeout phase=<current> still_local=<count>` at ERROR, emit `[:flezha, :shutdown, :timeout]`, advance to finalise, and return `{:partial, still_local}` to the caller. The coordinator SHALL NEVER block the BEAM beyond this bound — orchestrator-side grace period is the wall-clock backstop.

`Coordinator.shutdown/2` SHALL be idempotent: the first call drives the state machine; subsequent calls during `:quiescing | :draining | :finalising` SHALL log `[shutdown] already in progress (phase=<phase>), joining`, register the caller as a waiter, and return the same outcome to all waiters.

#### Scenario: Three-phase clean shutdown on a node with peers performs live handover

- **GIVEN** a controller hosting two Operators (`operator-a` and `operator-b`) with a peer node `controller-2` in the namespace hub
- **WHEN** a user calls `Flezha.Shutdown.Coordinator.shutdown(:cli, [])`
- **THEN** the coordinator logs `[shutdown] phase=quiesce`, transitions through `quiesce → drain → finalise`, calls `Migration.drain/2` on the namespace hub **before** stopping the hub supervisor, both Operators migrate to `controller-2` through live HotSwap handover (their processes were alive when handover state was prepared), a `:drain_progress` event reports `%{migrated: 2, forced: 0, ...}`, the hub supervisor stops only afterwards, and the call returns `:ok`

#### Scenario: Busy operator defers, then migrates within the drain window

- **GIVEN** a draining controller hosting a durable bounded Operator that is mid-iteration when drain begins
- **WHEN** the consent query runs
- **THEN** the Operator defers, remains alive and locally hosted, and after it suspends at its next committed iteration boundary the drain's consent retry migrates it to a peer — counted in `migrated`, not `forced`

#### Scenario: Deferred operator still busy at the drain deadline is force-migrated

- **GIVEN** a draining controller whose remaining budget expires while one Operator still defers
- **WHEN** `Migration.drain/2` reaches its `:timeout` deadline
- **THEN** the Operator is force-migrated through the best-effort handover path, the per-hub `:drain_progress` event reports it under `forced`, a WARNING is logged, and shutdown proceeds to finalise

#### Scenario: No-peer node skips migration wait

- **GIVEN** a controller running solo (no other nodes connected to its ProcessHub)
- **WHEN** `Coordinator.shutdown(:sigterm)` is invoked
- **THEN** `Migration.drain/2` returns `{:error, :no_target_nodes}`, the coordinator logs `[shutdown] drain skipped (no peers); state will be restored on next boot via on-disk WAL/snapshot`, stops the hub supervisor so locally hosted Operators terminate and their WAL fsyncs flush in finalise, and returns `:ok`

#### Scenario: Drain precondition failure degrades instead of wedging

- **GIVEN** a namespace hub whose drain call returns `{:error, :locked}` (pending distribution work at shutdown time)
- **WHEN** the drain phase folds that hub's outcome
- **THEN** the coordinator logs the reason at WARNING, treats that hub like the no-peer path, stops the hub supervisor, and completes shutdown within the configured timeout

#### Scenario: Drain timeout escalates to finalise with partial outcome

- **GIVEN** a controller hosting 50 Operators whose migration throughput is below the configured timeout budget (e.g. `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS=5000`)
- **WHEN** the coordinator's drain phase exceeds the overall shutdown timeout
- **THEN** the coordinator logs `[shutdown] timeout phase=drain still_local=<N>` at ERROR, emits `[:flezha, :shutdown, :timeout]` with `%{still_local: [operator_ids…]}`, runs the finalise phase, and returns `{:partial, [operator_ids…]}` to its caller

#### Scenario: Idempotent re-entry — second `shutdown/1` joins the in-flight one

- **GIVEN** a coordinator already in `:draining` from a prior `Coordinator.shutdown(:sigterm)`
- **WHEN** a user runs `flz controller stop` (which calls `Coordinator.shutdown(:cli)`) before the first call returns
- **THEN** the second call logs `[shutdown] already in progress (phase=drain), joining`, registers as a waiter, and receives the same final outcome (`:ok` or `{:partial, …}`) as the first caller

#### Scenario: Finalise step crash does not abort shutdown

- **GIVEN** `FsyncBatcher` has already terminated before the finalise phase runs
- **WHEN** the coordinator attempts to flush it
- **THEN** the `:exit` is caught by `safe_finalise_step`, a warning is logged, and the coordinator proceeds to the next finalise step without crashing

#### Scenario: A tick during a declared drain plans nothing local

- **GIVEN** a coordinator that has declared `{:node_draining, deadline}` and a misbound operator whose Overwatch is on this node
- **WHEN** the convergence loop ticks
- **THEN** it SHALL plan no `rebind_operator` placing onto this node and no `publish_binding` for a local row, and the drain's forced pass SHALL run as the last step inside the budget

#### Scenario: The drain fact clears with finalise

- **WHEN** the finalise phase completes
- **THEN** the loop SHALL no longer hold `{:node_draining, _}`, and a tick afterwards SHALL plan as on any node

### Requirement: SIGTERM SHALL trigger graceful shutdown in every BEAM run mode

`Flezha.Shutdown.SignalHandler` is a GenServer started **first** in `Flezha.Application.start/2`'s children list (before `Flezha.Service.EventManager`). Its `init/1` SHALL return `{:ok, state, {:continue, :install_signal_handler}}`.

**SIGTERM.** `handle_continue/2` SHALL call `:os.set_signal(:sigterm, :handle)` as an explicit, idempotent assertion of the platform default, and SHALL register itself with the `erl_signal_server` `gen_event` so signal notifications reach it. Registration SHALL replace kernel's `erl_signal_handler`, because that handler calls `init:stop()` on `sigterm` independently: with both registered the shutdown is driven by `Flezha.Application.prep_stop/1` and recorded as `reason: :prep_stop`, which contradicts the dispatch contract below.

The registered `gen_event` handler SHALL do nothing but forward the signal to the `SignalHandler` GenServer, so it cannot raise and be removed — a removed handler leaves the node unable to react to SIGTERM at all. It SHALL be registered as a supervised handler so that a `SignalHandler` crash removes it rather than leaving a handler forwarding to a dead process, and on `terminate/2` it SHALL re-add kernel's `erl_signal_handler`, so any removal degrades to `init:stop()` rather than to an unresponsive node.

Because replacing kernel's `erl_signal_handler` also removes its handling of the other signals it owned, `SignalHandler` SHALL preserve those semantics: `sigquit` SHALL halt the BEAM and `sigusr1` SHALL halt it with `"Received SIGUSR1"`, matching `erl_signal_handler` exactly. Only SIGTERM's disposition is deliberately changed.

**SIGINT.** `:os.set_signal/2` SHALL NOT be called for `sigint`. ERTS reserves SIGINT for the break handler and does not accept it — it is absent from the BIF's accepted signal list, and the call raises `badarg` in every run mode and under every break-handler flag. SIGINT therefore never reaches `erl_signal_server`, and SHALL NOT be treated as an install failure. A release node runs with the break handler disabled (`controller-packaging`), so SIGINT ends it at once with no drain; under `mix run --no-halt` and `iex -S mix` ERTS answers it with the break handler.

The SignalHandler SHALL receive forwarded signals and dispatch as follows:

- **First** signal: log `[shutdown] received <signal>` and `Task.start` a call to `Flezha.Shutdown.Coordinator.shutdown(reason)` followed by `:init.stop()`. The `[:flezha, :shutdown, :requested]` event carrying `%{reason: :sigterm}` SHALL be emitted once, by the Coordinator from the reason it is given — the SignalHandler SHALL NOT emit its own, which would double-report every signalled shutdown. The Task pattern keeps the SignalHandler GenServer responsive to the second/third signals. The trailing `:init.stop()` re-enters `Flezha.Application.prep_stop/1`, whose `Coordinator.shutdown(:prep_stop)` SHALL be absorbed by the coordinator's existing `phase: :done` short-circuit rather than running a second drain.
- **Second** signal during shutdown: log `[shutdown] already in progress, ignoring <signal>`. NOT re-dispatched.
- **Third** signal: log `[shutdown] forced halt requested via third <signal>` at WARN, call `:erlang.halt(143)`. User-controlled escape hatch.

This wiring SHALL apply equally to release mode (`bin/flzvm start`), `mix run --no-halt`, `iex -S mix`, and any escript packaging — the SignalHandler is in the application's supervision tree, which exists in every mode.

When `:flezha, :graceful_shutdown_enabled` is `false` (env `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false`), `SignalHandler` SHALL NOT call `:os.set_signal/2` and SHALL NOT register with `erl_signal_server`. Kernel's `erl_signal_handler` therefore remains the owner and today's behaviour is unchanged.

If the SIGTERM install or the `erl_signal_server` registration fails, the SignalHandler SHALL catch the exception, log `[shutdown] os.set_signal unavailable: <reason>; falling back to release-mode SIGTERM` at WARN, latch a degraded state, and continue running so application start does not fail. Because the impossible `sigint` call is gone, this state is reached only when the platform genuinely refuses.

`Flezha.Shutdown.Coordinator.status/0` SHALL surface the handler's state under `signal_handler_status: :ok | :degraded`. It SHALL read that value from `SignalHandler` on demand rather than caching a value pushed to it: `SignalHandler` is started first and terminated last, so a push at install time is delivered before the Coordinator exists and is silently lost.

#### Scenario: Handlers are installed on every boot

- **GIVEN** a controller booting in any supported run mode with graceful shutdown enabled
- **WHEN** `SignalHandler`'s `handle_continue/2` runs
- **THEN** `[shutdown] OS signal handlers installed (SIGTERM)` is logged, no `os.set_signal unavailable` WARN is logged, `SignalHandler` is a registered `erl_signal_server` handler, and `Flezha.Shutdown.Coordinator.status/0` reports `signal_handler_status: :ok`

#### Scenario: SIGTERM in `iex -S mix` triggers graceful shutdown

- **GIVEN** a controller running under `iex -S mix` with one Operator locally hosted and a peer node available
- **WHEN** the OS sends SIGTERM to the BEAM process
- **THEN** the SignalHandler logs `[shutdown] received sigterm`, exactly one `[:flezha, :shutdown, :requested]` event is emitted and it carries `%{reason: :sigterm}`, the coordinator runs quiesce → drain → finalise exactly once, the Operator migrates to the peer, and the BEAM exits with status 0

#### Scenario: The trailing `:init.stop()` does not run a second drain

- **GIVEN** a first SIGTERM whose `Coordinator.shutdown(:sigterm)` has completed
- **WHEN** the same Task calls `:init.stop()` and `Flezha.Application.prep_stop/1` calls `Coordinator.shutdown(:prep_stop)`
- **THEN** the coordinator is in `phase: :done`, returns its recorded outcome immediately, and emits no second `[:flezha, :shutdown, :requested]` event and no second phase sequence

#### Scenario: Third SIGTERM during a wedged drain force-halts the BEAM

- **GIVEN** a coordinator stuck in drain (e.g. peer migration not progressing)
- **WHEN** a user sends a second SIGTERM (logged & ignored) followed by a third SIGTERM
- **THEN** the SignalHandler logs `[shutdown] forced halt requested via third sigterm` at WARN and calls `:erlang.halt(143)`; the BEAM exits with status 143 immediately

#### Scenario: SIGINT is left to ERTS

- **GIVEN** a controller booting with graceful shutdown enabled
- **WHEN** `SignalHandler` installs its handlers
- **THEN** `:os.set_signal/2` is never called with `sigint`, no `badarg` is raised, the degraded state is not latched, and SIGINT stays with ERTS, which ends a release node on it

#### Scenario: SIGTERM with kill-switch off behaves like today (immediate BEAM exit)

- **GIVEN** the controller is started with `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false`
- **WHEN** the OS sends SIGTERM
- **THEN** the SignalHandler neither calls `:os.set_signal/2` nor registers with `erl_signal_server`, kernel's `erl_signal_handler` remains the owner, and the BEAM terminates on SIGTERM without the SignalHandler invoking the coordinator (today's behaviour, modulo `prep_stop/1` running for releases)

#### Scenario: A genuinely unsupported platform degrades gracefully

- **GIVEN** a host OS where the SIGTERM install or the `erl_signal_server` registration raises (e.g. an unsupported sandbox)
- **WHEN** the SignalHandler `handle_continue/2` runs
- **THEN** the exception is caught, `[shutdown] os.set_signal unavailable: <reason>; falling back to release-mode SIGTERM` is logged at WARN, the SignalHandler continues running, application start completes, and `flz controller status` reports `signal_handler_status: :degraded`

### Requirement: `Application.prep_stop/1` SHALL await the shutdown coordinator

`Flezha.Application.prep_stop/1` SHALL call `Flezha.Shutdown.Coordinator.shutdown(:prep_stop)` and SHALL block until the coordinator returns or its bounded timeout elapses. The coordinator's bounded timeout (`graceful_shutdown_timeout_ms`) is itself bounded by the OTP shutdown timeout for `Flezha.Application` — users MUST size both knobs together (documented in `docs/operations/graceful_shutdown.md`). The previous synchronous `Flezha.Service.HubManager.stop_hub_manager()` call SHALL be removed from `prep_stop/1` and SHALL be issued from inside the coordinator's drain phase instead.

When `:flezha, :graceful_shutdown_enabled` is `false`, `prep_stop/1` SHALL fall back to the legacy direct call `Flezha.Service.HubManager.stop_hub_manager()` (today's behaviour).

#### Scenario: Release-mode `bin/flezha stop` runs the coordinator

- **GIVEN** a controller running as a release with two locally hosted Operators and an available peer
- **WHEN** a user runs `bin/flezha stop` (which issues `:init.stop()` to the running BEAM)
- **THEN** `Application.prep_stop/1` is called, which calls `Coordinator.shutdown(:prep_stop)`; the coordinator drains both Operators via HotSwap; `prep_stop/1` returns and the BEAM completes its supervision-tree shutdown — exit status 0 with both Operators running on the peer

#### Scenario: prep_stop honours the kill-switch fall-through

- **GIVEN** `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false`
- **WHEN** the BEAM receives `:init.stop()`
- **THEN** `prep_stop/1` calls the legacy `Flezha.Service.HubManager.stop_hub_manager()` directly (no coordinator) and returns immediately — equivalent to today's behaviour

### Requirement: `flz controller stop` SHALL run the coordinator over the CLI socket in every controller run mode

`cli/src/controller.rs` `handle_stop/1` SHALL replace the `bin/flezha stop` shell-out with an RPC over the existing `runner_control_channel.sock` to a new `Flezha.Service.CliManager.controller_stop/1` handler that calls `Flezha.Shutdown.Coordinator.shutdown(:cli, opts)`. The CLI SHALL stream coordinator progress (the `[:flezha, :shutdown, :*]` telemetry events, formatted) to the user's terminal until the coordinator returns.

Once the coordinator returns and its outcome line is printed, the handler SHALL stop the node with `:init.stop()`, whatever the outcome, so the command ends the controller rather than leaving a drained node running. `--no-wait` stops the node the same way once the drain it dispatched returns.

The CLI SHALL accept the following flags:

- `--timeout-ms <N>`: overrides `:flezha, :graceful_shutdown_timeout_ms` for this invocation only.
- `--no-wait`: issues the shutdown RPC and exits immediately without waiting for completion.
- `--force`: skips quiesce + drain entirely; equivalent to running `:init.stop()` with `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false`. Used as a user escape hatch.

Exit codes:

- `0` — clean shutdown (all Operators migrated to peers, or no peers and local termination).
- `1` — partial drain (timeout elapsed; some Operators still local at finalise).
- `2` — already shutting down (idempotent re-issue; the in-flight shutdown completed cleanly OR with a partial outcome — exit code reflects that outcome).
- `3` — controller not reachable on the CLI socket.

The RPC SHALL work whether the controller is running as a release, under `mix run --no-halt`, or under `iex -S mix` — it depends only on the CLI socket existing.

#### Scenario: `flz controller stop` succeeds in release mode

- **GIVEN** a release-mode controller hosting one Operator with an available peer
- **WHEN** a user runs `flz controller stop`
- **THEN** the CLI streams drain progress (`[:flezha, :shutdown, :drain_progress]` events formatted as `drain: migrated=1 remaining=0 elapsed_ms=…`), prints `[shutdown] complete duration_ms=… outcome=clean`, exits with status `0`, and the controller's BEAM exits

#### Scenario: `flz controller stop` works in `iex -S mix`

- **GIVEN** a controller running under `iex -S mix` with one Operator and an available peer
- **WHEN** a user runs `flz controller stop` from a separate shell
- **THEN** the CLI sees the iex BEAM via the CLI socket, drives the coordinator through quiesce → drain → finalise, and exits with status `0`. The iex session shows `[shutdown] complete` and the BEAM exits, returning the user to the host shell

#### Scenario: `flz controller stop --force` skips drain

- **GIVEN** a controller in any state
- **WHEN** the user runs `flz controller stop --force`
- **THEN** the coordinator logs `[shutdown] forced (skipping drain)`, runs only finalise, and exits — no migration is attempted; equivalent to user-driven `:erlang.halt(0)` after WAL flush

#### Scenario: `flz controller stop` against an unreachable controller exits 3

- **GIVEN** the controller socket at `${FLEZHA_SOCKET_FILES_PATH}/runner_control_channel.sock` does not exist or is not connectable
- **WHEN** a user runs `flz controller stop`
- **THEN** the CLI prints `error: controller not reachable on <socket_path>` to stderr and exits with status `3`

### Requirement: Configuration knobs SHALL surface graceful-shutdown behaviour to users

The controller SHALL expose two configuration knobs, readable from both `config/runtime.exs` (Elixir) and the docker entrypoint (shell):

- `:flezha, :graceful_shutdown_enabled` — boolean, default `true`. Env: `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` (truthy values: `1, true, yes, on`; falsy: `0, false, no, off`; case-insensitive).
- `:flezha, :graceful_shutdown_timeout_ms` — positive integer, default `60_000`. Env: `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`. The default SHALL be declared once, as the `graceful_shutdown_timeout_ms` row of `Flezha.Config.Durations`; `runtime.exs` translates the env name and carries no default of its own.

`kubernetes/flezha-configmap.yaml` SHALL declare the timeout knob so that the BEAM and the entrypoint shell read it from one place. The DaemonSet's `terminationGracePeriodSeconds` and the docker-compose `stop_grace_period` SHALL be set 15 seconds longer than `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS / 1000` to leave headroom for finalise and supervision-tree shutdown — users changing one MUST change the others. The pairing SHALL be a derived row of the same table (`orchestrator_grace_ms`, the timeout plus 15 s of headroom) and a test SHALL read the DaemonSet and both compose files and fail when either departs from it. The pairing is documented in `docs/operations/graceful_shutdown.md`.

#### Scenario: Default boot has graceful shutdown enabled

- **GIVEN** a fresh controller boot with no env overrides
- **WHEN** the SignalHandler initialises
- **THEN** `:os.set_signal(:sigterm, :handle)` is called and `flz controller status` reports `graceful_shutdown_enabled: true, graceful_shutdown_timeout_ms: 60000`

#### Scenario: User overrides timeout via env

- **GIVEN** the controller is started with `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS=120000`
- **WHEN** `Coordinator.shutdown/2` is invoked without an explicit timeout
- **THEN** the coordinator's drain phase uses 120 000 ms as the budget; `flz controller status` reports `graceful_shutdown_timeout_ms: 120000`

#### Scenario: Falsy env value disables the mechanism

- **GIVEN** the controller is started with `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=off`
- **WHEN** the application boots
- **THEN** `SignalHandler` does not install OS signal handlers, `prep_stop/1` falls back to the legacy `HubManager.stop_hub_manager/0` call, and `flz controller status` reports `graceful_shutdown_enabled: false`

#### Scenario: A manifest that breaks the pairing fails a test

- **GIVEN** `graceful_shutdown_timeout_ms` at its default of `60_000`
- **WHEN** `kubernetes/flezha-daemonset.yaml`'s `terminationGracePeriodSeconds` or a compose file's `stop_grace_period` is set to anything but 75 seconds
- **THEN** the pairing test SHALL fail naming the file and the value the derived row expects

### Requirement: Graceful shutdown SHALL be observable via structured logs and telemetry

The controller SHALL emit the following telemetry events under `[:flezha, :shutdown, :*]`:

- `:requested` — `%{reason: :sigterm | :sigint | :prep_stop | :cli | :force, timestamp_ms: integer()}`. Emitted at the entry of `Coordinator.shutdown/2`.
- `:phase_started` — `%{phase: :quiesce | :drain | :finalise}`. One per phase.
- `:phase_completed` — `%{phase: :quiesce | :drain | :finalise, duration_ms: integer()}`. One per phase.
- `:drain_progress` — `%{migrated: integer(), remaining: integer(), elapsed_ms: integer()}`. Emitted on each 500 ms drain poll.
- `:timeout` — `%{phase: :quiesce | :drain | :finalise, still_local: [operator_ids…]}`. Emitted only when the bounded timeout elapses.
- `:complete` — `%{duration_ms: integer(), outcome: :clean | :partial | :forced, reason: atom()}`. Emitted at coordinator return.

Structured log lines (INFO unless noted) SHALL accompany each event:

- `[shutdown] received <signal>` (signal handler)
- `[shutdown] phase=quiesce | phase=drain locally_hosted=N | phase=finalise`
- `[shutdown] drain_progress migrated=M remaining=R elapsed_ms=E` (every 1 s during drain)
- `[shutdown] timeout phase=<phase> still_local=<count>` (ERROR)
- `[shutdown] complete duration_ms=T outcome=<outcome>`

`flz controller status` (existing CLI command, extended) SHALL include three new fields: `shutdown_in_progress: bool`, `shutdown_phase: :quiesce | :drain | :finalise | nil`, and `signal_handler_status: :ok | :degraded`. External readiness/liveness probes MAY consult these fields to drain traffic before the BEAM stops accepting work.

#### Scenario: Telemetry consumer observes a complete shutdown lifecycle

- **GIVEN** a telemetry handler attached to `[:flezha, :shutdown, :_]`
- **WHEN** `Coordinator.shutdown(:cli)` runs to completion on a 2-Operator node with a peer
- **THEN** the handler observes events in order: `:requested`, `:phase_started{quiesce}`, `:phase_completed{quiesce}`, `:phase_started{drain}`, one or more `:drain_progress`, `:phase_completed{drain}`, `:phase_started{finalise}`, `:phase_completed{finalise}`, `:complete{outcome=clean}`

#### Scenario: `flz controller status` surfaces shutdown state mid-drain

- **GIVEN** a controller in `:draining` from an in-flight `Coordinator.shutdown/2`
- **WHEN** a user runs `flz controller status` from a separate shell
- **THEN** the CLI returns `shutdown_in_progress: true, shutdown_phase: drain, signal_handler_status: ok` so external probes can stop sending new work

#### Scenario: Forced shutdown emits :complete with outcome=forced

- **GIVEN** a user runs `flz controller stop --force`
- **WHEN** the coordinator finishes its abbreviated finalise-only path
- **THEN** the `:complete` telemetry event has `%{outcome: :forced, reason: :force}`

### Requirement: `kubernetes/flezha-daemonset.yaml` SHALL set a preStop hook and termination grace period

The DaemonSet's controller container SHALL declare:

- `lifecycle.preStop.exec.command: ["/flezha/bin/flz", "controller", "stop", "--timeout-ms", "$(FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS)"]`. The preStop runs synchronously before k8s sends SIGTERM and is the **primary** path for k8s pod termination.
- `terminationGracePeriodSeconds: 75` (the default — `graceful_shutdown_timeout_ms / 1000` + 15 s headroom for finalise + supervision-tree shutdown).
- `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` SHALL be sourced from `kubernetes/flezha-configmap.yaml` so a single change adjusts both BEAM and entrypoint behaviour.

#### Scenario: K8s pod stop runs preStop, then SIGTERM, exits within grace

- **GIVEN** a deployed DaemonSet with a controller pod hosting one Operator and a peer pod available
- **WHEN** the user runs `kubectl delete pod <pod>`
- **THEN** k8s runs `flz controller stop --timeout-ms 60000` via the preStop hook (which drives the coordinator), then sends SIGTERM (which the SignalHandler observes as a second-signal during shutdown and ignores), then waits up to 75 s; the pod exits cleanly within grace, the Operator is running on the peer, and the new pod (when scheduled) finds no orphaned local state

#### Scenario: PreStop timeout knob honoured by configmap

- **GIVEN** a user updates `kubernetes/flezha-configmap.yaml` to set `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS=120000`
- **WHEN** the DaemonSet is reapplied
- **THEN** the preStop command resolves to `flz controller stop --timeout-ms 120000`; the user MUST also bump `terminationGracePeriodSeconds` to `135` to keep the 15 s headroom (documented invariant)

### Requirement: `docker/{prod,dev}/docker-compose.yml` SHALL set explicit stop signal and grace period

Every controller service entry in `docker/prod/docker-compose.yml`, `docker/dev/docker-compose.yml`, `docker/dev/docker-compose.healing.yml`, and `docker/dev/docker-compose.testing.yml` SHALL declare:

- `stop_signal: SIGTERM` (explicit; equals docker's default — documents intent)
- `stop_grace_period: 75s` (matches the k8s default; docker-compose default is 10 s, far too short)

Users tuning `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` away from `60000` SHALL update `stop_grace_period` to `(FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS / 1000) + 15s`. Documented in `docs/operations/graceful_shutdown.md`.

#### Scenario: `docker compose stop` allows graceful shutdown to complete

- **GIVEN** a `docker compose up` of `docker/prod/docker-compose.yml` with a controller hosting an Operator and a peer service available
- **WHEN** the user runs `docker compose stop`
- **THEN** docker sends SIGTERM, the controller's SignalHandler picks it up, the coordinator drains in ≤ 60 s; docker's 75 s grace period elapses without a SIGKILL; the controller exits cleanly with the Operator migrated to the peer

### Requirement: Teardown at every scope SHALL run one declared, ordered step list under one budget

Every teardown SHALL be one walk, `Flezha.Shutdown.run(scope, target, budget)` — the
user's operator stop, the supervisor's terminate, the namespace stop and the node
drain alike — folding the discovered `Flezha.Shutdown.Step` implementations of that scope
(`:process | :operator | :namespace | :node`) in their declared order, each once, each
handed the budget still remaining, stopping at the budget and naming the steps not run in
its return. A step SHALL run in the CALLER's process, because a teardown releases what the
caller owns — the links its plugin table holds, the ETS tables its marks live in, the
routes that name `self()` — and SHALL return inside the budget it is handed; every
cross-process call inside one is bounded as `Flezha.SDK.SafeCall.bounded/3` requires. The walk
SHALL re-check the deadline between steps, and a step that raises, exits or answers
`{:error, _}` SHALL halt it. The steps are declared by the resource they tear down — the migrate task first,
the class registry, the control unit, the plugin table (one step: the on-terminate hooks,
the teardown and the manager's unregister), the stream-channel flush (flush, never count,
before deregister), the state directory (one guard: `.tmp`/`.rewriting` or a completion
record), the quota ledger, the hub — and a new resource with a teardown is one new step,
editing no stop path. The budget is the node's `:graceful_shutdown_timeout_ms` at `:node`
and is derived downward from it; the `:operator` stop has one attribute.
`Flezha.Shutdown.quiesced?/2` SHALL answer per scope and target, so a one-namespace stop
does not read as the node not shutting down. The offline `flz reset` SHALL share the
roots declaration the `:node` steps print and none of the mechanism.

#### Scenario: A user stop tears each resource down once
- **WHEN** a user stops an operator
- **THEN** the `:operator` walk runs, the plugin table is torn down exactly once, the state directory is deleted under the one guard, and the quota ledger is released, in the declared order

#### Scenario: A step that spends the budget leaves the rest unrun, and they are named
- **GIVEN** a step that returns only once the remaining budget is spent
- **WHEN** the walk re-checks the deadline
- **THEN** it SHALL run no further step, and the return SHALL name the ones it did not run

#### Scenario: A step runs where the resource it releases is owned
- **WHEN** a step unlinks a plugin's owned pid, writes the mark table, or seeds a route naming `self()`
- **THEN** it SHALL run in the process the walk was called from, so the resource it releases is the one the caller holds

#### Scenario: A teardown outside the steps fails the seam test
- **WHEN** a state directory is removed, or `Extension.Table.teardown_all/1` is called, from a stop path outside the step implementations
- **THEN** the seam test SHALL fail naming the module, function and arity

#### Scenario: Every step is discovered and ordered
- **WHEN** the step implementations are enumerated
- **THEN** each SHALL have a unique order within its scope, and the four stop entry points SHALL reach `Shutdown.run/3`

#### Scenario: A namespace stop reads as quiesced for that namespace only
- **GIVEN** one namespace being stopped on a node that is not shutting down
- **WHEN** a runner connects to another namespace, or an operator is started there
- **THEN** it SHALL be admitted; `quiesced?(:namespace, ns)` SHALL be true for the stopping namespace and `quiesced?(:node, _)` false

