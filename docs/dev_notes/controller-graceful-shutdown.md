# Graceful shutdown (`controller-graceful-shutdown`)

> **Superseded in part** by `fix-signal-handler-boot-race`: the signal-install
> description below is no longer accurate. SIGINT is never installed (ERTS
> refuses it) and signals reach `SignalHandler` through an `erl_signal_server`
> registration that replaces kernel's `erl_signal_handler`. See
> [fix-signal-handler-boot-race](fix-signal-handler-boot-race.md).

- **Modules**: `lib/flezha/shutdown/` — `Coordinator`, `SignalHandler`.
  Coordinator drives `quiesce → drain → finalise`; SignalHandler
  installs OS handlers via `:os.set_signal/2` so SIGTERM/SIGINT
  trigger graceful shutdown in every BEAM run mode.
- **Supervision tree**: SignalHandler is the **first** child in
  `Flezha.Application.start/2` (handlers installed before any
  service can observe a delivered signal); Coordinator is inserted
  after `Flezha.Service.HubManager` (drain queries the hub).
  `prep_stop/1` calls `Coordinator.shutdown(:prep_stop)`.
- **Hot path gating**: `Coordinator.shutting_down?/0` is an ETS-backed
  flag (no GenServer hop). Read by `OperativeManager.start_operative/2` and
  `FleetManager.connect_handler/4`.
- **Runtime config knobs**: `:graceful_shutdown_enabled` (default
  `true`, env `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED`),
  `:graceful_shutdown_timeout_ms` (default `60_000`, env
  `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`). **Pairing rule**: orchestrator
  grace period (`terminationGracePeriodSeconds` /
  `stop_grace_period`) MUST be `(timeout_ms / 1000) + 15s`. Bump in
  lockstep.
- **CLI**: `flz controller stop` (drives the coordinator over the
  CLI socket — works in release/mix/iex), `flz controller status`
  (surfaces `shutdown_in_progress`, `shutdown_phase`,
  `signal_handler_status`). Implemented in `cli/src/controller.rs` /
  `Flezha.Service.CliManager.controller_{stop,status}`.
- **Telemetry**: `[:flezha, :shutdown, :requested|:phase_started|:phase_completed|:drain_progress|:timeout|:complete]`.
- **Test config**: `config/test.exs` sets
  `:graceful_shutdown_enabled` to `false` so `Application.stop/1`
  teardown remains fast.
- **Detailed notes**: `docs/operations/graceful_shutdown.md`.

