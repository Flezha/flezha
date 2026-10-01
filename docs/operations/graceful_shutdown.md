# Graceful controller shutdown

For users running a Controller: the three shutdown phases, how busy Operators defer
their migration, the knobs and the orchestrator grace period they pair with,
`flz controller stop`, telemetry and status, the third-SIGTERM override and the disable flag.

Graceful shutdown is on by default. `Flezha.Shutdown.Coordinator` runs
**quiesce → drain → finalise** on SIGTERM, `Application.prep_stop/1` or
`flz controller stop`. SIGINT is not among them: ERTS reserves it for the break
handler and `:os.set_signal/2` refuses it, so Ctrl-C keeps its default meaning. Every path converges on the idempotent `Coordinator.shutdown/2`: a
second caller during an in-flight shutdown joins as a waiter and receives the same outcome.

## Triggers by run mode

| Run mode                          | Trigger                                    | Path                                                                                |
| --------------------------------- | ------------------------------------------ | ----------------------------------------------------------------------------------- |
| Any run mode                      | OS SIGTERM                                 | `Flezha.Shutdown.SignalHandler` (via `erl_signal_server`) → coordinator → `:init.stop()` |
| Release (`bin/flzvm start`)       | `bin/flzvm stop`                           | `:init.stop()` → `prep_stop/1` → coordinator                                        |
| Any of the above                  | `flz controller stop`                      | RPC into `Flezha.Service.CliManager.controller_stop/1` → coordinator                |
| `docker stop` / compose stop      | SIGTERM via tini, then the entrypoint trap | Trap runs `flz controller stop --timeout-ms ${FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS}` |
| `kubectl delete pod`              | `lifecycle.preStop` runs first             | Same `flz controller stop`, then SIGTERM (already shutting down)                    |

Every one of these needs the BEAM to *receive* SIGTERM. A container that
starts as root and drops privileges must do so with a mechanism that
replaces the process image (`exec setpriv`), so the BEAM stays PID 1.
Wrapping it in `su` breaks the drain outright: util-linux `su` answers
SIGTERM by force-killing its child about two seconds later, regardless of
`stop_grace_period` or `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`. The dev and
chaos harnesses go through `scripts/harness-boot.sh`, which does this
correctly on both the privileged and unprivileged paths.

## Phases

1. **Quiesce** marks the node quiescing (`Flezha.Shutdown.quiesced?/2`, ETS-backed) and declares `{:node_draining, deadline}` to the convergence loop, which then plans nothing onto this node:
   `Flezha.Service.OperatorManager.start_operator/3` answers `{:error, :shutting_down}` and
   `Flezha.Runner.FleetManager` refuses new runner connections. Running Operators and
   connected runners are not touched.
2. **Drain** migrates every locally hosted Operator to a peer **while it is still alive**:
   one `Flezha.Service.HubManager.drain_hub/2` per running namespace hub, concurrently, each
   calling `ProcessHub.Service.Migration.drain/2` — children move through the migration-consent
   gate (live `HotSwap{handover: true}` handover) and whatever still defers at the deadline is
   force-migrated. **Locally empty is the completion signal**: `drain/2` returns when the
   deferred list is empty, but HotSwap stops a local child only after the remote start
   succeeded, so `drain_hub/2` also waits until the hub has no local children. 2 s of the
   budget is reserved for the deadline force pass and the coordinator keeps a further 1 s for
   finalise. Per hub, `{:ok, %{migrated, forced}}` feeds the summary (`forced > 0` logs a
   WARNING); `{:error, :no_target_nodes}` is the solo-node path (no migration; the next boot
   restores from the on-disk WAL via [auto-restore](auto_restore_on_boot.md)); any other
   precondition error (`:partitioned | :locked | :draining`, a crashed drain task) degrades to
   the same path with a WARNING. The hub supervisor stops only after the drains return.
3. **Finalise** closes the audit-log writer, flushes `Flezha.State.FsyncBatcher`
   (`:flush_all`) and stops the transports (`Flezha.Transport.Server.stop_all/0`); each step is
   bounded to 5 s and a failure is logged, never fatal.

The sequence is bounded by `:flezha, :graceful_shutdown_timeout_ms`. On expiry: ERROR log,
`[:flezha, :shutdown, :timeout]`, the hub is stopped, finalise runs, and the call returns
`{:partial, [operator_ids]}`. Shutdown never wedges on a drain precondition.

## Migration consent

Every ProcessHub redistribution (node join, convergence rebind, the drain above) asks each
Overwatch for consent (`ProcessHub.Strategy.Migration.MigrationConsent`); it answers `:defer`
while a move would tear mid-critical work:

| Busy signal                         | Safe barrier (Overwatch calls `Migration.signal_migration_ready/1`) |
| ----------------------------------- | ------------------------------------------------------------ |
| Late-input correction pending       | The correction transaction completes                         |
| Durable steps operator not suspended      | Suspension at a committed iteration boundary                 |
| Effectively-once outbox not flushed | Consent is re-queried on the next retry tick                 |

A deferring child is parked in the hub's deferred list, re-queried every `retry_interval_ms`
and force-migrated (best-effort handover, journal/WAL recovery on the target) after
`max_defer_ms`. No reply within `consent_timeout_ms` counts as `:defer`, which is how an
Overwatch inside a synchronous correction transaction defers without special handling.

| Knob                                      | Env                                   | Default                                                                                   |
| ----------------------------------------- | ------------------------------------- | ----------------------------------------------------------------------------------------- |
| `:migration_consent, :consent_timeout_ms` | `FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS` | `5000`                                                                                    |
| `:migration_consent, :retry_interval_ms`  | `FLEZHA_MIGRATION_RETRY_INTERVAL_MS`  | `2000`                                                                                    |
| `:migration_consent, :max_defer_ms`       | `FLEZHA_MIGRATION_MAX_DEFER_MS`       | `graceful_shutdown_timeout_ms` — a deferring Operator never outlives the shutdown window |

## Configuration

| Knob                                     | Env                                   | Default | Notes                                                            |
| ---------------------------------------- | ------------------------------------- | ------- | ---------------------------------------------------------------- |
| `:flezha, :graceful_shutdown_enabled`    | `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED`    | `true`  | True for `1`, `true`, `yes`, `on` (case-insensitive); else false |
| `:flezha, :graceful_shutdown_timeout_ms` | `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` | `60000` | Bounds the whole sequence                                        |

**Pairing rule.** The orchestrator's grace period MUST be
`FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS / 1000 + 15 s` (60000 ms pairs with 75 s, 120000 ms with
135 s) — headroom for finalise and container teardown. Bump both in lockstep or the pod is
SIGKILLed mid-drain.

| Layer               | File                                                                            | Setting                               |
| ------------------- | ------------------------------------------------------------------------------- | ------------------------------------- |
| Coordinator timeout | `kubernetes/flezha-configmap.yaml`                                              | `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` |
| Kubernetes pod      | `kubernetes/flezha-daemonset.yaml`                                              | `terminationGracePeriodSeconds`       |
| docker compose      | `docker/{prod,dev}/docker-compose.yml`, `docker/dev/docker-compose.healing.yml` | `stop_grace_period`                   |

## `flz controller stop`

Drives the coordinator over the CLI socket in every run mode, streaming drain progress to
stderr; needs the `controller.stop` permission. `--timeout-ms <N>` overrides the configured
drain timeout for this invocation; `--no-wait` dispatches the shutdown and exits 0 without
waiting; `--force` skips quiesce and drain and runs finalise only.

| Exit | Meaning                                                                                                   |
| ---- | --------------------------------------------------------------------------------------------------------- |
| 0    | Clean shutdown (every Operator migrated to a peer, or `--force` finalised cleanly); a stop issued while one is in flight follows it and exits with its outcome |
| 3    | `controller.unreachable`: the controller is not running, or the socket path is wrong                      |
| 4    | `authz.*`: refused for want of a token or a role                                                          |
| 7    | `shutdown.partial_drain`: the timeout elapsed with Operators still local at finalise                      |

Every code is listed in [errors.md](../errors.md).

`flz controller status` reports `shutdown_in_progress`, `shutdown_phase`
(`:quiesce | :drain | :finalise | nil`), `signal_handler_status` (`:degraded` when the
platform refused the SIGTERM install or the `erl_signal_server` registration — read live
from the handler, not cached), `graceful_shutdown_enabled` and
`graceful_shutdown_timeout_ms`; readiness probes can poll it to drain traffic early.

## Telemetry

Under `[:flezha, :shutdown, …]`; each phase boundary also logs a `[shutdown] …` line.

| Event              | Measurements                                      | Metadata                                                                     |
| ------------------ | ------------------------------------------------- | ---------------------------------------------------------------------------- |
| `:requested`       | `%{}`                                             | `%{reason, timestamp_ms}` — emitted once, at the entry of `Coordinator.shutdown/2` |
| `:phase_started`   | `%{}`                                             | `%{phase: :quiesce \| :drain \| :finalise}`                                  |
| `:phase_completed` | `%{duration_ms}`                                  | `%{phase}`                                                                   |
| `:drain_progress`  | `%{migrated, forced, remaining_hubs, elapsed_ms}` | `%{namespace}`, one per completed hub                                        |
| `:timeout`         | `%{}`                                             | `%{phase, still_local: [operator_ids]}`                                     |
| `:complete`        | `%{duration_ms}`                                  | `%{outcome: :clean \| :partial \| :forced, reason}`                          |

## Who owns SIGTERM

`SignalHandler` registers a forwarding handler on `erl_signal_server` and **removes
kernel's `erl_signal_handler`**. The two cannot coexist: kernel's calls `init:stop()` on
its own, so with both registered the shutdown is driven by `prep_stop/1` and recorded as
`reason: :prep_stop` about a millisecond before the handler can dispatch.

The registration is added before kernel's is removed, so a failed install leaves kernel's
handler owning the signal rather than leaving the node with neither. If the forwarding
handler is ever removed (a `SignalHandler` crash removes it, since it is supervised), it
re-adds kernel's handler on the way out — the node degrades to a plain `init:stop()`
drain, never to one that ignores SIGTERM. Taking ownership also means `sigquit` and
`sigusr1`, which kernel's handler owned, are reproduced by `SignalHandler`.

## The third-SIGTERM override

If the drain wedges (a peer not progressing), `Flezha.Shutdown.SignalHandler` escalates on
repeated user-issued signals: the first SIGTERM starts graceful shutdown and then
`:init.stop()`; the second is logged and ignored; the third logs a WARNING and calls
`:erlang.halt(143)` immediately. The orchestrator grace period is the wall-clock backstop.

## Disable flag

`FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false` reverts to a direct stop: `SignalHandler` starts but
installs no OS handlers and registers nothing on `erl_signal_server`, so kernel's
`erl_signal_handler` stays the owner (the BEAM terminates on SIGTERM) and `Application.prep_stop/1` calls
`Flezha.Service.HubManager.stop_hub_manager/0` directly; the prod container entrypoint's
disabled branch SIGTERMs the BEAM instead of calling `flz controller stop`. `config/test.exs`
sets the flag to `false` so application teardown stays fast.

## See also

- [Auto-restore on boot](auto_restore_on_boot.md) — what the solo-node path relies on
- [Reset](reset.md) — `flz reset` clears nothing until the controller is stopped
