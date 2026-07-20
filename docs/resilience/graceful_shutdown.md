# Graceful controller shutdown

Default-on (`FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=true`). The controller
runs a three-phase orderly shutdown — **quiesce → drain → finalise**
— driven by `Flezha.Shutdown.Coordinator` and triggered by SIGTERM,
SIGINT, `Application.prep_stop/1`, or `flz controller stop`.

## End-to-end behaviour by run mode

| Run mode                     | Trigger                                  | What happens                                                                |
| ---------------------------- | ---------------------------------------- | --------------------------------------------------------------------------- |
| Release (`bin/flzvm start`)  | OS SIGTERM, or `bin/flezha stop`         | Release script issues `:init.stop()` → `prep_stop/1` → coordinator          |
| `mix run --no-halt`          | OS SIGTERM                               | `Flezha.Shutdown.SignalHandler` (`:os.set_signal/2`) → coordinator          |
| `iex -S mix`                 | OS SIGTERM, Ctrl-C (SIGINT)              | Same SignalHandler path                                                     |
| Any of the above             | `flz controller stop`                    | RPC into `CliManager.controller_stop/1` → coordinator                       |
| `docker stop` / compose stop | SIGTERM via Tini, then entrypoint trap   | Trap `exec`s `flz controller stop --timeout-ms ${FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS}` |
| `kubectl delete pod`         | `lifecycle.preStop` runs first           | `flz controller stop --timeout-ms $(FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS)` then SIGTERM (no-op — already shutting down) |

All paths converge on `Flezha.Shutdown.Coordinator.shutdown/2`, which
is **idempotent**: concurrent callers register as waiters and observe
the same outcome.

## Phases

1. **Quiesce** — sets a process-tree-visible flag (`Coordinator.shutting_down?/0`).
   - `Flezha.Service.OperativeManager.start_operative/2` returns `{:error, :shutting_down}`.
   - `Flezha.Runner.FleetManager` rejects new runner connect attempts.
   - Already-running Operatives and connected runners are NOT touched.
2. **Drain** — migrates locally-hosted Operatives to peers **while they
   are still alive**: one `ProcessHub.Service.Migration.drain/2` call per
   running namespace hub (concurrent via `Task.async_stream`). Each drain
   removes the node from the distribution cluster-wide and redistributes
   every local child through the migration-consent gate (live
   `HotSwap{handover: true}` handover — see "Migration consent" below);
   children still deferring at the drain deadline are force-migrated
   through the best-effort handover path. The hub supervisor stops only
   **after** the drains complete.
   - **Locally-empty is the completion signal.** `drain/2` returns once
     the deferred list is empty, but migration start requests are async
     and HotSwap stops a local child only after the remote start
     succeeded — so the coordinator additionally waits (bounded) until
     the hub has no local children before stopping it. A 2 s grace window
     is carved out of the drain timeout so a deadline force pass can
     still land its handover.
   - **Outcome fold per hub**: `{:ok, %{migrated, forced}}` counts into
     the drain summary (`forced > 0` logs a WARNING);
     `{:error, :no_target_nodes}` is the solo-node path (skip migration,
     WAL restores on next boot); any other precondition error
     (`:partitioned | :locked | :draining`) degrades to the same path
     with a WARNING — shutdown never wedges on a drain precondition.
   - **No-peer case**: solo nodes skip the migration wait; local
     termination flushes WAL state to disk for restart-survival via
     `auto-restore-on-boot`.
3. **Finalise** — synchronously flushes `Flezha.State.FsyncBatcher`
   (`:flush_all`) and stops transports via
   `Flezha.Transport.Server.stop_all/0`.

The whole sequence is bounded by
`:flezha, :graceful_shutdown_timeout_ms`. On timeout: ERROR log,
`[:flezha, :shutdown, :timeout]` telemetry event, advance to
finalise, return `{:partial, [operative_ids…]}`.

## Migration consent — busy Operatives defer their move

Every ProcessHub redistribution (node join, convergence rebind, the
drain above) asks each Overwatch for consent before migrating it
(`ProcessHub.Strategy.Migration.MigrationConsent`). The Overwatch
answers `:defer` while moving it would tear mid-critical work:

| Busy signal                            | Safe barrier (signals `migration_ready/2`)        |
| -------------------------------------- | ------------------------------------------------- |
| Late-input correction pending          | Correction transaction completes                  |
| Durable bounded, not suspended         | Suspension at a committed iteration boundary      |
| Effectively-once outbox not flushed    | Consent re-query on the next retry tick           |

A deferring child is parked in the hub's deferred list, re-queried
every `retry_interval`, and force-migrated (best-effort handover +
journal/WAL recovery on the target) after `max_defer_time`. The
synchronous correction transaction needs no special handling: the
Overwatch cannot answer mid-run, and a consent no-reply defers it.
A query failure fails open to `:ready` — worst case is pre-consent
behaviour, never a wedged migration.

| Knob                                        | Default                          | Env                                  |
| ------------------------------------------- | -------------------------------- | ------------------------------------ |
| `:migration_consent, :consent_timeout_ms`   | `5000`                           | `FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS` |
| `:migration_consent, :retry_interval_ms`    | `2000`                           | `FLEZHA_MIGRATION_RETRY_INTERVAL_MS`  |
| `:migration_consent, :max_defer_ms`         | `graceful_shutdown_timeout_ms`   | `FLEZHA_MIGRATION_MAX_DEFER_MS`       |

`max_defer_ms` defaults to the shutdown budget so a deferring
Operative can never outlive the shutdown window.

## Configuration

| Knob                                           | Default | Env                                            |
| ---------------------------------------------- | ------- | ---------------------------------------------- |
| `:flezha, :graceful_shutdown_enabled`          | `true`  | `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED`             |
| `:flezha, :graceful_shutdown_timeout_ms`       | `60000` | `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`          |

`FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` accepts (case-insensitive):
- Truthy: `1`, `true`, `yes`, `on`
- Falsy: `0`, `false`, `no`, `off`

### Configmap pairing — one knob, three values

The orchestrator's grace period MUST be 15 seconds longer than
`FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS / 1000` to leave headroom for
finalise + supervision-tree shutdown + container teardown:

| Layer                  | File                              | Setting                              |
| ---------------------- | --------------------------------- | ------------------------------------ |
| Coordinator timeout    | `kubernetes/flezha-configmap.yaml` | `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` |
| K8s pod grace period   | `kubernetes/flezha-daemonset.yaml` | `terminationGracePeriodSeconds`       |
| docker-compose grace   | `docker/{prod,dev}/docker-compose.yml` | `stop_grace_period`               |

**Worked example** — bumping the coordinator timeout to 120 000 ms:

```yaml
# kubernetes/flezha-configmap.yaml
FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS: "120000"

# kubernetes/flezha-daemonset.yaml
terminationGracePeriodSeconds: 135  # 120 + 15

# docker/prod/docker-compose.yml
stop_grace_period: 135s
```

## `flz controller stop` — CLI

Drives the coordinator over the controller's CLI socket. Works in
every run mode (release, mix, iex).

Flags:
- `--timeout-ms <N>` — overrides the configured drain timeout for
  this invocation.
- `--no-wait` — fire-and-forget: dispatch the shutdown and exit
  immediately.
- `--force` — skip quiesce + drain entirely; finalise (flush WAL +
  close transports) only. Operator escape hatch.

Exit codes:

| Code | Meaning                                                                        |
| ---- | ------------------------------------------------------------------------------ |
| 0    | Clean shutdown (all Operatives migrated to peers, or `--force` finalised cleanly)  |
| 1    | Partial drain — timeout elapsed, some Operatives still local at finalise           |
| 2    | Already shutting down — coordinator was in-flight; outcome reflects that one   |
| 3    | Controller socket unreachable (controller not running, or wrong socket path)   |

## Observability

### Telemetry events (under `[:flezha, :shutdown, …]`)

| Event              | Measurements                                               | Metadata                                       |
| ------------------ | ---------------------------------------------------------- | ---------------------------------------------- |
| `:requested`       | `%{}`                                                      | `%{reason, timestamp_ms}`                      |
| `:phase_started`   | `%{}`                                                      | `%{phase: :quiesce \| :drain \| :finalise}`    |
| `:phase_completed` | `%{duration_ms}`                                           | `%{phase}`                                     |
| `:drain_progress`  | `%{migrated, forced, remaining_hubs, elapsed_ms}` (one per completed hub) | `%{namespace}`                  |
| `:timeout`         | `%{}`                                                      | `%{phase, still_local: [operative_ids…]}`      |
| `:complete`        | `%{duration_ms}`                                           | `%{outcome: :clean \| :partial \| :forced, reason}` |

### Structured log lines

```
[shutdown] received SIGTERM
[shutdown] phase=quiesce reason=:sigterm
[shutdown] phase=drain locally_hosted=2
[shutdown] drain migrated=2 forced=0
[shutdown] phase=finalise
[shutdown] complete duration_ms=842 outcome=clean
```

### `flz controller status`

Surfaces:
- `shutdown_in_progress: bool`
- `shutdown_phase: :quiesce | :drain | :finalise | nil`
- `signal_handler_status: :ok | :degraded`
- `graceful_shutdown_enabled`, `graceful_shutdown_timeout_ms`

External readiness probes can poll this to drain traffic before the
BEAM stops accepting work.

## The third-SIGTERM escape hatch

If the drain wedges (e.g. peer not progressing), `Flezha.Shutdown.SignalHandler`
escalates on the third operator-issued SIGTERM:

1. **First**: triggers graceful shutdown.
2. **Second**: logged, ignored (idempotent).
3. **Third**: WARN log, `:erlang.halt(143)` immediately.

The orchestrator's grace period is the wall-clock backstop; the
third-SIGTERM is the operator's manual override.

## Kill-switch

Set `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false` to revert to the legacy
direct-stop behaviour:

- `SignalHandler.init/1` does NOT call `:os.set_signal/2`.
- `Application.prep_stop/1` calls `Flezha.Service.HubManager.stop_hub_manager/0`
  directly (today's behaviour).
- The container entrypoint trap falls back to `snapshot-on-stop.sh`
  + `kill -TERM $AGENT_PID` (until `auto-restore-on-boot` lands and
  `cluster-snapshot-restore` is removed).
