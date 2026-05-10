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
   - `Flezha.Service.RelayManager.start_relay/2` returns `{:error, :shutting_down}`.
   - `Flezha.Executor.FleetManager` rejects new executor connect attempts.
   - Already-running Relays and connected executors are NOT touched.
2. **Drain** — captures the locally-hosted Relay set per namespace,
   then calls `Flezha.Service.HubManager.stop_hub_manager/0` so
   ProcessHub's coordinator dispatches `cluster_leave` to peers
   (their migration strategies — `HotSwap{handover: true}` —
   take ownership). Polls peer registry dumps every 500 ms until each
   captured Relay is visible on at least one peer node, or the bounded
   timeout elapses.
   - **No-peer case**: solo nodes skip the migration wait; local
     termination flushes WAL state to disk for restart-survival via
     `auto-restore-on-boot`.
3. **Finalise** — synchronously flushes `Flezha.State.FsyncBatcher`
   (`:flush_all`) and stops transports via
   `Flezha.Transport.Server.stop_all/0`.

The whole sequence is bounded by
`:flezha, :graceful_shutdown_timeout_ms`. On timeout: ERROR log,
`[:flezha, :shutdown, :timeout]` telemetry event, advance to
finalise, return `{:partial, [relay_ids…]}`.

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
| 0    | Clean shutdown (all Relays migrated to peers, or `--force` finalised cleanly)  |
| 1    | Partial drain — timeout elapsed, some Relays still local at finalise           |
| 2    | Already shutting down — coordinator was in-flight; outcome reflects that one   |
| 3    | Controller socket unreachable (controller not running, or wrong socket path)   |

## Observability

### Telemetry events (under `[:flezha, :shutdown, …]`)

| Event              | Measurements                                               | Metadata                                       |
| ------------------ | ---------------------------------------------------------- | ---------------------------------------------- |
| `:requested`       | `%{}`                                                      | `%{reason, timestamp_ms}`                      |
| `:phase_started`   | `%{}`                                                      | `%{phase: :quiesce \| :drain \| :finalise}`    |
| `:phase_completed` | `%{duration_ms}`                                           | `%{phase}`                                     |
| `:drain_progress`  | `%{migrated, remaining, elapsed_ms}`                       | `%{}`                                          |
| `:timeout`         | `%{}`                                                      | `%{phase, still_local: [relay_ids…]}`          |
| `:complete`        | `%{duration_ms}`                                           | `%{outcome: :clean \| :partial \| :forced, reason}` |

### Structured log lines

```
[shutdown] received SIGTERM
[shutdown] phase=quiesce reason=:sigterm
[shutdown] phase=drain locally_hosted=2
[shutdown] drain_progress migrated=1 remaining=1 elapsed_ms=523
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
