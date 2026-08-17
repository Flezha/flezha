# Data-plane liveness instrumentation (`data-plane-liveness-instrumentation`)

- **Purpose**: localize a silent `io_in`/output stall to one layer.
  Landed as pure instrumentation; the classifier verdict has SINCE been
  wired into remediation (see below). Closes the 22:14 candle incident's "which layer
  wedged?" gap (BEAM channel vs NIF driver thread vs runner).
- **NIF driver self-report**: `flz_nif/src/driver_stats.rs` —
  `DriverStats{loop_iterations, last_loop_ms, cmd_queue_depth:
  AtomicU64; streams: Mutex<HashMap>}` shared `Arc` into the
  flz-uds-driver loop (`uds_driver.rs` bumps the heartbeat once per
  iteration, rebuilds the per-stream snapshot once per iteration —
  never per frame). `driver_stats/1` NIF (in `server.rs`, registered
  via auto-discovery `init!`) returns the map; `:undefined` for QUIC
  (`TransportServer.stats: Option<Arc<DriverStats>>`).
- **BEAM counters**: `lib/flezha/transport/liveness_counters.ex` —
  always-on `:counters`/`:atomics` + public ETS registry (mirrors
  `Observability.Counters`). `StreamChannel` bumps `io_in_received /
  send_frame_calls / nil_drops / flz_data_received / last_send_ms`,
  plus heartbeat/credit slots (`hb_count`, `hb_dispatches`,
  `hb_credit_parked`, `credit_active`, `credit_ledger_level`,
  `credit_overrun_total`, `stale_writer_refused`);
  `Operative.Overwatch` bumps `io_in_received / forwarded / filtered_nil`.
  Dead-owner entries pruned on `snapshot_all/0`.
- **Metric emission**: `Transport.Server.get_stats/1` merges
  `Native.driver_stats`; `OtpBackend.TransportSource` (the
  `:flezha_transport` source) emits per-transport driver metrics +
  flat global BEAM aggregates (bounded cardinality — classifier does
  per-entity localization).
- **Stall classifier**: `lib/flezha/convergence/stall_classifier.ex`
  — pure `classify(current, prev, opts)` returning one of
  `driver_loop_frozen | nif_queue_backing_up | socket_write_blocked |
  runner_not_consuming | runner_pool_wedged | channel_not_sending |
  operative_not_forwarding | :ok` (deepest-wedged-layer-wins; each
  verdict has an "is it being fed?" advancing-delta precondition).
  `Convergence.Loop` builds the snapshot per tick and emits
  `[:flezha, :data_plane, :stall_localized]` (with `%{layer: …}`) on
  verdict change. The verdict now ALSO feeds remediation: the
  convergence Diff/Planner mint wedge ops from it
  (`diff.ex resolve_wedge/3` → `planner.ex wedge_ops/1`) and
  `/healthz` reports a `:fabric` verdict as 503. Bridged to the bus
  via `TelemetryBridge`.
- **Runtime config knob**: `:observability, :data_plane_liveness`
  (default `true`, env `FLEZHA_DATA_PLANE_LIVENESS`) gates the ~1 Hz
  poll + classifier; counters stay always-on.
- **Procs fallback** (docs, not built): sample
  `/proc/<beam_pid>/task/*/` for `flz-uds-driver` threads in `D`/frozen
  `utime` (`bin/flzvm pid`).
- **Detailed notes**: `docs/resilience/transport_liveness.md`.

