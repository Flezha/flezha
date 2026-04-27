# Changelog

## Unreleased

### Added

- Operator ↔ agent connections now heal automatically across either-side
  restarts; processors resume traffic within 5 s of reconnect without
  operator intervention. Stream rebind is enforced as a hard contract on
  `Flezha.ComputeEngine.StreamChannel`, the UDS/QUIC acceptors atomically
  bind incoming streams only to nil-ref channels, and
  `Flezha.ComputeEngine.FleetManager.compute_engine_connect/3` re-issues
  `AddProcessor` for processors ProcessHub still has tagged to a dead
  compute_engine_id matching the connecting operator's profile. Rust and
  Java SDK `AddProcessor` handlers are now idempotent on duplicate
  `processor_id` (log + skip).
- `mix test.connection_healing` opt-in alias that runs
  `test/scripts/connection_healing.sh` against the dev compose stack to
  exercise both restart paths end-to-end. Requires Docker on the host;
  not part of the default `mix test` budget.
- `test/flezha/deployment/k8s_manifest_test.exs` k8s manifest sanity test
  that asserts the daemonset / configmap fields the connection-healing
  flow relies on (`flezha-sockets` volume, `FLEZHA_SOCKET_FILES_PATH`,
  `FLEZHA_OPERATOR_KEEPALIVE`) stay well-formed.

## 0.2.0 — Cluster snapshot & restore (replaces DETS recovery)

Restart-survival is now operator-driven via `flz cluster snapshot/restore`.
The previous DETS-based automatic recovery subsystem has been removed
along with the SYSMODE coordinator GenServer that drove it.

### Added

- New top-level CLI commands `flz cluster snapshot <file>` and
  `flz cluster restore <file>` for explicit, operator-driven cluster-wide
  state capture and restore. Both commands accept `--namespace NS` for
  per-namespace operations; default behaviour without the flag is
  cluster-wide. See [`docs/cluster-snapshot.md`](docs/cluster-snapshot.md).
- `Flezha.Cluster.Snapshot` and `Flezha.Cluster.Snapshot.Coordinator`
  Elixir modules implementing the gather/apply RPC fan-out and the
  atomic ETF file write.
- `Flezha.Cluster.Snapshot.PluginRecoverability` predicate that decides
  which plugins survive a snapshot/restore cycle.
- `Flezha.Processor.Builder` now recognises `:initial_state_binary` in
  opts; `Flezha.Processor.Overwatch` sends a new `ProcessorApplyState`
  control command to the compute engine before the first input is
  forwarded so a re-spawned processor begins with primed state.
- New protobuf message `ProcessorApplyState` and corresponding handler
  in the Rust and Java SDKs. Default impl returns the canonical
  `apply_state_not_supported` error so older SDK builds are detectable;
  the agent logs once per processor on apply failure.
- `:flezha, :snapshot_state_cap_bytes` configuration key (default 8 MB),
  with `FLEZHA_SNAPSHOT_STATE_CAP_BYTES` env-var override.

### Removed (BREAKING)

- The entire DETS-based recovery subsystem:
  `Flezha.Recovery.Store`, `Flezha.Recovery.PluginStore`,
  `Flezha.Recovery.Coordinator`, `Flezha.Recovery.SysMode`. The agent no
  longer writes child specs to a DETS file on processor start, no longer
  reads DETS at boot, no longer maintains a SYSMODE state machine, and
  no longer exchanges SYSMODE with peers. Recovery from an unattended
  crash now requires the operator to run `flz cluster restore` against
  a previously-captured snapshot file.
- `Flezha.Processor.InitState.Providers.Sibling` along with its
  registration in `:enabled_plugins`, its capability spec
  (`processor-init-state-sibling`), and the `lib/flezha/processor/init_state/`
  tree. Restart-survival init-state seeding is now handled by
  `flz cluster restore` priming `:initial_state_binary` directly. The
  generic `processor-init-state` provider chain remains for non-snapshot
  sources (HTTP, S3, computed defaults, custom modules implementing
  `on_processor_init/2`).
- Application env keys: `:flezha, :recovery_window_ms`,
  `:flezha, :recovery_data_dir`, `:flezha, :recovery_enabled`,
  `:flezha, :recovery_autostart`, `:flezha, :prefer_explicit_restore?`.
  Env-var overrides removed: `FLEZHA_RECOVERY_*`, `FLEZHA_DATA_DIR`,
  `FLEZHA_PREFER_EXPLICIT_RESTORE`.
- ProcessHub `post_children_start_hook` and `child_unregistered_hook`
  callbacks into the recovery coordinator.
- `failure-recovery` capability and its spec.
- `scripts/recovery_e2e.sh` shell-driven recovery smoke test.
- `docker/prod/docker-compose.local-test.yml` overlay.

### Plugin installer

- `Flezha.Plugin.Installer.Store` no longer co-locates its DETS file
  under the (now-removed) `recovery_data_dir`. New config knob:
  `:flezha, :plugin_installer_data_dir` (defaults to the installer's
  `:plugin_root`, which itself defaults to `app_data/plugins`).
