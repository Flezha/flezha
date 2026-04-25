# Changelog

## 0.2.0 — Cluster snapshot & restore

### Added

- New top-level CLI commands `flz cluster snapshot <file>` and
  `flz cluster restore <file>` for explicit, operator-driven cluster-wide
  state capture and restore. See [`docs/cluster-snapshot.md`](docs/cluster-snapshot.md).
- `Flezha.Cluster.Snapshot` and `Flezha.Cluster.Snapshot.Coordinator`
  Elixir modules implementing the gather/apply RPC fan-out and the atomic
  ETF file write.
- `Flezha.Processor.Builder` now recognises `:initial_state_binary` in opts;
  `Flezha.Processor.Overwatch` sends a new `ProcessorApplyState` control
  command to the compute engine before the first input is forwarded so a
  re-spawned processor begins with primed state.
- New protobuf message `ProcessorApplyState` and corresponding handler in
  the Rust and Java SDKs. Default impl returns the canonical
  `apply_state_not_supported` error so older SDK builds are detectable;
  the agent throttles repeated WARNs to one per factory_id per BEAM.
- `Flezha.Recovery.PluginRecoverability` shared module — moves the
  `recoverable?/1` allow-list out of `Recovery.Coordinator` so both the
  legacy DETS path and the new snapshot path apply the same rule.
- New configuration keys: `:flezha, :prefer_explicit_restore?` (default
  `true`) and `:flezha, :snapshot_state_cap_bytes` (default 8 MB), with
  `FLEZHA_PREFER_EXPLICIT_RESTORE` and `FLEZHA_SNAPSHOT_STATE_CAP_BYTES`
  env-var overrides.

### Changed

- `Flezha.Recovery.SysMode` accepts new cast notifications
  `{:explicit_restore_in_progress, ref}` and `{:explicit_restore_done, ref, result}`
  from `Flezha.Cluster.Snapshot.Coordinator`. The first transitions
  SYSMODE directly from `:bootstrap` to `:normal`, suppressing the
  bootstrap-window timer. Telemetry events
  `[:flezha, :recovery, :explicit_restore_started]` and
  `[:flezha, :recovery, :explicit_restore_completed]` were added.
- Boot-time INFO log when the bootstrap-window timer fires while
  `:prefer_explicit_restore?` is `true`, naming `flz cluster restore` as
  the supported alternative.

### Deprecated

- `Flezha.Recovery.Store`, `Flezha.Recovery.PluginStore`, and
  `Flezha.Recovery.Coordinator` carry `@deprecated` module attributes —
  use `Flezha.Cluster.Snapshot` for restart-survival. Targeted for
  removal in the next major version.
- `Flezha.Recovery.Coordinator.run_recovery/0` emits a one-time WARN on
  first invocation per BEAM naming the new path.

### Removed (BREAKING)

- `Flezha.Processor.InitState.Providers.Sibling` has been removed along
  with its registration in `:enabled_plugins`, its capability spec
  (`processor-init-state-sibling`), and the `lib/flezha/processor/init_state/`
  tree. Restart-survival init-state seeding is now handled by
  `flz cluster restore` priming `:initial_state_binary` directly. The
  generic `processor-init-state` provider chain remains for non-snapshot
  sources (HTTP, S3, computed defaults, custom modules implementing
  `on_processor_init/2`).
- The WAIT_FACTORY gate machinery in `Flezha.Recovery.SysMode` is gone:
  the `:recovery_wait_factory`, `:recovery_wait_namespace`, and
  `:recovery_wait_timeout_ms` application env keys; the
  `FLEZHA_RECOVERY_WAIT_FACTORY` / `_NAMESPACE` / `_TIMEOUT_MS` env vars;
  the `:wait_factory_poll` and `:wait_factory_timeout` GenServer
  messages; the `factories_all_advertised?/2` helper; and the previously-
  undocumented `:recover-wait-factory` intermediate SYSMODE state.
- Operators with the removed env vars set at boot get a single WARN line
  pointing at `docs/cluster-snapshot.md`. The values are otherwise
  ignored.
