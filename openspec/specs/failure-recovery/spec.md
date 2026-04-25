# failure-recovery Specification

## Purpose

The failure-recovery capability enables the Flezha agent to durably persist locally hosted processors and recoverable plugins, and to automatically re-instantiate them after a restart when no peer node has already taken over. It defines a SYSMODE lifecycle (`:bootstrap`, `:recover`, `:normal`), peer-to-peer SYSMODE exchange to avoid redundant recovery in a cluster, DETS-backed durable stores, and a resilient, idempotent recovery loop executed after a configurable window.

## Requirements

### Requirement: SYSMODE lifecycle

The Flezha agent SHALL maintain a system-mode value with exactly three states — `:bootstrap`, `:recover`, `:normal` — owned by a single GenServer reachable as `Flezha.Recovery.SysMode`. The value MUST be readable by in-node callers and observable via telemetry.

When the application config `:flezha, :prefer_explicit_restore?` is `true` (default), SYSMODE SHALL accept two cast notifications from `Flezha.Cluster.Snapshot.Coordinator`:

- `{:explicit_restore_in_progress, ref}` — SYSMODE transitions `:bootstrap → :normal` directly, cancels any pending bootstrap-window timer, and emits telemetry `[:flezha, :recovery, :explicit_restore_started]`. The DETS-driven `Flezha.Recovery.Coordinator.run_recovery/0` SHALL NOT be invoked once this notification is observed for the current boot.
- `{:explicit_restore_done, ref, result}` — SYSMODE emits telemetry `[:flezha, :recovery, :explicit_restore_completed]` with the result.

When `:prefer_explicit_restore?` is `false`, SYSMODE SHALL ignore those notifications and behave exactly as the prior spec described.

The pre-existing scenarios (initial mode `:bootstrap`, empty DETS keeps mode at `:bootstrap`, non-empty DETS transitions to `:recover`, window elapses runs recovery, normal peer cancels recovery, recovery disabled via config) all remain valid for the case where no explicit restore is invoked. They are restated below for completeness.

#### Scenario: Initial mode is :bootstrap
- **WHEN** the agent starts and `Flezha.Recovery.SysMode` completes `init/1`
- **THEN** the initial SYSMODE value is `:bootstrap`
- **AND** no recovery work has been executed yet

#### Scenario: Empty DETS keeps mode at :bootstrap until window elapses
- **WHEN** the recovery DETS file is empty (or absent) at startup AND no explicit restore is in progress
- **THEN** SYSMODE remains `:bootstrap` for the configured recovery window
- **AND** transitions to `:normal` exactly once the window elapses

#### Scenario: Non-empty DETS transitions to :recover
- **WHEN** the recovery DETS file contains at least one persisted child-spec row at startup AND no explicit restore is in progress
- **THEN** SYSMODE transitions to `:recover` immediately after `init/1`
- **AND** a one-shot window timer is armed with the configured duration

#### Scenario: Window elapses from :recover runs recovery
- **WHEN** the window timer fires and SYSMODE is `:recover` and no peer with SYSMODE `:normal` has been observed AND no explicit restore is in progress
- **THEN** the agent executes recovery by starting every DETS-persisted child spec via `Flezha.Service.ProcessorManager.start_processor/2`
- **AND** SYSMODE transitions to `:normal` after the recovery loop completes
- **AND** a `[:flezha, :recovery, :completed]` telemetry event is emitted with counts of recovered and failed specs

#### Scenario: Normal peer observed during window cancels recovery
- **WHEN** a peer reports SYSMODE `:normal` while this node's SYSMODE is `:recover` and the window timer has not yet fired
- **THEN** the window timer is cancelled
- **AND** SYSMODE transitions to `:normal` without executing the recovery loop
- **AND** DETS rows are retained (they will be overwritten or removed by ProcessHub hooks as state propagates)

#### Scenario: Recovery disabled via config
- **WHEN** `:flezha, :recovery_enabled` is set to `false`
- **THEN** SYSMODE transitions to `:normal` immediately on startup regardless of DETS contents
- **AND** the recovery loop is never executed

#### Scenario: Explicit restore suppresses the bootstrap timer (NEW)
- **GIVEN** an agent has just booted with non-empty DETS, SYSMODE is `:bootstrap`, the bootstrap window timer is armed, and `:prefer_explicit_restore?` is `true`
- **WHEN** an operator runs `flz cluster restore` and the coordinator sends `{:explicit_restore_in_progress, ref}`
- **THEN** SYSMODE cancels the timer, transitions directly to `:normal`, `run_recovery/0` is not invoked for this boot, telemetry `[:flezha, :recovery, :explicit_restore_started]` fires, and a single INFO line `[recovery] explicit restore in progress (initiator: <node>) — bootstrap timer suppressed` is emitted

#### Scenario: Explicit restore notification ignored when feature is off (NEW)
- **GIVEN** `:prefer_explicit_restore?` is `false`
- **WHEN** the coordinator sends `{:explicit_restore_in_progress, ref}` to SYSMODE
- **THEN** SYSMODE ignores the cast, the bootstrap window timer continues to count down, the DETS replay path remains active, and the configuration is logged once at startup so operators understand the dual-path situation

### Requirement: Durable persistence of locally hosted processors

The agent SHALL durably record every processor child spec it is hosting to a node-local DETS table, and SHALL remove entries when those processors are unregistered. The DETS table MUST only contain child specs for processors this node is actually hosting.

This requirement is **DEPRECATED** in favour of `cluster-snapshot-restore`. It SHALL remain functional in this release for backwards compatibility and for unattended-restart scenarios where no operator runs `flz cluster restore`. A future change (out of scope for this proposal) is expected to remove this requirement once downstream consumers have migrated.

The pre-existing scenarios (locally started processor is persisted, remote-only start is not persisted, unregistered child is removed) all remain valid.

#### Scenario: Locally started processor is persisted
- **WHEN** ProcessHub fires `post_children_start_hook` with a child whose `nodes` list includes the current `node()`
- **THEN** a DETS row keyed by `child_id` is written containing the namespace, the processor struct needed to reconstruct the child spec, the storing node name, and the insertion timestamp
- **AND** the write completes before the hook handler returns

#### Scenario: Remote-only start is not persisted
- **WHEN** ProcessHub fires `post_children_start_hook` with a child whose `nodes` list does not include the current `node()`
- **THEN** no DETS row is written on this node

#### Scenario: Unregistered child is removed
- **WHEN** ProcessHub fires `child_unregistered_hook` for a `child_id` present in DETS
- **THEN** the corresponding DETS row is deleted

#### Scenario: Deprecation INFO line on agent boot (NEW)
- **WHEN** the agent boots with `:prefer_explicit_restore?` true and finds non-empty DETS rows that have not been touched by an explicit restore in the current boot
- **THEN** a single INFO line at boot mentions `[recovery] DETS-based replay is deprecated — prefer 'flz cluster restore' (see docs/cluster-snapshot.md)`. This line MUST not fire when DETS is empty (no false positives on cold start).

### Requirement: The WAIT_FACTORY gate SHALL be removed

The `:recovery_wait_factory`, `:recovery_wait_namespace`, and `:recovery_wait_timeout_ms` application env keys, the corresponding `FLEZHA_RECOVERY_WAIT_FACTORY` / `_NAMESPACE` / `_TIMEOUT_MS` environment variables, the `:wait_factory_poll` and `:wait_factory_timeout` GenServer messages in `Flezha.Recovery.SysMode`, the `factories_all_advertised?/2` helper, and the intermediate `:recover-wait-factory` SYSMODE state SHALL be removed. The gate was added to coordinate DETS replay with a downstream consumer's factory-registration startup; with `cluster-snapshot-restore` providing explicit, operator-driven restore, the gate has no role in either the new path (which suppresses the bootstrap timer outright) or the deprecated fallback path (which uses the basic `recovery_window_ms` timer).

The basic `:recovery_window_ms` timer remains in place for the deprecated DETS replay fallback. SYSMODE state remains exactly the three documented values (`:bootstrap`, `:recover`, `:normal`) — the previously-undocumented `:recover-wait-factory` intermediate is gone.

For backwards compatibility on operator config: when an operator-set environment variable for any of the removed names is observed at boot, the agent SHALL emit a single startup WARN naming the removed variable and pointing at `docs/cluster-snapshot.md`. The variable's value is otherwise ignored.

#### Scenario: Removed env vars produce a single WARN at boot
- **GIVEN** an operator has `FLEZHA_RECOVERY_WAIT_FACTORY=market-init-state` set at agent startup
- **WHEN** the agent boots
- **THEN** a single WARN line `[recovery] FLEZHA_RECOVERY_WAIT_FACTORY is no longer supported — see docs/cluster-snapshot.md` is emitted, the variable's value is not read further, and SYSMODE follows the standard three-state machine

#### Scenario: Startup log line drops removed fields
- **WHEN** SYSMODE emits its startup INFO line (per the existing `[recovery] data_dir=... rows=N mode=... window_ms=... enabled=...` shape)
- **THEN** the line does not include `wait_factory=...` or `wait_namespace=...` fields, and structured-log consumers grepping for those tokens find none

#### Scenario: SYSMODE has only three states
- **WHEN** SYSMODE telemetry is observed across a full boot+recover lifecycle
- **THEN** the only values seen are `:bootstrap`, `:recover`, `:normal` — never `:recover-wait-factory`

### Requirement: Configurable recovery data directory

The recovery DETS file SHALL be located under a configurable directory so operators can mount persistent storage in containerized deployments.

#### Scenario: Default data directory
- **WHEN** neither `FLEZHA_DATA_DIR` env var nor `:flezha, :recovery_data_dir` is set
- **THEN** the DETS file is created at `app_data/recovery/recovery.dets` relative to the application working directory
- **AND** the `app_data/recovery/` directory is created if it does not exist

#### Scenario: Env-var override
- **WHEN** `FLEZHA_DATA_DIR=/var/lib/flezha` is set at startup
- **THEN** the DETS file is created at `/var/lib/flezha/recovery.dets`
- **AND** the directory is created if it does not exist and is writable

#### Scenario: Configurable window duration
- **WHEN** `FLEZHA_RECOVERY_WINDOW_MS=30000` is set at startup
- **THEN** the recovery window used for both `:bootstrap → :normal` and `:recover → :normal` transitions is 30 000 ms

### Requirement: SYSMODE exchange with connected peers

The agent SHALL exchange its SYSMODE value with every peer node it connects to, and SHALL honor SYSMODE values received from peers when deciding whether to cancel its own pending recovery.

#### Scenario: Outbound exchange on successful connect
- **WHEN** `Flezha.Cluster.Orchestrator` successfully completes `Node.connect(peer)` returning `true`
- **THEN** the Orchestrator notifies `Flezha.Recovery.SysMode` which sends a `{:peer_sysmode, this_node(), current_mode}` message to the peer's `Flezha.Recovery.SysMode` process
- **AND** delivery failure (peer process not yet up) does not crash the Orchestrator or the local SysMode

#### Scenario: Peer with empty DETS reports :bootstrap not :normal
- **WHEN** peer A (empty DETS, SYSMODE still `:bootstrap`) connects to peer B (SYSMODE `:recover`) during B's window
- **THEN** A sends SYSMODE `:bootstrap` to B
- **AND** B does NOT cancel its recovery timer based on this message
- **AND** B continues to wait for a peer reporting `:normal`

#### Scenario: Peer with :normal cancels our recovery
- **WHEN** a peer reports SYSMODE `:normal` while this node is in `:recover`
- **THEN** this node cancels its recovery timer, skips the recovery loop, and transitions to `:normal`

#### Scenario: Message from peer after window already elapsed
- **WHEN** a peer reports any SYSMODE value after this node has already transitioned to `:normal`
- **THEN** the message is acknowledged and discarded without state change

### Requirement: Recovery execution is resilient and idempotent

The recovery loop SHALL tolerate per-processor and per-plugin failures, SHALL not double-start processors or plugins that have already been registered since DETS was populated, and SHALL leave both DETS tables in a consistent state afterwards.

#### Scenario: Per-row failure does not abort the loop
- **WHEN** one processor row fails `Flezha.Service.ProcessorManager.start_processor/2` (e.g. validation error)
- **THEN** the failure is logged
- **AND** the loop continues with the next row
- **AND** the final telemetry event reports the failure count

#### Scenario: Stale row rejected by validator is removed
- **WHEN** a DETS row fails validation during recovery with a non-transient error
- **THEN** the row is deleted from its DETS table after the failure is logged

#### Scenario: :normal peer connects mid-recovery aborts remaining
- **WHEN** a peer reports SYSMODE `:normal` while the recovery loop is iterating and more rows remain (in either the processor or plugin phase)
- **THEN** the current in-flight `start_processor/2` or `enable_plugin/2` call completes
- **AND** no further DETS rows in either phase are attempted
- **AND** SYSMODE transitions to `:normal`

### Requirement: Durable persistence of locally enabled plugins

The agent SHALL durably record every **recoverable** plugin enabled via `Flezha.Service.PluginManager.enable_plugin/2` to a node-local DETS table, and SHALL remove entries on `disable_plugin/1`. Plugins whose driver reports `recoverable?/1 == false` MUST NOT be persisted.

#### Scenario: Recoverable plugin is persisted on enable
- **WHEN** `PluginManager.enable_plugin/2` succeeds for a plugin whose driver returns `recoverable?(plugin) == true`
- **THEN** a DETS row keyed by `plugin_id` is written containing the full plugin struct (with transient fields such as connection pids stripped), the storing node name, and the insertion timestamp
- **AND** the write completes before `enable_plugin/2` returns to the caller

#### Scenario: Non-recoverable plugin is not persisted
- **WHEN** `PluginManager.enable_plugin/2` succeeds for a plugin whose driver returns `recoverable?(plugin) == false` (e.g. a `BeamMsgForward` provider pointing at a caller pid)
- **THEN** no DETS row is written
- **AND** the plugin still functions normally in the live registry until the agent is restarted

#### Scenario: Disabled plugin is removed
- **WHEN** `PluginManager.disable_plugin/1` succeeds for a plugin that was previously persisted
- **THEN** the corresponding DETS row is deleted
- **AND** subsequent startups do not attempt to re-enable that plugin

#### Scenario: Plugin DETS survives agent restart
- **WHEN** the agent is stopped (graceful or crash) and restarted with the same data directory mounted
- **THEN** `Flezha.Recovery.PluginStore` opens the existing plugin DETS file on startup
- **AND** rows persisted before the stop are readable after the restart

### Requirement: Plugin recovery after processor recovery

When the recovery window elapses with SYSMODE `:recover` and no `:normal` peer observed, the agent SHALL run plugin recovery as a distinct phase *after* processor recovery completes, using `PluginManager.enable_plugin/2` with the persisted plugin struct.

#### Scenario: Plugins re-enabled on their owning processor
- **WHEN** the processor phase completes and there is at least one row in `:flz_recovery_plugins`
- **THEN** each plugin row is iterated serially
- **AND** `Flezha.Service.PluginManager.enable_plugin(plugin_id, plugin)` is called with the persisted plugin struct
- **AND** the per-phase telemetry includes `plugins_recovered`, `plugins_failed`, and `plugins_skipped_missing_processor` counts

#### Scenario: Plugin pointing at missing processor is skipped
- **WHEN** a plugin row's `processor_id` / `namespace` does not resolve to a running processor after the processor phase (either the processor failed to recover or was concurrently stopped)
- **THEN** a warning is logged naming the plugin and processor
- **AND** the plugin row is deleted from DETS
- **AND** the plugin is not enabled and the loop continues with the next row

#### Scenario: Plugin re-enable failure does not abort the loop
- **WHEN** `PluginManager.enable_plugin/2` returns an error for one plugin row (e.g. Kafka broker unreachable, WebSocket URI rejects the handshake, provider module missing)
- **THEN** the failure is logged
- **AND** the loop continues with the next row
- **AND** permanent failures (module undefined, plugin_id conflict) cause the row to be deleted; transient failures (network errors) leave the row intact for a future recovery

#### Scenario: Plugin recovery is skipped when processor recovery was aborted by a :normal peer
- **WHEN** SYSMODE transitions to `:normal` via `:peer_normal` during the processor phase
- **THEN** phase 2 is not entered
- **AND** no persisted plugins are re-enabled on this startup
- **AND** all plugin DETS rows remain intact for the next restart cycle

### Requirement: Supervision ordering

The recovery subsystem processes SHALL start in the agent supervision tree before any process capable of starting processors or enabling plugins, so that ProcessHub hooks are registered and both DETS stores are open before child or plugin activity can occur.

#### Scenario: Stores open before HubManager and PluginManager
- **WHEN** the Flezha application supervision tree starts
- **THEN** `Flezha.Recovery.Store` and `Flezha.Recovery.PluginStore` start and open their DETS files before `Flezha.Service.HubManager` starts
- **AND** both stores start before `Flezha.Service.PluginManager` starts (so plugin-enable hooks find an open plugin store)
- **AND** `Flezha.Recovery.Coordinator` registers ProcessHub hooks before `Flezha.ComputeEngine.FleetSupervisor` starts
- **AND** if either store fails to open its DETS file, application startup fails with a descriptive error
