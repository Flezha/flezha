# auto-restore-on-boot Specification

## Purpose

The auto-restore-on-boot capability provides automatic, operator-free recovery of the Flezha cluster's running state across controller restarts, full-cluster cold boots, ProcessHub redistribution, and runner pod restarts. It activates ProcessHub's `durable-registry` and `coordinator-bootstrap-recovery` capabilities per namespace, registers Flezha-side hooks that surface the replay lifecycle to observability, and wires per-Entity WAL/snapshot restoration into the data-stream attach path. Registry-dependent services gate on recovery completion rather than on the replay hooks. The capability subsumes the prior `cluster-snapshot-restore` and `plugin-subscription-persistence` capabilities — boot-survival no longer requires operator-driven snapshot/restore CLI nor a parallel plugin-subscription store.
## Requirements
### Requirement: Configure ProcessHub for durable registry and auto-recovery per namespace

`Flezha.Service.HubManager.process_hub/1` SHALL pass `registry_backend: {:durable_ets, [path: <path>]}` and `auto_recovery: <config>` to every `ProcessHub.child_spec/1` call. This activates ProcessHub's `durable-registry` and `coordinator-bootstrap-recovery` capabilities per namespace.

The backend SHALL be one whose reads are served from memory, so the reconcile's per-round registry read does not touch disk, and whose writes are mirrored to disk synchronously so the file stays the record a cold cluster recovers from. `ProcessHub.Service.Storage.DurableEts` qualifies. `ProcessHub.Service.Storage.Dets` is permitted by shape — it shares the on-disk format, so a hub may be switched between the two against the same `:path` — but pays a disk read per round.

The live registry MUST still start empty on such a boot. `ProcessRegistry.local_data/1` selects rows by `node_pids` membership and the node name is stable across a restart, so a backend that exposed the previous boot's rows as live state would have `Synchronizer.broadcastable_local_data/1` advertise dead pids to peers as this node's local children.

The DETS path SHALL resolve to `app_data/process_hub/<hub_id>/registry.dets` by default, with the root configurable via `:flezha, :process_hub_registry_root` (env var `FLEZHA_PROCESS_HUB_REGISTRY_ROOT`).

The recovery configuration SHALL carry only the keys `ProcessHub.Service.Recovery.parse_config/1` reads — `[reconcile_grace_ms:, reconcile_interval_ms:, stopped_row_ttl_ms:]` — so no key advertises a control ProcessHub ignores; a key it no longer reads is accepted with a WARN and silently dropped, which is indistinguishable from working. Values come from runtime config `:flezha, :process_hub_reconcile_grace_ms` (default `45_000`), `:process_hub_reconcile_interval_ms` (default `30_000`) and `:process_hub_stopped_row_ttl_ms` (default `2_592_000_000`, 30 days).

The grace SHALL exceed the runner handshake window. The first reconcile round is what moves a hub `:recovering` → `:normal`, so it both gates boot and is the round that starts orphans — and an orphan start needs a connected runner to place against.

The kill-switch `:flezha, :auto_restore_enabled` (env var `FLEZHA_AUTO_RESTORE_ENABLED`, default `true`) SHALL gate the `auto_recovery` field: when `false`, pass `auto_recovery: false` to `ProcessHub.child_spec` so ProcessHub transitions immediately to `:normal` and never reconciles.

The atom in the registry-backend tuple MUST resolve to a real backend module via `ProcessHub.Initializer.resolve_registry_backend/1` — currently exactly `:durable_ets` (→ `ProcessHub.Service.Storage.DurableEts`), `:dets` (→ `ProcessHub.Service.Storage.Dets`) or `:ets` (→ `ProcessHub.Service.Storage.Ets`). Backend identifiers that route to non-existent modules SHALL NOT appear in `lib/flezha/service/hub_manager.ex`'s `registry_backend_config/1`: the `{module, opts}` fall-through clause treats an unknown atom as a module name and crashes `setup_storage/2` on the first runner connect with `(UndefinedFunctionError) function :<atom>.open/2 is undefined`, killing every fresh controller boot. `:durable_ets` is permitted here because both preconditions this requirement previously set are now met — `deps/process_hub/lib/process_hub/service/storage/durable_ets.ex` exists, and `resolve_registry_backend/1` carries an explicit `{:durable_ets, opts}` clause.

#### Scenario: Default config opts in to durable + auto-recovery

- **GIVEN** an application boots with default Flezha config
- **WHEN** `HubManager` constructs the ProcessHub child spec for namespace `"default"`
- **THEN** the spec contains `registry_backend: {:durable_ets, [path: "app_data/process_hub/<hub_id>/registry.dets"]}` and `auto_recovery: [reconcile_grace_ms: 45_000, reconcile_interval_ms: 30_000, stopped_row_ttl_ms: 2_592_000_000]`

#### Scenario: Kill-switch disables auto-recovery

- **GIVEN** `FLEZHA_AUTO_RESTORE_ENABLED=false`
- **WHEN** the spec is constructed
- **THEN** the spec contains `auto_recovery: false`; the persistent registry is still configured (mutations still durably written) but no recovery state machine engages — ProcessHub jumps to `:normal` immediately

#### Scenario: Registry-backend atom resolves to a real backend

- **WHEN** `Flezha.Service.HubManager.registry_backend_config/1` is called for any `hub_id` with `:auto_restore_enabled` at its `true` default
- **THEN** the returned tuple's first element is `:durable_ets`
- **AND** `ProcessHub.Initializer.resolve_registry_backend/1` routes that atom to `ProcessHub.Service.Storage.DurableEts`
- **AND** the matching backend module file exists at `deps/process_hub/lib/process_hub/service/storage/durable_ets.ex`

#### Scenario: A boot that skips replay leaves the on-disk registry intact

- **GIVEN** a registry file holding at least one operative row
- **WHEN** the backend is opened with `recovery_replay: false` (the marker-present boot), then closed, then reopened with `recovery_replay: true`
- **THEN** the first open exposes no rows as live state
- **AND** the reopen still finds every row that was on disk before it

#### Scenario: Boot succeeds end-to-end against a fresh image

- **GIVEN** the controller is started from a freshly-built `alfetahe/flezha:latest-dev` image with an empty `app_data/process_hub/` directory
- **WHEN** an SDK opens its first UDS `connect_control_channel` frame
- **THEN** `ProcessHub.Initializer.setup_storage/2` returns successfully for `:default_hub`
- **AND** `Flezha.Service.HubManager.register_handler/2` completes with `:ok`
- **AND** the controller does not crash, the BEAM does not exit, and the agent stays Ready
- **AND** a subsequent `bin/flz operative list` returns exit 0 (empty list on first boot)

### Requirement: `post_recovery_replay` hook emits Flezha-side telemetry

`Flezha.Boot.RecoveryHooks` SHALL register hook handlers on every namespace hub for both replay-lifecycle hooks, and both SHALL be thin forwarders that return promptly — ProcessHub blocks the coordinator on the synchronous `pre_recovery_replay` handler, so any work there stalls the recovery gate.

The `post_recovery_replay` handler SHALL emit `[:flezha, :boot, :recovery_complete]` telemetry with `%{hub_id: id, namespace: ns, replay_outcome: term}` metadata. The `pre_recovery_replay` handler SHALL emit `[:flezha, :boot, :pre_recovery_replay]` and return. Used by Phoenix LiveDashboard, Prometheus scrapers, and any external observability tooling.

When the kill-switch `:flezha, :auto_restore_enabled` is `false`, ProcessHub does not fire these hooks, so registration SHALL be a no-op.

#### Scenario: Telemetry fires after replay

- **WHEN** ProcessHub fires `post_recovery_replay` for a namespace hub
- **THEN** Flezha emits `[:flezha, :boot, :recovery_complete]` with the namespace and ProcessHub's replay outcome; subscribed handlers receive the event

#### Scenario: The synchronous handler does not hold the recovery gate

- **WHEN** ProcessHub fires `pre_recovery_replay` synchronously with no runner connected
- **THEN** the handler emits `[:flezha, :boot, :pre_recovery_replay]` and returns without polling FleetManager, and replay proceeds

### Requirement: Cspec-write-first ordering for plugin mutations on top of persistent registry

`Flezha.Service.PluginManager.enable_plugin/2` and `disable_plugin/1` SHALL mutate the ProcessHub cspec via `ProcessHub.Service.ProcessRegistry.update/3` BEFORE issuing the runtime `:plugin_subscribe`/`:plugin_unsubscribe` to the Overwatch. ProcessHub's `durable-registry` capability ensures the cspec mutation is durable on disk before `update/3` returns.

If the cspec write fails, the runtime mutation SHALL NOT be issued; the caller SHALL receive `{:error, :registry_unavailable}` (the propagated ProcessHub error).

If the runtime mutation fails after a successful cspec write, the cspec is correct (claims plugin enabled); on next Operative restart, `Overwatch.init/1`'s existing `subscribe_initial_plugins/2` path subscribes the plugin from the cspec. Self-healing on next start; no operator action needed.

The previous self-heal-on-next-procedure contract from `plugin-subscription-persistence` is no longer needed — the partial-failure window is closed by the registry-write-first ordering combined with the runtime mutation's retry-on-restart semantics.

#### Scenario: Crash between cspec write and runtime subscribe self-heals on restart

- **GIVEN** a operative `p1` running; `enable_plugin("kafka_p1", %Plugin{...})` succeeds in writing the cspec via `ProcessRegistry.update/3`; the controller VM crashes before the runtime `:plugin_subscribe` is issued
- **WHEN** the controller restarts; ProcessHub's persistent registry contains `p1` with `opts[:plugins]` including `kafka_p1`
- **AND** `p1`'s Overwatch is restored (via ProcessHub's recovery path or peer-redistribution)
- **THEN** `Overwatch.init/1` reads `operative.opts[:plugins]` from the cspec; `subscribe_initial_plugins/2` subscribes `kafka_p1`; the plugin is live without any operator action

#### Scenario: Cspec write fails — runtime mutation aborted

- **GIVEN** ProcessHub's persistent registry backend is unreachable (DETS file IO error)
- **WHEN** `enable_plugin/2` is called and `ProcessRegistry.update/3` returns `{:error, _}`
- **THEN** the call returns `{:error, :registry_unavailable}` and no `:plugin_subscribe` is issued

### Requirement: Entity state restoration fires on first stream attach

When an Entity's data stream attaches to a `StreamChannel` AND the owning Operative has `persistence_enabled = true` AND `Restore.assemble_payload/1` returns a payload with non-empty `snapshot_bytes` OR non-empty `change_log_bytes` AND the Operative did NOT arrive via HotSwap, the controller SHALL invoke `Flezha.State.Commands.dispatch_restore_init_state/2` for that Entity exactly once per Operative-startup BEFORE allowing input frames to flow.

The Operative's `arrived_via_hotswap?` flag SHALL be set to `true` by `Overwatch.alter_handover_state/2` on the destination of a HotSwap migration. When this flag is set, `Flezha.State.Commands.dispatch_restore_init_state/2` SHALL NOT fire — the in-band handover state stands.

This works identically across: ProcessHub-driven cold-boot replay, fresh `flz operative start`, ProcessHub redistribution, AddOperative reissue after runner restart.

#### Scenario: ProcessHub replay + Entity stream attach ships RestoreInitState

- **GIVEN** ProcessHub's persistent registry is replayed; a Operative is started; a runner connects and is assigned
- **WHEN** the SDK opens a data stream for an Entity that has on-disk WAL records under `app_data/state/{operative_class}/{procedure_id}/{entity_key}.wal`
- **THEN** the controller ships `RestoreInitState{entity_id, snapshot_bytes, change_log_bytes}` over the control channel BEFORE forwarding any input frames

#### Scenario: HotSwap-arrived Entity skips dispatch

- **GIVEN** a Operative arrives via HotSwap carrying a non-empty serialized state; `arrived_via_hotswap? = true`
- **WHEN** the Entity's data stream attaches
- **THEN** `dispatch_restore_init_state/2` SHALL NOT fire; the in-band handover state is the basis for going-forward processing

#### Scenario: HotSwap-arrived Entity with an EMPTY handover does not skip dispatch

- **GIVEN** a Operative migrates while its source runner is already gone, so `prepare_handover_state/1` has no runner to serialize from and ships an empty payload
- **WHEN** `alter_handover_state/2` runs on the destination
- **THEN** `arrived_via_hotswap?` SHALL remain `false` — an empty handover carries nothing to double-apply, and the on-disk state is the only surviving copy
- **AND** neither the bind-time restore nor the `kick_restore` repair SHALL be suppressed

### Requirement: A migration whose handover is empty SHALL pull the source controller's Entity files

`state_root` is node-local and MUST NOT be assumed shared. `prepare_handover_state/1` sources the operative's state from its **runner**, so a migration triggered by that runner's death necessarily ships an empty payload — and the source controller's disk then holds the only surviving copy.

`prepare_handover_state/1` SHALL stamp `source_node` on the handover map. When `alter_handover_state/2` receives an empty payload and a `source_node` other than this node, the destination SHALL pull every Entity of the operative from that node via `Flezha.State.WalTransfer.pull_operative/3` and then drive the ordinary per-Entity replay (`{:kick_restore, runner_id}`).

The pull SHALL be:

1. **Destination-driven.** The source only answers reads (`entities/1`, `manifest/1`, `read_chunk/3` over `:erpc`), so a source that dies mid-transfer fails the pull instead of stranding a partial file on the destination.
2. **Atomic per file.** Chunks are written to a `.transferring` sibling, fsynced, then renamed onto the destination path. Any failure removes the artefact and leaves that Entity cold — never half-restored.
3. **Scoped by the Entity's own file set.** A requested name is resolved against `Flezha.State.Paths.entity_files/1`, never joined onto the state directory, so a peer cannot read arbitrary paths off the node.
4. **Off the Overwatch loop**, since the copy is bounded by file size.
5. **Non-fatal.** An Entity that cannot be pulled is skipped; the operative keeps the Entities that did land. Telemetry: `[:flezha, :state, :wal_pulled]` / `[:flezha, :state, :wal_pull_failed]`.

Source-side files SHALL NOT be deleted at migration completion; the Retention sweeper reaps orphans.

This is one-shot transfer at migration time, NOT replication: a controller that dies together with its disk takes its operatives' state with it.

#### Scenario: Controller loses all runners — operatives migrate with their state

- **GIVEN** controller A hosting operative `p1` with Entity state on A's local disk; peer controller B with a runner for `p1`'s factory
- **WHEN** A's last runner disconnects and `hub_migrate` drains the namespace hub
- **THEN** `p1` migrates to B through the consent gate
- **AND** because A's runner is gone the handover payload is empty, so B pulls `p1`'s Entity files from A and replays them
- **AND** `p1` resumes on B with its pre-migration state; A's files remain on A until Retention sweeps them

#### Scenario: A peer cannot read outside the Entity's file set

- **GIVEN** a destination requesting file name `../../../etc/passwd` for an Entity
- **WHEN** `read_chunk/3` runs on the source
- **THEN** it SHALL return `{:error, :not_in_manifest}` and read nothing

#### Scenario: A failed pull leaves no partial file

- **GIVEN** a transfer that errors part-way through a file
- **WHEN** the destination handles the error
- **THEN** the `.transferring` artefact SHALL be removed and the destination path SHALL NOT exist

### Requirement: State-directory cleanup ONLY on explicit operative stop

State files under `app_data/state/{operative_class}/{procedure_id}/{entity_key}.{wal,snap.N,outbox.dedup}` SHALL be removed ONLY when `Flezha.Service.OperativeManager.stop_operative/2` is called by an operator. ProcessHub's `child_unregistered_hook` (which fires on HotSwap source-side, redistribution-away, etc.) SHALL NOT delete state files.

`stop_operative/2` SHALL: (a) call `ProcessHub.stop_children/3` (which fires the existing hooks → cspec deletion via persistent registry); (b) on success, recursively delete `app_data/state/{operative_class}/{procedure_id}/` for each procedure. Skip with WARN log if any `.tmp` or `.rewriting` file is present.

The Retention sweeper handles long-term orphans via `time_travel_retention_hours`.

#### Scenario: HotSwap source-side does NOT delete state files

- **GIVEN** operative `p1` running on controller A with WAL files
- **WHEN** ProcessHub HotSwap migrates `p1` to controller B
- **AND** A's `child_unregistered_hook` fires after the source process terminates
- **THEN** A's persistent-registry row is removed by ProcessHub; A's `app_data/state/...` files SHALL still exist
- **AND** B's persistent-registry row is written by ProcessHub's `post_children_start_hook`; B's WAL is populated by WAL-follows-operative

#### Scenario: flz operative stop deletes state files

- **WHEN** the runner runs `flz operative stop default p1`
- **THEN** `OperativeManager.stop_operative/2` calls `ProcessHub.stop_children/3`; on success, recursively deletes `app_data/state/MarketOperative/btc/...`
- **AND** if any `.tmp` or `.rewriting` file is present, deletion is skipped with a WARN log

### Requirement: Single explicitly-tolerated data-loss case

The mechanism SHALL preserve every change accepted (returned `:ok` to the caller) before the failure, modulo the inherent ≤ 10 ms in-flight fsync window AND the following single tolerated case:

- **Partial-cluster restart after full outage**: when the entire cluster has been shut down at some point AND only a SUBSET of controller nodes is later restarted, rows held only on the non-rejoining nodes SHALL be permanently lost.

Every other graceful-shutdown scenario — single-controller restart with peers up, cluster-wide cold boot with all nodes rejoining, runner pod restart, HotSwap migration, rolling deploy, partition heal, mixed-version rolling upgrade — SHALL preserve all accepted changes.

Ungraceful kill (SIGKILL of a controller without orchestration giving it time to migrate) is OUT OF SCOPE for state preservation; whichever node's local disk holds the only WAL copy is what survives.

#### Scenario: Subset rejoin loses subset rows

- **GIVEN** a three-node cluster A/B/C; full graceful cluster shutdown; only A and B come back
- **WHEN** A and B boot, ProcessHub coordinators enter `:recovery_pending`, exchange modes, both transition to `:recovering`, replay their local persistent registries
- **THEN** rows that were only on C's persistent registry are lost; A's and B's rows are replayed
- **AND** the contract is satisfied: this is the explicitly-tolerated case

### Requirement: `flz boot status` and `flz boot await` CLI

The CLI SHALL provide:

- `flz boot status` — for each namespace hub, print: hub_id, ProcessHub `recovery_state`, persistent-registry row count, last replay outcome (telemetry-aggregated), pending pull-on-attach transfers. It SHALL additionally print the readiness verdict and one line per unmet readiness precondition, so an operator can see which precondition is holding the node.
- `flz boot await [--timeout=N]` — block until the readiness verdict is `:ready`, or the timeout elapses. It SHALL read the verdict rather than iterating hubs itself, and SHALL exit non-zero when the timeout elapses with the verdict below `:ready`, naming the unmet preconditions. A timeout of zero SHALL evaluate the verdict once and return, so the same verb serves a container healthcheck.

`flz boot await` SHALL NOT report success on the grounds that the set of things to wait for is empty. A controller that has not yet created its namespace hubs SHALL be reported as not ready.

`flz boot` SHALL expose no import path for the `.flz` ETF format written by the deleted `flz cluster snapshot`. Nothing has written that format since `9235bcde` (2026-05-09), which deleted the producing verb and introduced the durable registry in the same commit, so no reachable node holds a file it could read.

The option reader SHALL be unchanged: `--timeout`, `--timeout=`, `-t` and `-t=` are all read, and a flag written with no value is refused by name rather than silently taking the default.

#### Scenario: flz boot status reports per-hub state

- **WHEN** the runner runs `flz boot status` while one namespace hub is `:normal` and another is still `:recovery_pending`
- **THEN** the CLI prints both hubs with their respective states and metrics

#### Scenario: flz boot status names the unmet precondition

- **WHEN** the runner runs `flz boot status` while the durable registry is still loading
- **THEN** the CLI prints the readiness verdict and names the durable-registry precondition as unmet

#### Scenario: flz boot await blocks until the verdict is ready

- **WHEN** the runner runs `flz boot await --timeout=30000` while one hub is still in `:recovery_pending`
- **THEN** the CLI blocks; once the readiness verdict reaches `:ready` (or the timeout elapses), it returns; on timeout, exits non-zero naming the preconditions still unmet

#### Scenario: flz boot await refuses to report success on an empty hub set

- **GIVEN** a controller that has not yet created any namespace hub
- **WHEN** the runner runs `flz boot await`
- **THEN** the CLI SHALL NOT report that all namespaces reached `:normal`, and SHALL block or exit non-zero according to its timeout

#### Scenario: A zero timeout evaluates the verdict once

- **WHEN** the runner runs `flz boot await --timeout=0`
- **THEN** the CLI evaluates the readiness verdict once and exits zero only if it is `:ready`

#### Scenario: The legacy snapshot import is refused as an unknown verb

- **WHEN** the runner runs `flz boot import-legacy-snapshot /path/to/snap.flz`
- **THEN** the CLI refuses it as an unrecognised subcommand, names the verbs `flz boot` does accept, and exits non-zero

### Requirement: Recovery replay SHALL NOT resurrect operatives onto dead nodes

ProcessHub recovery replay (`Flezha.Service.HubManager.alter_child_data/2`) SHALL drop
any `{node, pid}` entries that reference nodes absent from the live cluster when it
rewrites a operative's child-spec to an active runner, so the replayed operative is placed on a
live node rather than being treated as "already running" on a gone node. This complements
the generic ProcessHub `post_node_leave` reaping (see `controller-leadership`) and is
defense-in-depth for the case where reaping has not yet fired at replay time.

#### Scenario: Stale dead-node pid does not block respawn
- **GIVEN** a operative's persisted registry entry lists a pid on a node that is no longer in the cluster
- **WHEN** recovery replay runs `alter_child_data`
- **THEN** the dead-node entry SHALL be dropped and the operative SHALL be (re)started on a live node + live runner
- **AND** the operative SHALL resume producing rather than remaining registered-but-silent on the dead node

### Requirement: Recovery and on-demand hub creation SHALL be gated on cluster formation

Recovery-triggering hub creation SHALL occur only after the cluster is formed (or the
bounded degrade-to-solo timeout has elapsed), so a hub is never created on a
not-yet-clustered node and then diverge from its peers. After the gate, the reconciler
owns post-replay convergence (re-place, rebind, reissue) so a operative that replay started
but did not bring to production is still healed.

The gate SHALL be one seam that every hub-creating path waits on, and no path SHALL write its own wait: boot-time creation of the declared set, on-demand creation for a connecting runner, and the schedule hub's own creation SHALL all reach the cluster through it. Boot-time creation SHALL wait on the gate once for the whole declared set rather than once per namespace, because cluster formation is a property of the node and not of a namespace. The wait SHALL stay synchronous for the on-demand path, which runs inside the fleet manager's own call and must answer that caller rather than defer it.

#### Scenario: Replay on a freshly-clustered node converges
- **GIVEN** a controller that has just joined the cluster and replayed its registry
- **WHEN** the reconciler runs after `post_recovery_replay`
- **THEN** every replayed operative SHALL be driven to a producing state on a live runner, or marked `FAILED` if no runner can serve it

#### Scenario: No hub is created before the gate clears
- **GIVEN** a node whose cluster has not formed and whose degrade-to-solo window has not elapsed
- **WHEN** boot-time creation of the declared set runs and a runner connects for a namespace
- **THEN** neither path SHALL have created a hub until the gate clears, and both SHALL create theirs once it does

### Requirement: Boot ordering is gated on recovery completion, not on the replay hook

Registry-dependent services SHALL gate their readiness on `HubManager.await_recovery_complete/1` rather than on the `pre_recovery_replay` hook doing the waiting. `Flezha.Runner.FleetManager.init/1` and the convergence `Loop` SHALL block on `await_recovery_complete` before serving registry-dependent work. The `pre_recovery_replay` hook handler is consequently a thin telemetry forwarder (it emits `[:flezha, :boot, :pre_recovery_replay]` and returns; it does NOT poll FleetManager and there is no `:pre_replay_fleet_wait_ms` budget), because the upstream ProcessHub recovery-marker gate queues cluster events until the cluster is formed.

#### Scenario: a registry consumer waits for recovery before serving
- **GIVEN** a controller boot where a hub must replay its durable registry
- **WHEN** `FleetManager.init/1` runs
- **THEN** it calls `HubManager.await_recovery_complete/1` and proceeds only once every hub reaches `:normal`/`:skipped` (or the bounded timeout elapses)
- **AND** the `pre_recovery_replay` hook itself only forwards telemetry and does not block on the fleet.

### Requirement: The auto-restore kill-switch tolerates a non-boolean value

`HubManager.durable_registry_pending?/0` SHALL read `:auto_restore_enabled` as a kill-switch where only an explicit `false` disables auto-restore (`Application.get_env(:flezha, :auto_restore_enabled, true) != false`). A `nil` or otherwise non-boolean value SHALL be treated as enabled and MUST NOT crash the convergence Loop tick that reads this predicate.

#### Scenario: a nil config value does not crash the loop tick
- **GIVEN** `:auto_restore_enabled` is set to `nil` (e.g. cross-test env pollution)
- **WHEN** the convergence Loop tick calls `durable_registry_pending?/0`
- **THEN** the predicate treats auto-restore as enabled and returns a boolean without raising
- **AND** the Loop tick completes normally.

### Requirement: A sole-instance restart preserves operatives with no operator step

A plain restart of the sole controller SHALL preserve its operative list. The durable registry is never wiped to force a peer sync, so the first reconcile round finds every operative missing from the cluster and starts it, and `/healthz` SHALL honestly report `recovering`/`restoring_registry` until rebound. There SHALL be no arming step and no restart variant that discards the list on purpose.

#### Scenario: a plain sole-instance restart preserves the operative list
- **GIVEN** a sole controller has operatives bound
- **WHEN** the controller restarts, with nothing armed beforehand
- **THEN** the first reconcile round replays the operative list from the durable registry, `/healthz` reports `recovering` until rebound, and the operatives return.

### Requirement: A last-runner loss SHALL move operatives one by one through the planner

When this controller's last runner for a namespace disconnects while a peer controller's fleet advertises the operatives' factory, `Flezha.Runner.FleetManager` SHALL declare the disconnect and run no migration of its own: the convergence Diff's `replace_operatives` (no local runner for the factory, a peer fleet has one) plans `replace_operative` per child, executed through `Migration.migrate_child/3` and ProcessHub's migration-consent gate with live handover (`migration_strategy: HotSwap{handover: true}`), at most the loop's per-tick cap per tick and under the loop's table — NOT via a hub drain, which held this node out of the namespace's distribution for its budget and force-migrated whatever still deferred, and NOT via direct hub teardown, which would kill the Overwatches without a handover and orphan their state. Nothing is forced: a child that defers stays put, visible and misbound, for the next tick. The `FleetManager` call handling the disconnect SHALL NOT block on any move. No `Migration.drain/2` SHALL run, so this node's place in the distribution is never held open by a drain marker that outlives the move: a runner reconnecting while operatives are still leaving is a placement candidate at once.

Once this node's fleet no longer advertises the namespace AND this node hosts none of the namespace's operatives, the convergence loop SHALL reclaim the hub here through `Flezha.Service.HubManager.stop_hub_if_vacant/1`, so a controller that cannot host is not handed children by the distribution. That guard SHALL be node-local: a peer's operatives never keep this node's hub alive, and this node's own — running or durably suspended — always do. A later runner connect starts the hub again, so the reclaim costs a returning runner nothing.

`ProcessHub.Service.Migration.drain/2` remains the function graceful shutdown and the namespace stop reach through the single seam `Flezha.Service.HubManager.drain_hub/2`, callable from `lib/flezha/shutdown/` only. `drain_hub/2` SHALL wait for the local children to actually be gone before its caller proceeds — the migration start requests are async and HotSwap stops a local child only AFTER the remote start succeeds — by monitoring the hub's local children, never by polling.

#### Scenario: A returning runner is not blocked by a stale drain

- **GIVEN** controller A lost its last runner for a namespace and the loop has begun moving its operatives
- **WHEN** a runner reconnects to A shortly afterwards
- **THEN** no drain marker SHALL be holding A out of the distribution, the runner SHALL be a placement candidate at once, and the operatives not yet moved SHALL be rebound to it rather than moved

#### Scenario: Nothing is forced, and the vacated hub is reclaimed

- **GIVEN** controller A whose last runner for a namespace disconnects, with peer controller B in the hub cluster
- **WHEN** the disconnect is declared
- **THEN** each child SHALL be planned as `replace_operative` and moved through the consent gate with handover, no `Migration.drain/2` SHALL run, and the `FleetManager` call handling the disconnect SHALL NOT block
- **AND** once A hosts none of the namespace's operatives, the loop SHALL stop A's hub, so B is the only node left in the namespace's cluster

### Requirement: The controller SHALL publish one readiness verdict, folded from a declared precondition table

The controller SHALL answer "is this node ready to serve?" from exactly one place, `Flezha.Boot.Readiness`, which SHALL hold a declared table of readiness preconditions and SHALL fold them into a single verdict of `:booting`, `:recovering` or `:ready` together with the per-precondition rows that produced it.

Each precondition SHALL be one row of that table carrying a key, the verdict it holds the node at while unmet, and the predicate that decides it. Adding a readiness precondition SHALL be the addition of one row; no reader SHALL re-derive a precondition of its own. The table SHALL carry at least: the boot sequence having completed, every namespace hub having left ProcessHub's recovery state machine, and the durable registry having finished loading.

The readiness verdict SHALL be readable without passing through the boot process's mailbox, so a probe issued while the boot sequence is still running is answered rather than queued.

The verdict SHALL fail closed. A precondition whose fact has not been established, or cannot be read, SHALL count as unmet, and `:ready` SHALL require every precondition to be affirmatively met. No absence of information SHALL produce a ready answer.

A precondition that is met SHALL be decided live rather than latched at boot, so a node whose bounded boot recovery wait elapsed becomes ready when the condition it was waiting on is finally satisfied, without a restart. In particular a bounded recovery wait that elapses without every hub reaching `:normal` SHALL leave the verdict at `:recovering`, SHALL NOT be reported as ready merely because the boot sequence continued past it, and SHALL NOT hold the node at `:recovering` once those hubs reach `:normal`.

The controller SHALL serve the verdict over an HTTP readiness endpoint distinct from its health endpoint, returning success only when the verdict is `:ready` and failure otherwise, with a body naming the verdict and the unmet precondition keys. The readiness endpoint SHALL share the health endpoint's listener, port and enable switch, and SHALL introduce no configuration key of its own. The health endpoint's own verdicts, status codes and bodies SHALL be unchanged by it: the health endpoint answers whether the node should be restarted and the readiness endpoint answers whether the node should be routed to, and neither SHALL be derived from the other.

Exactly three surfaces SHALL read the verdict — the `flz boot` verbs, the HTTP readiness endpoint, and the container layer through `flz boot await` — and no other module SHALL compute readiness from hub recovery state, from a boot latch, or from the existence of a socket file.

#### Scenario: A precondition that is unmet holds the verdict below ready

- **GIVEN** a controller whose namespace hubs are still in `:recovery_pending`
- **WHEN** the readiness verdict is read
- **THEN** it SHALL be `:recovering`, and the rows SHALL name the hub-recovery precondition as the unmet one

#### Scenario: A bounded recovery wait that times out does not report ready

- **GIVEN** a controller whose bounded boot recovery wait elapses with a hub still recovering
- **WHEN** the boot sequence continues past the wait, as it is required to
- **THEN** the application SHALL still start, and the readiness verdict SHALL remain `:recovering` until that hub reaches `:normal`

#### Scenario: A restarted controller with an unreloaded durable registry is not ready

- **GIVEN** a controller that has completed its boot sequence, has no hub running, and holds a durable registry on disk for a hub that has not started
- **WHEN** the health endpoint and the readiness endpoint are queried on the same tick
- **THEN** the health endpoint SHALL report that a restore is pending and the readiness endpoint SHALL answer failure naming the unmet durable-registry precondition, so the readiness answer is never less informed than the health answer

#### Scenario: A node with no hubs yet is not ready

- **GIVEN** a controller in its first tick, before any namespace hub has been created
- **WHEN** the readiness verdict is read
- **THEN** it SHALL be `:booting`, and SHALL NOT be `:ready` on the grounds that the set of hubs to wait for is empty

#### Scenario: A readiness precondition is added as one row

- **WHEN** a new readiness precondition is introduced
- **THEN** it SHALL appear as one row of the precondition table with its own predicate, and no reader of the verdict SHALL be edited

#### Scenario: The verdict is answerable during the boot sequence

- **GIVEN** a controller whose boot sequence is still running
- **WHEN** the readiness verdict is read
- **THEN** it SHALL be answered as `:booting` rather than blocking until the boot sequence completes

#### Scenario: The readiness endpoint refuses while the health endpoint succeeds

- **GIVEN** a controller whose boot sequence has not completed
- **WHEN** the health endpoint and the readiness endpoint are queried on the same tick
- **THEN** the health endpoint SHALL answer success, so the kubelet does not restart the pod, and the readiness endpoint SHALL answer failure naming the unmet boot-sequence precondition

#### Scenario: The readiness endpoint changes nothing about the health endpoint

- **WHEN** the readiness endpoint is added to the controller's listener
- **THEN** the health endpoint SHALL keep every verdict, status code and body field it had, and SHALL NOT gain a readiness verdict or read the readiness table

### Requirement: A bound control socket SHALL NOT be reported as readiness

The container layer SHALL NOT treat the existence of the controller's control socket as the controller being ready. The socket SHALL be named as what it is — the transport being bound, and the precondition for issuing any control RPC at all — and readiness SHALL be established afterwards by reading the readiness verdict.

The controller entrypoint SHALL wait for the control socket, and SHALL then wait on the readiness verdict before reporting the controller up. The image `HEALTHCHECK` SHALL evaluate the readiness verdict rather than test for the socket file.

#### Scenario: The entrypoint waits for readiness, not for the socket alone

- **GIVEN** a controller container whose control socket has appeared while its hubs are still replaying
- **WHEN** the entrypoint's readiness wait runs
- **THEN** it SHALL NOT report the controller up until the readiness verdict is `:ready`

#### Scenario: The image healthcheck reads the verdict

- **WHEN** the image `HEALTHCHECK` command runs against a controller whose hubs are still replaying
- **THEN** it SHALL exit non-zero, and SHALL exit zero only once the readiness verdict is `:ready`

### Requirement: A hub SHALL exist for every namespace this node has evidence for

The controller SHALL create a hub at boot for every namespace it already holds evidence for, so that on-demand creation by a connecting runner is the exception rather than the only rule. The evidence SHALL be derived from exactly two sources, and no namespace name SHALL be written into that derivation:

- every directory under the configured process-hub registry root that holds a hub registry file, mapped back to its namespace, which is durable state the node must recover;
- every entry of the durable namespace catalog, which is operator intent.

The union of the two is the node's **declared set**. A namespace in neither source SHALL still receive a hub when a runner connects for it, through the unchanged on-demand path.

Boot-time creation SHALL go through the same start authority as every other hub, so a hub created at boot is indistinguishable from one created on connect in its registry backend, its recovery configuration, its hooks and its recovery-state seeding. It SHALL NOT be performed on the controller's boot sequence: the sequence SHALL complete without waiting for it, and the application SHALL start whether or not the declared set has been created.

A declared hub that fails to start SHALL be retried on the shared backoff ladder, SHALL emit telemetry naming the namespace and the reason, and SHALL NOT crash the node or the creating process. The auto-restore kill-switch SHALL govern boot-time creation: when it is disabled the declared set SHALL be empty and no hub SHALL be created at boot.

The registry-root walk that produces the first source SHALL be the same walk that decides whether an unstarted hub holds a registry, so the two readers of that root share one derivation rather than keeping two.

#### Scenario: A restarted controller recovers before any runner attaches

- **GIVEN** a controller whose disk holds a durable registry for a namespace with operatives, and no runner connected
- **WHEN** the controller starts
- **THEN** that namespace SHALL have a live hub, and its durable registry SHALL replay, without waiting for a runner to connect

#### Scenario: A namespace in the catalog alone gets a hub

- **GIVEN** a controller with an empty registry root and a catalog holding a namespace
- **WHEN** the controller starts
- **THEN** that namespace SHALL have a live hub

#### Scenario: A clean state root has a non-empty declared set

- **GIVEN** a controller booting against a clean state root, where the catalog seeds only the system namespace
- **WHEN** the controller starts
- **THEN** the declared set SHALL be non-empty and that namespace SHALL be given a hub, so readiness is answered over something rather than vacuously

#### Scenario: A namespace nobody declared is still served on connect

- **GIVEN** a namespace present in neither the registry root nor the catalog
- **WHEN** a runner connects for it
- **THEN** its hub SHALL be created on that connect, as before

#### Scenario: A failed hub start does not take the node down

- **GIVEN** a declared namespace whose hub start fails
- **WHEN** the failure occurs
- **THEN** the application SHALL remain started, the failure SHALL be retried on the shared backoff ladder with telemetry naming the namespace and the reason, and no process SHALL crash on it

#### Scenario: The kill-switch turns off boot-time creation

- **GIVEN** a controller with the auto-restore kill-switch disabled
- **WHEN** it starts
- **THEN** it SHALL create no hub at boot, and its readiness SHALL NOT be held for one

### Requirement: A vacancy reclaim SHALL NOT destroy an unreplayed registry

A hub SHALL be reclaimed as vacant only when this node holds none of its operatives, its declared set is empty, AND its recovery has reached a terminal state. Reclaiming a hub deletes its registry directory, so each of those three is a reason not to.

This is what boot-time creation makes reachable, because it puts hubs exactly where runners are not, and a live-placement read is blind in both of the windows that follow. A hub does not load its durable registry into the live registry when it starts: the rows stay on disk until the reconcile runs, a whole reconcile grace later, and every live read answers "empty" until then. After the reconcile the rows are back but nothing has placed them, because placing an operative needs a runner and there is none. A reclaim keyed on live placement alone would therefore delete, seconds or a grace after boot, the durable state boot-time creation exists to recover.

A hub whose recovery has settled and whose declared set is empty SHALL still be reclaimed, so a node whose fleet does not serve a namespace still leaves that namespace's cluster.

#### Scenario: A replayed registry survives the reclaim

- **GIVEN** a controller that created a hub at boot for a namespace whose registry is on disk, with no runner attached and the reconcile not yet run
- **WHEN** the vacancy reclaim runs for that namespace
- **THEN** the hub SHALL still be running and its registry directory SHALL still be on disk

#### Scenario: A declared operative keeps its hub after recovery settles

- **GIVEN** a controller whose hub was declared to hold an operative, with recovery settled, no runner to place it on and nothing running it
- **WHEN** the vacancy reclaim runs for that namespace
- **THEN** the hub SHALL still be running and its registry directory SHALL still be on disk

#### Scenario: A settled empty hub is still reclaimed

- **GIVEN** a hub whose recovery has reached a terminal state, declared to hold nothing, holding no operatives of this node and serving no runners
- **WHEN** the vacancy reclaim runs for it
- **THEN** the hub SHALL be stopped, as before

### Requirement: Readiness SHALL hold until every declared namespace has a hub

The readiness precondition table SHALL carry a row that is unmet while boot-time creation of the declared set has not completed, so a controller that has not yet created its hubs is not reported as ready. It SHALL be unmet before that creation has run at all, which is the window in which readiness would otherwise be answered over an empty hub set. The row SHALL hold the verdict at `:recovering`.

The row SHALL be decided live on every read, so a hub that starts on a later retry SHALL flip the node to ready with no restart and no second boot. It SHALL fail closed with the rest of the table: a fact that cannot be read counts as unmet, never as met. It SHALL be answerable while the supervision tree is still coming up, so reading it SHALL NOT wait on the creating process, whose own first act is to wait on the cluster gate for up to the formation window.

What the row answers SHALL be whether the declared set has been created, not whether every hub is still running. A hub the node later reclaims because it serves none of that namespace's operatives is a deliberate teardown: nothing brings such a hub back, so a row that refused for it would hold a healthy controller out of rotation for the rest of its life.

The row SHALL read the same kill-switch that governs boot-time creation, so a node that creates no hubs at boot is not held unready waiting for hubs it will never create.

The health verdict SHALL NOT be derived from this row. A node holding readiness for a missing hub SHALL keep answering its health endpoint as before, so a controller that is retrying is not restarted out from under itself.

#### Scenario: A declared namespace with no hub is not ready

- **GIVEN** a controller whose boot sequence has completed and whose declared set holds a namespace it has not created a hub for
- **WHEN** the readiness verdict is read
- **THEN** it SHALL be `:recovering`, and the rows SHALL name the declared-hub precondition as unmet

#### Scenario: A late hub flips the node to ready with no restart

- **GIVEN** a controller held unready by a declared namespace whose hub start failed
- **WHEN** a retry starts that hub
- **THEN** the readiness verdict SHALL become `:ready` on the next read, with no restart and no further boot step

#### Scenario: Readiness refuses while health succeeds

- **GIVEN** a controller whose declared hub has not started
- **WHEN** the health endpoint and the readiness endpoint are queried on the same tick
- **THEN** the health endpoint SHALL answer success and the readiness endpoint SHALL answer failure naming the unmet declared-hub precondition

#### Scenario: A hub reclaimed as vacant does not hold the node unready

- **GIVEN** a controller that created its declared set at boot, and one of those namespaces whose hub it has since reclaimed because it serves none of that namespace's operatives
- **WHEN** the readiness verdict is read
- **THEN** that namespace SHALL NOT hold it, and the node SHALL stay ready

