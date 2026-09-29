# auto-restore-on-boot Specification

## Purpose

The auto-restore-on-boot capability provides automatic, user-free recovery of the Flezha cluster's running state across controller restarts, full-cluster cold boots, ProcessHub redistribution, and runner pod restarts. It activates ProcessHub's `durable-registry` and `coordinator-bootstrap-recovery` capabilities per namespace, registers Flezha-side hooks that surface the replay lifecycle to observability, and wires per-Cell WAL/snapshot restoration into the data-stream attach path. Registry-dependent services gate on recovery completion rather than on the replay hooks. The capability subsumes the prior `cluster-snapshot-restore` and `plugin-subscription-persistence` capabilities — boot-survival no longer requires user-driven snapshot/restore CLI nor a parallel plugin-subscription store.
## Requirements
### Requirement: Configure ProcessHub for durable registry and auto-recovery per namespace

`Flezha.Service.HubManager.process_hub/1` SHALL pass `registry_backend: {:durable_ets, [path: <path>]}` and `auto_recovery: <config>` to every `ProcessHub.child_spec/1` call. This activates ProcessHub's `durable-registry` and `coordinator-bootstrap-recovery` capabilities per namespace.

The backend SHALL be one whose reads are served from memory, so the reconcile's per-round registry read does not touch disk, and whose writes are mirrored to disk synchronously so the file stays the record a cold cluster recovers from. `ProcessHub.Service.Storage.DurableEts` qualifies. `ProcessHub.Service.Storage.Dets` is permitted by shape — it shares the on-disk format, so a hub may be switched between the two against the same `:path` — but pays a disk read per round.

The live registry MUST still start empty on such a boot. `ProcessRegistry.local_data/1` selects rows by `node_pids` membership and the node name is stable across a restart, so a backend that exposed the previous boot's rows as live state would have `Synchronizer.broadcastable_local_data/1` advertise dead pids to peers as this node's local children.

The DETS path SHALL resolve to `app_data/process_hub/<hub_id>/registry.dets` by default, with the root configurable via `:flezha, :process_hub_registry_root` (env var `FLEZHA_PROCESS_HUB_REGISTRY_ROOT`).

The recovery configuration SHALL carry only the keys `ProcessHub.Service.Recovery.parse_config/1` reads, currently `[reconcile_grace_ms:, reconcile_interval_ms:, cluster_settle_ms:, remote_manifest:]`, so no key advertises a control ProcessHub ignores; a key it no longer reads is accepted with a WARN and silently dropped, which is indistinguishable from working. Values come from runtime config `:flezha, :process_hub_reconcile_grace_ms` (default `45_000`), `:process_hub_reconcile_interval_ms` (default `30_000`) and `:process_hub_cluster_settle_ms` (default `2_000`, env var `FLEZHA_PROCESS_HUB_CLUSTER_SETTLE_MS`). `remote_manifest` carries the durability tier's declared-manifest module when a tier is configured and `nil` otherwise. `stopped_row_ttl_ms` SHALL NOT be passed: ProcessHub deprecated the key and Flezha holds no row for it.

The grace SHALL be the cap on the first reconcile round rather than the path every boot takes. The round opens on peer evidence, so a controller whose hubs have no peers leaves `:recovering` once the cluster settle window has elapsed, and `:process_hub_reconcile_grace_ms` bounds only the case where a connected peer never delivers its registry data. Its `45_000` default is sized for a peer that is itself still booting, which is the slowest answer a healthy cluster produces, and is no longer tied to ProcessHub's `sync_interval` or to any runner handshake window.

An operator whose runner has not connected when its start is placed SHALL be started unbound and rebound by the convergence loop, rather than waited for. The placement records no runner, the Overwatch holds the operator, and the binding happens when a runner advertising its factory connects. No configuration value SHALL be sized as a runner handshake window, because none of them waits for a runner.

The kill-switch `:flezha, :auto_restore_enabled` (env var `FLEZHA_AUTO_RESTORE_ENABLED`, default `true`) SHALL gate the `auto_recovery` field: when `false`, pass `auto_recovery: false` to `ProcessHub.child_spec` so ProcessHub transitions immediately to `:normal` and never reconciles.

The atom in the registry-backend tuple MUST resolve to a real backend module via `ProcessHub.Initializer.resolve_registry_backend/1` — currently exactly `:durable_ets` (→ `ProcessHub.Service.Storage.DurableEts`), `:dets` (→ `ProcessHub.Service.Storage.Dets`) or `:ets` (→ `ProcessHub.Service.Storage.Ets`). Backend identifiers that route to non-existent modules SHALL NOT appear in `lib/flezha/service/hub_manager.ex`'s `registry_backend_config/1`: the `{module, opts}` fall-through clause treats an unknown atom as a module name and crashes `setup_storage/2` on the first runner connect with `(UndefinedFunctionError) function :<atom>.open/2 is undefined`, killing every fresh controller boot. `:durable_ets` is permitted here because both preconditions this requirement previously set are now met — `deps/process_hub/lib/process_hub/service/storage/durable_ets.ex` exists, and `resolve_registry_backend/1` carries an explicit `{:durable_ets, opts}` clause.

#### Scenario: Default config opts in to durable + auto-recovery

- **GIVEN** an application boots with default Flezha config
- **WHEN** `HubManager` constructs the ProcessHub child spec for namespace `"default"`
- **THEN** the spec contains `registry_backend: {:durable_ets, [path: "app_data/process_hub/<hub_id>/registry.dets"]}`, and its `auto_recovery` carries `reconcile_grace_ms: 45_000`, `reconcile_interval_ms: 30_000`, `cluster_settle_ms: 2_000` and `remote_manifest: nil`

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

- **GIVEN** a registry file holding at least one operator row
- **WHEN** the backend is opened with `recovery_replay: false` (the marker-present boot), then closed, then reopened with `recovery_replay: true`
- **THEN** the first open exposes no rows as live state
- **AND** the reopen still finds every row that was on disk before it

#### Scenario: Boot succeeds end-to-end against a fresh image

- **GIVEN** the controller is started from a freshly-built `flezha/flezha:latest-dev` image with an empty `app_data/process_hub/` directory
- **WHEN** an SDK opens its first UDS `connect_control_channel` frame
- **THEN** `ProcessHub.Initializer.setup_storage/2` returns successfully for `:default_hub`
- **AND** the connect starts the namespace's hub and the runner's session completes its handshake
- **AND** the controller does not crash, the BEAM does not exit, and the agent stays Ready
- **AND** a subsequent `bin/flz operator list` returns exit 0 (empty list on first boot)

#### Scenario: A controller with no peers is ready in seconds

- **GIVEN** a controller booting with default configuration, a durable registry on disk and no peer node connected
- **WHEN** `flz boot await` is run against it
- **THEN** the verdict SHALL reach `:ready` within 10 seconds of boot rather than after `:process_hub_reconcile_grace_ms`, and the `hubs_recovered` precondition SHALL be the last one met

#### Scenario: A connected peer that never answers still bounds the wait

- **GIVEN** a controller whose namespace hub has a connected peer that delivers no registry data
- **WHEN** `:process_hub_reconcile_grace_ms` elapses
- **THEN** the first reconcile round runs anyway, the hub reaches `:normal`, and the readiness verdict becomes `:ready`

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

### Requirement: Cell state restoration fires on first stream attach

When a Cell's data stream attaches to a `StreamChannel` AND the owning Operator has `persistence_enabled = true` AND `Restore.assemble_payload/1` returns a payload with non-empty `snapshot_bytes` OR non-empty `change_log_bytes` AND the Operator did NOT arrive via HotSwap, the controller SHALL invoke `Flezha.State.Commands.dispatch_restore_init_state/2` for that Cell exactly once per Operator-startup BEFORE allowing input frames to flow.

The Operator's `arrived_via_hotswap?` flag SHALL be set to `true` by `Overwatch.alter_handover_state/2` on the destination of a HotSwap migration. When this flag is set, `Flezha.State.Commands.dispatch_restore_init_state/2` SHALL NOT fire — the in-band handover state stands.

This works identically across: ProcessHub-driven cold-boot replay, fresh `flz operator start`, ProcessHub redistribution, AddOperator reissue after runner restart.

#### Scenario: ProcessHub replay + Cell stream attach ships RestoreInitState

- **GIVEN** ProcessHub's persistent registry is replayed; an Operator is started; a runner connects and is assigned
- **WHEN** the SDK opens a data stream for a Cell that has on-disk WAL records under `app_data/state/{operator_class}/{tasklet_id}/{cell_key}.wal`
- **THEN** the controller ships `RestoreInitState{cell_id, snapshot_bytes, change_log_bytes}` over the control channel BEFORE forwarding any input frames

#### Scenario: HotSwap-arrived Cell skips dispatch

- **GIVEN** an Operator arrives via HotSwap carrying a non-empty serialized state; `arrived_via_hotswap? = true`
- **WHEN** the Cell's data stream attaches
- **THEN** `dispatch_restore_init_state/2` SHALL NOT fire; the in-band handover state is the basis for going-forward processing

#### Scenario: HotSwap-arrived Cell with an EMPTY handover does not skip dispatch

- **GIVEN** an Operator migrates while its source runner is already gone, so `prepare_handover_state/1` has no runner to serialize from and ships an empty payload
- **WHEN** `alter_handover_state/2` runs on the destination
- **THEN** `arrived_via_hotswap?` SHALL remain `false` — an empty handover carries nothing to double-apply, and the on-disk state is the only surviving copy
- **AND** neither the bind-time restore nor the `kick_restore` repair SHALL be suppressed

### Requirement: A migration whose handover is empty SHALL pull the source controller's Cell files

`state_root` is node-local and MUST NOT be assumed shared. `prepare_handover_state/1` sources the operator's state from its **runner**, falling back only to the final state a runner handed its Overwatch on a graceful shutdown, which a later bind of the operator to any runner discards. A migration triggered by a runner's death therefore ships an empty payload unless that runner left such a state and no runner has been bound since, and the source controller's disk then holds the only surviving copy.

`prepare_handover_state/1` SHALL stamp `source_node` on the handover map. When `alter_handover_state/2` receives an empty payload and a `source_node` other than this node, the destination SHALL pull every durable file of the operator from that node via `Flezha.State.WalTransfer.pull_operator/3` and then run the ordinary per-Cell replay, both through `Flezha.Operator.Replay.dispatch_restore/3`, the step every bind's restore runs. An operator's durable files are its Cells' file sets and, for a bounded instance, its journal and its durable input log, which SHALL travel together: a journal pulled without its log would seed the instance's floor in an empty offset space, under which no retained input is ever owed. The same pull SHALL serve a partition member placed on a node other than the one its group was last assembled on.

The pull SHALL be:

1. **Destination-driven.** The source only answers reads (`Paths.durable_refs/1`, `manifest/1`, `read_chunk/3` over `:erpc`), so a source that dies mid-transfer fails the pull instead of stranding a partial file on the destination.
2. **Atomic per file.** Chunks are written to a `.transferring` sibling, fsynced, then renamed onto the destination path. Any failure removes the artefact and leaves that Cell cold — never half-restored.
3. **Scoped by the reference's own file set.** A requested name is resolved against the file set the reference names, `Flezha.State.Paths.cell_files/1` for a Cell or the journal's reserved identity and the input log's one path for the log, never joined onto the state directory, so a peer cannot read arbitrary paths off the node.
4. **Ahead of the bind's resume read, and never over state already here.** A bind whose operator names a foreign `state_source_node` pulls before it reads the instance's journal, restore count and input-log tail, so a bounded operator member resumes from what arrived; a node that already holds any of the instance's durable files pulls nothing, so a later restore never overwrites newer progress with the copy the source was left holding. The migration's pull runs off the Overwatch loop, since the copy is bounded by file size.
5. **Non-fatal.** A Cell that cannot be pulled is skipped; the operator keeps the Cells that did land. Telemetry: `[:flezha, :state, :wal_pulled]` / `[:flezha, :state, :wal_pull_failed]`.

Source-side files SHALL NOT be deleted at migration completion; the Retention sweeper reaps orphans.

This is one-shot transfer at migration time, NOT replication: a controller that dies together with its disk takes its operators' state with it.

#### Scenario: Controller loses all runners — operators migrate with their state

- **GIVEN** controller A hosting operator `p1` with Cell state on A's local disk and no snapshot of `p1` held by its Overwatch; peer controller B with a runner for `p1`'s factory
- **WHEN** A's last runner disconnects and, after the withdrawal grace, A stops serving `p1`'s factory, so the hub's redistribution assigns `p1` to B
- **THEN** `p1` migrates to B through the consent gate
- **AND** because A's runner is gone the handover payload is empty, so B pulls `p1`'s Cell files from A and replays them
- **AND** `p1` resumes on B with its pre-migration state; A's files remain on A until Retention sweeps them

#### Scenario: A durable bounded operator migrates with its journal and log

- **GIVEN** controller A hosting a durable `per_input` bounded instance with three committed iterations and one retained input above its floor, and peer controller B with a runner for its factory
- **WHEN** A's last runner disconnects and the hub assigns the instance to B
- **THEN** B pulls the instance's Cells, its `task.journal` and its `input.log` from A, byte for byte, and A's copies stay for Retention

#### Scenario: A peer cannot read outside the Cell's file set

- **GIVEN** a destination requesting file name `../../../etc/passwd` for a Cell
- **WHEN** `read_chunk/3` runs on the source
- **THEN** it SHALL return `{:error, :not_in_manifest}` and read nothing

#### Scenario: A peer cannot read outside the log's file set

- **GIVEN** a destination requesting file name `kind` for an input-log reference
- **WHEN** `read_chunk/3` runs on the source
- **THEN** it SHALL return `{:error, :not_in_manifest}` and read nothing

#### Scenario: A failed pull leaves no partial file

- **GIVEN** a transfer that errors part-way through a file
- **WHEN** the destination handles the error
- **THEN** the `.transferring` artefact SHALL be removed and the destination path SHALL NOT exist

### Requirement: State-directory cleanup ONLY on explicit operator stop

State files under `app_data/state/{operator_class}/{tasklet_id}/{cell_key}.{wal,snap.N,outbox.dedup}` SHALL be removed ONLY when `Flezha.Service.OperatorManager.stop_operator/2` is called by a user. ProcessHub's `child_unregistered_hook` (which fires on HotSwap source-side, redistribution-away, etc.) SHALL NOT delete state files.

`stop_operator/2` SHALL: (a) call `ProcessHub.stop_children/3` (which fires the existing hooks → cspec deletion via persistent registry); (b) on success, recursively delete `app_data/state/{operator_class}/{tasklet_id}/` for each tasklet. Skip with WARN log if any `.tmp` or `.rewriting` file is present.

The Retention sweeper handles long-term orphans via `time_travel_retention_hours`.

#### Scenario: HotSwap source-side does NOT delete state files

- **GIVEN** operator `p1` running on controller A with WAL files
- **WHEN** ProcessHub HotSwap migrates `p1` to controller B
- **AND** A's `child_unregistered_hook` fires after the source process terminates
- **THEN** A's persistent-registry row is removed by ProcessHub; A's `app_data/state/...` files SHALL still exist
- **AND** B's persistent-registry row is written by ProcessHub's `post_children_start_hook`; B's WAL is populated by WAL-follows-operator

#### Scenario: flz operator stop deletes state files

- **WHEN** the runner runs `flz operator stop default p1`
- **THEN** `OperatorManager.stop_operator/2` calls `ProcessHub.stop_children/3`; on success, recursively deletes `app_data/state/MarketOperator/btc/...`
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

- `flz boot status` — for each namespace hub, print: hub_id, ProcessHub `recovery_state`, persistent-registry row count, last replay outcome (telemetry-aggregated), pending pull-on-attach transfers. It SHALL additionally print the readiness verdict and one line per unmet readiness precondition, so a user can see which precondition is holding the node.
- `flz boot await [--timeout=N]` — block until the readiness verdict is `:ready`, or the timeout elapses. It SHALL read the verdict rather than iterating hubs itself, and SHALL exit non-zero when the timeout elapses with the verdict below `:ready`, naming the unmet preconditions. A timeout of zero SHALL evaluate the verdict once and return, so the same verb serves a container healthcheck.

`flz boot await` SHALL NOT report success on the grounds that the set of things to wait for is empty. A controller that has not yet created its namespace hubs SHALL be reported as not ready.

`flz boot` SHALL expose no import path for the `.flz` ETF format written by the deleted `flz cluster snapshot`. Nothing has written that format since `11a6d848` (2026-05-09), which deleted the producing verb and introduced the durable registry in the same commit, so no reachable node holds a file it could read.

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

### Requirement: Recovery replay SHALL NOT resurrect operators onto dead nodes

ProcessHub recovery replay (`Flezha.Service.HubManager.alter_child_data/2`) SHALL drop
any `{node, pid}` entries that reference nodes absent from the live cluster when it
rewrites an operator's child-spec to an active runner, so the replayed operator is placed on a
live node rather than being treated as "already running" on a gone node. This complements
the generic ProcessHub `post_node_leave` reaping (see `controller-leadership`) and is
defense-in-depth for the case where reaping has not yet fired at replay time.

#### Scenario: Stale dead-node pid does not block respawn
- **GIVEN** an operator's persisted registry entry lists a pid on a node that is no longer in the cluster
- **WHEN** recovery replay runs `alter_child_data`
- **THEN** the dead-node entry SHALL be dropped and the operator SHALL be (re)started on a live node + live runner
- **AND** the operator SHALL resume producing rather than remaining registered-but-silent on the dead node

### Requirement: Recovery and on-demand hub creation SHALL be gated on cluster formation

Recovery-triggering hub creation SHALL occur only after the cluster is formed (or the
bounded degrade-to-solo timeout has elapsed), so a hub is never created on a
not-yet-clustered node and then diverge from its peers. After the gate, the reconciler
owns post-replay convergence (rebind, reissue) so an operator that replay started
but did not bring to production is still healed.

The gate SHALL be one seam that every hub-creating path waits on, and no path SHALL write its own wait: boot-time creation of the declared set, on-demand creation for a connecting runner, and the schedule hub's own creation SHALL all reach the cluster through it. Boot-time creation SHALL wait on the gate once for the whole declared set rather than once per namespace, because cluster formation is a property of the node and not of a namespace. The wait SHALL stay synchronous for the on-demand path, which runs inside the fleet manager's own call and must answer that caller rather than defer it.

#### Scenario: Replay on a freshly-clustered node converges
- **GIVEN** a controller that has just joined the cluster and replayed its registry
- **WHEN** the reconciler runs after `post_recovery_replay`
- **THEN** every replayed operator SHALL be driven to a producing state on a live runner, or marked `FAILED` if no runner can serve it

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

### Requirement: A sole-instance restart preserves operators with no user step

A plain restart of the sole controller SHALL preserve its operator list. The durable registry is never wiped to force a peer sync, so the first reconcile round finds every operator missing from the cluster and starts it, and `/healthz` SHALL honestly report `recovering`/`restoring_registry` until rebound. There SHALL be no arming step and no restart variant that discards the list on purpose.

#### Scenario: a plain sole-instance restart preserves the operator list
- **GIVEN** a sole controller has operators bound
- **WHEN** the controller restarts, with nothing armed beforehand
- **THEN** the first reconcile round replays the operator list from the durable registry, `/healthz` reports `recovering` until rebound, and the operators return.

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
- every entry of the durable namespace catalog, which is user intent.

The union of the two is the node's **declared set**. A namespace in neither source SHALL still receive a hub when a runner connects for it, through the unchanged on-demand path.

Boot-time creation SHALL go through the same start authority as every other hub, so a hub created at boot is indistinguishable from one created on connect in its registry backend, its recovery configuration, its hooks and its recovery-state seeding. It SHALL NOT be performed on the controller's boot sequence: the sequence SHALL complete without waiting for it, and the application SHALL start whether or not the declared set has been created.

A declared hub that fails to start SHALL be retried on the shared backoff ladder, SHALL emit telemetry naming the namespace and the reason, and SHALL NOT crash the node or the creating process. The auto-restore kill-switch SHALL govern boot-time creation: when it is disabled the declared set SHALL be empty and no hub SHALL be created at boot.

The registry-root walk that produces the first source SHALL be the same walk that decides whether an unstarted hub holds a registry, so the two readers of that root share one derivation rather than keeping two.

#### Scenario: A restarted controller recovers before any runner attaches

- **GIVEN** a controller whose disk holds a durable registry for a namespace with operators, and no runner connected
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

A hub SHALL be reclaimed as vacant only when this node holds none of its operators, its declared set is empty, AND its recovery has reached a terminal state. Reclaiming a hub deletes its registry directory, so each of those three is a reason not to.

This is what boot-time creation makes reachable, because it puts hubs exactly where runners are not, and a live-placement read is blind in both of the windows that follow. A hub does not load its durable registry into the live registry when it starts: the rows stay on disk until the reconcile runs, a whole reconcile grace later, and every live read answers "empty" until then. After the reconcile the rows are back but nothing has placed them, because placing an operator needs a runner and there is none. A reclaim keyed on live placement alone would therefore delete, seconds or a grace after boot, the durable state boot-time creation exists to recover.

A hub whose recovery has settled and whose declared set is empty SHALL still be reclaimed, so a node whose fleet does not serve a namespace still leaves that namespace's cluster.

#### Scenario: A replayed registry survives the reclaim

- **GIVEN** a controller that created a hub at boot for a namespace whose registry is on disk, with no runner attached and the reconcile not yet run
- **WHEN** the vacancy reclaim runs for that namespace
- **THEN** the hub SHALL still be running and its registry directory SHALL still be on disk

#### Scenario: A declared operator keeps its hub after recovery settles

- **GIVEN** a controller whose hub was declared to hold an operator, with recovery settled, no runner to place it on and nothing running it
- **WHEN** the vacancy reclaim runs for that namespace
- **THEN** the hub SHALL still be running and its registry directory SHALL still be on disk

#### Scenario: A settled empty hub is still reclaimed

- **GIVEN** a hub whose recovery has reached a terminal state, declared to hold nothing, holding no operators of this node and serving no runners
- **WHEN** the vacancy reclaim runs for it
- **THEN** the hub SHALL be stopped, as before

### Requirement: Readiness SHALL hold until every declared namespace has a hub

The readiness precondition table SHALL carry a row that is unmet while boot-time creation of the declared set has not completed, so a controller that has not yet created its hubs is not reported as ready. It SHALL be unmet before that creation has run at all, which is the window in which readiness would otherwise be answered over an empty hub set. The row SHALL hold the verdict at `:recovering`.

The row SHALL be decided live on every read, so a hub that starts on a later retry SHALL flip the node to ready with no restart and no second boot. It SHALL fail closed with the rest of the table: a fact that cannot be read counts as unmet, never as met. It SHALL be answerable while the supervision tree is still coming up, so reading it SHALL NOT wait on the creating process, whose own first act is to wait on the cluster gate for up to the formation window.

What the row answers SHALL be whether the declared set has been created, not whether every hub is still running. A hub the node later reclaims because it serves none of that namespace's operators is a deliberate teardown: nothing brings such a hub back, so a row that refused for it would hold a healthy controller out of rotation for the rest of its life.

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

- **GIVEN** a controller that created its declared set at boot, and one of those namespaces whose hub it has since reclaimed because it serves none of that namespace's operators
- **WHEN** the readiness verdict is read
- **THEN** that namespace SHALL NOT hold it, and the node SHALL stay ready

### Requirement: A declared operator with no runner SHALL be held unbound, not crash-restarted

A declared operator whose bind finds no runner advertising its factory SHALL remain alive and unbound, recording the outcome on its own state, and SHALL NOT stop. Its rebind SHALL be owned by the convergence loop alone, on the backoff ladder the loop already keys for an unbound operator, so no supervisor restart runs as a second retry mechanism beside it. The Controller SHALL NOT depend on the operator's death to report a bind that found no runner: a start request whose caller is waiting SHALL still be refused, from the operator's recorded outcome rather than from its exit.

The held operator SHALL bind without user action once a runner advertising its factory connects, through the full initialisation path a never-bound operator requires, never through the handover path that assumes a previous runner holds its state.

Holding SHALL be bounded by nothing but the arrival of a runner: a namespace whose runners never return holds its operators indefinitely, reported unbound on every operations surface, and SHALL NOT exhaust its distributed supervisor's restart intensity however many operators it declares.

#### Scenario: A restored operator in a runnerless namespace stays alive

- **WHEN** a controller restores a declared operator into a namespace no runner serves, and its bind wait expires
- **THEN** the operator SHALL still be alive after the wait, its supervisor SHALL record no restart for it, and no crash report SHALL be written

#### Scenario: Thousands of held operators do not take the namespace down

- **WHEN** a namespace declares more operators than its distributed supervisor's restart intensity permits and no runner serves any of them
- **THEN** every operator SHALL still be alive after every bind wait has expired, and the namespace's supervision tree SHALL still be running

#### Scenario: A held operator binds when a runner arrives

- **WHEN** a runner advertising the factory of a held operator connects to its namespace
- **THEN** the convergence loop SHALL bind that operator through the full initialisation path, and the operator SHALL report healthy without user action

#### Scenario: A waiting start request is still refused

- **WHEN** a user starts an operator in a namespace whose runner disappears between admission and bind
- **THEN** the start SHALL be refused with the bind's own reason, and the operator SHALL NOT be left running unbound

### Requirement: Declared-set-write-first ordering for plugin mutations

`Flezha.Service.PluginManager.enable_plugin/2`, `disable_plugin_group/1` (through `detach/3`) and `persist_plugin_opt/5` SHALL commit the plugin mutation to the hub's declared-children set BEFORE issuing the runtime `:plugin_subscribe`/`:plugin_unsubscribe` to the Overwatch. The declared set is named here deliberately: the hub keeps two records of a child, and an auto-recovery hub never replays its process registry from disk, so the declared set is the only one a restart reads. A mutation held only in the registry row is lost by the restart it was written to survive.

The declared-set mutation SHALL be issued through the single start authority (`Flezha.Service.OperatorManager`), per `durable-operator-intent`, and never by the plugin manager reaching into the hub's declared-set API directly. An operator with no declared entry SHALL be refused rather than have one invented for it, so a plugin mutation can never resurrect an operator a stop removed for good.

Where durable starts are disabled, the declared set holds nothing at start either, so the mutation SHALL succeed without writing rather than fail or promise durability it cannot give.

If the durable write fails, the runtime mutation SHALL NOT be issued and the caller SHALL receive the propagated error. A hub that refuses the write because its declared list is parked SHALL be reported as a refusal naming the reason; an enable SHALL NOT answer success while persisting nothing.

If the runtime mutation fails after a successful durable write, and the controller is still running to observe it, the enable SHALL roll back: every durable record the enable wrote SHALL be removed along with the in-memory claim, so a failed enable leaves no entry behind on any surface. A controller that dies between the two runs no rollback, so its write stands and `Lifecycle.complete_init/4` subscribes the plugin from the restored spec on the next start. Self-healing on next start; no user action needed.

#### Scenario: Crash between the durable write and the runtime subscribe self-heals on restart

- **GIVEN** an operator `p1` running; `enable_plugin("kafka_p1", %Plugin{...})` succeeds in committing the plugin to the declared set; the controller VM crashes before the runtime `:plugin_subscribe` is issued
- **WHEN** the controller restarts and the reconcile round starts `p1` from its declared entry, which includes `kafka_p1` in `opts[:plugins]`
- **AND** `p1`'s Overwatch is restored (via ProcessHub's recovery path or peer-redistribution)
- **THEN** `Lifecycle.complete_init/4` reads `operator.opts[:plugins]` and `Plugins.prepare/2` subscribes `kafka_p1`; the plugin is live without any user action

#### Scenario: Durable write fails — runtime mutation aborted

- **GIVEN** the hub's durable write is unavailable (the hub is not alive, no leader is reachable, or the declared list is parked)
- **WHEN** `enable_plugin/2` is called and the durable write returns an error
- **THEN** the call returns that error, names the reason, and no `:plugin_subscribe` is issued

#### Scenario: Durable starts disabled — the mutation persists nothing and says so

- **GIVEN** a controller running with auto-restore disabled, which declares nothing at operator start
- **WHEN** `enable_plugin/2` is called
- **THEN** no declared-set write is attempted, the runtime subscribe proceeds, and the caller is not told the plugin is durable

#### Scenario: A failed enable leaves no durable entry

- **GIVEN** `enable_plugin/2` commits the durable write and the subsequent runtime `:plugin_subscribe` fails
- **WHEN** the enable rolls back
- **THEN** every durable record that enable wrote is removed as well as the in-memory claim
- **AND** a later restart SHALL NOT resurrect a plugin whose enable never succeeded
- **AND** the rollback SHALL NOT run on the teardown path, where removing the entry would erase a migrating operator's whole plugin manifest

### Requirement: `flz boot await` SHALL wait for a controller it cannot reach yet

`flz boot await` SHALL treat a controller it cannot reach as not ready yet and ask
again inside the same timeout, rather than exiting on the first failed connect, and
SHALL print nothing while it waits. Its first request SHALL carry the whole timeout and
each later request only what is left of it. A controller that answered SHALL be asked
once: its verdict, ready or not, is the answer. A controller lost in the middle of the
call answers nothing and fails the same way as one never reached, so it SHALL be asked
again inside what is left of the timeout. When the timeout runs out with the controller
still unreachable, the CLI SHALL exit 3 with the error `controller.unreachable`, naming the causes a failed connect
cannot tell apart: a controller that is not up yet or stopped after it was reached, a
cookie that is not the controller's, and a node name that is not the controller's. A
timeout of zero SHALL ask exactly once.

#### Scenario: A controller started a moment ago is waited for

- **GIVEN** a controller launched with `flz controller start --mode daemon` whose distribution is not up yet
- **WHEN** the user runs `flz boot await --timeout 30`
- **THEN** the CLI asks again until the controller answers, prints nothing while it cannot reach it, and exits with the controller's verdict
- **AND** its first request asks the controller for 30 000 ms and every later one for less than the one before

#### Scenario: A controller that never answers is given up on at the timeout

- **WHEN** `flz boot await --timeout 1` cannot reach a controller
- **THEN** the CLI asks more than once, then exits 3 with a line naming a controller not up yet or stopped after it was reached, the cookie and the node name

#### Scenario: A controller that answers not ready is asked once

- **WHEN** the controller answers a verdict below `:ready`
- **THEN** the CLI exits 7 with the error `boot.not_ready`, the verdict and the unmet preconditions in its details, on stderr, and does not ask again

#### Scenario: A zero timeout asks once even when the controller is unreachable

- **WHEN** `flz boot await --timeout 0` cannot reach a controller
- **THEN** the CLI asks exactly once and exits 3 with `controller.unreachable`

### Requirement: A last-runner loss withdraws this controller's groups and the hub moves the operators

When this controller's last placeable runner for a factory disconnects, `Flezha.Runner.FleetManager` SHALL declare the disconnect, SHALL run no migration of its own, and SHALL withdraw the factory's group from the namespace hub as `operator-placement` states: after `group_withdraw_grace_ms`, or at once for a retiring runner. The withdrawal SHALL be the only thing that moves the operators: the hub's redistribution reassigns each to a controller still serving the factory and moves it through ProcessHub's migration-consent gate with live handover (`migration_strategy: HotSwap{handover: true}`), as a controller join does. A child that defers SHALL stay put until it consents or reaches `max_defer_time`, when it SHALL be force-moved through the existing best-effort handover. When no controller serves the factory the operators SHALL stay on this node, unbound. The `FleetManager` call handling the disconnect SHALL NOT block on any move. No `Migration.drain/2` SHALL run, so this node's place in the distribution is never held open by a drain marker: a runner reconnecting inside the grace is a placement candidate at once, and one reconnecting after it serves the group again at once.

Once this node's fleet no longer advertises the namespace AND this node hosts none of the namespace's operators, the convergence loop SHALL reclaim the hub here through `Flezha.Service.HubManager.stop_hub_if_vacant/1`, so a controller that cannot host is not handed children by the distribution. That guard SHALL be node-local: a peer's operators never keep this node's hub alive, and this node's own — running or durably suspended — always do. A later runner connect starts the hub again, so the reclaim costs a returning runner nothing.

`ProcessHub.Service.Migration.drain/2` remains the function graceful shutdown and the namespace stop reach through the single seam `Flezha.Service.HubManager.drain_hub/2`, callable from `lib/flezha/shutdown/` only. `drain_hub/2` SHALL wait for the local children to actually be gone before its caller proceeds — the migration start requests are async and HotSwap stops a local child only AFTER the remote start succeeds — by monitoring the hub's local children, never by polling.

#### Scenario: A returning runner is not blocked by a stale drain

- **GIVEN** controller A lost its last runner for a namespace and the withdrawal grace is running
- **WHEN** a runner reconnects to A before it elapses
- **THEN** no drain marker SHALL be holding A out of the distribution, no group SHALL have been withdrawn, and A's operators SHALL be rebound to the runner rather than moved

#### Scenario: The hub moves the operators, and the vacated hub is reclaimed

- **GIVEN** controller A whose last runner for a namespace disconnects, with peer controller B serving the same factories
- **WHEN** the grace elapses
- **THEN** A SHALL withdraw its groups in one announcement, each child SHALL move to B through the consent gate with handover, no `Migration.drain/2` SHALL run, and the `FleetManager` call handling the disconnect SHALL NOT have blocked
- **AND** once A hosts none of the namespace's operators, the loop SHALL stop A's hub, so B is the only node left in the namespace's cluster

