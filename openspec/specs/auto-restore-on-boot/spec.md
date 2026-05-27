# auto-restore-on-boot Specification

## Purpose

The auto-restore-on-boot capability provides automatic, operator-free recovery of the Flezha cluster's running state across controller restarts, full-cluster cold boots, ProcessHub redistribution, and executor pod restarts. It activates ProcessHub's `durable-registry` and `coordinator-bootstrap-recovery` capabilities per namespace, registers Flezha-side hooks to coordinate replay with FleetManager readiness, and wires per-Partition WAL/snapshot restoration into the data-stream attach path. The capability subsumes the prior `cluster-snapshot-restore` and `plugin-subscription-persistence` capabilities — boot-survival no longer requires operator-driven snapshot/restore CLI nor a parallel plugin-subscription store.
## Requirements
### Requirement: Configure ProcessHub for durable registry and auto-recovery per namespace

`Flezha.Service.HubManager.process_hub/1` SHALL pass `registry_backend: {:dets, [path: <path>]}` and `auto_recovery: <config>` to every `ProcessHub.child_spec/1` call. This activates ProcessHub's `durable-registry` and `coordinator-bootstrap-recovery` capabilities per namespace.

The DETS path SHALL resolve to `app_data/process_hub/<hub_id>/registry.dets` by default, with the root configurable via `:flezha, :process_hub_registry_root` (env var `FLEZHA_PROCESS_HUB_REGISTRY_ROOT`).

The recovery configuration SHALL be `[recovery_window_ms: <flezha_recovery_window_ms>, replay_timeout_ms: <flezha_boot_replay_timeout_ms>]` where the values come from runtime config: `:flezha, :recovery_window_ms` (default `10_000`) and `:flezha, :boot_replay_timeout_ms` (default `60_000`).

The kill-switch `:flezha, :auto_restore_enabled` (env var `FLEZHA_AUTO_RESTORE_ENABLED`, default `true`) SHALL gate the `auto_recovery` field: when `false`, pass `auto_recovery: false` to `ProcessHub.child_spec` so ProcessHub transitions immediately to `:normal` without replay or peer handshake.

The atom in the registry-backend tuple MUST resolve to a real backend module via `ProcessHub.Initializer.resolve_registry_backend/1` — currently exactly `:dets` (→ `ProcessHub.Service.Storage.Dets`) or `:ets` (→ `ProcessHub.Service.Storage.Ets`). Backend identifiers that route to non-existent modules SHALL NOT appear in `lib/flezha/service/hub_manager.ex`'s `registry_backend_config/1`. In particular, `:durable_ets` was observed as a regression in the commit immediately following `b0a00e7 Implement auto-restore-on-boot`: `ProcessHub.Initializer.resolve_registry_backend/1`'s `{module, opts}` fall-through clause treated `:durable_ets` as a module name and crashed `setup_storage/2` on first executor connect with `(UndefinedFunctionError) function :durable_ets.open/2 is undefined (module :durable_ets is not available)`, killing every fresh controller boot. If a future upstream `process_hub` ships an actual `Storage.DurableEts` backend (DETS-on-disk + ETS-fast-reads), this requirement MUST be amended to allow `:durable_ets` only AFTER `deps/process_hub/lib/process_hub/service/storage/durable_ets.ex` exists AND `ProcessHub.Initializer.resolve_registry_backend/1` carries an explicit `{:durable_ets, opts}` clause.

#### Scenario: Default config opts in to durable + auto-recovery

- **GIVEN** an application boots with default Flezha config
- **WHEN** `HubManager` constructs the ProcessHub child spec for namespace `"default"`
- **THEN** the spec contains `registry_backend: {:dets, [path: "app_data/process_hub/<hub_id>/registry.dets"]}` and `auto_recovery: [recovery_window_ms: 10_000, replay_timeout_ms: 60_000]`

#### Scenario: Kill-switch disables auto-recovery

- **GIVEN** `FLEZHA_AUTO_RESTORE_ENABLED=false`
- **WHEN** the spec is constructed
- **THEN** the spec contains `auto_recovery: false`; the persistent registry is still configured (mutations still durably written) but no recovery state machine engages — ProcessHub jumps to `:normal` immediately

#### Scenario: Registry-backend atom resolves to a real backend

- **WHEN** `Flezha.Service.HubManager.registry_backend_config/1` is called for any `hub_id` with `:auto_restore_enabled` at its `true` default
- **THEN** the returned tuple's first element is `:dets`
- **AND** `ProcessHub.Initializer.resolve_registry_backend/1` routes that atom to `ProcessHub.Service.Storage.Dets`
- **AND** the matching backend module file exists at `deps/process_hub/lib/process_hub/service/storage/dets.ex`

#### Scenario: Boot succeeds end-to-end against a fresh image

- **GIVEN** the controller is started from a freshly-built `alfetahe/flezha:latest-dev` image with an empty `app_data/process_hub/` directory
- **WHEN** an SDK opens its first UDS `connect_control_channel` frame
- **THEN** `ProcessHub.Initializer.setup_storage/2` returns successfully for `:default_hub`
- **AND** `Flezha.Service.HubManager.register_handler/2` completes with `:ok`
- **AND** the controller does not crash, the BEAM does not exit, and the agent stays Ready
- **AND** a subsequent `bin/flz relay list` returns exit 0 (empty list on first boot)

### Requirement: `pre_recovery_replay` hook handler waits for FleetManager readiness

`Flezha.Boot.RecoveryHooks` SHALL register a synchronous hook handler for ProcessHub's `pre_recovery_replay` hook on every namespace hub. The handler SHALL:

1. Receive the hook event when ProcessHub is about to start replaying its persistent registry.
2. Poll `Flezha.Executor.FleetManager` for at least one active executor whose advertised factories cover the union of `factory_id` values referenced by the hub's persistent registry rows.
3. Block (with bounded sleep) for up to `:flezha, :pre_replay_fleet_wait_ms` (default `30_000`).
4. Return regardless of result (replay proceeds; missing-factory rows will become pending in the existing `start_children` flow).
5. Log INFO `[boot] pre_recovery_replay: waiting for executors for factories <X, Y, Z>` on entry and `[boot] pre_recovery_replay: ready (or timeout)` on exit.

If the kill-switch `:flezha, :auto_restore_enabled` is `false`, the hook handler SHALL NOT be registered (because `auto_recovery: false` means the hook never fires anyway).

#### Scenario: Handler blocks until at least one matching executor connects

- **GIVEN** ProcessHub's persistent registry contains 3 children using factory `crypto_trades`; no executor is connected yet; coordinator enters `:recovering`
- **WHEN** ProcessHub fires `pre_recovery_replay` synchronously
- **THEN** Flezha's handler blocks; an executor advertising `crypto_trades` connects 2 s later
- **AND** the handler observes the executor and returns; ProcessHub's replay proceeds with the eligible factory

#### Scenario: Handler times out and replay proceeds anyway

- **GIVEN** the persistent registry's factories are all unsupported (no matching executors) and `pre_replay_fleet_wait_ms = 5_000`
- **WHEN** the handler times out at t=5s
- **THEN** the handler returns; ProcessHub's replay proceeds; each child's `start_children` raises in the existing `child_data_alter` hook with `"No active compute engines available"`; ProcessHub's existing per-child error handling captures this; the `recovery_replay_completed` telemetry shows `failed: N`

### Requirement: `post_recovery_replay` hook emits Flezha-side telemetry

`Flezha.Boot.RecoveryHooks` SHALL register an async `post_recovery_replay` handler that emits `[:flezha, :boot, :recovery_complete]` telemetry with `%{hub_id: id, namespace: ns, replay_outcome: term}` metadata. Used by Phoenix LiveDashboard, Prometheus scrapers, and any external observability tooling.

#### Scenario: Telemetry fires after replay

- **WHEN** ProcessHub fires `post_recovery_replay` for a namespace hub
- **THEN** Flezha emits `[:flezha, :boot, :recovery_complete]` with the namespace and ProcessHub's replay outcome; subscribed handlers receive the event

### Requirement: Cspec-write-first ordering for plugin mutations on top of persistent registry

`Flezha.Service.PluginManager.enable_plugin/2` and `disable_plugin/1` SHALL mutate the ProcessHub cspec via `ProcessHub.Service.ProcessRegistry.update/3` BEFORE issuing the runtime `:plugin_subscribe`/`:plugin_unsubscribe` to the Overwatch. ProcessHub's `durable-registry` capability ensures the cspec mutation is durable on disk before `update/3` returns.

If the cspec write fails, the runtime mutation SHALL NOT be issued; the caller SHALL receive `{:error, :registry_unavailable}` (the propagated ProcessHub error).

If the runtime mutation fails after a successful cspec write, the cspec is correct (claims plugin enabled); on next Relay restart, `Overwatch.init/1`'s existing `subscribe_initial_plugins/2` path subscribes the plugin from the cspec. Self-healing on next start; no operator action needed.

The previous self-heal-on-next-operation contract from `plugin-subscription-persistence` is no longer needed — the partial-failure window is closed by the registry-write-first ordering combined with the runtime mutation's retry-on-restart semantics.

#### Scenario: Crash between cspec write and runtime subscribe self-heals on restart

- **GIVEN** a relay `p1` running; `enable_plugin("kafka_p1", %Plugin{...})` succeeds in writing the cspec via `ProcessRegistry.update/3`; the controller VM crashes before the runtime `:plugin_subscribe` is issued
- **WHEN** the controller restarts; ProcessHub's persistent registry contains `p1` with `opts[:plugins]` including `kafka_p1`
- **AND** `p1`'s Overwatch is restored (via ProcessHub's recovery path or peer-redistribution)
- **THEN** `Overwatch.init/1` reads `relay.opts[:plugins]` from the cspec; `subscribe_initial_plugins/2` subscribes `kafka_p1`; the plugin is live without any operator action

#### Scenario: Cspec write fails — runtime mutation aborted

- **GIVEN** ProcessHub's persistent registry backend is unreachable (DETS file IO error)
- **WHEN** `enable_plugin/2` is called and `ProcessRegistry.update/3` returns `{:error, _}`
- **THEN** the call returns `{:error, :registry_unavailable}` and no `:plugin_subscribe` is issued

### Requirement: Partition state restoration fires on first stream attach

When a Partition's data stream attaches to a `StreamChannel` AND the owning Relay has `persistence_enabled = true` AND `Restore.assemble_payload/1` returns a payload with non-empty `snapshot_bytes` OR non-empty `change_log_bytes` AND the Relay did NOT arrive via HotSwap, the controller SHALL invoke `Flezha.Relay.Overwatch.dispatch_restore_init_state/2` for that Partition exactly once per Relay-startup BEFORE allowing input frames to flow.

The Relay's `arrived_via_hotswap?` flag SHALL be set to `true` by `Overwatch.alter_handover_state/2` on the destination of a HotSwap migration. When this flag is set, `dispatch_restore_init_state/2` SHALL NOT fire — the in-band handover state stands.

This works identically across: ProcessHub-driven cold-boot replay, fresh `flz relay start`, ProcessHub redistribution, AddRelay reissue after executor restart.

#### Scenario: ProcessHub replay + Partition stream attach ships RestoreInitState

- **GIVEN** ProcessHub's persistent registry is replayed; a Relay is started; an executor connects and is assigned
- **WHEN** the SDK opens a data stream for a Partition that has on-disk WAL records under `app_data/state/{relay_class}/{shard_id}/{partition_key}.wal`
- **THEN** the controller ships `RestoreInitState{partition_id, snapshot_bytes, change_log_bytes}` over the control channel BEFORE forwarding any input frames

#### Scenario: HotSwap-arrived Partition skips dispatch

- **GIVEN** a Relay arrives via HotSwap; `arrived_via_hotswap? = true`
- **WHEN** the Partition's data stream attaches
- **THEN** `dispatch_restore_init_state/2` SHALL NOT fire; the in-band handover state is the basis for going-forward processing

### Requirement: Pull-on-attach recovers WAL when ProcessHub places a Partition on a node without local state

When `dispatch_restore_init_state/2` fires for a Partition AND `Restore.assemble_payload/1` returns an empty payload AND the cspec indicates a previously-running relay (i.e. ProcessHub's persistent registry held this row before today's controller boot), the controller SHALL attempt a one-shot WAL/snapshot transfer from peer controllers before falling through to cold-start `relay-init-state`.

Protocol:

1. Broadcast `Flezha.Boot.WalTransfer.query/1` Erlang-RPC to all peer controllers serving the same namespace, with the partition key `{relay_class, shard_id, partition_key}`.
2. Each peer responds (timeout `:flezha, :wal_transfer_query_timeout_ms`, default `2_000`) with `:none` or `{:ok, %{last_persisted_seq: N, snapshot_seq: M, file_sizes: %{}}}`.
3. Select the highest-`last_persisted_seq` peer (lexicographic tiebreaker on node name); if all peers respond `:none`, log INFO and proceed with cold start.
4. Stream the selected peer's `app_data/state/{relay_class}/{shard_id}/{partition_key}.{wal,snap.<latest>,outbox.dedup}` files via `WalTransfer.stream/2`. Write to `.transferring` files first, fsync, atomic rename.
5. On peer-disconnect-mid-stream, retry against the next-best peer once; on second failure, delete `.transferring` files, log WARN, fall through to cold start.
6. After successful transfer, re-invoke `Restore.assemble_payload/1` and continue the dispatch flow.
7. Telemetry: `[:flezha, :boot, :wal_pulled]` on success; `[:flezha, :boot, :wal_pull_failed]` on failure.

Pull-on-attach SHALL fire AT MOST ONCE per Partition per cluster lifecycle. Once local WAL exists, future restarts skip the peer query.

#### Scenario: Cold-boot redistribution drift recovered by pull-on-attach

- **GIVEN** a 3-controller cluster; staggered graceful shutdowns; all return together; ProcessHub places relay `X` (which migrated A→B→C) back on its hash-original owner A
- **WHEN** ProcessHub recovers, A's coordinator transitions to `:normal`, the SDK opens X's Partition stream, and `dispatch_restore_init_state/2` fires
- **AND** A's local WAL is older than C's
- **THEN** A broadcasts the WAL-availability query; C responds with the highest seq; A pulls C's WAL files via streaming RPC
- **AND** A's local WAL after pull matches C's pre-shutdown state byte-for-byte

#### Scenario: Pull retries once on peer disconnect

- **GIVEN** A starts pulling 500 MB WAL from C; C crashes at 200 MB
- **WHEN** the streaming RPC errors with `:nodedown`
- **THEN** A retries the query; B responds with the next-best `last_persisted_seq`; A pulls from B
- **AND** if B also fails, A logs WARN, deletes `.transferring` files, and falls through to cold start

#### Scenario: All peers respond `:none` — fall through to cold start

- **GIVEN** the WAL files do not exist on any controller
- **WHEN** pull-on-attach broadcasts the query
- **THEN** all peers respond `:none`; the controller logs INFO and proceeds with `relay-init-state` cold-start chain

### Requirement: HotSwap migration also transfers WAL files (WAL-follows-relay)

When a HotSwap migration completes on the destination controller (`Overwatch.alter_handover_state/2` fires), the destination SHALL pull the source's WAL/snapshot files via `WalTransfer.stream/2` for each Partition, in parallel with consuming the in-memory handover state.

Source-side WAL files SHALL NOT be deleted at HotSwap completion. The destination's `WalWriter` opens the pulled WAL file and continues appending from `last_persisted_seq + 1` going forward. Source orphans are reaped by the existing Retention sweeper.

If the WAL transfer fails, the destination SHALL log ERROR with the partition_id and start with a fresh empty WAL — the in-memory handover state is authoritative; write-ahead-logging resumes from a clean slate.

#### Scenario: HotSwap pulls WAL alongside in-memory handover

- **GIVEN** relay `p1` running on A; HotSwap to B
- **WHEN** B's `alter_handover_state/2` consumes A's in-memory state
- **THEN** B also streams A's WAL files into B's local `app_data/state/...`
- **AND** B's WalWriter continues appending records starting at `seq = last_persisted_seq + 1`
- **AND** A's WAL files remain on A's disk until Retention sweeps them

### Requirement: `hub_migrate` SHALL invoke ProcessHub clean-leave to preserve state

`Flezha.Executor.FleetManager`'s `hub_migrate` path (Branch B/C in `do_handle_executor_disconnect/2`) SHALL invoke `ProcessHub.leave/2` (or whichever ProcessHub-provided leave function honors `migration_strategy: HotSwap{handover: true}`) such that each child relay migrates to a peer controller via state-preserving handover, NOT via direct hub teardown that would orphan state on the leaving node.

This is a verify-and-fix requirement: if the current implementation already invokes `ProcessHub.leave/2`, add a regression test. If it bypasses the migration strategy, change the implementation to honor it.

#### Scenario: Controller loses all executors — relays migrate with state to peers

- **GIVEN** controller A with relay `p1` (state at seq=500); peer controller B with capacity for `p1`'s factory
- **WHEN** A's executors all disconnect; FleetManager enters Branch B and invokes `hub_migrate`
- **THEN** ProcessHub HotSwaps `p1` from A to B with handover; B's `alter_handover_state/2` receives the in-memory state; B's WalTransfer pulls the WAL from A
- **AND** `p1` resumes on B with terminal seq=500; no data loss

### Requirement: State-directory cleanup ONLY on explicit relay stop

State files under `app_data/state/{relay_class}/{shard_id}/{partition_key}.{wal,snap.N,outbox.dedup}` SHALL be removed ONLY when `Flezha.Service.RelayManager.stop_relay/2` is called by an operator. ProcessHub's `child_unregistered_hook` (which fires on HotSwap source-side, redistribution-away, etc.) SHALL NOT delete state files.

`stop_relay/2` SHALL: (a) call `ProcessHub.stop_children/3` (which fires the existing hooks → cspec deletion via persistent registry); (b) on success, recursively delete `app_data/state/{relay_class}/{shard_id}/` for each shard. Skip with WARN log if any `.tmp` or `.rewriting` file is present.

The Retention sweeper handles long-term orphans via `time_travel_retention_hours`.

#### Scenario: HotSwap source-side does NOT delete state files

- **GIVEN** relay `p1` running on controller A with WAL files
- **WHEN** ProcessHub HotSwap migrates `p1` to controller B
- **AND** A's `child_unregistered_hook` fires after the source process terminates
- **THEN** A's persistent-registry row is removed by ProcessHub; A's `app_data/state/...` files SHALL still exist
- **AND** B's persistent-registry row is written by ProcessHub's `post_children_start_hook`; B's WAL is populated by WAL-follows-relay

#### Scenario: flz relay stop deletes state files

- **WHEN** the executor runs `flz relay stop default p1`
- **THEN** `RelayManager.stop_relay/2` calls `ProcessHub.stop_children/3`; on success, recursively deletes `app_data/state/MarketRelay/btc/...`
- **AND** if any `.tmp` or `.rewriting` file is present, deletion is skipped with a WARN log

### Requirement: Single explicitly-tolerated data-loss case

The mechanism SHALL preserve every change accepted (returned `:ok` to the caller) before the failure, modulo the inherent ≤ 10 ms in-flight fsync window AND the following single tolerated case:

- **Partial-cluster restart after full outage**: when the entire cluster has been shut down at some point AND only a SUBSET of controller nodes is later restarted, rows held only on the non-rejoining nodes SHALL be permanently lost.

Every other graceful-shutdown scenario — single-controller restart with peers up, cluster-wide cold boot with all nodes rejoining, executor pod restart, HotSwap migration, rolling deploy, partition heal, mixed-version rolling upgrade — SHALL preserve all accepted changes.

Ungraceful kill (SIGKILL of a controller without orchestration giving it time to migrate) is OUT OF SCOPE for state preservation; whichever node's local disk holds the only WAL copy is what survives.

#### Scenario: Subset rejoin loses subset rows

- **GIVEN** a three-node cluster A/B/C; full graceful cluster shutdown; only A and B come back
- **WHEN** A and B boot, ProcessHub coordinators enter `:recovery_pending`, exchange modes, both transition to `:recovering`, replay their local persistent registries
- **THEN** rows that were only on C's persistent registry are lost; A's and B's rows are replayed
- **AND** the contract is satisfied: this is the explicitly-tolerated case

### Requirement: `flz boot status` and `flz boot await` CLI

The CLI SHALL provide:

- `flz boot status` — for each namespace hub, print: hub_id, ProcessHub `recovery_state` (queried via `ProcessHub.recovery_state/1`), persistent-registry row count, last replay outcome (telemetry-aggregated), pending pull-on-attach transfers.
- `flz boot await [--timeout=N]` — block until every namespace hub reports `:normal` (calls `ProcessHub.await_normal/2` per hub).
- `flz boot import-legacy-snapshot <file> [--force]` — one-shot tool that reads a `.flz` ETF file from the deleted `flz cluster snapshot` and writes its child specs into ProcessHub's persistent registry via `ProcessHub.start_children/3` (which the persistent backend captures durably). Refuses on non-empty registry without `--force`. Scheduled for removal in the next release.

#### Scenario: flz boot status reports per-hub state

- **WHEN** the executor runs `flz boot status` while one namespace hub is `:normal` and another is still `:recovery_pending`
- **THEN** the CLI prints both hubs with their respective states and metrics

#### Scenario: flz boot await blocks until all hubs are :normal

- **WHEN** the executor runs `flz boot await --timeout=30000` while one hub is still in `:recovery_pending`
- **THEN** the CLI blocks; once all hubs reach `:normal` (or the timeout), it returns; on timeout, exits non-zero with which hubs are still pending

### Requirement: Recovery replay SHALL NOT resurrect relays onto dead nodes

ProcessHub recovery replay (`Flezha.Service.HubManager.alter_child_data/2`) SHALL drop
any `{node, pid}` entries that reference nodes absent from the live cluster when it
rewrites a relay's child-spec to an active executor, so the replayed relay is placed on a
live node rather than being treated as "already running" on a gone node. This complements
the generic ProcessHub `post_node_leave` reaping (see `controller-leadership`) and is
defense-in-depth for the case where reaping has not yet fired at replay time.

#### Scenario: Stale dead-node pid does not block respawn
- **GIVEN** a relay's persisted registry entry lists a pid on a node that is no longer in the cluster
- **WHEN** recovery replay runs `alter_child_data`
- **THEN** the dead-node entry SHALL be dropped and the relay SHALL be (re)started on a live node + live executor
- **AND** the relay SHALL resume producing rather than remaining registered-but-silent on the dead node

### Requirement: Recovery and on-demand hub creation SHALL be gated on cluster formation

Recovery-triggering hub creation SHALL occur only after the cluster is formed (or the
bounded degrade-to-solo timeout has elapsed), so a hub is never created on a
not-yet-clustered node and then diverge from its peers. After the gate, the reconciler
owns post-replay convergence (re-place, rebind, reissue) so a relay that replay started
but did not bring to production is still healed.

#### Scenario: Replay on a freshly-clustered node converges
- **GIVEN** a controller that has just joined the cluster and replayed its registry
- **WHEN** the reconciler runs after `post_recovery_replay`
- **THEN** every replayed relay SHALL be driven to a producing state on a live executor, or marked `FAILED` if no executor can serve it

