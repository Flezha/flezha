# operator-agent-connection-healing Specification

## Purpose
TBD - created by archiving change heal-connections-on-restart. Update Purpose after archive.
## Requirements
### Requirement: Operator → agent data streams SHALL automatically heal across an agent restart

When the agent process restarts and an operator's reconnect loop succeeds against the new agent process, every processor that ProcessHub keeps registered against this node + this namespace + this operator's profile SHALL have its data-stream traffic resume within 5 s of the new agent reaching ready, without any operator restart, manual `kubectl rollout restart` cascade, or external `flz processor start` invocation.

The mechanism has two halves:

1. **Stream rebind.** `Flezha.ComputeEngine.StreamChannel` SHALL clear its `stream_ref` to `nil` synchronously inside its `handle_info({:flz_closed, _stream_ref, _reason}, state)` clause. A `StreamChannel` whose `stream_ref` is `nil` is "rebindable". `Flezha.Socket.StreamAcceptor` (UDS path) and `Flezha.Transport.Server` (QUIC path) SHALL, when accepting a new data stream for a `compute_engine_id`, look up rebindable channels for that `compute_engine_id` BEFORE spawning a fresh channel, and bind the new `stream_ref` via an atomic `GenServer.call(channel, {:bind, new_ref})` that returns `:ok` only if the channel was nil-ref at call time. On `:already_bound` the acceptor SHALL try the next candidate; if none, spawn fresh.
2. **AddProcessor reissue.** `Flezha.ComputeEngine.FleetManager.compute_engine_connect/3` SHALL, AFTER the existing `try_migrate_compute_engine` call, enumerate every processor currently registered in ProcessHub for this `node()` + this namespace whose `assigned_compute_engine` matches the connecting operator's profile (declared factories overlap the processor's `factory_id`) AND was NOT already handled by `try_migrate_compute_engine`. For each match, FleetManager SHALL re-issue `AddProcessor` to the new operator via the existing `Overwatch.compute_engine_migrate` → `start_processor_sdk` path, so the operator rebuilds its local processor registry and triggers fresh data-stream opens.

The Rust SDK's `command_add_processor` handler SHALL be idempotent: an `AddProcessor` for a `processor_id` already in the local registry SHALL be a no-op (logged, not raised).

#### Scenario: Operator's processors resume traffic after a `docker compose restart agent`
- **GIVEN** the dev compose stack is up with one agent, one Rust operator, and one processor producing data to a Kafka topic
- **AND** the operator's reconnect loop is running normally
- **WHEN** the operator runs `docker compose restart agent`
- **AND** the new agent process reaches ready
- **AND** the operator's reconnect loop opens a new control channel within its existing backoff window
- **THEN** within 5 s of the agent reaching ready, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have called `flz processor start`, restarted the operator container, or issued any other re-assignment operation between the restart and the assertion

#### Scenario: StreamChannel rebinds to a fresh stream_ref instead of dropping traffic
- **GIVEN** a `StreamChannel` for processor `p1` was bound to `stream_ref_old`
- **AND** the agent process died and restarted, causing `{:flz_closed, stream_ref_old, _reason}` to fire on the channel
- **WHEN** the operator reconnects and opens a new data stream that the acceptor routes to this `compute_engine_id`'s `StreamSupervisor`
- **THEN** the acceptor SHALL discover the existing `StreamChannel` is nil-ref (via `:stream_ref_status` call) and call `{:bind, stream_ref_new}` on it
- **AND** the channel's `stream_ref` SHALL update to `stream_ref_new`
- **AND** subsequent outbound frames from the channel SHALL be sent on `stream_ref_new`, not silently dropped

#### Scenario: Idempotent AddProcessor reissue does not crash the SDK
- **GIVEN** the Rust SDK's local registry already contains processor `p1`
- **WHEN** the agent issues `AddProcessor{processor_id: "p1", ...}` to the SDK as part of the reissue catch-up
- **THEN** the SDK SHALL log "AddProcessor: processor already registered, skipping" (or similar) and continue without raising
- **AND** the SDK's local registry SHALL still contain exactly one entry for `p1`

#### Scenario: Two operators with identical factory sets — first reconnect claims local orphans
- **GIVEN** namespace `ns1` had operator A and operator B both connected to the agent before restart, both declaring factory set `{f1, f2}`, with processors `p1` (assigned to A) and `p2` (assigned to B) at restart time
- **AND** the agent restarts
- **WHEN** operator A reconnects first
- **THEN** FleetManager SHALL match A's factory set against ProcessHub's local processors and re-issue `AddProcessor` for both `p1` and `p2` to A
- **AND** when operator B subsequently reconnects, the orphans are already claimed; B SHALL fall through to today's no-orphan path and receive new `AddProcessor` commands only as ProcessHub starts new processors and `child_data_alter` picks B

#### Scenario: ProcessHub redistributes a processor mid-reissue (race)
- **GIVEN** FleetManager has decided to re-issue `AddProcessor{processor_id: "p1"}` to a reconnecting operator
- **AND** ProcessHub redistributes `p1` to a peer agent at the same moment
- **WHEN** the SDK receives the late `AddProcessor` for `p1`
- **THEN** the SDK SHALL accept it (idempotent — adds to local registry)
- **AND** the SDK SHALL detect on first I/O attempt that `p1` is no longer hosted locally and SHALL log and remove it (existing path; no new behaviour required) — OR the agent's subsequent `RemProcessor` (issued by ProcessHub's redistribution) SHALL remove it
- **AND** no crash SHALL occur on either side

### Requirement: Operator restart while the agent is alive SHALL heal stream binding cleanly

When an operator (Rust/Java SDK) process restarts and its reconnect loop succeeds against the still-alive agent, the existing `try_migrate_compute_engine` reroute path (`fleet_manager.ex:288-305`) SHALL continue to issue `AddProcessor` for processors preserved in the `:inactive` peer entry, AND the new data streams the operator opens SHALL bind cleanly to nil-ref `StreamChannel`s without silent traffic drops.

#### Scenario: Operator restart with single-replica-in-namespace (Branch C path)
- **GIVEN** operator A is the only operator in namespace `ns1` on this agent, running processors `p1` and `p2`
- **WHEN** operator A's process dies and restarts within `compute_engine_keepalive` (default 30 s)
- **AND** operator A's reconnect loop opens a new control channel
- **THEN** `try_migrate_compute_engine` SHALL find the `:inactive` peer entry, claim it, and re-issue `AddProcessor` for both `p1` and `p2` (today's behaviour, regression-guarded)
- **AND** when operator A's new data streams attach, they SHALL bind to the existing nil-ref `StreamChannel`s for `p1` and `p2` rather than spawning duplicates

#### Scenario: Operator restart with multiple replicas in namespace (Branch A path)
- **GIVEN** operators A and B both connected to the agent in namespace `ns1`, A running `p1` and B running `p2`
- **WHEN** operator A's process dies (B stays alive)
- **THEN** Branch A (`compute_engine_reroute`) SHALL move `p1` to operator B (today's behaviour)
- **AND** `p1`'s `StreamChannel` SHALL update its binding to B's stream(s), not to A's dead stream_ref
- **AND** when operator A subsequently reconnects, `try_migrate_compute_engine` SHALL find no `:inactive` peer (B already took the work) and FleetManager's reissue catch-up SHALL find no ProcessHub-local processors profile-matching A; A SHALL receive new `AddProcessor` commands only as ProcessHub places new processors on it via `child_data_alter`

### Requirement: Existing `cluster-snapshot-restore` capability SHALL be preserved unchanged

This change SHALL NOT modify any code path used by the manual `flz cluster snapshot` or `flz cluster restore` CLI commands, and SHALL NOT modify the per-processor periodic state-capture mechanism (`overwatch_state_snapshot_interval_ms`). Operators relying on the snapshot/restore CLI for whole-cluster recovery SHALL continue to use it identically.

#### Scenario: Whole-cluster snapshot/restore continues to work end-to-end
- **GIVEN** an agent with several processors running with non-trivial state captured via the periodic snapshot mechanism
- **WHEN** the operator runs `flz cluster snapshot /tmp/x.flz` then shuts down the agent then starts a fresh agent then runs `flz cluster restore /tmp/x.flz`
- **THEN** the processors SHALL be restored with their captured state binaries, exactly as before this change

### Requirement: SDK SHALL ship final state to the agent on graceful shutdown so reconnect-resume is loss-free

When the Rust/Java SDK shuts down gracefully (SIGTERM, `Drop` of the SDK handle, explicit `disconnect()` call), it SHALL synchronously serialize each local processor's state via the same per-driver serializer used for `MigrationSerialization`, and emit `OperatorShutdownNotification{processor_id, serialized_state}` on the control channel for each processor. The agent's `ControlChannel` SHALL forward each notification to the corresponding `Overwatch` GenServer (looked up via `Identificator.overwatch(processor_id)`), which SHALL replace `state.snapshot` with the supplied binary atomically. The agent SHALL respond with `OperatorShutdownAck{processor_id}` once the snapshot is enqueued.

The SDK SHALL wait at most 2 s per processor for the ack (with parallelism across processors) and SHALL exit regardless of ack receipt after the timeout. The SDK MUST NOT block the container's shutdown grace period if the agent has crashed or become unresponsive.

`OperatorShutdownNotification` and `OperatorShutdownAck` are additive opcodes: agents that don't recognise them SHALL log and ignore (existing protobuf-decoder behaviour); SDKs that don't send them fall back to the periodic-snapshot freshness shipped by `heal-connections-on-restart`.

#### Scenario: Graceful operator shutdown ships fresh state to the agent
- **GIVEN** an operator running processor `p1` with current in-memory state `S_now` and the most recent periodic snapshot at `T - 45s` showing state `S_old`
- **WHEN** the operator receives SIGTERM and runs its graceful-shutdown handler
- **THEN** the operator SHALL serialize `p1`'s current state (yielding `S_now`) and emit `OperatorShutdownNotification{processor_id: "p1", serialized_state: S_now}` on the control channel
- **AND** the agent's `Overwatch` for `p1` SHALL replace `state.snapshot` with `S_now`
- **AND** the agent SHALL emit `OperatorShutdownAck{processor_id: "p1"}` back to the operator
- **AND** when a new operator subsequently reconnects and is assigned `p1` via the AddProcessor reissue path (delivered by `heal-connections-on-restart`), the state delivered via `maybe_apply_initial_state` SHALL be `S_now` (not `S_old`)

#### Scenario: SDK exits after ack timeout if agent is unresponsive
- **GIVEN** an operator running processor `p1` and the agent process is in the middle of crashing
- **WHEN** the operator receives SIGTERM and emits `OperatorShutdownNotification{processor_id: "p1", ...}`
- **AND** no `OperatorShutdownAck` arrives within 2 s
- **THEN** the operator SHALL log "OperatorShutdownAck timeout for p1" and SHALL proceed to exit
- **AND** the operator SHALL NOT block its container's shutdown grace period waiting for the ack

#### Scenario: Older agent ignores the notification gracefully (forward-compat)
- **GIVEN** an operator running an SDK build that emits `OperatorShutdownNotification`
- **AND** the agent is an older build that does not recognise the protobuf opcode
- **WHEN** the operator emits the notification on shutdown
- **THEN** the agent SHALL log a "unrecognised control command" warning and discard the message
- **AND** no agent crash, no SDK crash, no protocol downgrade beyond losing this one feature

#### Scenario: Older SDK with no graceful-shutdown handler still works (back-compat)
- **GIVEN** an operator running an SDK build that does not implement the graceful-shutdown handler
- **WHEN** the operator process exits
- **THEN** the agent's `Overwatch` snapshot for each of the operator's processors SHALL still hold the most recent periodic-snapshot binary (today's behaviour)
- **AND** the AddProcessor reissue path SHALL still ship that snapshot to a reconnecting operator — just up to `overwatch_state_snapshot_interval_ms` stale, exactly as `heal-connections-on-restart` ships

#### Scenario: Disabled-snapshot operators still benefit from the shutdown notification
- **GIVEN** an operator's processor is configured with `snapshot_interval: 0` (periodic snapshots disabled — e.g. flezha_market where Postgres is authoritative)
- **WHEN** the operator shuts down gracefully and emits `OperatorShutdownNotification` for the processor
- **THEN** the agent's `Overwatch` SHALL still accept the notification and store the binary in `state.snapshot`
- **AND** the snapshot SHALL be available for the next AddProcessor reissue, even though no periodic snapshots were ever taken

#### Scenario: Notification arrives for a processor whose Overwatch has already migrated away
- **GIVEN** processor `p1`'s `Overwatch` was on this agent but ProcessHub migrated `p1` to a peer agent moments before the operator's shutdown notification arrives
- **WHEN** the agent receives `OperatorShutdownNotification{processor_id: "p1", ...}`
- **AND** `Identificator.overwatch("p1")` resolves to `:undefined`
- **THEN** the agent SHALL log a warning naming `p1` and SHALL still emit `OperatorShutdownAck{processor_id: "p1"}` so the SDK can proceed to exit
- **AND** the destination agent's Overwatch already received the latest state via HotSwap handover, so no functional loss

### Requirement: A dev-compose integration test SHALL exercise both restart paths end to end

The repository SHALL include an integration test that uses `docker/dev/docker-compose.yml` (extended with the `rust` operator service that is currently commented out) to exercise the agent-restart and operator-restart healing paths end-to-end. The test MUST use the existing dev compose stack and SHALL NOT introduce new Docker contexts or `docker run` invocations.

#### Scenario: Agent-restart path
- **GIVEN** the dev compose stack is up with one agent, one Rust operator, and one processor producing data to a Kafka topic
- **WHEN** the test runs `docker compose restart agent` and waits for agent ready
- **THEN** within 5 s of agent ready, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have restarted the operator container or issued any explicit re-assignment

#### Scenario: Operator-restart path
- **GIVEN** the same setup
- **WHEN** the test runs `docker compose restart rust` (the operator container) and waits for it to reconnect
- **THEN** within 5 s of the operator's first successful reconnect, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have restarted the agent container or issued any explicit re-assignment

### Requirement: A k8s manifest sanity test SHALL prevent volume/env-var drift

The repository SHALL include a unit test that loads `kubernetes/flezha-daemonset.yaml` and `kubernetes/flezha-configmap.yaml` and asserts the volumes and env vars this change relies on are present and well-formed, so that future deployment-manifest edits cannot silently break the dev/k8s parity.

#### Scenario: Required volumes and env vars are present
- **GIVEN** the k8s manifests at `kubernetes/flezha-daemonset.yaml` and `kubernetes/flezha-configmap.yaml`
- **WHEN** the manifest sanity test runs
- **THEN** the test SHALL assert: a `flezha-sockets` volume exists in the daemonset and is mounted at `/flezha/sockets`; the daemonset's container env contains `FLEZHA_SOCKET_FILES_PATH=/flezha/sockets`; the configmap defines `FLEZHA_OPERATOR_KEEPALIVE` with a positive integer (in milliseconds)
- **AND** the test SHALL fail with a clear message naming the missing field if any assertion fails
