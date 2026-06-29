# executor-controller-connection-healing Specification

## Purpose
TBD - created by archiving change heal-connections-on-restart. Update Purpose after archive.
## Requirements
### Requirement: Executor → Controller data streams SHALL automatically heal across a Controller restart

When the Controller process restarts and an Executor's reconnect loop succeeds against the new Controller process, every Operative that ProcessHub keeps registered against this node + this namespace + this Executor's profile SHALL have its data-stream traffic resume within 5 s of the new Controller reaching ready, without any Executor restart, manual `kubectl rollout restart` cascade, or external `flz operative start` invocation.

The mechanism has two halves:

1. **Stream rebind.** `Flezha.Executor.StreamChannel` SHALL clear its `stream_ref` to `nil` synchronously inside its `handle_info({:flz_closed, _stream_ref, _reason}, state)` clause. A `StreamChannel` whose `stream_ref` is `nil` is "rebindable". `Flezha.Socket.StreamAcceptor` (UDS path) and `Flezha.Transport.Server` (QUIC path) SHALL, when accepting a new data stream for an `executor_id`, look up rebindable channels for that `executor_id` BEFORE spawning a fresh channel, and bind the new `stream_ref` via an atomic `GenServer.call(channel, {:bind, new_ref})` that returns `:ok` only if the channel was nil-ref at call time. On `:already_bound` the acceptor SHALL try the next candidate; if none, spawn fresh.
2. **AddOperative reissue.** `Flezha.Executor.FleetManager.executor_connect/3` SHALL, AFTER the existing `try_migrate_executor` call, enumerate every Operative currently registered in ProcessHub for this `node()` + this namespace whose `assigned_executor` matches the connecting Executor's profile (declared factories overlap the Operative's `factory_id`) AND was NOT already handled by `try_migrate_executor`. For each match, FleetManager SHALL re-issue `AddOperative` to the new Executor via the existing `Overwatch.executor_migrate` → `start_operative_sdk` path, so the Executor rebuilds its local Operative registry and triggers fresh data-stream opens.

The Rust SDK's `command_add_operative` handler SHALL be idempotent: an `AddOperative` for a `operative_id` already in the local registry SHALL be a no-op (logged, not raised).

#### Scenario: Executor's Operatives resume traffic after a `docker compose restart controller`
- **GIVEN** the dev compose stack is up with one Controller, one Rust Executor, and one Operative producing data to a Kafka topic
- **AND** the Executor's reconnect loop is running normally
- **WHEN** the operator runs `docker compose restart controller`
- **AND** the new Controller process reaches ready
- **AND** the Executor's reconnect loop opens a new control channel within its existing backoff window
- **THEN** within 5 s of the Controller reaching ready, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have called `flz operative start`, restarted the Executor container, or issued any other re-assignment operation between the restart and the assertion

#### Scenario: StreamChannel rebinds to a fresh stream_ref instead of dropping traffic
- **GIVEN** a `StreamChannel` for Operative `r1` was bound to `stream_ref_old`
- **AND** the Controller process died and restarted, causing `{:flz_closed, stream_ref_old, _reason}` to fire on the channel
- **WHEN** the Executor reconnects and opens a new data stream that the acceptor routes to this `executor_id`'s `StreamSupervisor`
- **THEN** the acceptor SHALL discover the existing `StreamChannel` is nil-ref and call `{:bind, stream_ref_new}` on it
- **AND** the channel's `stream_ref` SHALL update to `stream_ref_new`
- **AND** subsequent outbound frames from the channel SHALL be sent on `stream_ref_new`, not silently dropped

#### Scenario: Idempotent AddOperative reissue does not crash the SDK
- **GIVEN** the Rust SDK's local registry already contains Operative `r1`
- **WHEN** the Controller issues `AddOperative{operative_id: "r1", ...}` to the SDK as part of the reissue catch-up
- **THEN** the SDK SHALL log "AddOperative: operative already registered, skipping" (or similar) and continue without raising
- **AND** the SDK's local registry SHALL still contain exactly one entry for `r1`

### Requirement: Rust SDK command decode SHALL treat a clean peer close as a transport event, not a panic

The Rust SDK's command-frame decode path (today located at `sdk/rust/src/command/mod.rs:53-54`, currently `CommandRequest::decode(message.as_bytes()).expect("Failed to decode message")`) SHALL distinguish three transport outcomes when reading a frame from the controller's control socket:

1. **Clean peer close** — the read returns 0 bytes. The SDK SHALL classify this as a graceful disconnect, drive the existing reconnect loop, and SHALL NOT panic any runtime thread. The executor process SHALL remain alive across this event.
2. **Malformed frame** — the read returns N bytes (N > 0) and `prost::Message::decode` returns `Err(prost::DecodeError)`. The SDK SHALL log a structured warning containing the decode error's `description` and `stack`, return a typed `Err(CommandDecodeError::Malformed { stack: String })` from `get_handler`, and drive the existing reconnect loop. The SDK SHALL NOT panic any runtime thread.
3. **Well-formed frame** — the read returns N bytes (N > 0) and `prost::Message::decode` returns `Ok`. The SDK SHALL dispatch the command exactly as today.

`get_handler` (or its successor) SHALL return `Result<(CommandActionType, String), CommandDecodeError>`. Every call site within `sdk/rust/src/command/` and adjacent modules SHALL propagate the `Result` or map `Err` to the existing reconnect path; no call site SHALL `.unwrap()` or `.expect()` the result. The literal `.expect("Failed to decode message")` SHALL NOT exist anywhere on this path.

This requirement is a precondition for the existing "Executor → Controller data streams SHALL automatically heal across a Controller restart" requirement: if the SDK panics on the disconnect that immediately precedes reconnect, the 5-second resume contract is unreachable. The production trace `thread '<unnamed>' panicked at /flezha-sdk/sdk/rust/src/command/mod.rs:54:52: Failed to decode message: DecodeError { description: "buffer underflow", stack: [("CommandRequest", "command")] }` observed at 07:40:01 in the flezha_market dev cluster is a violation of this requirement.

#### Scenario: Controller closes the control socket cleanly, SDK reconnects without panic
- **GIVEN** the dev compose stack is up with one Controller, one Rust Executor, and Operatives steadily streaming
- **AND** the operator captures the Executor's PID before the test: `EXEC_PID=$(docker compose -f /home/moon/Code/flezha_market/docker/dev/docker-compose.yml exec -T market-executor cat /var/run/market-executor.pid)`
- **WHEN** the operator runs `docker compose -f /home/moon/Code/flezha_market/docker/dev/docker-compose.yml restart flezha-controller`
- **AND** the new Controller process reaches ready
- **THEN** the Rust Executor process SHALL still be alive (same PID — `docker compose ... exec -T market-executor cat /var/run/market-executor.pid` returns the same value as `EXEC_PID`)
- **AND** the Executor's stderr SHALL NOT contain `thread '<unnamed>' panicked` or `Failed to decode message: DecodeError`
- **AND** within 5 seconds of the new Controller reaching ready, the Kafka topic end-offset for `btc_demo.ohlc.v1` SHALL advance past its pre-restart value (verified via `kafka-get-offsets.sh`)

#### Scenario: Decoder receives 0-byte input on socket EOF and returns a clean signal
- **GIVEN** a unit test in `sdk/rust/src/command/mod.rs`'s `#[cfg(test)] mod tests`
- **WHEN** the test invokes the SDK's command-read entry point with an empty byte slice (`&[]`) simulating EOF
- **THEN** the function SHALL return a value indicating "peer closed" (`Ok(FrameOutcome::PeerClosed)` or the equivalent typed variant chosen in the apply step)
- **AND** the call SHALL NOT panic
- **AND** the call SHALL NOT return `Err(CommandDecodeError::Malformed { … })` (a 0-byte read is not a malformed frame)

#### Scenario: Decoder receives a truncated frame and returns a typed error
- **GIVEN** the same unit test harness
- **WHEN** the test invokes the SDK's command-read entry point with a 3-byte slice that is a valid `prost` length prefix but truncated payload (e.g. `&[0x08, 0x01, 0x12]`)
- **THEN** the function SHALL return `Err(CommandDecodeError::Malformed { stack })` where `stack` contains the `prost` error's `description` and `stack` fields
- **AND** the call SHALL NOT panic
- **AND** the executor's reconnect loop SHALL be driven exactly as for a clean close (the controller is treated as untrustworthy and the SDK reconnects rather than parsing arbitrary bytes)

#### Scenario: Existing 5-second resume contract holds after a clean controller restart
- **GIVEN** the dev compose stack steady-state (same setup as the existing "Executor's Operatives resume traffic after a `docker compose restart controller`" scenario in this capability)
- **WHEN** the operator runs `docker compose ... restart flezha-controller`
- **THEN** the existing scenario's THEN clauses SHALL all hold (offsets advance within 5 s of new-Controller-ready, no `flz operative start` is required, no executor restart is required)
- **AND** the Rust Executor process SHALL have the same PID throughout (this is the new precondition this requirement adds — the existing scenario was silent on it, today it is violated by the panic)

### Requirement: Rust SDK runtime threads SHALL NOT panic on any control-socket transport-edge event

For any byte sequence the SDK reads from the control socket — including 0-byte reads, truncated frames, frames with unknown command variants, frames whose `reference_id` is empty, frames whose `Command` oneof is `None` — the SDK SHALL surface a typed `Result::Err` to its caller and SHALL NOT `panic!`, `.unwrap()`, or `.expect()` from within a runtime thread. The error path SHALL drive the existing reconnect loop (clean transport restart) for transport-edge events (clean close, truncated read) and structured logging plus reconnect for protocol-edge events (unknown variant, missing oneof).

The existing `Command::ExecutorShutdownNotification`, `Command::RestoreInitStateResponse`, and `Command::CompactSegmentResponse` clauses (currently `CommandActionType::NotificationIgnore`) demonstrate the desired pattern for protocol-edge events: log and drop. The decode-edge event class (this requirement) extends the same defensive posture one layer below the variant match.

#### Scenario: `Command` oneof is None and SDK does not panic
- **GIVEN** the SDK receives a well-formed `CommandRequest` whose `command` field is `None` (a future protocol contract violation or a defensive serialization bug)
- **WHEN** the SDK enters `get_handler`'s `wrapper_command.command.unwrap()` site (today on `sdk/rust/src/command/mod.rs:58`)
- **THEN** the SDK SHALL return a typed `Err(CommandDecodeError::EmptyCommand)` (or equivalent) instead of panicking on `.unwrap()`
- **AND** the runtime thread SHALL remain alive
- **AND** the SDK SHALL log a structured warning and continue processing subsequent frames

### Requirement: Executor restart while the Controller is alive SHALL heal stream binding cleanly

When an Executor (Rust/Java SDK) process restarts and its reconnect loop succeeds against the still-alive Controller, the existing `try_migrate_executor` reroute path SHALL continue to issue `AddOperative` for Operatives preserved in the `:inactive` peer entry, AND the new data streams the Executor opens SHALL bind cleanly to nil-ref `StreamChannel`s without silent traffic drops.

#### Scenario: Executor restart with single-replica-in-namespace
- **GIVEN** Executor A is the only Executor in namespace `ns1` on this Controller, running Operatives `r1` and `r2`
- **WHEN** Executor A's container restarts and reconnects
- **THEN** `try_migrate_executor` SHALL re-issue `AddOperative` for `r1` and `r2` against A; data streams MUST rebind to the existing `StreamChannel`s without traffic loss

### Requirement: `FLEZHA_EXECUTOR_KEEPALIVE` controls inactive-Executor retention

The Controller SHALL retain a disconnected Executor's peer entry (including its assigned Operatives in the `:inactive` set) for the duration specified by the `FLEZHA_EXECUTOR_KEEPALIVE` environment variable (default 30 seconds). After that window elapses without reconnect, the Controller SHALL discard the inactive entry and ProcessHub SHALL redistribute its Operatives.

#### Scenario: Reconnect within keepalive preserves Operatives
- **GIVEN** an Executor with assigned Operatives `[r1, r2]` disconnects
- **AND** `FLEZHA_EXECUTOR_KEEPALIVE=30`
- **WHEN** the same Executor reconnects 10 seconds later with the same identity
- **THEN** the Controller SHALL re-issue `AddOperative` for `r1` and `r2` to the reconnected Executor without redistributing them

#### Scenario: Reconnect after keepalive triggers redistribution
- **GIVEN** an Executor with assigned Operatives disconnects and `FLEZHA_EXECUTOR_KEEPALIVE=30`
- **WHEN** no reconnect occurs within 30 seconds
- **THEN** the Controller SHALL discard the inactive entry; ProcessHub SHALL redistribute the Operatives to other Executors via normal placement

### Requirement: Existing `cluster-snapshot-restore` capability SHALL be preserved unchanged

This change SHALL NOT modify any code path used by the manual `flz cluster snapshot` or `flz cluster restore` CLI commands, and SHALL NOT modify the per-operative periodic state-capture mechanism (`overwatch_state_snapshot_interval_ms`). Executors relying on the snapshot/restore CLI for whole-cluster recovery SHALL continue to use it identically.

#### Scenario: Whole-cluster snapshot/restore continues to work end-to-end
- **GIVEN** an agent with several operatives running with non-trivial state captured via the periodic snapshot mechanism
- **WHEN** the executor runs `flz cluster snapshot /tmp/x.flz` then shuts down the agent then starts a fresh agent then runs `flz cluster restore /tmp/x.flz`
- **THEN** the operatives SHALL be restored with their captured state binaries, exactly as before this change

### Requirement: SDK SHALL ship final state to the agent on graceful shutdown so reconnect-resume is loss-free

When the Rust/Java SDK shuts down gracefully (SIGTERM, `Drop` of the SDK handle, explicit `disconnect()` call), it SHALL synchronously serialize each local operative's state via the same per-driver serializer used for `MigrationSerialization`, and emit `ExecutorShutdownNotification{operative_id, serialized_state}` on the control channel for each operative. The agent's `ControlChannel` SHALL forward each notification to the corresponding `Overwatch` GenServer (looked up via `Identificator.overwatch(operative_id)`), which SHALL replace `state.snapshot` with the supplied binary atomically. The agent SHALL respond with `ExecutorShutdownAck{operative_id}` once the snapshot is enqueued.

The SDK SHALL wait at most 2 s per operative for the ack (with parallelism across operatives) and SHALL exit regardless of ack receipt after the timeout. The SDK MUST NOT block the container's shutdown grace period if the agent has crashed or become unresponsive.

`ExecutorShutdownNotification` and `ExecutorShutdownAck` are additive opcodes: agents that don't recognise them SHALL log and ignore (existing protobuf-decoder behaviour); SDKs that don't send them fall back to the periodic-snapshot freshness shipped by `heal-connections-on-restart`.

#### Scenario: Graceful executor shutdown ships fresh state to the agent
- **GIVEN** an executor running operative `p1` with current in-memory state `S_now` and the most recent periodic snapshot at `T - 45s` showing state `S_old`
- **WHEN** the executor receives SIGTERM and runs its graceful-shutdown handler
- **THEN** the executor SHALL serialize `p1`'s current state (yielding `S_now`) and emit `ExecutorShutdownNotification{operative_id: "p1", serialized_state: S_now}` on the control channel
- **AND** the agent's `Overwatch` for `p1` SHALL replace `state.snapshot` with `S_now`
- **AND** the agent SHALL emit `ExecutorShutdownAck{operative_id: "p1"}` back to the executor
- **AND** when a new executor subsequently reconnects and is assigned `p1` via the AddOperative reissue path (delivered by `heal-connections-on-restart`), the state delivered via `maybe_apply_initial_state` SHALL be `S_now` (not `S_old`)

#### Scenario: SDK exits after ack timeout if agent is unresponsive
- **GIVEN** an executor running operative `p1` and the agent process is in the middle of crashing
- **WHEN** the executor receives SIGTERM and emits `ExecutorShutdownNotification{operative_id: "p1", ...}`
- **AND** no `ExecutorShutdownAck` arrives within 2 s
- **THEN** the executor SHALL log "ExecutorShutdownAck timeout for p1" and SHALL proceed to exit
- **AND** the executor SHALL NOT block its container's shutdown grace period waiting for the ack

#### Scenario: Older agent ignores the notification gracefully (forward-compat)
- **GIVEN** an executor running an SDK build that emits `ExecutorShutdownNotification`
- **AND** the agent is an older build that does not recognise the protobuf opcode
- **WHEN** the executor emits the notification on shutdown
- **THEN** the agent SHALL log a "unrecognised control command" warning and discard the message
- **AND** no agent crash, no SDK crash, no protocol downgrade beyond losing this one feature

#### Scenario: Older SDK with no graceful-shutdown handler still works (back-compat)
- **GIVEN** an executor running an SDK build that does not implement the graceful-shutdown handler
- **WHEN** the executor process exits
- **THEN** the agent's `Overwatch` snapshot for each of the executor's operatives SHALL still hold the most recent periodic-snapshot binary (today's behaviour)
- **AND** the AddOperative reissue path SHALL still ship that snapshot to a reconnecting executor — just up to `overwatch_state_snapshot_interval_ms` stale, exactly as `heal-connections-on-restart` ships

#### Scenario: Disabled-snapshot executors still benefit from the shutdown notification
- **GIVEN** an executor's operative is configured with `snapshot_interval: 0` (periodic snapshots disabled — e.g. flezha_market where Postgres is authoritative)
- **WHEN** the executor shuts down gracefully and emits `ExecutorShutdownNotification` for the operative
- **THEN** the agent's `Overwatch` SHALL still accept the notification and store the binary in `state.snapshot`
- **AND** the snapshot SHALL be available for the next AddOperative reissue, even though no periodic snapshots were ever taken

#### Scenario: Notification arrives for a operative whose Overwatch has already migrated away
- **GIVEN** operative `p1`'s `Overwatch` was on this agent but ProcessHub migrated `p1` to a peer agent moments before the executor's shutdown notification arrives
- **WHEN** the agent receives `ExecutorShutdownNotification{operative_id: "p1", ...}`
- **AND** `Identificator.overwatch("p1")` resolves to `:undefined`
- **THEN** the agent SHALL log a warning naming `p1` and SHALL still emit `ExecutorShutdownAck{operative_id: "p1"}` so the SDK can proceed to exit
- **AND** the destination agent's Overwatch already received the latest state via HotSwap handover, so no functional loss

### Requirement: A dev-compose integration test SHALL exercise both restart paths end to end

The repository SHALL include an integration test that uses `docker/dev/docker-compose.yml` (extended with the `rust` executor service that is currently commented out) to exercise the agent-restart and executor-restart healing paths end-to-end. The test MUST use the existing dev compose stack and SHALL NOT introduce new Docker contexts or `docker run` invocations.

#### Scenario: Agent-restart path
- **GIVEN** the dev compose stack is up with one agent, one Rust executor, and one operative producing data to a Kafka topic
- **WHEN** the test runs `docker compose restart agent` and waits for agent ready
- **THEN** within 5 s of agent ready, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have restarted the executor container or issued any explicit re-assignment

#### Scenario: Executor-restart path
- **GIVEN** the same setup
- **WHEN** the test runs `docker compose restart rust` (the executor container) and waits for it to reconnect
- **THEN** within 5 s of the executor's first successful reconnect, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have restarted the agent container or issued any explicit re-assignment

### Requirement: A k8s manifest sanity test SHALL prevent volume/env-var drift

The repository SHALL include a unit test that loads `kubernetes/flezha-daemonset.yaml` and `kubernetes/flezha-configmap.yaml` and asserts the volumes and env vars this change relies on are present and well-formed, so that future deployment-manifest edits cannot silently break the dev/k8s parity.

#### Scenario: Required volumes and env vars are present
- **GIVEN** the k8s manifests at `kubernetes/flezha-daemonset.yaml` and `kubernetes/flezha-configmap.yaml`
- **WHEN** the manifest sanity test runs
- **THEN** the test SHALL assert: a `flezha-sockets` volume exists in the daemonset and is mounted at `/flezha/sockets`; the daemonset's container env contains `FLEZHA_SOCKET_FILES_PATH=/flezha/sockets`; the configmap defines `FLEZHA_EXECUTOR_KEEPALIVE` with a positive integer (in milliseconds)
- **AND** the test SHALL fail with a clear message naming the missing field if any assertion fails

### Requirement: Rust SDK SHALL bound the wait for the controller's registration ack and reconnect on timeout

After the Rust SDK opens a control channel and writes its `connect_control_channel` init frame, it SHALL require the controller to send at least one frame (the assign-executor-id command issued by `FleetManager.executor_connect/3`) within a bounded window. The window SHALL default to 10 s and be overridable via `FLEZHA_CONTROL_ACK_TIMEOUT_MS`. If no frame arrives within the window, the SDK SHALL treat the connection as a non-servicing controller, post `ExecutorMessage::Reconnect`, and drive the existing reconnect loop. Once the first frame arrives the SDK SHALL clear the read deadline and read indefinitely (steady state is unchanged).

This closes the warm-restart hang where the SDK connects to a Controller that accepts the socket but never completes registration — a Controller mid-shutdown (quiesce rejects the connect via `{:error, :shutting_down}` without closing the stream) or a Controller process being replaced during a rolling restart. Without the bound the control reader blocks forever, the executor never reaches the live Controller, and no operative is re-registered (`start_hub` is never called).

The SDK's `connect_control_channel` init write SHALL NOT panic the reader thread on failure: a write error (peer vanished between connect and write) SHALL post `ExecutorMessage::Reconnect` and return, not `.unwrap()`.

#### Scenario: Controller accepts the socket but never acknowledges → SDK reconnects
- **GIVEN** a control channel whose peer reads the `connect_control_channel` frame and then stays silent (a Controller mid-shutdown or being replaced)
- **WHEN** `FLEZHA_CONTROL_ACK_TIMEOUT_MS` elapses with no controller frame
- **THEN** the SDK SHALL post `ExecutorMessage::Reconnect` and SHALL NOT block the reader thread indefinitely
- **AND** the executor SHALL reconnect (backoff loop) until it lands on a Controller that completes registration

#### Scenario: First controller frame clears the ack deadline
- **GIVEN** a freshly opened control channel under the ack deadline
- **WHEN** the controller sends its assign-executor-id command within the window
- **THEN** the SDK SHALL clear the read timeout and read subsequent frames with no deadline
- **AND** steady-state disconnect detection (EOF → reconnect) SHALL be unchanged

### Requirement: Controller-side cold-restart operative startup SHALL self-heal via an end-to-end ping before `:start_operative`

When the Controller restarts and `Flezha.Operative.Overwatch` GenServers come up from durable registry replay, each Overwatch SHALL probe the resolved Executor's end-to-end control-channel health via a `:ping_executor` round-trip BEFORE issuing `{:start_operative, …}` to the Executor's `ControlUnit`. The ping SHALL traverse the same `ControlChannel → SDK → ControlChannel` path that `AddOperative` uses, so a successful round-trip proves the channel is bound, the SDK is listening, and the reply path is intact.

The ping SHALL use a default 2 s timeout (configurable via `:overwatch_ping_timeout_ms`, env `FLEZHA_OVERWATCH_PING_TIMEOUT_MS`). On `:pong` the Overwatch SHALL proceed to `start_operative_sdk/2`. On timeout or `{:error, _}` the Overwatch SHALL invoke `Flezha.Executor.FleetManager.reset_control_channel/1` (clearing the `ControlChannel`'s `stream_ref` so the SDK's next data stream can claim it via the existing rebind path) and SHALL retry the whole sequence under the bounded-retry governor (see "Overwatch bounded retry + parked-error state").

The new `:ping_executor` command and its `PingResponse` SHALL be carried by the existing `CommandRequest` / `CommandResponse` envelopes on the `"control_command"` payload_id; SDKs without the handler SHALL log and ignore (existing Protox unknown-field behaviour).

#### Scenario: Healthy executor responds to ping and `:start_operative` proceeds
- **GIVEN** a operative `r1` whose Overwatch has just resolved live executor `e1`
- **AND** `e1`'s control channel is bound and its SDK is listening
- **WHEN** the Overwatch issues `Flezha.Executor.ControlUnit.ping_executor("e1", timeout_ms: 2_000)`
- **THEN** the call SHALL return `{:ok, :pong}` within 2 s
- **AND** the Overwatch SHALL invoke `start_operative_sdk("e1", r1)` immediately afterwards
- **AND** `reset_control_channel/1` SHALL NOT be invoked

#### Scenario: Stale control channel causes ping timeout, channel reset, retry
- **GIVEN** a operative `r1` whose Overwatch has just resolved executor `e1`
- **AND** `e1`'s `ControlChannel` GenServer is registered but its `stream_ref` points to a closed/stale transport (the SDK has reconnected since the last `AddOperative`)
- **WHEN** the Overwatch issues `ping_executor("e1", timeout_ms: 2_000)`
- **THEN** the call SHALL return `{:error, :timeout}`
- **AND** the Overwatch SHALL call `FleetManager.reset_control_channel("e1")` which sets the channel's `stream_ref` to `nil`
- **AND** the Overwatch SHALL NOT issue `start_operative_sdk/2` on the failed attempt
- **AND** the Overwatch SHALL re-resolve the live executor and retry the ping under the bounded-retry governor's next backoff delay

### Requirement: Overwatch bounded retry + parked-error state

`Flezha.Operative.Overwatch` SHALL, before issuing `:start_operative`, gate on an active executor being reachable: `await_active_executor/3` polls `OperativeManager.assign_executor/2` and confirms readiness (`executor_ready?/1` — the CU is registered AND, when `:overwatch_ping_enabled`, an end-to-end ping returns pong) within a bounded window. Only once an executor is confirmed ready does the bounded `:start_operative` retry run; on exhaustion the operative parks with `init_error` (alive, no upstream I/O) and the convergence Loop drives the un-park rebind. The un-park `:executor_migrate` SHALL run in a supervised task so `:stop_operative` stays responsive mid-retry.

#### Scenario: start gates on executor readiness before retrying
- **GIVEN** a operative is (re)starting and no executor for its factory is reachable yet
- **WHEN** Overwatch runs `await_active_executor/3`
- **THEN** it waits for a ready executor before issuing `:start_operative`, and if none becomes ready within the window it stops/parks rather than pinging a dead binding.

### Requirement: Control-channel liveness SHALL be implemented once in shared `flz_transport` and serve both SDKs

The control-channel ack-deadline and steady-state liveness SHALL be implemented once in
the shared `flz_transport` layer and consumed by BOTH the Rust SDK (`sock_conn`) and the
Java SDK (via `flz_jni`'s reader loop), reading the same `FLEZHA_CONTROL_ACK_TIMEOUT_MS`
configuration — this is the shared form of the existing Rust-only ack-deadline
requirement. The Rust SDK's private ack-deadline copy in `control_unit.rs` SHALL be
removed in favor of the shared implementation, so the liveness invariant lives in exactly
one place and Rust/Java parity holds by construction.

#### Scenario: Both SDKs share one ack-deadline implementation
- **GIVEN** the ack-deadline + liveness logic implemented in `flz_transport`
- **WHEN** either the Rust SDK or the Java SDK opens a control channel
- **THEN** both SHALL enforce the same first-frame deadline from the same shared code path
- **AND** the Rust SDK SHALL no longer carry a separate private ack-deadline implementation

### Requirement: Java SDK SHALL bound the wait for the controller's registration ack and reconnect on timeout

After the Java SDK opens a control channel and sends its handshake, it SHALL require the
controller to send at least one frame within `FLEZHA_CONTROL_ACK_TIMEOUT_MS` (default
10 s). If no frame arrives, the SDK SHALL treat the controller as non-servicing and drive
its `ReconnectCoordinator` to reconnect, exactly as the Rust SDK does. The Java reader's
blocking read (today timeout-less in `flz_jni`) SHALL honor the deadline. A handshake
send failure SHALL drive reconnect, not strand a dead thread. The
`assignLatch`-timeout path SHALL drive reconnect rather than warn-and-continue, and the
`ReconnectCoordinator` wiring SHALL be free of the startup race that can drop the first
connection-lost event.

#### Scenario: Controller accepts but never acknowledges → Java SDK reconnects
- **GIVEN** a Java executor whose controller accepts the control socket then stays silent (mid-shutdown or being replaced)
- **WHEN** `FLEZHA_CONTROL_ACK_TIMEOUT_MS` elapses with no controller frame
- **THEN** the Java SDK SHALL trigger `ReconnectCoordinator` and SHALL NOT block its reader thread indefinitely
- **AND** the executor SHALL reconnect until it lands on a controller that completes registration

#### Scenario: First controller frame clears the Java ack deadline
- **WHEN** the controller sends its assign-executor-id command within the window
- **THEN** the Java SDK SHALL clear the deadline and read subsequent frames with no timeout

### Requirement: Steady-state control-channel liveness detects a post-handshake wedge

Both SDKs SHALL run a steady-state liveness check on the control channel beyond the
first-frame ack-deadline — a jittered periodic probe or idle watchdog implemented in
shared `flz_transport` — so a controller that goes silent *after* a successful handshake
(a half-open socket or a hung controller that the OS never surfaces as a read error) is
detected and drives reconnect. The check SHALL NOT fire while normal control traffic is
flowing.

#### Scenario: Controller wedges after handshake
- **GIVEN** an executor with an established control channel that has completed registration
- **WHEN** the controller stops responding on the control channel (half-open / hung) with no clean close
- **THEN** the SDK's steady-state liveness check SHALL detect the silence within its window and drive reconnect

#### Scenario: Active control channel is not flagged
- **GIVEN** a control channel carrying normal control traffic
- **WHEN** the steady-state liveness check runs
- **THEN** it SHALL NOT trigger a reconnect

### Requirement: Control-channel liveness SHALL be enforced on the QUIC transport

The ack-deadline and steady-state liveness SHALL be enforced on the QUIC transport via a
QUIC-native mechanism (e.g. a read-with-timeout / timer select), because
`FlezhaStream::set_read_timeout` is a no-op on QUIC; the liveness guarantee SHALL NOT be
silently skipped when `FLEZHA_TRANSPORT=quic`.

#### Scenario: QUIC accept-but-never-ack is detected
- **GIVEN** an executor connected over QUIC to a controller that accepts but never acknowledges
- **WHEN** the ack window elapses
- **THEN** the SDK SHALL detect the dead control channel and reconnect, not rely solely on QUIC idle/keepalive expiry

### Requirement: Reconnect backoff SHALL be jittered and converged across SDKs

Both SDKs SHALL use a single shared reconnect backoff schedule with full jitter so that a
controller restart dropping many executors does not produce a synchronized reconnect
storm. Attempts SHALL remain unbounded (the executor must keep trying until a live
controller is found).

#### Scenario: Fleet reconnect does not thunder
- **GIVEN** N executors whose controller restarts simultaneously
- **WHEN** they all enter their reconnect loops
- **THEN** their attempts SHALL be spread by jitter rather than firing in lockstep

### Requirement: Java SDK transport-edge and decode events SHALL NOT crash the runtime thread

The Java SDK and its `flz_jni` reader SHALL NOT crash the runtime thread on any
control-socket event — clean peer close (0-byte read), truncated/malformed frame, unknown
command variant, or missing fields — and SHALL instead surface a typed error and drive
the reconnect path. This is the Java parity of the Rust "runtime threads SHALL NOT panic
on any control-socket transport-edge event" requirement.

#### Scenario: Controller closes the Java SDK control socket cleanly
- **GIVEN** a Java executor with operatives streaming
- **WHEN** the controller process restarts (clean close on the control socket)
- **THEN** the Java executor process SHALL stay alive and reconnect
- **AND** no reader/runtime thread SHALL die unrecoverably

#### Scenario: Java SDK receives a truncated control frame
- **WHEN** the Java reader receives a truncated/malformed frame
- **THEN** it SHALL drive reconnect and SHALL NOT crash the runtime thread

### Requirement: Executor liveness is transport-connection-close only

Executor liveness SHALL be derived solely from transport connection state: a socket close (`executor_disconnected`) drops the executor from the active set immediately and unconditionally (wrapped so side-effect failures cannot leave a ghost). There SHALL be NO proactive liveness ping-sweep that evicts an executor for a missed periodic ping — a single missed ping on a live-but-busy executor MUST NOT trigger eviction or cascade rebind.

#### Scenario: a frozen-but-connected executor is not evicted
- **GIVEN** an executor is briefly frozen (GC / host contention) but its transport connection is open
- **WHEN** the convergence Loop ticks
- **THEN** no eviction or rebind is issued for it (liveness is connection-state, not ping-timeout)
- **AND** when the same executor's connection actually closes, it is dropped from the active set immediately.

### Requirement: Controller computes the binding delta on `ExecutorReconnected`

On receiving `ExecutorReconnected { previous_id, new_id, reason }` the controller SHALL compute the binding delta against `previous_id`: if the SDK and controller agree on the id and bindings exist, it is a zero-frame no-op; otherwise the controller mints/uses the authoritative id, issues `ReassignExecutorId` if needed, and re-issues the operative set. On disconnect, eviction SHALL clear each operative's stale stream mapping via `HubManager.hook_remove_pid/2` so a reconnecting SDK's fresh stream binds cleanly to the nil-ref `StreamChannel`.

#### Scenario: stale stream mappings are cleared on disconnect
- **GIVEN** executor `default_X` with operatives bound to its stream channels disconnects
- **WHEN** the controller evicts `default_X`
- **THEN** each operative's stream mapping on `default_X` is removed (`hook_remove_pid/2`) and the channels become rebindable, so a fresh stream from a reconnecting process binds without a stale-pid conflict.

### Requirement: Convergence eviction is idempotent and safe on an active executor

`FleetManager.evict_executor/1` driven by the convergence Loop SHALL be a no-op when the target executor is currently `:active` (its connection is open), and SHALL be idempotent on an unknown id. The authoritative connect-handler path (`previous_executor_id` presented by the SDK itself) remains unconditional.

#### Scenario: a reconnected executor survives a stale eviction op
- **GIVEN** the Diff snapshot queued `default_X` for eviction but `default_X` reconnected before the heal ran
- **WHEN** `evict_executor(default_X)` executes
- **THEN** FleetManager observes `default_X` is `:active` and performs no eviction, leaving its operatives producing.

