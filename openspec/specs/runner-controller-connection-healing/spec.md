# runner-controller-connection-healing Specification

## Purpose
Defines how a Runner↔Controller connection heals itself without operator intervention. Covers automatic data-stream recovery across a Controller restart, identity-preserving reconnect (bindings are re-pointed at the new session rather than minted fresh and evicted — see capability `runner-identity-and-session`), and stale stream-mapping cleanup on disconnect so a reconnecting session binds cleanly. The goal is that a Controller restart costs no Runner restart, no `kubectl rollout restart` cascade, and no manual `flz operative start`.
## Requirements
### Requirement: Runner → Controller data streams SHALL automatically heal across a Controller restart

When the Controller process restarts and a Runner's reconnect loop succeeds against the new Controller process, every Operative that ProcessHub keeps registered against this node + this namespace + this Runner's profile SHALL have its data-stream traffic resume within 5 s of the new Controller reaching ready, without any Runner restart, manual `kubectl rollout restart` cascade, or external `flz operative start` invocation.

The mechanism has two halves:

1. **Stream rebind.** `Flezha.Runner.StreamChannel` SHALL clear its `stream_ref` to `nil` synchronously inside its `handle_info({:flz_closed, _stream_ref, _reason}, state)` clause. A `StreamChannel` whose `stream_ref` is `nil` is "rebindable". `Flezha.Runner.StreamAcceptor` (UDS path) and `Flezha.Transport.Server` (QUIC path) SHALL, when accepting a new data stream for a `runner_id`, look up rebindable channels for that `runner_id` BEFORE spawning a fresh channel, and bind the new `stream_ref` via an atomic `GenServer.call(channel, {:bind, new_ref})` that returns `:ok` only if the channel was nil-ref at call time. On `:already_bound` the acceptor SHALL try the next candidate; if none, spawn fresh.
2. **AddOperative reissue.** `Flezha.Runner.FleetManager.runner_connect/3` SHALL, AFTER the existing `try_migrate_runner` call, enumerate every Operative currently registered in ProcessHub for this `node()` + this namespace whose `assigned_runner` matches the connecting Runner's profile (declared factories overlap the Operative's `factory_id`) AND was NOT already handled by `try_migrate_runner`. For each match, FleetManager SHALL re-issue `AddOperative` to the new Runner via the existing `Overwatch.runner_migrate` → `start_operative_sdk` path, so the Runner rebuilds its local Operative registry and triggers fresh data-stream opens.

The Rust SDK's `command_add_operative` handler SHALL be idempotent: an `AddOperative` for a `operative_id` already in the local registry SHALL be a no-op (logged, not raised).

#### Scenario: Runner's Operatives resume traffic after a `docker compose restart controller`
- **GIVEN** the dev compose stack is up with one Controller, one Rust Runner, and one Operative producing data to a Kafka topic
- **AND** the Runner's reconnect loop is running normally
- **WHEN** the operator runs `docker compose restart controller`
- **AND** the new Controller process reaches ready
- **AND** the Runner's reconnect loop opens a new control channel within its existing backoff window
- **THEN** within 5 s of the Controller reaching ready, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have called `flz operative start`, restarted the Runner container, or issued any other re-assignment procedure between the restart and the assertion

#### Scenario: StreamChannel rebinds to a fresh stream_ref instead of dropping traffic
- **GIVEN** a `StreamChannel` for Operative `r1` was bound to `stream_ref_old`
- **AND** the Controller process died and restarted, causing `{:flz_closed, stream_ref_old, _reason}` to fire on the channel
- **WHEN** the Runner reconnects and opens a new data stream that the acceptor routes to this `runner_id`'s `StreamSupervisor`
- **THEN** the acceptor SHALL discover the existing `StreamChannel` is nil-ref and call `{:bind, stream_ref_new}` on it
- **AND** the channel's `stream_ref` SHALL update to `stream_ref_new`
- **AND** subsequent outbound frames sent through the channel's published binding SHALL go out on `stream_ref_new`, not silently dropped

#### Scenario: Idempotent AddOperative reissue does not crash the SDK
- **GIVEN** the Rust SDK's local registry already contains Operative `r1`
- **WHEN** the Controller issues `AddOperative{operative_id: "r1", ...}` to the SDK as part of the reissue catch-up
- **THEN** the SDK SHALL log "AddOperative: operative already registered, skipping" (or similar) and continue without raising
- **AND** the SDK's local registry SHALL still contain exactly one entry for `r1`

### Requirement: An outbound input frame is sent by its caller from the channel's published binding

`Flezha.Runner.StreamChannel` SHALL publish its bound `stream_ref`, together with its liveness counters, under its registered name — written at init (unbound), on `{:bind, new_ref}` and on `{:flz_closed, ...}` (unbound again), and erased when the channel terminates. A sender — the Overwatch's ingest path and the durable tiers' re-forward — SHALL send through `StreamChannel.io_in/3` with the channel's name: it reads the published binding and pushes the frame to the transport from the calling process, with no round trip through the channel process that every operative on the runner shares. A binding that is unbound, a name that is unknown, or a `nil` channel SHALL answer `{:error, :no_stream}`, bump the channel's `nil_drops` liveness counter where a channel exists, and emit `[:flezha, :stream, :drop]` — so a stall on an unbound stream stays observable to the reconciler, whose probe rebinds it.

#### Scenario: A bound frame leaves from the caller
- **GIVEN** a `StreamChannel` bound to `stream_ref_1`
- **WHEN** an Overwatch ingests an input for an operative mapped to that channel
- **THEN** the frame is pushed on `stream_ref_1` by the Overwatch's own process, the channel's `send_frame_calls` counter advances, and no message is queued at the channel process

#### Scenario: An unbound channel answers no_stream and emits the drop
- **GIVEN** a `StreamChannel` whose stream closed and has not rebound
- **WHEN** an Overwatch ingests an input for it
- **THEN** `io_in/3` answers `{:error, :no_stream}`, the channel's `nil_drops` counter advances, and `[:flezha, :stream, :drop]` fires naming the operative

#### Scenario: A rebind republishes before the channel answers
- **WHEN** the acceptor's `{:bind, stream_ref_new}` call succeeds on a nil-ref channel
- **THEN** the next `io_in/3` on that channel's name sends on `stream_ref_new`; a close republishes the binding as unbound before the channel processes anything else

#### Scenario: A terminated channel's binding is gone
- **WHEN** a `StreamChannel` terminates
- **THEN** its published binding is erased and a sender using its name answers `{:error, :no_stream}` rather than pushing on a stale ref

### Requirement: Rust SDK command decode SHALL treat a clean peer close as a transport event, not a panic

The Rust SDK's command-frame decode path (today located at `sdk/rust/src/command/mod.rs:53-54`, currently `CommandRequest::decode(message.as_bytes()).expect("Failed to decode message")`) SHALL distinguish three transport outcomes when reading a frame from the controller's control socket:

1. **Clean peer close** — the read returns 0 bytes. The SDK SHALL classify this as a graceful disconnect, drive the existing reconnect loop, and SHALL NOT panic any runtime thread. The runner process SHALL remain alive across this event.
2. **Malformed frame** — the read returns N bytes (N > 0) and `prost::Message::decode` returns `Err(prost::DecodeError)`. The SDK SHALL log a structured warning containing the decode error's `description` and `stack`, return a typed `Err(CommandDecodeError::Malformed { stack: String })` from `get_handler`, and drive the existing reconnect loop. The SDK SHALL NOT panic any runtime thread.
3. **Well-formed frame** — the read returns N bytes (N > 0) and `prost::Message::decode` returns `Ok`. The SDK SHALL dispatch the command exactly as today.

`get_handler` (or its successor) SHALL return `Result<(CommandActionType, String), CommandDecodeError>`. Every call site within `sdk/rust/src/command/` and adjacent modules SHALL propagate the `Result` or map `Err` to the existing reconnect path; no call site SHALL `.unwrap()` or `.expect()` the result. The literal `.expect("Failed to decode message")` SHALL NOT exist anywhere on this path.

This requirement is a precondition for the existing "Runner → Controller data streams SHALL automatically heal across a Controller restart" requirement: if the SDK panics on the disconnect that immediately precedes reconnect, the 5-second resume contract is unreachable. The production trace `thread '<unnamed>' panicked at /flezha-sdk/sdk/rust/src/command/mod.rs:54:52: Failed to decode message: DecodeError { description: "buffer underflow", stack: [("CommandRequest", "command")] }` observed at 07:40:01 in the flezha_market dev cluster is a violation of this requirement.

#### Scenario: Controller closes the control socket cleanly, SDK reconnects without panic
- **GIVEN** the dev compose stack is up with one Controller, one Rust Runner, and Operatives steadily streaming
- **AND** the operator captures the Runner's PID before the test: `RUNNER_PID=$(docker compose -f /home/moon/Code/flezha_market/docker/dev/docker-compose.yml exec -T market-executor cat /var/run/market-executor.pid)`
- **WHEN** the operator runs `docker compose -f /home/moon/Code/flezha_market/docker/dev/docker-compose.yml restart flezha-controller`
- **AND** the new Controller process reaches ready
- **THEN** the Rust Runner process SHALL still be alive (same PID — `docker compose ... exec -T market-executor cat /var/run/market-executor.pid` returns the same value as `RUNNER_PID`)
- **AND** the Runner's stderr SHALL NOT contain `thread '<unnamed>' panicked` or `Failed to decode message: DecodeError`
- **AND** within 5 seconds of the new Controller reaching ready, the Kafka topic end-offset for `btc_demo.ohlc.v1` SHALL advance past its pre-restart value (verified via `kafka-get-offsets.sh`)

Note: the `market-executor` container/PID-file name above is owned by the separate `flezha_market` repository and is unchanged by this proposal — see this change's design.md Open Questions.

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
- **AND** the runner's reconnect loop SHALL be driven exactly as for a clean close (the controller is treated as untrustworthy and the SDK reconnects rather than parsing arbitrary bytes)

#### Scenario: Existing 5-second resume contract holds after a clean controller restart
- **GIVEN** the dev compose stack steady-state (same setup as the existing "Runner's Operatives resume traffic after a `docker compose restart controller`" scenario in this capability)
- **WHEN** the operator runs `docker compose ... restart flezha-controller`
- **THEN** the existing scenario's THEN clauses SHALL all hold (offsets advance within 5 s of new-Controller-ready, no `flz operative start` is required, no runner restart is required)
- **AND** the Rust Runner process SHALL have the same PID throughout (this is the new precondition this requirement adds — the existing scenario was silent on it, today it is violated by the panic)

### Requirement: Rust SDK runtime threads SHALL NOT panic on any control-socket transport-edge event

For any byte sequence the SDK reads from the control socket — including 0-byte reads, truncated frames, frames with unknown command variants, frames whose `reference_id` is empty, frames whose `Command` oneof is `None` — the SDK SHALL surface a typed `Result::Err` to its caller and SHALL NOT `panic!`, `.unwrap()`, or `.expect()` from within a runtime thread. The error path SHALL drive the existing reconnect loop (clean transport restart) for transport-edge events (clean close, truncated read) and structured logging plus reconnect for protocol-edge events (unknown variant, missing oneof).

The existing `Command::RunnerShutdownNotification`, `Command::RestoreInitStateResponse`, and `Command::CompactEntityResponse` clauses (currently `CommandActionType::NotificationIgnore`) demonstrate the desired pattern for protocol-edge events: log and drop. The decode-edge event class (this requirement) extends the same defensive posture one layer below the variant match.

#### Scenario: `Command` oneof is None and SDK does not panic
- **GIVEN** the SDK receives a well-formed `CommandRequest` whose `command` field is `None` (a future protocol contract violation or a defensive serialization bug)
- **WHEN** the SDK enters `get_handler`'s `wrapper_command.command.unwrap()` site (today on `sdk/rust/src/command/mod.rs:58`)
- **THEN** the SDK SHALL return a typed `Err(CommandDecodeError::EmptyCommand)` (or equivalent) instead of panicking on `.unwrap()`
- **AND** the runtime thread SHALL remain alive
- **AND** the SDK SHALL log a structured warning and continue processing subsequent frames

### Requirement: Runner restart while the Controller is alive SHALL heal stream binding cleanly

When a Runner (Rust/Java SDK) process restarts and its reconnect loop succeeds against the still-alive Controller, the existing `try_migrate_runner` reroute path SHALL continue to issue `AddOperative` for Operatives preserved in the `:inactive` peer entry, AND the new data streams the Runner opens SHALL bind cleanly to nil-ref `StreamChannel`s without silent traffic drops.

#### Scenario: Runner restart with single-replica-in-namespace
- **GIVEN** Runner A is the only Runner in namespace `ns1` on this Controller, running Operatives `r1` and `r2`
- **WHEN** Runner A's container restarts and reconnects
- **THEN** `try_migrate_runner` SHALL re-issue `AddOperative` for `r1` and `r2` against A; data streams MUST rebind to the existing `StreamChannel`s without traffic loss

### Requirement: `FLEZHA_RUNNER_KEEPALIVE` controls inactive-Runner retention

The Controller SHALL retain a disconnected Runner's peer entry (including its assigned Operatives in the `:inactive` set) for the duration specified by the `FLEZHA_RUNNER_KEEPALIVE` environment variable (default 30 seconds). After that window elapses without reconnect, the Controller SHALL discard the inactive entry and ProcessHub SHALL redistribute its Operatives.

#### Scenario: Reconnect within keepalive preserves Operatives
- **GIVEN** a Runner with assigned Operatives `[r1, r2]` disconnects
- **AND** `FLEZHA_RUNNER_KEEPALIVE=30`
- **WHEN** the same Runner reconnects 10 seconds later with the same identity
- **THEN** the Controller SHALL re-issue `AddOperative` for `r1` and `r2` to the reconnected Runner without redistributing them

#### Scenario: Reconnect after keepalive triggers redistribution
- **GIVEN** a Runner with assigned Operatives disconnects and `FLEZHA_RUNNER_KEEPALIVE=30`
- **WHEN** no reconnect occurs within 30 seconds
- **THEN** the Controller SHALL discard the inactive entry; ProcessHub SHALL redistribute the Operatives to other Runners via normal placement

### Requirement: Existing `cluster-snapshot-restore` capability SHALL be preserved unchanged

This change SHALL NOT modify any code path used by the manual `flz cluster snapshot` or `flz cluster restore` CLI commands, and SHALL NOT modify the per-operative periodic state-capture mechanism (`overwatch_state_snapshot_interval_ms`). Runners relying on the snapshot/restore CLI for whole-cluster recovery SHALL continue to use it identically.

#### Scenario: Whole-cluster snapshot/restore continues to work end-to-end
- **GIVEN** an agent with several operatives running with non-trivial state captured via the periodic snapshot mechanism
- **WHEN** the runner runs `flz cluster snapshot /tmp/x.flz` then shuts down the agent then starts a fresh agent then runs `flz cluster restore /tmp/x.flz`
- **THEN** the operatives SHALL be restored with their captured state binaries, exactly as before this change

### Requirement: SDK SHALL ship final state to the agent on graceful shutdown so reconnect-resume is loss-free

When the Rust/Java SDK shuts down gracefully (SIGTERM, `Drop` of the SDK handle, explicit `disconnect()` call), it SHALL synchronously serialize each local operative's state via the same per-driver serializer used for `MigrationSerialization`, and emit `RunnerShutdownNotification{operative_id, serialized_state}` on the control channel for each operative. The agent's `ControlChannel` SHALL forward each notification to the corresponding `Overwatch` GenServer (looked up via `Identificator.overwatch(operative_id)`), which SHALL replace `state.snapshot` with the supplied binary atomically. The agent SHALL respond with `RunnerShutdownAck{operative_id}` once the snapshot is enqueued.

The SDK SHALL wait at most 2 s per operative for the ack (with parallelism across operatives) and SHALL exit regardless of ack receipt after the timeout. The SDK MUST NOT block the container's shutdown grace period if the agent has crashed or become unresponsive.

`RunnerShutdownNotification` and `RunnerShutdownAck` are additive opcodes: agents that don't recognise them SHALL log and ignore (existing protobuf-decoder behaviour); SDKs that don't send them fall back to the periodic-snapshot freshness shipped by `heal-connections-on-restart`.

#### Scenario: Graceful runner shutdown ships fresh state to the agent
- **GIVEN** a runner running operative `p1` with current in-memory state `S_now` and the most recent periodic snapshot at `T - 45s` showing state `S_old`
- **WHEN** the runner receives SIGTERM and runs its graceful-shutdown handler
- **THEN** the runner SHALL serialize `p1`'s current state (yielding `S_now`) and emit `RunnerShutdownNotification{operative_id: "p1", serialized_state: S_now}` on the control channel
- **AND** the agent's `Overwatch` for `p1` SHALL replace `state.snapshot` with `S_now`
- **AND** the agent SHALL emit `RunnerShutdownAck{operative_id: "p1"}` back to the runner
- **AND** when a new runner subsequently reconnects and is assigned `p1` via the AddOperative reissue path (delivered by `heal-connections-on-restart`), the state delivered via `maybe_apply_initial_state` SHALL be `S_now` (not `S_old`)

#### Scenario: SDK exits after ack timeout if agent is unresponsive
- **GIVEN** a runner running operative `p1` and the agent process is in the middle of crashing
- **WHEN** the runner receives SIGTERM and emits `RunnerShutdownNotification{operative_id: "p1", ...}`
- **AND** no `RunnerShutdownAck` arrives within 2 s
- **THEN** the runner SHALL log "RunnerShutdownAck timeout for p1" and SHALL proceed to exit
- **AND** the runner SHALL NOT block its container's shutdown grace period waiting for the ack

#### Scenario: Older agent ignores the notification gracefully (forward-compat)
- **GIVEN** a runner running an SDK build that emits `RunnerShutdownNotification`
- **AND** the agent is an older build that does not recognise the protobuf opcode
- **WHEN** the runner emits the notification on shutdown
- **THEN** the agent SHALL log a "unrecognised control command" warning and discard the message
- **AND** no agent crash, no SDK crash, no protocol downgrade beyond losing this one feature

#### Scenario: Older SDK with no graceful-shutdown handler still works (back-compat)
- **GIVEN** a runner running an SDK build that does not implement the graceful-shutdown handler
- **WHEN** the runner process exits
- **THEN** the agent's `Overwatch` snapshot for each of the runner's operatives SHALL still hold the most recent periodic-snapshot binary (today's behaviour)
- **AND** the AddOperative reissue path SHALL still ship that snapshot to a reconnecting runner — just up to `overwatch_state_snapshot_interval_ms` stale, exactly as `heal-connections-on-restart` ships

#### Scenario: Disabled-snapshot runners still benefit from the shutdown notification
- **GIVEN** a runner's operative is configured with `snapshot_interval: 0` (periodic snapshots disabled — e.g. flezha_market where Postgres is authoritative)
- **WHEN** the runner shuts down gracefully and emits `RunnerShutdownNotification` for the operative
- **THEN** the agent's `Overwatch` SHALL still accept the notification and store the binary in `state.snapshot`
- **AND** the snapshot SHALL be available for the next AddOperative reissue, even though no periodic snapshots were ever taken

#### Scenario: Notification arrives for a operative whose Overwatch has already migrated away
- **GIVEN** operative `p1`'s `Overwatch` was on this agent but ProcessHub migrated `p1` to a peer agent moments before the runner's shutdown notification arrives
- **WHEN** the agent receives `RunnerShutdownNotification{operative_id: "p1", ...}`
- **AND** `Identificator.overwatch("p1")` resolves to `:undefined`
- **THEN** the agent SHALL log a warning naming `p1` and SHALL still emit `RunnerShutdownAck{operative_id: "p1"}` so the SDK can proceed to exit
- **AND** the destination agent's Overwatch already received the latest state via HotSwap handover, so no functional loss

### Requirement: A dev-compose integration test SHALL exercise both restart paths end to end

The repository SHALL include an integration test that uses `docker/dev/docker-compose.yml` (extended with the `rust` runner service that is currently commented out) to exercise the agent-restart and runner-restart healing paths end-to-end. The test MUST use the existing dev compose stack and SHALL NOT introduce new Docker contexts or `docker run` invocations.

#### Scenario: Agent-restart path
- **GIVEN** the dev compose stack is up with one agent, one Rust runner, and one operative producing data to a Kafka topic
- **WHEN** the test runs `docker compose restart agent` and waits for agent ready
- **THEN** within 5 s of agent ready, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have restarted the runner container or issued any explicit re-assignment

#### Scenario: Runner-restart path
- **GIVEN** the same setup
- **WHEN** the test runs `docker compose restart rust` (the runner container) and waits for it to reconnect
- **THEN** within 5 s of the runner's first successful reconnect, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have restarted the agent container or issued any explicit re-assignment

### Requirement: A k8s manifest sanity test SHALL prevent volume/env-var drift

The repository SHALL include a unit test that loads `kubernetes/flezha-daemonset.yaml` and `kubernetes/flezha-configmap.yaml` and asserts the volumes and env vars this change relies on are present and well-formed, so that future deployment-manifest edits cannot silently break the dev/k8s parity.

#### Scenario: Required volumes and env vars are present
- **GIVEN** the k8s manifests at `kubernetes/flezha-daemonset.yaml` and `kubernetes/flezha-configmap.yaml`
- **WHEN** the manifest sanity test runs
- **THEN** the test SHALL assert: a `flezha-sockets` volume exists in the daemonset and is mounted at `/flezha/sockets`; the daemonset's container env contains `FLEZHA_SOCKET_FILES_PATH=/flezha/sockets`; the configmap defines `FLEZHA_RUNNER_KEEPALIVE` with a positive integer (in milliseconds)
- **AND** the test SHALL fail with a clear message naming the missing field if any assertion fails

### Requirement: Rust SDK SHALL bound the wait for the controller's registration ack and reconnect on timeout

After the Rust SDK opens a control channel and writes its `connect_control_channel` init frame, it SHALL require the controller to send at least one frame (the assign-runner-id command issued by `FleetManager.runner_connect/3`) within a bounded window. The window SHALL default to 10 s and be overridable via `FLEZHA_CONTROL_ACK_TIMEOUT_MS`. If no frame arrives within the window, the SDK SHALL treat the connection as a non-servicing controller, post `RunnerMessage::Reconnect`, and drive the existing reconnect loop. Once the first frame arrives the SDK SHALL clear the read deadline and read indefinitely (steady state is unchanged).

This closes the warm-restart hang where the SDK connects to a Controller that accepts the socket but never completes registration — a Controller mid-shutdown (quiesce rejects the connect via `{:error, :shutting_down}` without closing the stream) or a Controller process being replaced during a rolling restart. Without the bound the control reader blocks forever, the runner never reaches the live Controller, and no operative is re-registered (`start_hub` is never called).

The SDK's `connect_control_channel` init write SHALL NOT panic the reader thread on failure: a write error (peer vanished between connect and write) SHALL post `RunnerMessage::Reconnect` and return, not `.unwrap()`.

#### Scenario: Controller accepts the socket but never acknowledges → SDK reconnects
- **GIVEN** a control channel whose peer reads the `connect_control_channel` frame and then stays silent (a Controller mid-shutdown or being replaced)
- **WHEN** `FLEZHA_CONTROL_ACK_TIMEOUT_MS` elapses with no controller frame
- **THEN** the SDK SHALL post `RunnerMessage::Reconnect` and SHALL NOT block the reader thread indefinitely
- **AND** the runner SHALL reconnect (backoff loop) until it lands on a Controller that completes registration

#### Scenario: First controller frame clears the ack deadline
- **GIVEN** a freshly opened control channel under the ack deadline
- **WHEN** the controller sends its assign-runner-id command within the window
- **THEN** the SDK SHALL clear the read timeout and read subsequent frames with no deadline
- **AND** steady-state disconnect detection (EOF → reconnect) SHALL be unchanged

### Requirement: Controller-side cold-restart operative startup SHALL self-heal via an end-to-end ping before `:start_operative`

When the Controller restarts and `Flezha.Operative.Overwatch` GenServers come up from durable registry replay, each Overwatch SHALL probe the resolved Runner's end-to-end control-channel health via a `:ping_runner` round-trip BEFORE issuing `{:start_operative, …}` to the Runner's `ControlUnit`. The ping SHALL traverse the same `ControlChannel → SDK → ControlChannel` path that `AddOperative` uses, so a successful round-trip proves the channel is bound, the SDK is listening, and the reply path is intact.

The ping SHALL use a default 2 s timeout (configurable via `:overwatch_ping_timeout_ms`, env `FLEZHA_OVERWATCH_PING_TIMEOUT_MS`). On `:pong` the Overwatch SHALL proceed to `start_operative_sdk/2`. On a ping error that reports a dead transport, and on the bounded-retry governor's LAST attempt whatever the reason, the Overwatch SHALL invoke `Flezha.Runner.FleetManager.reset_control_channel/1` (clearing the `ControlChannel`'s `stream_ref` so the SDK's next data stream can claim it via the existing rebind path). A ping that merely timed out on an earlier attempt SHALL NOT reset the channel: the channel is live and carries every other instance bound to that runner, and a busy runner is retried, not cut. Every failed attempt SHALL retry the whole sequence under the bounded-retry governor (see "Overwatch bounded retry + parked-error state"). The control-channel reader SHALL NOT perform disk I/O, so a commit storm on a runner cannot by itself push its ping past the timeout.

The new `:ping_runner` command and its `PingResponse` SHALL be carried by the existing `CommandRequest` / `CommandResponse` envelopes on the `"control_command"` payload_id; SDKs without the handler SHALL log and ignore (existing Protox unknown-field behaviour).

#### Scenario: Healthy runner responds to ping and `:start_operative` proceeds
- **GIVEN** a operative `r1` whose Overwatch has just resolved live runner `e1`
- **AND** `e1`'s control channel is bound and its SDK is listening
- **WHEN** the Overwatch issues `Flezha.Runner.ControlUnit.ping_runner("e1", timeout_ms: 2_000)`
- **THEN** the call SHALL return `{:ok, :pong}` within 2 s
- **AND** the Overwatch SHALL invoke `start_operative_sdk("e1", r1)` immediately afterwards
- **AND** `reset_control_channel/1` SHALL NOT be invoked

#### Scenario: Stale control channel causes ping failures, channel reset on the last attempt, retry
- **GIVEN** a operative `r1` whose Overwatch has just resolved runner `e1`
- **AND** `e1`'s `ControlChannel` GenServer is registered but its `stream_ref` points to a closed/stale transport (the SDK has reconnected since the last `AddOperative`)
- **WHEN** the Overwatch issues `ping_runner("e1", timeout_ms: 2_000)` and it fails on every attempt of the bounded-retry governor
- **THEN** the Overwatch SHALL NOT issue `start_operative_sdk/2` on any failed attempt
- **AND** the Overwatch SHALL re-resolve the live runner and retry the ping under the governor's backoff delays
- **AND** on the last attempt the Overwatch SHALL call `FleetManager.reset_control_channel("e1")` which sets the channel's `stream_ref` to `nil`

#### Scenario: A busy runner's timed-out ping does not cut the live channel
- **GIVEN** runner `e1` is bound to a hundred instances and its control channel is saturated by their starts
- **WHEN** one Overwatch's first ping attempt returns `{:error, :timeout}` and a later attempt returns `{:ok, :pong}`
- **THEN** `reset_control_channel("e1")` SHALL NOT have been invoked and no other instance on `e1` SHALL have seen `{:error, :no_connection_available}`

### Requirement: Overwatch bounded retry + parked-error state

`Flezha.Operative.Overwatch` SHALL, before issuing `:start_operative`, gate on an active runner being reachable: `await_active_runner/3` polls `OperativeManager.assign_runner/2` and confirms readiness (`runner_ready?/1` — the CU is registered AND, when `:overwatch_ping_enabled`, an end-to-end ping returns pong) within a bounded window. Only once a runner is confirmed ready does the bounded `:start_operative` retry run; on exhaustion the operative parks with `init_error` (alive, no upstream I/O) and the convergence Loop drives the un-park rebind. The un-park `:runner_migrate` SHALL run in a supervised task so `:stop_operative` stays responsive mid-retry.

#### Scenario: start gates on runner readiness before retrying
- **GIVEN** a operative is (re)starting and no runner for its factory is reachable yet
- **WHEN** Overwatch runs `await_active_runner/3`
- **THEN** it waits for a ready runner before issuing `:start_operative`, and if none becomes ready within the window it stops/parks rather than pinging a dead binding.

### Requirement: Control-channel liveness SHALL be implemented once in shared `flz_transport` and serve both SDKs

The control-channel ack-deadline and steady-state liveness SHALL be implemented once in
the shared `flz_transport` layer and consumed by BOTH the Rust SDK (`sock_conn`) and the
Java SDK (via `flz_core_ffi`'s JNI reader loop), reading the same `FLEZHA_CONTROL_ACK_TIMEOUT_MS`
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
blocking read SHALL honor the deadline. A handshake
send failure SHALL drive reconnect, not strand a dead thread. The
`assignLatch`-timeout path SHALL drive reconnect rather than warn-and-continue, and the
`ReconnectCoordinator` wiring SHALL be free of the startup race that can drop the first
connection-lost event.

#### Scenario: Controller accepts but never acknowledges → Java SDK reconnects
- **GIVEN** a Java runner whose controller accepts the control socket then stays silent (mid-shutdown or being replaced)
- **WHEN** `FLEZHA_CONTROL_ACK_TIMEOUT_MS` elapses with no controller frame
- **THEN** the Java SDK SHALL trigger `ReconnectCoordinator` and SHALL NOT block its reader thread indefinitely
- **AND** the runner SHALL reconnect until it lands on a controller that completes registration

#### Scenario: First controller frame clears the Java ack deadline
- **WHEN** the controller sends its assign-runner-id command within the window
- **THEN** the Java SDK SHALL clear the deadline and read subsequent frames with no timeout

### Requirement: Steady-state control-channel liveness detects a post-handshake wedge

Both SDKs SHALL run a steady-state liveness check on the control channel beyond the
first-frame ack-deadline — a jittered periodic probe or idle watchdog implemented in
shared `flz_transport` — so a controller that goes silent *after* a successful handshake
(a half-open socket or a hung controller that the OS never surfaces as a read error) is
detected and drives reconnect. The check SHALL NOT fire while normal control traffic is
flowing.

#### Scenario: Controller wedges after handshake
- **GIVEN** a runner with an established control channel that has completed registration
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
- **GIVEN** a runner connected over QUIC to a controller that accepts but never acknowledges
- **WHEN** the ack window elapses
- **THEN** the SDK SHALL detect the dead control channel and reconnect, not rely solely on QUIC idle/keepalive expiry

### Requirement: Reconnect backoff SHALL be jittered and converged across SDKs

Both SDKs SHALL use a single shared reconnect backoff schedule with full jitter so that a
controller restart dropping many runners does not produce a synchronized reconnect
storm. Attempts SHALL remain stream (the runner must keep trying until a live
controller is found).

#### Scenario: Fleet reconnect does not thunder
- **GIVEN** N runners whose controller restarts simultaneously
- **WHEN** they all enter their reconnect loops
- **THEN** their attempts SHALL be spread by jitter rather than firing in lockstep

### Requirement: Java SDK transport-edge and decode events SHALL NOT crash the runtime thread

The Java SDK and its JNI reader SHALL NOT crash the runtime thread on any
control-socket event — clean peer close (0-byte read), truncated/malformed frame, unknown
command variant, or missing fields — and SHALL instead surface a typed error and drive
the reconnect path. This is the Java parity of the Rust "runtime threads SHALL NOT panic
on any control-socket transport-edge event" requirement.

#### Scenario: Controller closes the Java SDK control socket cleanly
- **GIVEN** a Java runner with operatives streaming
- **WHEN** the controller process restarts (clean close on the control socket)
- **THEN** the Java runner process SHALL stay alive and reconnect
- **AND** no reader/runtime thread SHALL die unrecoverably

#### Scenario: Java SDK receives a truncated control frame
- **WHEN** the Java reader receives a truncated/malformed frame
- **THEN** it SHALL drive reconnect and SHALL NOT crash the runtime thread

### Requirement: Convergence eviction is idempotent and safe on an active runner

`FleetManager.evict_runner/2` driven by the convergence Loop SHALL be a no-op when the target runner is currently `:active` (its connection is open), and SHALL be idempotent on an unknown id. The authoritative connect-handler path (`previous_runner_id` presented by the SDK itself) remains unconditional.

The guard protects a runner that is merely *observed* healthy against a stale snapshot. It SHALL NOT protect one the data-plane classifier PROVED wedged: an eviction carrying `proved_wedged: true` SHALL proceed despite the open connection. A pool-wedged runner heartbeats and holds its socket while processing nothing, so its connection being open is exactly what would otherwise make it unevictable forever.

#### Scenario: a reconnected runner survives a stale eviction proc
- **GIVEN** the Diff snapshot queued `default_X` for eviction but `default_X` reconnected before the heal ran
- **WHEN** `evict_runner(default_X)` executes with no proved-wedge reason
- **THEN** FleetManager observes `default_X` is `:active` and performs no eviction, leaving its operatives producing.

#### Scenario: a classifier-proved wedged runner is evicted while connected
- **GIVEN** `default_X` is in the live active set and heartbeating, and the classifier reported `runner_pool_wedged` for it
- **WHEN** `evict_runner(default_X, proved_wedged: true)` executes
- **THEN** FleetManager evicts it despite the open connection, and its operatives rebind onto a live runner.

### Requirement: Runner liveness is session liveness, derived from one signal

Runner liveness SHALL be a property of the control-channel **session**, derived from a single signal: the inbound control-frame stream. The controller's `Flezha.Runner.ControlChannel` SHALL stamp a monotonic `last_seen` on every inbound control frame — including the keepalive ping the SDK already sends (formerly echoed and discarded) — and SHALL run one jittered watchdog that treats **sustained silence past a configurable window** as a connection close, routing it through the existing `{:flz_closed}` → `Flezha.Runner.FleetManager` `{:runner_disconnected}` path. A real socket close SHALL continue to drop the runner immediately and unconditionally (wrapped so side-effect failures cannot leave a ghost).

The window SHALL be **generous** (default 45 s, i.e. 3× the keepalive interval) and jittered, and the action SHALL be connection-close, NOT a redistribute sweep. There SHALL be NO controller-initiated periodic ping-sweep and NO eviction triggered by a *single* missed ping: a live-but-busy runner (whose independent keepalive timer keeps sending) and a briefly frozen runner (GC / host contention shorter than the window) MUST NOT be declared dead. The `ControlUnit` reply-watchdog SHALL NOT perform liveness eviction; its `fail_stuck_refs` behaviour MAY remain solely to unblock stuck callers.

QUIC SHALL enforce the same window via an explicit `max_idle_timeout` (no longer inheriting the transport library's default), so UDS and QUIC declare death within the same configured window.

**A close the controller itself initiated SHALL be diagnosed as expected, and diagnosed differently from a close it did not.** Before issuing a disconnect the controller SHALL record a *retiring* mark with a deadline for that runner id, and SHALL clear it when the disconnect completes. The mark SHALL ride the same TTL-expiring marks mechanism the runner-session fence already uses, pruned by the same periodic tick, and SHALL be readable without a process hop — a channel asking a GenServer whether its own close was expected would be asking the process that is busy tearing it down. The close handlers SHALL read the mark and report accordingly: an expected close SHALL be logged at `:info` naming the runner, and an unexpected close SHALL keep its existing `:warning`. The mark SHALL bound the difference to **reporting alone** — a marked close and an unmarked close SHALL drop the runner, clear the stream binding and reset credit identically, so the unconditional-drop guarantee above holds in both cases. The mark SHALL expire after a configurable window so that a stale mark cannot silence the diagnosis of a later, genuine close of the same reused runner id.

#### Scenario: an idle wedged-but-connected runner is detected on the control plane

- **GIVEN** a runner whose control connection is open but whose process has wedged, with no control request in flight
- **WHEN** no inbound control frame (including keepalive ping) arrives for longer than the configured liveness window
- **THEN** the controller's `ControlChannel` watchdog SHALL close the connection through the existing `{:runner_disconnected}` path
- **AND** this SHALL hold on UDS as well as QUIC

#### Scenario: a frozen-but-connected runner within the window is not evicted

- **GIVEN** a runner briefly frozen (GC / host contention) for less than the liveness window
- **WHEN** the watchdog ticks
- **THEN** no close, eviction, or rebind SHALL be issued for it
- **AND** a single missed keepalive ping SHALL NOT trigger any of these
- **AND** when its connection actually closes, it SHALL be dropped from the active set immediately

#### Scenario: the keepalive ping is the liveness signal, not a discarded echo

- **GIVEN** a steady-state control channel carrying only periodic keepalive pings
- **WHEN** a keepalive ping arrives
- **THEN** the controller SHALL update `last_seen` for that session
- **AND** the watchdog SHALL NOT fire while pings continue to arrive within the window

#### Scenario: a controller-initiated disconnect reports as expected

- **GIVEN** a runner the controller is deliberately disconnecting (`disconnect_runner`, `stop_namespace`, or a bench fleet teardown above them)
- **WHEN** its control and data sockets close
- **THEN** each close SHALL be logged at `:info` naming the runner, no `:warning` SHALL be emitted for it, and no incident telemetry SHALL be raised on account of the close

#### Scenario: an expected close drops the runner exactly like an unexpected one

- **GIVEN** two runners, one marked retiring and one not
- **WHEN** each one's socket closes
- **THEN** both SHALL be dropped from the active set immediately, both SHALL have their stream binding cleared and credit reset, and the only observable difference SHALL be the severity of the log line and the absence of the incident emit

#### Scenario: a stale mark does not silence a real close

- **GIVEN** a runner marked retiring whose mark was never cleared and has passed its expiry window
- **WHEN** that runner id reconnects and later closes unexpectedly
- **THEN** the close SHALL be reported as unexpected, at `:warning`, with its incident telemetry intact

### Requirement: Controller resolves a reconnect by swapping the binding, not minting-and-evicting

On any runner reconnect the controller SHALL resolve identity by the runner's presented **stable identity** (see capability `runner-identity-and-session`) and SHALL re-point that stable identity's existing operative bindings at the new session, rather than minting a fresh durable id and evicting the previous one. The controller SHALL NOT perform an unconditional `previous_runner_id` pre-eviction, and the in-process-reconnect operative reissue SHALL actually re-bind operatives (it SHALL NOT be a no-op stub). On disconnect, each operative's stale stream mapping SHALL be cleared (`HubManager.hook_remove_pid/2`) so the reconnecting session's fresh streams bind cleanly to nil-ref `StreamChannel`s.

#### Scenario: fresh-process restart preserves bindings without a pre-evict

- **GIVEN** a runner with operatives `[r1, r2]` whose process restarts and reconnects within the keepalive window, presenting the same persisted stable identity
- **WHEN** the controller handles the connect
- **THEN** it SHALL swap `r1` and `r2`'s bindings onto the new session token
- **AND** it SHALL NOT mint a new durable identity nor issue an unconditional eviction of a "previous" id

#### Scenario: in-process reconnect actually re-binds operatives

- **GIVEN** a runner whose control stream dropped but whose process survived, holding operatives `[r1]`
- **WHEN** it reconnects and the controller's view and the runner's view of bindings diverge
- **THEN** the controller SHALL re-bind `r1` onto the new session (not rely on a no-op reissue plus a later convergence tick)

### Requirement: A runner resolves its UDS socket directory from an ordered ladder

The socket directory SHALL be resolved by one implementation shared by every SDK, walking these
candidates in order:

1. `FLEZHA_SOCKET_FILES_PATH` when set — used outright, with no fallback to any later candidate;
2. `$XDG_RUNTIME_DIR/flezha` when `XDG_RUNTIME_DIR` is set;
3. `/run/flezha`;
4. `app_data/sockets`, relative to the working directory.

The controller SHALL take the first candidate it can create; a runner SHALL take the first
candidate holding a live control socket. When no variable is set and more than one candidate
holds a live control socket, a runner SHALL refuse to connect and SHALL name every candidate it
found, rather than selecting one.

A relative candidate SHALL be reported as its resolved absolute path wherever it is named to an
operator.

#### Scenario: Two processes with no configuration find each other
- **WHEN** a controller and a runner are started as separate processes by the same user with no Flezha environment variables set, from different working directories
- **THEN** both resolve the same candidate and the runner's control channel reaches the controller

#### Scenario: An explicit path never falls back
- **WHEN** `FLEZHA_SOCKET_FILES_PATH` names a directory holding no control socket, and a live control socket exists at a later ladder candidate
- **THEN** the runner does not connect, and its diagnosis names the configured path only

#### Scenario: Two live candidates are refused, not ranked
- **WHEN** no socket-path variable is set and live control sockets exist at both `$XDG_RUNTIME_DIR/flezha` and `/run/flezha`
- **THEN** the runner refuses to connect and reports both absolute paths and the variable that resolves the ambiguity

#### Scenario: A relative candidate is reported absolutely
- **WHEN** a runner resolves the `app_data/sockets` candidate and reports the path
- **THEN** the reported path is absolute

### Requirement: A QUIC runner dials a list of addresses re-resolved on every attempt

`FLEZHA_QUIC_ADDRESSES` SHALL accept a comma-separated list whose entries are each either a
literal address or a host name, with an optional port defaulting to `4433`. Every entry SHALL be
resolved on **every** connection attempt, never cached from a previous attempt or from startup,
and the resolved addresses SHALL be flattened into the candidate set that attempt walks. An entry
that fails to resolve SHALL count as one failed candidate and SHALL NOT abort the attempt.

`FLEZHA_QUIC_ADDRESS` SHALL NOT be honoured. Set alone, it SHALL be refused at startup naming the
replacement.

A failed QUIC connection SHALL name the address it dialled.

#### Scenario: A name is resolved rather than parsed
- **WHEN** `FLEZHA_QUIC_ADDRESSES` names a host that resolves to a controller's address
- **THEN** the runner connects, having performed a name resolution rather than an address parse

#### Scenario: A moved controller is followed without reconfiguration
- **WHEN** a connected runner's controller restarts at a different address behind the same name
- **THEN** the runner re-resolves the name on a subsequent attempt and connects to the new address with no configuration change

#### Scenario: An unresolvable entry does not mask a healthy one
- **WHEN** the list holds one name that does not resolve and one that resolves to a live controller
- **THEN** the runner connects through the second entry

#### Scenario: The retired variable is refused, not ignored
- **WHEN** `FLEZHA_QUIC_ADDRESS` is set and `FLEZHA_QUIC_ADDRESSES` is not
- **THEN** startup is refused with a message naming `FLEZHA_QUIC_ADDRESSES`

#### Scenario: A QUIC failure names its address
- **WHEN** a QUIC connection attempt fails
- **THEN** the reported error contains the address that was dialled

### Requirement: A reconnect prefers the address it last connected through

A runner that has connected at least once SHALL attempt that address first on every subsequent
attempt, and SHALL order the remaining candidates randomly. The preference SHALL be held for the
process lifetime and SHALL NOT be persisted across a runner restart.

Reconnecting to the controller that already holds a runner's bindings is a binding swap; a
reconnect elsewhere is a fresh connect that leaves the previous controller to heal that runner's
operatives independently.

#### Scenario: A reconnect returns to the same controller
- **WHEN** a runner connected through one of several candidates loses its connection and the same controller is reachable again
- **THEN** the first attempt of the reconnect is against that address, and the controller resolves the reconnect as a binding swap

#### Scenario: A fleet-wide restart does not converge on one candidate
- **WHEN** several runners sharing a candidate list reconnect after their controllers restart and no last-good address is reachable
- **THEN** the order in which each runner walks the remaining candidates is randomised per runner

### Requirement: A failed connection is diagnosed once before it is retried

Before entering its retry loop a runner SHALL emit exactly one diagnosis distinguishing these
situations and naming the fix for each:

| Observation | Reported meaning |
|---|---|
| socket directory absent | no controller has used this path |
| directory present, control socket absent | controller not running, or still starting |
| control socket present, access denied | the socket directory is the access boundary |
| control socket present, connection refused | a stale socket from a controller that died |

The access-denied diagnosis SHALL name the directory's owning uid and mode, the caller's uid, and
both remedies — running as the owning user, or the opt-in group mode. Every diagnosis SHALL name
the candidates walked. The diagnosis SHALL NOT be repeated on each retry.

#### Scenario: A permission failure is distinguished from a controller that is down
- **WHEN** a runner is started by a user who cannot traverse the socket directory of a running controller
- **THEN** the diagnosis reports the access boundary with both uids and the directory mode, and does not report the controller as not running

#### Scenario: A stale socket is distinguished from a missing one
- **WHEN** a control socket file exists but no process is listening on it
- **THEN** the diagnosis reports a stale socket, not an absent controller

#### Scenario: The diagnosis is emitted once
- **WHEN** a runner retries an unreachable controller across several backoff intervals
- **THEN** the diagnosis appears once and the subsequent retries do not repeat it

### Requirement: `flz runner check` reports controller reachability from a shell

The CLI SHALL provide `flz runner check`, running the same probe a runner runs and printing the
same diagnosis, with an exit status distinguishing reachable from unreachable. It SHALL require
no runner code, no running runner and no controller.

#### Scenario: The check names the resolved path and the verdict
- **WHEN** `flz runner check` is run on a host with a running controller the caller can reach
- **THEN** it prints the resolved absolute socket directory and a reachable verdict, and exits zero

#### Scenario: The check reports the same diagnosis a runner would
- **WHEN** `flz runner check` is run by a user who cannot traverse the socket directory
- **THEN** it prints the access-boundary diagnosis and exits non-zero

### Requirement: The controller prints its attach line on boot

On reaching ready the controller SHALL log one line naming the absolute socket directory it
established and the environment assignment a runner uses to attach to it. When QUIC is enabled
the line SHALL also name the listen address.

#### Scenario: The operator is told how to attach without consulting documentation
- **WHEN** a controller boots successfully
- **THEN** its log contains the resolved absolute socket directory and the `FLEZHA_SOCKET_FILES_PATH` assignment that attaches a runner to it

