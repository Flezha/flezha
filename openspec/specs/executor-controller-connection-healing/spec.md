# executor-controller-connection-healing Specification

## Purpose
TBD - created by archiving change heal-connections-on-restart. Update Purpose after archive.
## Requirements
### Requirement: Executor → Controller data streams SHALL automatically heal across a Controller restart

When the Controller process restarts and an Executor's reconnect loop succeeds against the new Controller process, every Relay that ProcessHub keeps registered against this node + this namespace + this Executor's profile SHALL have its data-stream traffic resume within 5 s of the new Controller reaching ready, without any Executor restart, manual `kubectl rollout restart` cascade, or external `flz relay start` invocation.

The mechanism has two halves:

1. **Stream rebind.** `Flezha.Executor.StreamChannel` SHALL clear its `stream_ref` to `nil` synchronously inside its `handle_info({:flz_closed, _stream_ref, _reason}, state)` clause. A `StreamChannel` whose `stream_ref` is `nil` is "rebindable". `Flezha.Socket.StreamAcceptor` (UDS path) and `Flezha.Transport.Server` (QUIC path) SHALL, when accepting a new data stream for an `executor_id`, look up rebindable channels for that `executor_id` BEFORE spawning a fresh channel, and bind the new `stream_ref` via an atomic `GenServer.call(channel, {:bind, new_ref})` that returns `:ok` only if the channel was nil-ref at call time. On `:already_bound` the acceptor SHALL try the next candidate; if none, spawn fresh.
2. **AddRelay reissue.** `Flezha.Executor.FleetManager.executor_connect/3` SHALL, AFTER the existing `try_migrate_executor` call, enumerate every Relay currently registered in ProcessHub for this `node()` + this namespace whose `assigned_executor` matches the connecting Executor's profile (declared factories overlap the Relay's `factory_id`) AND was NOT already handled by `try_migrate_executor`. For each match, FleetManager SHALL re-issue `AddRelay` to the new Executor via the existing `Overwatch.executor_migrate` → `start_relay_sdk` path, so the Executor rebuilds its local Relay registry and triggers fresh data-stream opens.

The Rust SDK's `command_add_relay` handler SHALL be idempotent: an `AddRelay` for a `relay_id` already in the local registry SHALL be a no-op (logged, not raised).

#### Scenario: Executor's Relays resume traffic after a `docker compose restart controller`
- **GIVEN** the dev compose stack is up with one Controller, one Rust Executor, and one Relay producing data to a Kafka topic
- **AND** the Executor's reconnect loop is running normally
- **WHEN** the operator runs `docker compose restart controller`
- **AND** the new Controller process reaches ready
- **AND** the Executor's reconnect loop opens a new control channel within its existing backoff window
- **THEN** within 5 s of the Controller reaching ready, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have called `flz relay start`, restarted the Executor container, or issued any other re-assignment operation between the restart and the assertion

#### Scenario: StreamChannel rebinds to a fresh stream_ref instead of dropping traffic
- **GIVEN** a `StreamChannel` for Relay `r1` was bound to `stream_ref_old`
- **AND** the Controller process died and restarted, causing `{:flz_closed, stream_ref_old, _reason}` to fire on the channel
- **WHEN** the Executor reconnects and opens a new data stream that the acceptor routes to this `executor_id`'s `StreamSupervisor`
- **THEN** the acceptor SHALL discover the existing `StreamChannel` is nil-ref and call `{:bind, stream_ref_new}` on it
- **AND** the channel's `stream_ref` SHALL update to `stream_ref_new`
- **AND** subsequent outbound frames from the channel SHALL be sent on `stream_ref_new`, not silently dropped

#### Scenario: Idempotent AddRelay reissue does not crash the SDK
- **GIVEN** the Rust SDK's local registry already contains Relay `r1`
- **WHEN** the Controller issues `AddRelay{relay_id: "r1", ...}` to the SDK as part of the reissue catch-up
- **THEN** the SDK SHALL log "AddRelay: relay already registered, skipping" (or similar) and continue without raising
- **AND** the SDK's local registry SHALL still contain exactly one entry for `r1`

### Requirement: Rust SDK command decode SHALL treat a clean peer close as a transport event, not a panic

The Rust SDK's command-frame decode path (today located at `sdk/rust/src/command/mod.rs:53-54`, currently `CommandRequest::decode(message.as_bytes()).expect("Failed to decode message")`) SHALL distinguish three transport outcomes when reading a frame from the controller's control socket:

1. **Clean peer close** — the read returns 0 bytes. The SDK SHALL classify this as a graceful disconnect, drive the existing reconnect loop, and SHALL NOT panic any runtime thread. The executor process SHALL remain alive across this event.
2. **Malformed frame** — the read returns N bytes (N > 0) and `prost::Message::decode` returns `Err(prost::DecodeError)`. The SDK SHALL log a structured warning containing the decode error's `description` and `stack`, return a typed `Err(CommandDecodeError::Malformed { stack: String })` from `get_handler`, and drive the existing reconnect loop. The SDK SHALL NOT panic any runtime thread.
3. **Well-formed frame** — the read returns N bytes (N > 0) and `prost::Message::decode` returns `Ok`. The SDK SHALL dispatch the command exactly as today.

`get_handler` (or its successor) SHALL return `Result<(CommandActionType, String), CommandDecodeError>`. Every call site within `sdk/rust/src/command/` and adjacent modules SHALL propagate the `Result` or map `Err` to the existing reconnect path; no call site SHALL `.unwrap()` or `.expect()` the result. The literal `.expect("Failed to decode message")` SHALL NOT exist anywhere on this path.

This requirement is a precondition for the existing "Executor → Controller data streams SHALL automatically heal across a Controller restart" requirement: if the SDK panics on the disconnect that immediately precedes reconnect, the 5-second resume contract is unreachable. The production trace `thread '<unnamed>' panicked at /flezha-sdk/sdk/rust/src/command/mod.rs:54:52: Failed to decode message: DecodeError { description: "buffer underflow", stack: [("CommandRequest", "command")] }` observed at 07:40:01 in the flezha_market dev cluster is a violation of this requirement.

#### Scenario: Controller closes the control socket cleanly, SDK reconnects without panic
- **GIVEN** the dev compose stack is up with one Controller, one Rust Executor, and Relays steadily streaming
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
- **GIVEN** the dev compose stack steady-state (same setup as the existing "Executor's Relays resume traffic after a `docker compose restart controller`" scenario in this capability)
- **WHEN** the operator runs `docker compose ... restart flezha-controller`
- **THEN** the existing scenario's THEN clauses SHALL all hold (offsets advance within 5 s of new-Controller-ready, no `flz relay start` is required, no executor restart is required)
- **AND** the Rust Executor process SHALL have the same PID throughout (this is the new precondition this requirement adds — the existing scenario was silent on it, today it is violated by the panic)

### Requirement: Rust SDK runtime threads SHALL NOT panic on any control-socket transport-edge event

For any byte sequence the SDK reads from the control socket — including 0-byte reads, truncated frames, frames with unknown command variants, frames whose `reference_id` is empty, frames whose `Command` oneof is `None` — the SDK SHALL surface a typed `Result::Err` to its caller and SHALL NOT `panic!`, `.unwrap()`, or `.expect()` from within a runtime thread. The error path SHALL drive the existing reconnect loop (clean transport restart) for transport-edge events (clean close, truncated read) and structured logging plus reconnect for protocol-edge events (unknown variant, missing oneof).

The existing `Command::ExecutorShutdownNotification`, `Command::RestoreInitStateResponse`, and `Command::CompactPartitionResponse` clauses (currently `CommandActionType::NotificationIgnore`) demonstrate the desired pattern for protocol-edge events: log and drop. The decode-edge event class (this requirement) extends the same defensive posture one layer below the variant match.

#### Scenario: `Command` oneof is None and SDK does not panic
- **GIVEN** the SDK receives a well-formed `CommandRequest` whose `command` field is `None` (a future protocol contract violation or a defensive serialization bug)
- **WHEN** the SDK enters `get_handler`'s `wrapper_command.command.unwrap()` site (today on `sdk/rust/src/command/mod.rs:58`)
- **THEN** the SDK SHALL return a typed `Err(CommandDecodeError::EmptyCommand)` (or equivalent) instead of panicking on `.unwrap()`
- **AND** the runtime thread SHALL remain alive
- **AND** the SDK SHALL log a structured warning and continue processing subsequent frames

### Requirement: Executor restart while the Controller is alive SHALL heal stream binding cleanly

When an Executor (Rust/Java SDK) process restarts and its reconnect loop succeeds against the still-alive Controller, the existing `try_migrate_executor` reroute path SHALL continue to issue `AddRelay` for Relays preserved in the `:inactive` peer entry, AND the new data streams the Executor opens SHALL bind cleanly to nil-ref `StreamChannel`s without silent traffic drops.

#### Scenario: Executor restart with single-replica-in-namespace
- **GIVEN** Executor A is the only Executor in namespace `ns1` on this Controller, running Relays `r1` and `r2`
- **WHEN** Executor A's container restarts and reconnects
- **THEN** `try_migrate_executor` SHALL re-issue `AddRelay` for `r1` and `r2` against A; data streams MUST rebind to the existing `StreamChannel`s without traffic loss

### Requirement: `FLEZHA_EXECUTOR_KEEPALIVE` controls inactive-Executor retention

The Controller SHALL retain a disconnected Executor's peer entry (including its assigned Relays in the `:inactive` set) for the duration specified by the `FLEZHA_EXECUTOR_KEEPALIVE` environment variable (default 30 seconds). After that window elapses without reconnect, the Controller SHALL discard the inactive entry and ProcessHub SHALL redistribute its Relays.

#### Scenario: Reconnect within keepalive preserves Relays
- **GIVEN** an Executor with assigned Relays `[r1, r2]` disconnects
- **AND** `FLEZHA_EXECUTOR_KEEPALIVE=30`
- **WHEN** the same Executor reconnects 10 seconds later with the same identity
- **THEN** the Controller SHALL re-issue `AddRelay` for `r1` and `r2` to the reconnected Executor without redistributing them

#### Scenario: Reconnect after keepalive triggers redistribution
- **GIVEN** an Executor with assigned Relays disconnects and `FLEZHA_EXECUTOR_KEEPALIVE=30`
- **WHEN** no reconnect occurs within 30 seconds
- **THEN** the Controller SHALL discard the inactive entry; ProcessHub SHALL redistribute the Relays to other Executors via normal placement

### Requirement: Existing `cluster-snapshot-restore` capability SHALL be preserved unchanged

This change SHALL NOT modify any code path used by the manual `flz cluster snapshot` or `flz cluster restore` CLI commands, and SHALL NOT modify the per-relay periodic state-capture mechanism (`overwatch_state_snapshot_interval_ms`). Executors relying on the snapshot/restore CLI for whole-cluster recovery SHALL continue to use it identically.

#### Scenario: Whole-cluster snapshot/restore continues to work end-to-end
- **GIVEN** an agent with several relays running with non-trivial state captured via the periodic snapshot mechanism
- **WHEN** the executor runs `flz cluster snapshot /tmp/x.flz` then shuts down the agent then starts a fresh agent then runs `flz cluster restore /tmp/x.flz`
- **THEN** the relays SHALL be restored with their captured state binaries, exactly as before this change

### Requirement: SDK SHALL ship final state to the agent on graceful shutdown so reconnect-resume is loss-free

When the Rust/Java SDK shuts down gracefully (SIGTERM, `Drop` of the SDK handle, explicit `disconnect()` call), it SHALL synchronously serialize each local relay's state via the same per-driver serializer used for `MigrationSerialization`, and emit `ExecutorShutdownNotification{relay_id, serialized_state}` on the control channel for each relay. The agent's `ControlChannel` SHALL forward each notification to the corresponding `Overwatch` GenServer (looked up via `Identificator.overwatch(relay_id)`), which SHALL replace `state.snapshot` with the supplied binary atomically. The agent SHALL respond with `ExecutorShutdownAck{relay_id}` once the snapshot is enqueued.

The SDK SHALL wait at most 2 s per relay for the ack (with parallelism across relays) and SHALL exit regardless of ack receipt after the timeout. The SDK MUST NOT block the container's shutdown grace period if the agent has crashed or become unresponsive.

`ExecutorShutdownNotification` and `ExecutorShutdownAck` are additive opcodes: agents that don't recognise them SHALL log and ignore (existing protobuf-decoder behaviour); SDKs that don't send them fall back to the periodic-snapshot freshness shipped by `heal-connections-on-restart`.

#### Scenario: Graceful executor shutdown ships fresh state to the agent
- **GIVEN** an executor running relay `p1` with current in-memory state `S_now` and the most recent periodic snapshot at `T - 45s` showing state `S_old`
- **WHEN** the executor receives SIGTERM and runs its graceful-shutdown handler
- **THEN** the executor SHALL serialize `p1`'s current state (yielding `S_now`) and emit `ExecutorShutdownNotification{relay_id: "p1", serialized_state: S_now}` on the control channel
- **AND** the agent's `Overwatch` for `p1` SHALL replace `state.snapshot` with `S_now`
- **AND** the agent SHALL emit `ExecutorShutdownAck{relay_id: "p1"}` back to the executor
- **AND** when a new executor subsequently reconnects and is assigned `p1` via the AddRelay reissue path (delivered by `heal-connections-on-restart`), the state delivered via `maybe_apply_initial_state` SHALL be `S_now` (not `S_old`)

#### Scenario: SDK exits after ack timeout if agent is unresponsive
- **GIVEN** an executor running relay `p1` and the agent process is in the middle of crashing
- **WHEN** the executor receives SIGTERM and emits `ExecutorShutdownNotification{relay_id: "p1", ...}`
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
- **THEN** the agent's `Overwatch` snapshot for each of the executor's relays SHALL still hold the most recent periodic-snapshot binary (today's behaviour)
- **AND** the AddRelay reissue path SHALL still ship that snapshot to a reconnecting executor — just up to `overwatch_state_snapshot_interval_ms` stale, exactly as `heal-connections-on-restart` ships

#### Scenario: Disabled-snapshot executors still benefit from the shutdown notification
- **GIVEN** an executor's relay is configured with `snapshot_interval: 0` (periodic snapshots disabled — e.g. flezha_market where Postgres is authoritative)
- **WHEN** the executor shuts down gracefully and emits `ExecutorShutdownNotification` for the relay
- **THEN** the agent's `Overwatch` SHALL still accept the notification and store the binary in `state.snapshot`
- **AND** the snapshot SHALL be available for the next AddRelay reissue, even though no periodic snapshots were ever taken

#### Scenario: Notification arrives for a relay whose Overwatch has already migrated away
- **GIVEN** relay `p1`'s `Overwatch` was on this agent but ProcessHub migrated `p1` to a peer agent moments before the executor's shutdown notification arrives
- **WHEN** the agent receives `ExecutorShutdownNotification{relay_id: "p1", ...}`
- **AND** `Identificator.overwatch("p1")` resolves to `:undefined`
- **THEN** the agent SHALL log a warning naming `p1` and SHALL still emit `ExecutorShutdownAck{relay_id: "p1"}` so the SDK can proceed to exit
- **AND** the destination agent's Overwatch already received the latest state via HotSwap handover, so no functional loss

### Requirement: A dev-compose integration test SHALL exercise both restart paths end to end

The repository SHALL include an integration test that uses `docker/dev/docker-compose.yml` (extended with the `rust` executor service that is currently commented out) to exercise the agent-restart and executor-restart healing paths end-to-end. The test MUST use the existing dev compose stack and SHALL NOT introduce new Docker contexts or `docker run` invocations.

#### Scenario: Agent-restart path
- **GIVEN** the dev compose stack is up with one agent, one Rust executor, and one relay producing data to a Kafka topic
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



### Requirement: Controller-side cold-restart relay startup SHALL self-heal via an end-to-end ping before `:start_relay`

When the Controller restarts and `Flezha.Relay.Overwatch` GenServers come up from durable registry replay, each Overwatch SHALL probe the resolved Executor's end-to-end control-channel health via a `:ping_executor` round-trip BEFORE issuing `{:start_relay, …}` to the Executor's `ControlUnit`. The ping SHALL traverse the same `ControlChannel → SDK → ControlChannel` path that `AddRelay` uses, so a successful round-trip proves the channel is bound, the SDK is listening, and the reply path is intact.

The ping SHALL use a default 2 s timeout (configurable via `:overwatch_ping_timeout_ms`, env `FLEZHA_OVERWATCH_PING_TIMEOUT_MS`). On `:pong` the Overwatch SHALL proceed to `start_relay_sdk/2`. On timeout or `{:error, _}` the Overwatch SHALL invoke `Flezha.Executor.FleetManager.reset_control_channel/1` (clearing the `ControlChannel`'s `stream_ref` so the SDK's next data stream can claim it via the existing rebind path) and SHALL retry the whole sequence under the bounded-retry governor (see "Overwatch bounded retry + parked-error state").

The new `:ping_executor` command and its `PingResponse` SHALL be carried by the existing `CommandRequest` / `CommandResponse` envelopes on the `"control_command"` payload_id; SDKs without the handler SHALL log and ignore (existing Protox unknown-field behaviour).

#### Scenario: Healthy executor responds to ping and `:start_relay` proceeds
- **GIVEN** a relay `r1` whose Overwatch has just resolved live executor `e1`
- **AND** `e1`'s control channel is bound and its SDK is listening
- **WHEN** the Overwatch issues `Flezha.Executor.ControlUnit.ping_executor("e1", timeout_ms: 2_000)`
- **THEN** the call SHALL return `{:ok, :pong}` within 2 s
- **AND** the Overwatch SHALL invoke `start_relay_sdk("e1", r1)` immediately afterwards
- **AND** `reset_control_channel/1` SHALL NOT be invoked

#### Scenario: Stale control channel causes ping timeout, channel reset, retry
- **GIVEN** a relay `r1` whose Overwatch has just resolved executor `e1`
- **AND** `e1`'s `ControlChannel` GenServer is registered but its `stream_ref` points to a closed/stale transport (the SDK has reconnected since the last `AddRelay`)
- **WHEN** the Overwatch issues `ping_executor("e1", timeout_ms: 2_000)`
- **THEN** the call SHALL return `{:error, :timeout}`
- **AND** the Overwatch SHALL call `FleetManager.reset_control_channel("e1")` which sets the channel's `stream_ref` to `nil`
- **AND** the Overwatch SHALL NOT issue `start_relay_sdk/2` on the failed attempt
- **AND** the Overwatch SHALL re-resolve the live executor and retry the ping under the bounded-retry governor's next backoff delay

### Requirement: Overwatch bounded retry + parked-error state

`Flezha.Relay.Overwatch.complete_init/3` SHALL wrap its `ping → resolve_init_state → start_relay_sdk` sequence in a bounded retry loop with exponential backoff and full jitter. The default schedule SHALL be 6 attempts at `200ms, 500ms, 1s, 2s, 5s, 10s` (configurable via `:overwatch_retry_attempts` and `:overwatch_retry_backoff_ms`). On every retry the Overwatch SHALL re-resolve the live executor via `RelayManager.assign_executor/2` so a fresh executor that came online during the backoff is picked up.

On exhaustion the Overwatch SHALL park in `init_error` state — the GenServer SHALL remain alive, the error reason SHALL be stored in `state.init_error`, and the supervisor SHALL NOT see a crash. The parked Overwatch SHALL respond:

- `{:error, reason}` to `:registry_executor_set` calls (existing behaviour at `lib/flezha/relay/overwatch.ex:311`),
- `{:reply, {:error, :startup_failed}, state}` to `{:io_in, _, _}` calls (refuse external `io_in` while parked),
- silent no-op + log on `{:io_in, _, _}` casts (drop and increment counter).

A periodic `:retry_startup` timer SHALL fire every 60 s (configurable via `:overwatch_retry_park_interval_ms`) in the parked state and SHALL re-attempt the full sequence so a transient outage recovers without operator intervention. The timer SHALL be cleared once `init_error` is `nil`.

#### Scenario: Bounded retry exits to parked state after N attempts
- **GIVEN** a relay `r1` whose ping or `:start_relay` repeatedly times out (the executor's SDK is gone)
- **WHEN** the Overwatch exhausts its 6 attempts (~19 s aggregate with the default schedule)
- **THEN** the Overwatch SHALL NOT crash
- **AND** `state.init_error` SHALL be set to the last error reason (e.g. `{:start_relay_failed, :timeout}`)
- **AND** the supervisor SHALL NOT receive an `:EXIT` from the Overwatch
- **AND** subsequent `:io_in` calls SHALL receive `{:error, :startup_failed}`

#### Scenario: Periodic background retry recovers from transient outage
- **GIVEN** a relay `r1` parked with `state.init_error == {:start_relay_failed, :timeout}`
- **AND** a fresh executor `e2` comes online in the same namespace + factory after the initial retry burst ends
- **WHEN** the parked-retry timer fires (default 60 s)
- **THEN** the Overwatch SHALL re-resolve the live executor via `RelayManager.assign_executor/2`
- **AND** the ping + `start_relay_sdk` round-trip SHALL succeed
- **AND** `state.init_error` SHALL be cleared to `nil`
- **AND** the parked-retry timer SHALL NOT re-arm
