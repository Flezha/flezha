# runner-controller-connection-healing Specification

## Purpose
Defines how a Runner↔Controller connection heals itself without user intervention. Covers automatic data-stream recovery across a Controller restart, identity-preserving reconnect (bindings are re-pointed at the new session rather than minted fresh and evicted — see capability `runner-identity-and-session`), and stale stream-mapping cleanup on disconnect so a reconnecting session binds cleanly. The goal is that a Controller restart costs no Runner restart, no `kubectl rollout restart` cascade, and no manual `flz relay start`.
## Requirements
### Requirement: Runner → Controller data streams SHALL automatically heal across a Controller restart

When the Controller process restarts and a Runner's reconnect loop succeeds against the new Controller process, every Relay that ProcessHub keeps registered against this node + this namespace + this Runner's profile SHALL have its data-stream traffic resume within 5 s of the new Controller reaching ready, without any Runner restart, manual `kubectl rollout restart` cascade, or external `flz relay start` invocation.

The mechanism has two halves:

1. **Stream rebind.** `Flezha.Runner.StreamChannel` SHALL clear its `stream_ref` to `nil` synchronously inside its `handle_info({:flz_closed, _stream_ref, _reason}, state)` clause. A `StreamChannel` whose `stream_ref` is `nil` is "rebindable". `Flezha.Runner.StreamAcceptor` (UDS path) and `Flezha.Transport.Server` (QUIC path) SHALL, when accepting a new data stream for a `runner_id`, look up rebindable channels for that `runner_id` BEFORE spawning a fresh channel, and bind the new `stream_ref` via an atomic `GenServer.call(channel, {:bind, new_ref})` that returns `:ok` only if the channel was nil-ref at call time. On `:already_bound` the acceptor SHALL try the next candidate; if none, spawn fresh.
2. **AddRelay reissue.** `Flezha.Runner.FleetManager.Session.runner_connect/3` SHALL, AFTER the existing `try_migrate_runner` call, enumerate every Relay currently registered in ProcessHub for this `node()` + this namespace whose `assigned_runner` matches the connecting Runner's profile (declared factories overlap the Relay's `factory_id`) AND was NOT already handled by `try_migrate_runner`. For each match, FleetManager SHALL re-issue `AddRelay` to the new Runner via the Overwatch's rebind (`{:rebind, _}`, `Rebind.rebind/2`) → `start_relay_sdk` path, so the Runner rebuilds its local Relay registry and triggers fresh data-stream opens.

The Rust SDK's `command_add_relay` handler SHALL be idempotent: an `AddRelay` for a `relay_id` already in the local registry SHALL be a no-op (logged, not raised).

#### Scenario: Runner's Relays resume traffic after a `docker compose restart controller`
- **GIVEN** the dev compose stack is up with one Controller, one Rust Runner, and one Relay producing data to a Kafka topic
- **AND** the Runner's reconnect loop is running normally
- **WHEN** the user runs `docker compose restart controller`
- **AND** the new Controller process reaches ready
- **AND** the Runner's reconnect loop opens a new control channel within its existing backoff window
- **THEN** within 5 s of the Controller reaching ready, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have called `flz relay start`, restarted the Runner container, or issued any other re-assignment tasklet between the restart and the assertion

#### Scenario: StreamChannel rebinds to a fresh stream_ref instead of dropping traffic
- **GIVEN** a `StreamChannel` for Relay `r1` was bound to `stream_ref_old`
- **AND** the Controller process died and restarted, causing `{:flz_closed, stream_ref_old, _reason}` to fire on the channel
- **WHEN** the Runner reconnects and opens a new data stream that the acceptor routes to this `runner_id`'s `StreamSupervisor`
- **THEN** the acceptor SHALL discover the existing `StreamChannel` is nil-ref and call `{:bind, stream_ref_new}` on it
- **AND** the channel's `stream_ref` SHALL update to `stream_ref_new`
- **AND** subsequent outbound frames sent through the channel's published binding SHALL go out on `stream_ref_new`, not silently dropped

#### Scenario: Idempotent AddRelay reissue does not crash the SDK
- **GIVEN** the Rust SDK's local registry already contains Relay `r1`
- **WHEN** the Controller issues `AddRelay{relay_id: "r1", ...}` to the SDK as part of the reissue catch-up
- **THEN** the SDK SHALL log "AddRelay: relay already registered, skipping" (or similar) and continue without raising
- **AND** the SDK's local registry SHALL still contain exactly one entry for `r1`

### Requirement: An outbound input frame is sent by its caller from the channel's published binding

`Flezha.Runner.StreamChannel` SHALL publish its bound `stream_ref`, together with its liveness counters, under its registered name — written at init (unbound), on `{:bind, new_ref}` and on `{:flz_closed, ...}` (unbound again), and erased when the channel shuts down; a channel that crashes leaves it published unbound. A sender — the Overwatch's ingest path and the durable tiers' re-forward — SHALL send through `StreamChannel.io_in/3` with the channel's name: it reads the published binding and pushes the frame to the transport from the calling process, with no round trip through the channel process that every relay on the runner shares. A binding that is unbound, a name that is unknown, or a `nil` channel SHALL answer `{:error, :no_stream}`, bump the channel's `nil_drops` liveness counter where a channel exists, and emit `[:flezha, :stream, :drop]`, so a stall on an unbound stream stays observable. An unbound channel SHALL be rebound by the acceptor when the SDK opens a fresh data stream for that runner, and by nothing else: no convergence heal primitive rebinds a data stream, so the sender's refusal is a report, not a request for a repair that is already under way.

#### Scenario: A bound frame leaves from the caller
- **GIVEN** a `StreamChannel` bound to `stream_ref_1`
- **WHEN** an Overwatch ingests an input for a relay mapped to that channel
- **THEN** the frame is pushed on `stream_ref_1` by the Overwatch's own process, the channel's `send_frame_calls` counter advances, and no message is queued at the channel process

#### Scenario: An unbound channel answers no_stream and emits the drop
- **GIVEN** a `StreamChannel` whose stream closed and has not rebound
- **WHEN** an Overwatch ingests an input for it
- **THEN** `io_in/3` answers `{:error, :no_stream}`, the channel's `nil_drops` counter advances, and `[:flezha, :stream, :drop]` fires naming the relay

#### Scenario: A rebind republishes before the channel answers
- **WHEN** the acceptor's `{:bind, stream_ref_new}` call succeeds on a nil-ref channel
- **THEN** the next `io_in/3` on that channel's name sends on `stream_ref_new`; a close republishes the binding as unbound before the channel processes anything else

#### Scenario: A terminated channel's binding is gone
- **WHEN** a `StreamChannel` terminates
- **THEN** its published binding is erased on a shutdown, or left unbound after a crash, and a sender using its name answers `{:error, :no_stream}` rather than pushing on a stale ref

### Requirement: A data channel that dies SHALL be repaired as a closed stream is

A `Flezha.Runner.StreamChannel` that exits for any reason SHALL leave no runner stream behind it: it SHALL close its stream on `terminate/2`, and a replacement that finds a predecessor's stream still published (a killed process runs no `terminate/2`) SHALL close that stream before publishing itself unbound. A channel that crashes SHALL leave its published entry unbound and only a shutdown SHALL erase it, so an entry found at init means the predecessor died. Every channel SHALL announce itself to its session's `Flezha.Runner.ControlChannel` at init, saying whether it found such an entry; for a returned channel, unless the runner is retiring, the control channel SHALL emit `[:flezha, :state, :output_stall]` for every relay the runner's identity owns on this node and SHALL ask the runner for a fresh pair set through the same rebind a closed stream asks for, under the same session budget. A returned channel announced while a rebind runs SHALL be owed one more rebind once that one answers. A channel exit other than a shutdown SHALL be logged in one line by the control channel and SHALL emit `[:flezha, :stream, :channel_down]` with the runner, the channel name and the exit reason. The channel MAY be capped by `FLEZHA_STREAM_CHANNEL_MAX_HEAP_BYTES`, off (`0`) by default: past it the runtime kills the channel, and the kill is repaired and reported as above.

#### Scenario: A killed bound channel is rebound
- **GIVEN** a bound `StreamChannel` of a live runner session carrying relay inputs
- **WHEN** the channel process is killed
- **THEN** its stream is closed, its replacement starts unbound, the control channel logs the death and emits `[:flezha, :stream, :channel_down]` with reason `:killed`, flags the runner's relays with `output_stall` and asks the runner for a fresh pair set, and the replacement is bound again by the acceptor

#### Scenario: The heap cap is set only when configured
- **WHEN** `FLEZHA_STREAM_CHANNEL_MAX_HEAP_BYTES` is positive
- **THEN** every `StreamChannel` runs with that `max_heap_size`, set to kill without the runtime's own report
- **AND** with `0`, the default, no `max_heap_size` is set

#### Scenario: A channel that returns during a rebind is rebound after it
- **GIVEN** a rebind of the runner's data plane is running
- **WHEN** a killed channel's replacement announces itself
- **THEN** once the running rebind answers, the control channel asks the runner for another fresh pair set

### Requirement: Rust SDK command decode SHALL treat a clean peer close as a transport event, not a panic

The Rust SDK's command-frame decode path (`crates/flz_core/src/command/mod.rs`) SHALL distinguish three transport outcomes when reading a frame from the controller's control socket:

1. **Clean peer close** — the read returns 0 bytes. The SDK SHALL classify this as a graceful disconnect, drive the existing reconnect loop, and SHALL NOT panic any runtime thread. The runner process SHALL remain alive across this event.
2. **Malformed frame** — the read returns N bytes (N > 0) and `prost::Message::decode` returns `Err(prost::DecodeError)`. The SDK SHALL log a structured warning containing the decode error's `description` and `stack`, return a typed `Err(CommandDecodeError::Malformed { stack: String })` from `get_handler`, and drive the existing reconnect loop. The SDK SHALL NOT panic any runtime thread.
3. **Well-formed frame** — the read returns N bytes (N > 0) and `prost::Message::decode` returns `Ok`. The SDK SHALL dispatch the command exactly as today.

`get_handler` (or its successor) SHALL return `Result<(CommandActionType, String), CommandDecodeError>`. Every call site within `sdk/rust/src/command/` and adjacent modules SHALL propagate the `Result` or map `Err` to the existing reconnect path; no call site SHALL `.unwrap()` or `.expect()` the result. The literal `.expect("Failed to decode message")` SHALL NOT exist anywhere on this path.

This requirement is a precondition for the existing "Runner → Controller data streams SHALL automatically heal across a Controller restart" requirement: if the SDK panics on the disconnect that immediately precedes reconnect, the 5-second resume contract is unreachable. The production trace `thread '<unnamed>' panicked at /flezha-sdk/sdk/rust/src/command/mod.rs:54:52: Failed to decode message: DecodeError { description: "buffer underflow", stack: [("CommandRequest", "command")] }` observed at 07:40:01 in the flezha_market dev cluster is a violation of this requirement.

#### Scenario: Controller closes the control socket cleanly, SDK reconnects without panic
- **GIVEN** the dev compose stack is up with one Controller, one Rust Runner, and Relays steadily streaming
- **AND** `$MARKET_COMPOSE` names `deploy/docker/dev/docker-compose.yml` in the user's own checkout of the separate `flezha_market` repository — the stack every `docker compose ...` in this scenario selects
- **AND** the user captures the Runner's PID before the test: `RUNNER_PID=$(docker compose -f "$MARKET_COMPOSE" exec -T market-executor cat /var/run/market-executor.pid)`
- **WHEN** the user runs `docker compose -f "$MARKET_COMPOSE" restart flezha-controller`
- **AND** the new Controller process reaches ready
- **THEN** the Rust Runner process SHALL still be alive (same PID — `docker compose ... exec -T market-executor cat /var/run/market-executor.pid` returns the same value as `RUNNER_PID`)
- **AND** the Runner's stderr SHALL NOT contain `thread '<unnamed>' panicked` or `Failed to decode message: DecodeError`
- **AND** within 5 seconds of the new Controller reaching ready, the Kafka topic end-offset for `btc_demo.ohlc.v1` SHALL advance past its pre-restart value (verified via `kafka-get-offsets.sh`)

Note: the `market-executor` container/PID-file name above is owned by the separate `flezha_market` repository and is unchanged by this proposal — see this change's design.md Open Questions.

#### Scenario: Decoder receives 0-byte input on socket EOF and returns a clean signal
- **GIVEN** a unit test in `crates/flz_core/src/command/mod.rs`'s `#[cfg(test)] mod tests`
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
- **GIVEN** the dev compose stack steady-state (same setup as the existing "Runner's Relays resume traffic after a `docker compose restart controller`" scenario in this capability)
- **WHEN** the user runs `docker compose ... restart flezha-controller`
- **THEN** the existing scenario's THEN clauses SHALL all hold (offsets advance within 5 s of new-Controller-ready, no `flz relay start` is required, no runner restart is required)
- **AND** the Rust Runner process SHALL have the same PID throughout (this is the new precondition this requirement adds — the existing scenario was silent on it, today it is violated by the panic)

### Requirement: Rust SDK runtime threads SHALL NOT panic on any control-socket transport-edge event

For any byte sequence the SDK reads from the control socket — including 0-byte reads, truncated frames, frames with unknown command variants, frames whose `reference_id` is empty, frames whose `Command` oneof is `None` — the SDK SHALL surface a typed `Result::Err` to its caller and SHALL NOT `panic!`, `.unwrap()`, or `.expect()` from within a runtime thread. The error path SHALL drive the existing reconnect loop (clean transport restart) for transport-edge events (clean close, truncated read) and structured logging plus reconnect for protocol-edge events (unknown variant, missing oneof).

The existing `Command::RunnerShutdownNotification`, `Command::RestoreInitStateResponse`, and `Command::CompactCellResponse` clauses (currently `CommandActionType::NotificationIgnore`) demonstrate the desired pattern for protocol-edge events: log and drop. The decode-edge event class (this requirement) extends the same defensive posture one layer below the variant match.

#### Scenario: `Command` oneof is None and SDK does not panic
- **GIVEN** the SDK receives a well-formed `CommandRequest` whose `command` field is `None` (a future protocol contract violation or a defensive serialization bug)
- **WHEN** the SDK enters `get_handler`'s command-classification step (`crates/flz_core/src/command/mod.rs`)
- **THEN** the SDK SHALL return a typed `Err(CommandDecodeError::EmptyCommand)` (or equivalent) instead of panicking on `.unwrap()`
- **AND** the runtime thread SHALL remain alive
- **AND** the SDK SHALL log a structured warning and continue processing subsequent frames

### Requirement: Runner restart while the Controller is alive SHALL heal stream binding cleanly

When a Runner process restarts and its reconnect loop succeeds against the still-alive Controller, the fleet manager SHALL declare the disconnect (`{:runner_disconnected, ns, runner_id}`) and, under `FLEZHA_RUNNER_REMOVAL_GRACE_MS`, preserve the runner as `:inactive` — a keepalive fact, not a remedy; the convergence loop SHALL plan `rebind_relay` per affected relay from the declared fact, choosing the returned or a surviving runner through the placement function, and the new data streams the Runner opens SHALL bind cleanly to nil-ref `StreamChannel`s without silent traffic drops. No reroute SHALL run inside the session handler. The rebind the loop drives SHALL be the one bind path: the Overwatch's rebind (`{:rebind, _}`, `Rebind.rebind/2`) runs `Lifecycle.complete_init/5` in its supervised bind task for every state, so the `AddRelay` it sends carries the relay's kind and, for a durable steps instance, its `task_journal`, `resume_cell_count` and `inbox_tail_count`, the persisted Cells are restored by that bind, and the undelivered inbox tail is delivered by it; the heal SHALL dispatch no second restore after the bind. When the replacement presents the dead session's stable identity before the loop has placed the relays, the takeover binds them through the same path instead, and the loop finds nothing left to plan.

#### Scenario: Runner restart with single-replica-in-namespace
- **GIVEN** Runner A is the only Runner in namespace `ns1` on this Controller, running Relays `r1` and `r2`
- **WHEN** Runner A's container restarts and reconnects
- **THEN** the disconnect SHALL have been declared, `r1` and `r2` SHALL each be bound onto the returned runner by exactly one `AddRelay` carrying their resume state, through `rebind_relay` or the takeover, whichever runs first, and the data streams MUST rebind to the existing `StreamChannel`s without traffic loss

#### Scenario: A durable steps relay rebound by the loop resumes at its stage
- **GIVEN** a durable steps instance on runner A with tasklet A committed and tasklet B in flight, and a runner B advertising the factory under a different stable identity
- **WHEN** runner A is killed, its disconnect is declared, and the loop plans `rebind_relay` onto runner B
- **THEN** the `AddRelay` runner B receives SHALL carry the folded `task_journal`, a `resume_cell_count` equal to the instance's persisted Cells and the undelivered `inbox_tail_count`, runner B's driver SHALL be held until those Cells are restored, tasklet A SHALL NOT run on runner B, and the instance SHALL reach its terminal completion

### Requirement: SDK SHALL ship final state to the agent on graceful shutdown so reconnect-resume is loss-free

When an SDK that carries `state_persistence` shuts down gracefully (SIGTERM, `Drop` of the SDK handle, explicit `disconnect()` call — or, for an erased host over `flz_core_ffi`, the host's own stop path calling `flz_core_shutdown`), it SHALL serialize each local relay's state via the same per-driver serializer used for `MigrationSerialization`, and emit `RunnerShutdownNotification{relay_id, serialized_state}` on the control channel for each relay, before the core reports shutting down to the host's pollers — an erased host's loops keep answering `live_state_read` until then. The agent's `ControlChannel` SHALL forward each notification to the corresponding `Overwatch` GenServer (looked up via `Identificator.overwatch(relay_id)`) only when the relay's ownership record names the sending session's stable identity or records no owner on this node for a relay whose durable state belongs to the sending session's namespace; that Overwatch SHALL replace `state.snapshot` with the supplied binary atomically. A notification naming a relay bound to another runner's identity SHALL be refused as `runner-identity-and-session` prescribes and SHALL NOT touch the snapshot. The agent SHALL respond with `RunnerShutdownAck{relay_id}` once the snapshot is enqueued, and SHALL respond with it for a refused notification too.

The SDK SHALL wait at most 2 s per relay for the ack (with parallelism across relays) and SHALL exit regardless of ack receipt after the timeout. The SDK MUST NOT block the container's shutdown grace period if the agent has crashed or become unresponsive.

`RunnerShutdownNotification` and `RunnerShutdownAck` are additive opcodes: agents that don't recognise them SHALL log and ignore (existing protobuf-decoder behaviour); SDKs that don't send them — an SDK that defers `state_persistence` by name — fall back to the periodic-snapshot freshness shipped by `heal-connections-on-restart`. Every shipped SDK carries `state_persistence`, so every shipped SDK sends them.

#### Scenario: Graceful runner shutdown ships fresh state to the agent
- **GIVEN** a runner running relay `p1` with current in-memory state `S_now` and the most recent periodic snapshot at `T - 45s` showing state `S_old`
- **WHEN** the runner receives SIGTERM and runs its graceful-shutdown handler
- **THEN** the runner SHALL serialize `p1`'s current state (yielding `S_now`) and emit `RunnerShutdownNotification{relay_id: "p1", serialized_state: S_now}` on the control channel
- **AND** the agent's `Overwatch` for `p1` SHALL replace `state.snapshot` with `S_now`
- **AND** the agent SHALL emit `RunnerShutdownAck{relay_id: "p1"}` back to the runner
- **AND** when a new runner subsequently reconnects and is assigned `p1` via the AddRelay reissue path (delivered by `heal-connections-on-restart`), the state delivered via `maybe_apply_initial_state` SHALL be `S_now` (not `S_old`)

#### Scenario: An erased host's own stop path ships the notification
- **GIVEN** a Node, Python, Ruby, PHP or Elixir runner running relay `p1`, whose runtime owns SIGTERM and stops the runner through the SDK's stop path
- **WHEN** the runner receives SIGTERM
- **THEN** `flz_core_shutdown` SHALL serialize `p1`'s state through the host's still-polling loops and emit `RunnerShutdownNotification{relay_id: "p1", ...}` before the poll reports shutting down
- **AND** `state_freshness_e2e_test.exs` under `FLEZHA_E2E_RUNTIME=node`, `python` and `elixir` SHALL see `Overwatch.state.snapshot` populated within its window

#### Scenario: A stopping runner does not re-register
- **GIVEN** an erased host whose stop path has called `flz_core_shutdown` and whose notification the agent has acknowledged
- **WHEN** the core reports shutting down to the host's pollers
- **THEN** the runner SHALL open no new control connection: a stopping runner that re-registered would be handed work it will never do

#### Scenario: SDK exits after ack timeout if agent is unresponsive
- **GIVEN** a runner running relay `p1` and the agent process is in the middle of crashing
- **WHEN** the runner receives SIGTERM and emits `RunnerShutdownNotification{relay_id: "p1", ...}`
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
- **THEN** the agent's `Overwatch` snapshot for each of the runner's relays SHALL still hold the most recent periodic-snapshot binary (today's behaviour)
- **AND** the AddRelay reissue path SHALL still ship that snapshot to a reconnecting runner — just up to `overwatch_state_snapshot_interval_ms` stale, exactly as `heal-connections-on-restart` ships

#### Scenario: Disabled-snapshot runners still benefit from the shutdown notification
- **GIVEN** a runner's relay is configured with `snapshot_interval: 0` (periodic snapshots disabled — e.g. flezha_market where Postgres is authoritative)
- **WHEN** the runner shuts down gracefully and emits `RunnerShutdownNotification` for the relay
- **THEN** the agent's `Overwatch` SHALL still accept the notification and store the binary in `state.snapshot`
- **AND** the snapshot SHALL be available for the next AddRelay reissue, even though no periodic snapshots were ever taken

#### Scenario: Notification arrives for a relay whose Overwatch has already migrated away
- **GIVEN** relay `p1`'s `Overwatch` was on this agent but ProcessHub migrated `p1` to a peer agent moments before the runner's shutdown notification arrives
- **WHEN** the agent receives `RunnerShutdownNotification{relay_id: "p1", ...}`
- **AND** `Identificator.overwatch("p1")` resolves to `:undefined`
- **THEN** the agent SHALL log a warning naming `p1` and SHALL still emit `RunnerShutdownAck{relay_id: "p1"}` so the SDK can proceed to exit
- **AND** the destination agent's Overwatch already holds the latest state from the move (its in-band handover, or for a persistent class moved cold, the copy it pulled), so no functional loss

#### Scenario: A notification for another runner's relay is acked and applied to nothing
- **GIVEN** relay `p1` bound to runner A's stable identity and a second session B in the namespace
- **WHEN** B emits `RunnerShutdownNotification{relay_id: "p1", serialized_state: S_b}`
- **THEN** `p1`'s `Overwatch` SHALL keep the snapshot it held, the refusal SHALL be counted and reported naming B, A and `p1`, and B SHALL still receive `RunnerShutdownAck{relay_id: "p1"}`

### Requirement: A dev-compose integration test SHALL exercise both restart paths end to end

The repository SHALL include an integration test that uses `deploy/docker/dev/docker-compose.yml` (extended with the `rust` runner service that is currently commented out) to exercise the agent-restart and runner-restart healing paths end-to-end. The test MUST use the existing dev compose stack and SHALL NOT introduce new Docker contexts or `docker run` invocations.

#### Scenario: Agent-restart path
- **GIVEN** the dev compose stack is up with one agent, one Rust runner, and one relay producing data to a Kafka topic
- **WHEN** the test runs `docker compose restart agent` and waits for agent ready
- **THEN** within 5 s of agent ready, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have restarted the runner container or issued any explicit re-assignment

#### Scenario: Runner-restart path
- **GIVEN** the same setup
- **WHEN** the test runs `docker compose restart rust` (the runner container) and waits for it to reconnect
- **THEN** within 5 s of the runner's first successful reconnect, the Kafka topic's `LOG-END-OFFSET` SHALL have advanced past its pre-restart value
- **AND** the test SHALL NOT have restarted the agent container or issued any explicit re-assignment

### Requirement: A k8s manifest sanity test SHALL prevent volume/env-var drift

The repository SHALL include a unit test that loads `deploy/kubernetes/flezha-daemonset.yaml` and `deploy/kubernetes/flezha-configmap.yaml` and asserts the volumes and env vars this change relies on are present and well-formed, so that future deployment-manifest edits cannot silently break the dev/k8s parity.

#### Scenario: Required volumes and env vars are present
- **GIVEN** the k8s manifests at `deploy/kubernetes/flezha-daemonset.yaml` and `deploy/kubernetes/flezha-configmap.yaml`
- **WHEN** the manifest sanity test runs
- **THEN** the test SHALL assert: a `flezha-sockets` volume exists in the daemonset and is mounted at `/flezha/sockets`; the daemonset's container env contains `FLEZHA_SOCKET_FILES_PATH=/flezha/sockets`; the configmap defines `FLEZHA_RUNNER_REMOVAL_GRACE_MS` with a positive integer (in milliseconds)
- **AND** the test SHALL fail with a clear message naming the missing field if any assertion fails

### Requirement: Rust SDK SHALL bound the wait for the controller's registration ack and reconnect on timeout

After the Rust SDK opens a control channel and writes its `connect_control_channel` init frame, it SHALL require the controller to send at least one frame (the assign-runner-id command issued by `FleetManager.runner_connect/3`) within a bounded window. The window SHALL default to 10 s and be overridable via `FLEZHA_CONTROL_ACK_TIMEOUT_MS`. If no frame arrives within the window, the SDK SHALL treat the connection as a non-servicing controller, post `RunnerMessage::Reconnect`, and drive the existing reconnect loop. Once the first frame arrives the SDK SHALL clear the read deadline and read indefinitely (steady state is unchanged).

This closes the warm-restart hang where the SDK connects to a Controller that accepts the socket but never completes registration — a Controller mid-shutdown (quiesce rejects the connect via `{:error, :shutting_down}` without closing the stream) or a Controller process being replaced during a rolling restart. Without the bound the control reader blocks forever, the runner never reaches the live Controller, and no relay is re-registered (`start_hub` is never called).

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

### Requirement: Controller-side cold-restart relay startup SHALL self-heal via an end-to-end ping before `:start_relay`

When the Controller restarts and `Flezha.Relay.Overwatch` GenServers come up from durable registry replay, each Overwatch SHALL probe the resolved Runner's end-to-end control-channel health via a `:ping_runner` round-trip BEFORE issuing `{:start_relay, …}` to the Runner's `ControlUnit`. The ping SHALL traverse the same `ControlChannel → SDK → ControlChannel` path that `AddRelay` uses, so a successful round-trip proves the channel is bound, the SDK is listening, and the reply path is intact.

The ping SHALL use a default 2 s timeout (configurable via `:overwatch_ping_timeout_ms`, env `FLEZHA_OVERWATCH_PING_TIMEOUT_MS`). On `:pong` the Overwatch SHALL proceed to `start_relay_sdk/2`. On a ping error the Overwatch SHALL NOT reset the channel itself: the attempt parks the relay with the ping's reason and the park is declared, and the convergence loop plans the primitive `reset_control_channel` (`Flezha.Runner.FleetManager.reset_control_channel/1`, clearing the `ControlChannel`'s `stream_ref` so the SDK's next data stream can claim it via the existing rebind path) — before the rebind when the reason reports a dead transport, and only once the parked key has walked two rungs when the ping merely timed out, because the channel is live and carries every other instance bound to that runner, and a busy runner is retried, not cut. The control-channel reader SHALL NOT perform disk I/O, so a commit storm on a runner cannot by itself push its ping past the timeout.

The `:ping_runner` command and its `PingResponse` SHALL be carried by the existing `CommandRequest` / `CommandResponse` envelopes on the `"control_command"` payload_id; SDKs without the handler SHALL log and ignore (existing Protox unknown-field behaviour).

#### Scenario: Healthy runner responds to ping and `:start_relay` proceeds
- **GIVEN** a relay `r1` whose Overwatch has just resolved live runner `e1`
- **AND** `e1`'s control channel is bound and its SDK is listening
- **WHEN** the Overwatch issues `Flezha.Runner.ControlUnit.ping_runner("e1", timeout_ms: 2_000)`
- **THEN** the call SHALL return `{:ok, :pong}` within 2 s
- **AND** the Overwatch SHALL invoke `start_relay_sdk("e1", r1)` immediately afterwards
- **AND** `reset_control_channel/1` SHALL NOT be invoked

#### Scenario: Stale control channel fails the ping, and the loop resets the channel
- **GIVEN** a relay `r1` whose Overwatch has just resolved runner `e1`
- **AND** `e1`'s `ControlChannel` GenServer is registered but its `stream_ref` points to a closed/stale transport (the SDK has reconnected since the last `AddRelay`)
- **WHEN** the Overwatch issues `ping_runner("e1", timeout_ms: 2_000)` and it fails reporting a dead transport
- **THEN** the Overwatch SHALL NOT issue `start_relay_sdk/2`, SHALL park `r1` with that reason and declare the park
- **AND** the loop's declaring tick SHALL plan `reset_control_channel` for `e1` — which sets the channel's `stream_ref` to `nil` — before `rebind_relay` for `r1`

#### Scenario: A busy runner's timed-out ping does not cut the live channel
- **GIVEN** runner `e1` is bound to a hundred instances and its control channel is saturated by their starts
- **WHEN** one Overwatch's ping returns `{:error, :timeout}` and its relay parks
- **THEN** `reset_control_channel("e1")` SHALL NOT be planned at the declaring tick nor at the next rung, and no other instance on `e1` SHALL have seen `{:error, :no_connection_available}`; only a parked key that has walked two rungs plans it

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
10 s), and SHALL reconnect when none arrives, exactly as the Rust SDK does. The Java SDK
takes this bound from the shared core and carries no control reader, ack wait or
reconnect ladder of its own: `FlzCore.start` crosses `flz_core_ffi`'s JNI binding into
`flz_core`, whose control reader arms the ack deadline on its blocking read and, when a
read outlives it, treats the controller as not servicing the runner and asks the core's
coordinator to reconnect. A handshake send failure SHALL drive the same reconnect rather
than strand the reader thread.

#### Scenario: Controller accepts but never acknowledges → Java SDK reconnects
- **GIVEN** a Java runner whose controller accepts the control socket then stays silent (mid-shutdown or being replaced)
- **WHEN** `FLEZHA_CONTROL_ACK_TIMEOUT_MS` elapses with no controller frame
- **THEN** the shared core's control reader SHALL ask for a reconnect and SHALL NOT block its reader thread indefinitely
- **AND** the runner SHALL reconnect until it lands on a controller that completes registration

#### Scenario: First controller frame clears the Java ack deadline
- **WHEN** the controller sends its assign-runner-id command within the window
- **THEN** the control reader SHALL clear the ack deadline and read subsequent frames under the steady-state idle window (`FLEZHA_CONTROL_IDLE_TIMEOUT_MS`) instead

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
storm. Attempts SHALL remain unbounded (the runner must keep trying until a live
controller is found). `flz_core` SHALL carry the schedule for every SDK: base 100 ms,
exponent capped at 8, delay capped by `FLEZHA_RECONNECT_MAX_BACKOFF_MS`, the controller's
`reconnect_max_backoff_ms` row, default 30 s, read through `flz_transport`'s knob reader.

The jitter is `flz_transport`'s one jitter rule for every SDK and every Rust retry site, the same
integer draw as the controller's `Flezha.Config.Backoff.jitter/2`: for the rung's delay `d` in
milliseconds it waits `⌊d/2⌋ + u` with `u` uniform in `1..=d`, and a ladder re-caps the result at
its cap, as the controller's ladders do, which satisfies the anti-thunder property this requirement
exists for. The schedule is one `flz_transport::backoff::Ladder` value; the tasklet retry, the
stream connect and the liveness timers name their own `Ladder` or the same jitter, and no Rust
site SHALL carry its own curve or jitter.

The rung SHALL be a property of the control channel rather than of one call into the
connect loop, so a reconnect starts where the last one left off instead of at rung zero.
A control session that ends before its handshake completed SHALL count as a failed
attempt, and the ladder SHALL be reset only by a handshake that completed: the controller
has written `assign_runner_id`, the runner has answered the `ConnectSocket` that follows
it, and the controller has accepted that answer. Reaching the socket is not admission and
neither is being admitted. A refusal closes the connection after `connect(2)` has already
succeeded, and a handshake accepted at its first frame can still fail at its second, so an
SDK that resets its rung on either one retries a controller that cannot finish a handshake
with it as fast as the kernel allows, which one runner can do fast enough to saturate that
controller.

The ack window is a different question from admission and SHALL NOT be answered by the
same stored fact. The deadline that distinguishes "no frame has arrived yet" from "the
session is idle" SHALL keep reading the first inbound frame, while the ladder reset SHALL
read the completed handshake, so that neither rule can be changed by editing the other.

#### Scenario: Fleet reconnect does not thunder
- **GIVEN** N runners whose controller restarts simultaneously
- **WHEN** they all enter their reconnect loops
- **THEN** their attempts SHALL be spread by jitter rather than firing in lockstep

#### Scenario: A refused session climbs the ladder
- **GIVEN** a controller that accepts the socket and closes it without admitting the runner
- **WHEN** the runner reconnects again and again
- **THEN** each attempt SHALL wait one rung further up the ladder, up to the 30 s cap, rather than reconnecting immediately

#### Scenario: A completed handshake puts the ladder back on its first rung
- **GIVEN** a runner whose handshake completed, however many refusals preceded it
- **WHEN** that session later ends and the runner reconnects
- **THEN** the reconnect SHALL NOT be delayed by any earlier refusal

#### Scenario: A handshake admitted and then failed climbs the ladder
- **GIVEN** a controller that writes `assign_runner_id` and then fails the runner's `ConnectSocket` answer, or severs the data streams before the handshake completes
- **WHEN** the runner reconnects into the same outcome repeatedly
- **THEN** each attempt SHALL wait one rung further up the ladder rather than repeating at rung one
- **AND** the runner SHALL NOT report every attempt as attempt 1

#### Scenario: The ack window still opens on the first inbound frame
- **GIVEN** a session whose handshake has not completed
- **WHEN** the controller's first frame arrives within the ack timeout
- **THEN** the connection SHALL switch to the idle deadline as it does today, and the reconnect ladder SHALL NOT be reset by that frame

#### Scenario: The runner's cap follows the controller's setting
- **GIVEN** a runner started with `FLEZHA_RECONNECT_MAX_BACKOFF_MS=2000`
- **WHEN** it reconnects past the rung whose delay exceeds 2 s
- **THEN** no wait SHALL exceed 2 s, and with the variable unset no wait SHALL exceed 30 s

#### Scenario: Every jittered wait lies in the one band
- **WHEN** a rung's delay `d` below the cap is jittered, on the runner or in the controller
- **THEN** the wait SHALL lie in `[⌊d/2⌋+1, ⌊d/2⌋+d]` milliseconds, and a ladder's wait SHALL then be re-capped at its cap

### Requirement: Java SDK transport-edge and decode events SHALL NOT crash the runtime thread

The Java SDK and its JNI reader SHALL NOT crash the runtime thread on any
control-socket event — clean peer close (0-byte read), truncated/malformed frame, unknown
command variant, or missing fields — and SHALL instead surface a typed error and drive
the reconnect path. This is the Java parity of the Rust "runtime threads SHALL NOT panic
on any control-socket transport-edge event" requirement.

#### Scenario: Controller closes the Java SDK control socket cleanly
- **GIVEN** a Java runner with relays streaming
- **WHEN** the controller process restarts (clean close on the control socket)
- **THEN** the Java runner process SHALL stay alive and reconnect
- **AND** no reader/runtime thread SHALL die unrecoverably

#### Scenario: Java SDK receives a truncated control frame
- **WHEN** the Java reader receives a truncated/malformed frame
- **THEN** it SHALL drive reconnect and SHALL NOT crash the runtime thread

### Requirement: Convergence eviction is idempotent and safe on an active runner

`FleetManager.evict_runner/2` driven by the convergence Loop SHALL be a no-op when the target runner is currently `:active` (its connection is open), and SHALL be idempotent on an unknown id. The authoritative connect-handler path (`previous_runner_id` presented by the SDK itself) remains unconditional.

The guard protects a runner that is merely *observed* healthy against a stale snapshot. It SHALL NOT protect one the data-plane classifier PROVED wedged: an eviction carrying `proved_wedged: true` SHALL proceed despite the open connection. A pool-wedged runner heartbeats and holds its socket while processing nothing, so its connection being open is exactly what would otherwise make it unevictable forever.

#### Scenario: a reconnected runner survives a stale eviction
- **GIVEN** the Diff snapshot queued `default_X` for eviction but `default_X` reconnected before the heal ran
- **WHEN** `evict_runner(default_X)` executes with no proved-wedge reason
- **THEN** FleetManager observes `default_X` is `:active` and performs no eviction, leaving its relays producing.

#### Scenario: a classifier-proved wedged runner is evicted while connected
- **GIVEN** `default_X` is in the live active set and heartbeating, and the classifier reported `runner_pool_wedged` for it
- **WHEN** `evict_runner(default_X, proved_wedged: true)` executes
- **THEN** FleetManager evicts it despite the open connection, and its relays rebind onto a live runner.

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

On any runner reconnect the controller SHALL resolve identity by the runner's presented **stable identity** (see capability `runner-identity-and-session`) and SHALL NOT mint a fresh durable id nor perform an unconditional `previous_runner_id` pre-eviction. What it does with the identity's existing relay bindings SHALL follow the incarnation the connect presents. The held incarnation is the same process: the controller SHALL re-point the bindings at the new session, each Overwatch publishing its own tag and route, and SHALL send the process nothing for a relay it still runs. A different incarnation, or none, is a new process: the controller SHALL bind each relay onto the new session through the one bind path, `Lifecycle.complete_init/4` in the Overwatch's supervised bind task, which sends `AddRelay` with the relay's resume state, restores its persisted Cells and delivers its undelivered inbox tail; an Overwatch with a bind task already in flight SHALL leave it to finish or park. On disconnect, each relay's stale stream mapping SHALL be cleared (`HubManager.hook_remove_pid/2`) so the reconnecting session's fresh streams bind cleanly to nil-ref `StreamChannel`s.

#### Scenario: fresh-process restart preserves bindings without a pre-evict

- **GIVEN** a runner with relays `[r1, r2]` whose process restarts and reconnects within the keepalive window, presenting the same persisted stable identity and a new incarnation
- **WHEN** the controller handles the connect
- **THEN** it SHALL bind `r1` and `r2` onto the new session token through the one bind path, each with one `AddRelay` carrying its resume state
- **AND** it SHALL NOT mint a new durable identity nor issue an unconditional eviction of a "previous" id

#### Scenario: in-process reconnect actually re-binds relays

- **GIVEN** a runner whose control stream dropped but whose process survived, holding relays `[r1]`
- **WHEN** it reconnects presenting the same stable identity and the same incarnation
- **THEN** the controller SHALL re-point `r1` onto the new session, `r1`'s Overwatch SHALL publish its tag and route on it, and the process SHALL receive no `AddRelay`, `RestoreInitState` or inbox tail for `r1`

#### Scenario: a connect with a stable identity and no incarnation is a new process

- **GIVEN** stable identity `S` held by a live session
- **WHEN** a connect presents `S` and no incarnation
- **THEN** the controller SHALL handle it as a restarted process and bind `S`'s relays onto the new session through the one bind path

### Requirement: A runner resolves its UDS socket directory from an ordered ladder

The socket directory SHALL be resolved by one implementation shared by every SDK, walking these
candidates in order:

1. `FLEZHA_SOCKET_FILES_PATH` when set — used outright, with no fallback to any later candidate;
2. `$XDG_RUNTIME_DIR/flezha` when `XDG_RUNTIME_DIR` is set;
3. `/run/flezha`;
4. `storage/app/sockets`, relative to the working directory.

The controller SHALL take the first candidate it can create; a runner SHALL take the first
candidate holding a live control socket. When no variable is set and more than one candidate
holds a live control socket, a runner SHALL refuse to connect and SHALL name every candidate it
found, rather than selecting one.

A relative candidate SHALL be reported as its resolved absolute path wherever it is named to a
user.

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
- **WHEN** a runner resolves the `storage/app/sockets` candidate and reports the path
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
relays independently.

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

#### Scenario: The user is told how to attach without consulting documentation
- **WHEN** a controller boots successfully
- **THEN** its log contains the resolved absolute socket directory and the `FLEZHA_SOCKET_FILES_PATH` assignment that attaches a runner to it

### Requirement: A graceful ship waits at most the gate bound for a Cell inside an activation

The per-Cell serialize a graceful shutdown runs SHALL wait at most 5 s for the Cell's handout exclusivity; a Cell still inside an activation at that bound SHALL be omitted from the `RunnerShutdownNotification`, with a log line naming the Cell and the bound, and the notification SHALL still carry every other Cell's state. The bound applies to every path that runs the ship — SIGTERM, `Drop` of the runner handle, the host's own stop entry, and the controller-ordered terminal disconnect — and to a live state read of the same Cell, so that no command interaction can hold the runner's control thread for the life of the process. A controller-ordered terminal disconnect SHALL therefore end the process with the non-zero terminal status whatever the host's loops are doing: the exit is the core's, from its own thread.

#### Scenario: A terminal disconnect ordered to a wedged runner ends it
- **GIVEN** a runner one of whose Cells is inside an activation that never returns
- **WHEN** the controller orders a terminal disconnect
- **THEN** the runner SHALL acknowledge, ship the state of every other Cell, log the busy Cell as omitted, and exit with a non-zero status within the bound plus the ack wait
- **AND** its session SHALL close and its relays SHALL be rerouted by the existing disconnect path

#### Scenario: The omitted Cell's state is at most one activation stale
- **GIVEN** the busy Cell's last completed activation left state `S`
- **WHEN** the notification ships without it
- **THEN** the controller's snapshot for that relay SHALL hold `S` from the periodic snapshot or the WAL, exactly as it would after a crash, and nothing newer existed to ship

#### Scenario: A live read of a wedged Cell is refused, not held
- **WHEN** a live state read names a Cell inside an activation older than the bound
- **THEN** the runner SHALL answer with an error naming the Cell as busy after the bound, and the control thread SHALL go on serving later commands

### Requirement: A per-runner data socket name SHALL be bounded and composed by one implementation

A per-runner data socket SHALL be named `<stream type>_stream_<id field>.sock` in the resolved
socket directory, composed by one implementation that every SDK reaches, and the `<id field>`
SHALL be at most 24 bytes.

A runner id of 24 bytes or fewer SHALL be used as it stands. A longer one SHALL be replaced by
its first 11 bytes, a `~`, and the first 12 lowercase hexadecimal characters of the SHA-256
digest of the runner id's UTF-8 bytes.

The shortening SHALL depend on the runner id alone and SHALL NOT depend on the socket
directory. The controller and the runner compose this name independently, from directories they
resolved separately, so a rule that consulted either side's directory could produce two names
for one runner.

Composing a socket path that would exceed the platform's limit for a Unix domain socket path
SHALL be refused where the path is composed, naming the path and its byte length, rather than
surfacing as a listener that failed to start. The limit SHALL be the size of the platform's
`sun_path` less its NUL, read from the platform's own socket address definition rather than
spelled as a number: 107 bytes on Linux and 103 bytes on macOS.

The controller SHALL NOT mint a runner id longer than the bound this name applies. The bound
SHALL be declared once and read by both the minting side and the composing side, so a minted id
reaches its socket unshortened and a user diagnosing a socket sees the id the controller
reports. Shortening remains the rule for an id the controller did not mint (an SDK's own stable
identity) and is not removed by the mint's bound.

The control socket SHALL be named by the same implementation that names a data socket. No
component SHALL spell the control socket's filename itself.

#### Scenario: A long runner id is shortened, deterministically
- **WHEN** a runner id longer than 24 bytes is composed into a data socket name
- **THEN** the id field is exactly 24 bytes, carries the id's first 11 bytes, a `~`, and 12 hexadecimal characters, and is byte-identical on every composition of the same id

#### Scenario: A short runner id stays readable
- **WHEN** a runner id of 24 bytes or fewer is composed into a data socket name
- **THEN** the id field is that runner id unchanged

#### Scenario: The controller and the runner compose one name
- **WHEN** the controller binds a data socket for a runner id and that runner dials its data socket for the same id and stream type in the same directory
- **THEN** both compose the same absolute path

#### Scenario: A path that cannot fit is refused where it is built
- **WHEN** a socket directory is long enough that the composed data socket path would exceed the platform's limit
- **THEN** composition refuses, naming the path, its byte length and the limit, and no listener is started

#### Scenario: The limit is the platform's own
- **WHEN** the composing implementation is built for Linux, or for macOS
- **THEN** its limit is 107 bytes on Linux and 103 bytes on macOS, with no per-platform number spelled in Flezha's code

#### Scenario: A shortened name is still recognised as Flezha's own
- **WHEN** a directory holds a data socket whose id field was shortened
- **THEN** the predicate that decides which files in a socket directory belong to Flezha recognises it, and `flz reset --include-sockets` clears it

#### Scenario: A minted runner id reaches its socket unshortened
- **WHEN** the controller mints a runner id for any namespace and binds that runner's data sockets
- **THEN** the id field in each socket's name is the minted id in full, and the socket file on disk carries the id the controller reports for that runner

#### Scenario: The control socket's name has one speller
- **WHEN** the controller establishes its socket directory and a runner of any SDK dials the control channel
- **THEN** both obtain the control socket's filename from the one composing implementation, and no caller concatenates it

### Requirement: A bind is one attempt, and its failure is a park the loop is told about

`Flezha.Relay.Overwatch` SHALL, before issuing `:start_relay`, gate on an active runner being reachable: `RelayManager.await_runner/5` polls the fleet for a runner and confirms readiness (`runner_ready?/1` — the CU is registered AND, when `:overwatch_ping_enabled`, an end-to-end ping returns pong) within a bounded window. Once a runner is confirmed ready the Overwatch SHALL make ONE `:start_relay` attempt; a failure halts the relay with `halt_reason` (alive, plugins prepared, no upstream I/O) carrying the attempt's reason, and the halt is declared to the convergence loop, which drives the un-park `rebind_relay` on the `{:halted, relay_id}` key of its own table — eligible at the declaring tick, then on the `:heal` row's rungs, which "Bounded calls and backoff prevent heal-induced churn" states and this requirement does not restate. The Overwatch SHALL hold no retry list, no jitter and no per-attempt runner re-resolution; a runner connecting in the namespace releases the cooldown keys of the relays it can serve instead, the halt's among them. The un-park rebind (`{:rebind, _}`, `Rebind.rebind/2`) SHALL run in a supervised task so `:stop_relay` stays responsive, and the task SHALL report its outcome and decide nothing.

#### Scenario: start gates on runner readiness before its one attempt
- **GIVEN** a relay is (re)starting and no runner for its factory is reachable yet
- **WHEN** the Overwatch runs `RelayManager.await_runner/5`
- **THEN** it waits for a ready runner before issuing `:start_relay`, and if none becomes ready within the window it holds the relay unbound rather than pinging a dead binding.

#### Scenario: a failed attempt is not retried in place
- **GIVEN** a relay whose one `:start_relay` attempt fails
- **WHEN** the Overwatch handles the failure
- **THEN** it SHALL halt at once with the reason, declare `{:relay_halted, {ns, id}, _}`, and issue no second attempt of its own; the next attempt SHALL be the loop's `rebind_relay`

### Requirement: The runner's verdict record SHALL carry the control silence and the data silence as two facts

The convergence loop SHALL publish one verdict record per connected runner whose facts carry `control_silent_ms` — the age of the last inbound control frame, which `Flezha.Runner.ControlChannel` SHALL stamp into the shared liveness counters on every inbound control frame beside the `last_seen` it keeps for its own watchdog — `data_silent_ms`, `heartbeats_advancing?`, `dispatches_advancing?` and the runner's roster membership. The rosters the loop reads cover the running namespaces only, so a runner whose namespace hub is not running is held by none of them while its session is open; the record's state SHALL therefore be `failed` when no roster the tick read holds the runner AND its control session has been silent for at least the liveness window or was never measured, `unknown` when its facts could not be read or when no roster holds it while its control session is still speaking inside that window, `degraded` with the runner-scoped layer as reason when the data-plane classifier proves it wedged, and `healthy` otherwise. Every runner the liveness sample holds a live channel for SHALL have a record, and its channel records SHALL be judged by the same two facts, so the runner row and its channel rows never disagree. The session watchdog, its window and its close SHALL be unchanged: the record reports the two silences side by side and decides nothing about the session, so a session that is alive over a data plane that is dead is readable as one row, and the control-plane watchdog's `[:flezha, :runner, :liveness_timeout]` SHALL reach the observability bus through the detector table's declaration of it.

#### Scenario: a live session over a dead data plane is one row

- **GIVEN** a runner whose keepalive pings keep arriving while its data-plane counters stopped advancing past the stall window
- **WHEN** the convergence loop ticks
- **THEN** the runner's record SHALL show a control silence under the liveness window and a data silence over the stall window, and its state SHALL be `degraded` naming the runner-scoped layer

#### Scenario: a runner the roster dropped is failed

- **GIVEN** a runner whose control session the watchdog closed after the liveness window
- **WHEN** the convergence loop ticks
- **THEN** the runner's record SHALL be `failed`, every relay bound to it SHALL be `failed` with reason `dead_runner`, and the loop's remedy SHALL be the existing rebind

#### Scenario: a live runner whose namespace hub is not running is unknown, not dead

- **GIVEN** a runner whose control frames keep arriving inside the liveness window while its namespace's hub is not running, so no roster the tick read holds it
- **WHEN** the convergence loop ticks
- **THEN** the runner's record and every one of its channel records SHALL be `unknown` with reason `{:unobserved, :active_runners}`, and `/healthz` SHALL NOT join `dead_runner` into its reason for that runner

#### Scenario: the watchdog is unchanged and its event reaches an output

- **GIVEN** no inbound control frame arrives for longer than the configured liveness window
- **WHEN** the watchdog fires
- **THEN** it SHALL close the session exactly as before, and the `[:flezha, :runner, :liveness_timeout]` it executes SHALL appear on the observability bus as a signal with no hand-written bridge line

### Requirement: A shutdown notification the wire refuses SHALL be held, never printed and dropped

A refused `RunnerShutdownNotification` SHALL be treated as the durable frame it
is: when its write to the control channel is refused it SHALL be held for replay
on the next bound stream, and counted if it is shed, rather than reported to
standard error or a log line and lost. The frame carries the relay's final state, which is the
whole reason reconnect-resume is loss-free; a runner that prints the failure and
exits leaves the agent holding a snapshot as stale as the last periodic one,
which is the outcome the shutdown ship exists to prevent.

This SHALL hold in every runtime: the shared core's ship path SHALL answer a
refused write this way, and SHALL NOT make the loss visible only as a line a
user has to be reading at the time.

#### Scenario: The control channel is refused while the runner is shutting down

- **WHEN** a runner writes `RunnerShutdownNotification` during graceful shutdown
  and the write is refused
- **THEN** the frame SHALL be held for the next bound stream and SHALL NOT be
  discarded behind a printed line

#### Scenario: A shed shutdown ship is counted

- **WHEN** a held shutdown notification is shed at the ring's bound
- **THEN** the shed SHALL be counted where every drop-oldest on this wire is
  counted, so a lost final state is visible as a number rather than as silence

#### Scenario: The ack timeout is unchanged by the hold

- **WHEN** the notification is written successfully and no `RunnerShutdownAck`
  arrives within the 2 s budget
- **THEN** the runner SHALL proceed to exit exactly as it does today, because an
  unacked frame and an unwritten frame are different facts

### Requirement: One slow stream reader SHALL NOT stall a runner's connection

The runner's QUIC client SHALL serve every stream of a connection without ever
waiting for one stream's reader. The client drives all of a connection's streams
from a single thread that also expires the connection's timers, pumps its socket
and takes its commands, so a wait for a reader is a wait for the keep-alive, for
the idle timeout, and for every other stream on that connection including the
control channel. A reader that has stopped reading SHALL cost its own stream and
nothing else.

Backpressure SHALL be carried by the protocol rather than by the driver. When a
stream's reader is too far behind to take another chunk, the client SHALL suspend
that stream and leave what it has not read where the transport's own flow control
can see it, so the peer is slowed by the connection rather than by a stalled
thread. A chunk the reader would not take SHALL be held and offered again, never
discarded and never reordered: a stream is ordered bytes and a gap in it cannot
be repaired by any later read.

A suspended stream SHALL resume once its reader has room, without waiting for the
connection's next timer, and SHALL deliver every byte the peer wrote, once, in
the order it was written.

#### Scenario: A reader that stops reading leaves the connection serving

- **GIVEN** a runner connection carrying two streams
- **WHEN** the controller writes far more on one stream than its reader will take,
  and that reader stops reading
- **THEN** the client SHALL go on serving the other stream and the connection's
  timers, and SHALL NOT wait for the stopped reader

#### Scenario: A suspended stream resumes without losing or reordering a byte

- **WHEN** the reader of a suspended stream drains what it has
- **THEN** the stream SHALL resume, and the reader SHALL receive every byte the
  peer wrote, exactly once and in the order it was written

### Requirement: The controller SHALL NOT record a stream handover it has not been answered

The controller SHALL treat a runner's own answer to `ConnectSocket` as the outcome of the
handover, and SHALL NOT report the handover as successful before that answer arrives. A
clause that stores a reference for a later reply SHALL leave its caller unanswered until
the reply is resolved, so that a refusal reaches the caller instead of being logged into a
reply nobody is waiting on.

A handover the runner refuses SHALL end the control session rather than leave the runner
admitted with no data plane. Ending the session is what makes the failure self-clearing:
the controller reclaims the runner's relays and places them somewhere that works.

The decision SHALL NOT be held inside a node-wide process. `FleetManager` serves every
namespace on the node. The outcome SHALL therefore be resolved without any node-wide call blocking on
an untrusted peer.

#### Scenario: A refused socket connect fails the handshake
- **GIVEN** a runner that answers `ConnectSocket` with an error
- **WHEN** the controller resolves that answer
- **THEN** the session SHALL NOT be recorded as connected, the control session SHALL end, and the runner's relays SHALL be reclaimed for placement

#### Scenario: An accepted socket connect completes the handshake
- **GIVEN** a runner that answers `ConnectSocket` successfully
- **WHEN** the controller resolves that answer
- **THEN** the handover SHALL complete as it does today and the handshake SHALL count as completed for the reconnect ladder

#### Scenario: No node-wide process waits on the peer
- **WHEN** a runner is slow to answer or never answers `ConnectSocket`
- **THEN** `FleetManager` SHALL continue to serve other namespaces throughout

### Requirement: A data-socket connect that cannot be completed SHALL be refused, never panicked

The shared core SHALL surface every failure of a `ConnectSocket` execution to the
controller as a refusal, and no production path under `crates/flz_core/src/command/` SHALL
`panic!`, `unwrap` or `expect` on it. The UDS handler SHALL take the same error channel the
QUIC handler already takes, so that a refusal has somewhere to go instead of unwinding the
coordinator thread that owns the control stream.

The refusal SHALL be paired with the controller ending the session, because the thread
death it replaces was self-clearing: the stream closed and the runner's relays were
reclaimed. A refusal that leaves the session alive and the runner unplaceable is worse than
the panic it replaces, so it SHALL NOT be introduced on its own.

#### Scenario: An unresolvable socket directory is refused
- **GIVEN** a data socket directory that cannot be resolved
- **WHEN** the runner executes `ConnectSocket`
- **THEN** the core SHALL answer the controller with a refusal naming the directory, and the coordinator thread SHALL remain alive

#### Scenario: A socket the controller has not bound is refused
- **GIVEN** a data socket path the controller has not yet bound
- **WHEN** the runner dials it
- **THEN** the core SHALL answer with a refusal rather than panicking, and the controller SHALL end the session

#### Scenario: A descriptor or lock failure is refused
- **GIVEN** a `try_clone` that fails under descriptor exhaustion, or a poisoned stream-table lock
- **WHEN** either is reached while installing the connected streams
- **THEN** the core SHALL answer with a refusal rather than unwinding the thread

### Requirement: A lost data plane SHALL be repaired inside the live session

A runner whose data streams close while its control channel is still alive SHALL NOT tear
down the control session to get them back. A data plane is whole or gone: a runner that
loses one data stream lets the rest go, and that is its request for a rebind, carried on
the wire it already has. The controller, which sees its own end of every data stream
close, SHALL answer by re-issuing `ConnectSocket` for the live session, the remedy
`FleetManager.rebind_data_streams/1` implements.

The re-issue SHALL release every data channel of the session first. The runner opens a
whole pair set, and a pool that still holds the old streams binds only as many fresh ones
as it has free channels and closes the rest, which leaves the runner emitting on sockets
nobody reads. One rebind runs at a time, because every channel of a severed data plane
reports the same fault.

Ending the control session for a data-plane fault is what turns one severed stream into a
reconnect loop: the session ends, the runner reconnects, the controller admits it, the
same fault severs it again, and the remedy is never reached because the runner tore the
session down first. A repair inside the live session SHALL NOT touch the reconnect ladder,
because the session did not end.

A repair that cannot be completed SHALL end the session rather than repeat without bound:
a session gets three rebinds, and the data-plane loss after them ends it, so a runner
whose data plane cannot be bound is reclaimed and placed elsewhere. A runner whose data
plane stays gone with no rebind arriving keeps its own heartbeat and write-failure ladders
as the backstop.

#### Scenario: Severed data streams are rebound without a reconnect
- **GIVEN** a runner whose control channel is alive and every secondary stream breaks at the controller's end
- **WHEN** the runner and the controller observe the breaks
- **THEN** the runner SHALL NOT reconnect for it, the controller SHALL release the session's data channels and re-issue `ConnectSocket`, and production SHALL resume on the rebound streams
- **AND** the session SHALL be the same one, with no reconnect attempt logged for that fault

#### Scenario: A dead control channel still reconnects
- **GIVEN** a runner whose control channel itself closes
- **WHEN** the close is observed
- **THEN** the existing reconnect path SHALL run unchanged, on the ladder rung the session left behind

#### Scenario: A repair that cannot complete ends the session
- **GIVEN** a controller that cannot bind the runner's data streams
- **WHEN** the rebind is refused three times in one session
- **THEN** the control session SHALL end, so the runner reconnects and its relays are reclaimed and placed elsewhere

### Requirement: A disconnect SHALL tear down the session's tree whatever the roster holds

A runner disconnect SHALL run its teardown for the disconnecting session regardless of
whether the roster still keys the runner's namespace. The roster records where a runner may
be placed; it SHALL NOT gate whether a session that has ended is dismantled. A disconnect
handler that returns early on a missing roster key leaves the runner's whole supervision
tree running with no session behind it.

Teardown SHALL be idempotent: a second disconnect for a runner already torn down SHALL be a
no-op rather than an error.

#### Scenario: A disconnect for an unkeyed namespace still tears down
- **GIVEN** a runner whose namespace the roster no longer keys
- **WHEN** its control session disconnects
- **THEN** its relays SHALL be detached and its supervision tree SHALL be stopped

#### Scenario: A repeated disconnect changes nothing
- **GIVEN** a runner whose tree has already been stopped by a disconnect
- **WHEN** a second disconnect arrives for the same runner
- **THEN** the handler SHALL make no further change and SHALL NOT raise

### Requirement: A controller SHALL refuse to boot on a socket directory with no room for a data socket

A controller with UDS enabled SHALL refuse to boot when its socket directory leaves no room for
the longest data socket a runner can be given: a `secondary` data socket whose id field is at
the bound the data socket name applies. It SHALL compose that path through the implementation
that composes every data socket, so the boot and a runner's connect apply one arithmetic and
one address limit. The refusal SHALL name the directory, its byte length, the address limit and
`FLEZHA_SOCKET_FILES_PATH`, and SHALL come before anything in the directory is removed.

With a 24-byte id field, the directory is at most 60 bytes under Linux's 107-byte address limit
and at most 56 bytes under macOS's 103-byte limit. A directory longer than that but short
enough to hold the control socket would otherwise let a controller boot and then refuse at
connect every runner whose id is long.

#### Scenario: A directory with no room for a data socket refuses the boot
- **WHEN** a controller on Linux prepares a socket directory of 61 bytes that already holds a file
- **THEN** it refuses to boot, naming the directory, its length of 61 bytes, the 107-byte address limit and `FLEZHA_SOCKET_FILES_PATH`, and the file is still there

#### Scenario: A directory at the limit is prepared
- **WHEN** a controller on Linux prepares a socket directory of 60 bytes
- **THEN** the directory is prepared

### Requirement: `FLEZHA_RUNNER_REMOVAL_GRACE_MS` controls inactive-Runner retention

The Controller SHALL retain a disconnected Runner's peer entry (including its assigned Relays in the `:inactive` set) for the duration specified by the `FLEZHA_RUNNER_REMOVAL_GRACE_MS` environment variable, application key `:runner_removal_grace_ms` (default 30 000 ms; `0` disables the hold). After that window elapses without reconnect, the Controller SHALL discard the inactive entry and ProcessHub SHALL redistribute its Relays. The former name `FLEZHA_RUNNER_KEEPALIVE` SHALL NOT be read.

#### Scenario: Reconnect within keepalive preserves Relays
- **GIVEN** a Runner with assigned Relays `[r1, r2]` disconnects
- **AND** `FLEZHA_RUNNER_REMOVAL_GRACE_MS=30000`
- **WHEN** the same Runner reconnects 10 seconds later with the same identity
- **THEN** the Controller SHALL re-issue `AddRelay` for `r1` and `r2` to the reconnected Runner without redistributing them

#### Scenario: Reconnect after keepalive triggers redistribution
- **GIVEN** a Runner with assigned Relays disconnects and `FLEZHA_RUNNER_REMOVAL_GRACE_MS=30000`
- **WHEN** no reconnect occurs within 30 seconds
- **THEN** the Controller SHALL discard the inactive entry; ProcessHub SHALL redistribute the Relays to other Runners via normal placement

#### Scenario: The former name is not read
- **WHEN** a Controller starts with `FLEZHA_RUNNER_KEEPALIVE=0` and the new variable unset
- **THEN** the removal grace SHALL be the default 30 000 ms

