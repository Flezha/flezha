# native-transport Specification

## Purpose
TBD - created by archiving change elixir-sdk-native-transport. Update Purpose after archive.
## Requirements
### Requirement: Native stream handles satisfy the existing stream contract through one polymorphic seam

The system SHALL provide a native stream handle type (`%Flezha.SDK.LocalStream{}`, the Elixir SDK's in-node stream — one process per stream, carrying a `:controller` and an `:sdk` side) and a dispatch seam (`Flezha.Transport.Stream`) such that `send_frame/4`, `set_active/1`, `close/1`, `controlling_process/2`, and `stream_id/1` work on both NIF stream resources and native handles, with the NIF arm delegating to `Flezha.Transport.Native` unchanged.

The five modules that call per-stream NIF operations (`transport/server.ex`, `stream_acceptor.ex`, `control_channel.ex`, `stream_channel.ex`, `fleet_manager.ex`) SHALL go through the seam; no per-stream `Native.` call SHALL remain outside `native.ex` and the seam.

The control channel SHALL write through `send_frame/4` (payload-ids `control_command` / `control_ping`) rather than pre-framing in Elixir — `send_data/2` is not part of the seam; the wire bytes are unchanged because the NIF frames in Rust.

<!-- docs-citation-record:begin -->The Elixir-side pre-framing helper `Flezha.Payloader.build_from_bin/3` is removed, and naming it here is the record of that removal.<!-- docs-citation-record:end -->

Server-lifecycle NIFs (`start_uds_server/2`, `start_quic_server/5`, `stop_server/1`, `driver_stats/1`) are out of the seam's scope. `send_frame/4` on a native handle SHALL deliver `{:flz_data, stream_id, payload_id, message, metadata}` to the other side's current owner process through the stream process, without serialization or framing, preserving per-stream order, and no native-transport code SHALL assume `message` is a binary.

#### Scenario: Data frame delivered as a plain message
- **WHEN** a process calls `Flezha.Transport.Stream.send_frame(native_handle, "operative-1", message, metadata)`
- **THEN** the handle's owner process receives `{:flz_data, id, "operative-1", message, metadata}` with `message` and `metadata` passed by term identity (no encode/decode)

#### Scenario: Ownership transfer
- **WHEN** `controlling_process(native_handle, new_pid)` is called
- **THEN** subsequent inbound frames for that side of the stream are delivered to `new_pid` and no longer to the previous owner, and frames that arrived while the side had no active owner are flushed to `new_pid` on `set_active/1` in order

#### Scenario: Control frames are framed once, in Rust
- **WHEN** the control channel sends a command over a UDS or QUIC stream after the switch
- **THEN** the bytes on the wire are identical to the pre-change `send_data(Payloader.build_from_bin(...))` path, and the test-only `write_fun` seam observes `{stream_ref, payload_id, message, metadata}`

#### Scenario: No stray NIF call sites
- **WHEN** the lib tree is audited after the refactor
- **THEN** grep finds no per-stream `Native.send_data`/`send_frame`/`set_active`/`close`/`controlling_process`/`stream_id` call outside `native.ex` and the seam module

#### Scenario: UDS and QUIC unaffected
- **WHEN** the existing UDS and QUIC integration suites run against the polymorphic seam
- **THEN** all pass with no behaviour change

### Requirement: Native connection lifecycle reuses the runner handshake path
The `:native` `Transport.Server` instance SHALL accept connections via a local function call (`connect/2` — init payload map plus the caller's control stream handle; no listener, no NIF server) and dispatch them through the same `FleetManager.connect_handler/4` seam the UDS/QUIC `connect_control_channel` path reaches, with `transport: :native`. The init payload SHALL carry the same content contract as the JSON handshake — `namespace`, `factories` (list of id/description entries), and `stable_id` — passed as a term (no JSON round-trip); the controller's stable-identity resolution (fence superseded session → swap bindings → evict) SHALL apply unchanged. `RunnerSupervisor.init/1` SHALL branch on the transport with explicit clauses — `:uds` starts StreamAcceptors, `:quic` and `:native` skip them, an unknown atom raises naming the value — and native data streams SHALL bind through the same routine QUIC data streams use (`Transport.Server.bind_data_stream/3`: `Flezha.Runner.StreamPool.bind_stream_to_first_nil/2`, then ownership transfer and activation, else close), not a copy of it. The `:native` server instance SHALL be started only when `:native` is configured in `:transports` or when an SDK runner start requests it (idempotently, under `Flezha.Supervisor`).

#### Scenario: Native runner connects
- **WHEN** an in-node SDK calls `connect/2` with a payload carrying `namespace`, `factories`, and `stable_id`
- **THEN** FleetManager registers an active runner with a fresh session token, a RunnerSupervisor starts without StreamAcceptors, and the controller issues `AssignRunnerId` and `ConnectSocket` over the native control stream

#### Scenario: Reconnect with the same stable identity takes over
- **WHEN** a second native connection presents a `stable_id` that already has a live session
- **THEN** the controller resolves it newest-wins — the old session is fenced, its operative bindings swap to the new session token, and the old session is evicted — identically to a UDS/QUIC reconnect

#### Scenario: Stream close synthesized from process death
- **WHEN** the SDK-side owner process of a bound native stream dies
- **THEN** the controller-side channel bound to that stream receives `{:flz_closed, stream_id, reason}` with a binary reason and runs its existing disconnect/unbind handling

#### Scenario: Unknown transport atom fails loudly
- **WHEN** a RunnerSupervisor is started with a transport atom that is neither `:uds`, `:quic`, nor `:native`
- **THEN** init raises an error naming the atom instead of silently starting StreamAcceptors

#### Scenario: The bind routine is named by its owning module
- **WHEN** the lib tree is audited for the data-stream bind routine
- **THEN** exactly one implementation exists, `Flezha.Runner.StreamPool.bind_stream_to_first_nil/2`, and `Flezha.Runner.StreamSup` contains only supervision

### Requirement: Native transport integrates with stats and liveness without exemptions
`Transport.Server.get_stats(:native)` SHALL return the standard stats map with the `:driver` key absent, because the native transport runs no driver thread — the key is absent, never `:undefined`. Native streams SHALL participate in `LivenessCounters` and the secondary-channel heartbeat liveness check (`:io_liveness_check`, 30 s threshold) identically to other transports. The stall classifier SHALL require no native-specific branch: a native-only snapshot carries no driver stats, so the driver clauses simply find nothing to judge, while a UDS or QUIC snapshot carries the one driver self-report shape both server drivers produce.

That one shape SHALL be one shape in the code as well as in the report: every server transport SHALL produce its self-report from the single driver loop, so a field is present for one transport exactly when it is present for the other, and the classifier SHALL need no branch on which transport a snapshot came from.

#### Scenario: Stats shape
- **WHEN** `Transport.Server.get_stats(:native)` is called on a running native transport
- **THEN** it returns `{:ok, %{transport: :native, status: :up, pending_streams: n}}` with no `:driver` key

#### Scenario: Idle native runner does not trigger output_stall
- **WHEN** a native runner with a bound secondary stream is idle but emitting `data_heartbeat` frames
- **THEN** no `[:flezha, :state, :output_stall]` telemetry fires across at least three liveness threshold windows

#### Scenario: The driver self-report has one producer
- **WHEN** the driver stats of a UDS server and of a QUIC server are compared field by field
- **THEN** the same fields SHALL be present in both, because both are produced by the one driver loop, and a field missing from one SHALL be a failing test rather than a documented difference

### Requirement: Native transport is same-node only and opt-in
The native connect path SHALL reject callers from a different BEAM node, SHALL never open a listener of any kind, and SHALL only start when explicitly configured (`:native` in `:transports`) or when an SDK runner start requests it. Transport parsing in `config/runtime.exs` SHALL validate `FLEZHA_TRANSPORT`/`FLEZHA_TRANSPORTS` values against the whitelist `[:uds, :quic, :native]` and refuse boot on an unknown value, replacing the current unvalidated `String.to_atom/1`.

#### Scenario: Remote node rejected
- **WHEN** the native connect function is invoked from a process on another distributed-Erlang node
- **THEN** the connection is refused with an explicit error and no runner is registered

#### Scenario: Not started by default
- **WHEN** the application boots with default configuration
- **THEN** no `:native` Transport.Server instance is running

#### Scenario: Unknown transport value refused at boot
- **WHEN** `FLEZHA_TRANSPORTS=uds,bogus` is set
- **THEN** boot fails with an error naming `bogus` and the accepted values

### Requirement: One driver loop serves every server transport

The transport server driver SHALL be one loop implemented once, and a transport SHALL
be one implementation of the loop's transport contract. `flz_nif` SHALL hold exactly
one generic driver loop (`DriverLoop<T: Transport>`) owning the iteration clock, the
liveness heartbeat, the command drain and every `ServerCommand` arm, send-side framing,
the send-gate ledger, the gate observation and the per-iteration stats snapshot. A
transport implementation SHALL supply only wire operations — bind and accept, read,
write and flush, close, shutdown, and its own poll timeout — and SHALL NOT restate any
job the loop owns. A transport SHALL NOT carry its own copy of a stat, a command arm
or a clock.

A capability the loop provides SHALL be provided to every transport at once. A
capability one transport has and another lacks is a defect of the loop, not a backlog
item for the transport that lacks it.

#### Scenario: A transport is one implementation

- **WHEN** the `flz_nif` source is audited for driver loops
- **THEN** exactly one loop exists, and each supported server transport appears as one
  implementation of the transport contract with no loop of its own

#### Scenario: One conformance table covers both transports

- **WHEN** the driver conformance test runs
- **THEN** the same table of cases SHALL run against every transport implementation,
  asserting for each case that the command is handled, the send-gate ledger balances,
  the stats snapshot carries the field the case moves, and the close reason is the
  same word

#### Scenario: A capability added to the loop reaches every transport

- **WHEN** a job is added to the driver loop
- **THEN** it SHALL be reachable by every transport implementation without editing
  any of them, and no implementation SHALL be able to opt out by omission

### Requirement: A stream close is reported with one word for every transport

The driver SHALL report a stream close with one vocabulary shared by every transport.
The close reason carried in `{:flz_closed, stream_id, reason}` SHALL come from a single
declared set of reasons, and the same wire event SHALL produce the same reason word
regardless of which transport observed it. A reason SHALL be declared once and rendered
once; a transport SHALL NOT spell a shared event with a word of its own.

A reason that only one transport can produce, because the wire only offers it there,
SHALL still be declared in the one set rather than written as a literal at the site
that produces it.

The NIF's client read path SHALL report a close for a stream whose peer ended it even
while the rest of the connection lives on: one stream ending is that stream's close, and
its reader SHALL NOT wait for the whole connection to die before it hears it.

#### Scenario: A peer ending the stream reads the same over every transport

- **WHEN** a connected peer ends a stream cleanly over UDS and, separately, over QUIC
- **THEN** both closes SHALL deliver the same reason word to the owning process

#### Scenario: A transport-specific reason is still declared

- **WHEN** a transport reports an event only its wire can produce, such as a stream
  reset or a lost connection
- **THEN** the reason SHALL be a member of the one declared set, and the set SHALL name
  which transports can produce it

#### Scenario: The client agrees with the server drivers

- **WHEN** the NIF's client read path reports a close
- **THEN** it SHALL use the same declared reason set as the server drivers, so a reason
  read from a log does not depend on which side of the connection produced it

#### Scenario: One stream of a live QUIC client connection ends

- **WHEN** the peer ends one stream of a QUIC client connection, by a clean finish or by
  a reset, while the connection itself stays up
- **THEN** the client's reader of that stream SHALL read it as ended, as it reads a UDS
  stream whose peer ended it, and the NIF's client read path SHALL report the close to
  the stream's owner
- **AND** the client SHALL NOT take the end of that one stream as the loss of the
  connection

