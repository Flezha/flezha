# native-transport Specification

## Purpose
TBD - created by archiving change elixir-sdk-native-transport. Update Purpose after archive.
## Requirements
### Requirement: Native stream handles satisfy the existing stream contract through one polymorphic seam

The system SHALL provide a native stream handle type (`%Flezha.Transport.Local.Stream{}` — one process per stream, carrying a `:controller` and an `:sdk` side) and a dispatch seam (`Flezha.Transport.Stream`) such that `send_frame/4`, `set_active/1`, `close/1`, `controlling_process/2`, and `stream_id/1` work on both NIF stream resources and native handles, with the NIF arm delegating to `Flezha.Transport.Native` unchanged.

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

#### Scenario: Stats shape
- **WHEN** `Transport.Server.get_stats(:native)` is called on a running native transport
- **THEN** it returns `{:ok, %{transport: :native, status: :up, pending_streams: n}}` with no `:driver` key

#### Scenario: Idle native runner does not trigger output_stall
- **WHEN** a native runner with a bound secondary stream is idle but emitting `data_heartbeat` frames
- **THEN** no `[:flezha, :state, :output_stall]` telemetry fires across at least three liveness threshold windows

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

