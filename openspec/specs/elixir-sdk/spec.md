# elixir-sdk Specification

## Purpose
TBD - created by archiving change elixir-sdk-native-transport. Update Purpose after archive.
## Requirements
### Requirement: The Elixir runner entry point mirrors the cross-SDK runner-as-handle surface
The SDK SHALL expose `Flezha.SDK.create_runner/2` — `create_runner(namespace, opts)` with `transport: :native | :uds | :quic` defaulting to `:native` — returning a runner handle carrying its own declared factory set (empty at creation; no connection, no singleton claim, no process spawn), with `define_stream/2` and its inverse `remove_stream/2`, and the lifecycle verbs `start/1`, `await_termination/1`, `run/1` (exactly `start/1` then `await_termination/1`, never an independent implementation), `close/1`, and `namespace/1`. The process-wide active-runner slot SHALL be claimed atomically inside `start/1` — any number of unstarted handles may exist; the loser receives `{:error, {:already_running, ns}}`; double-start on one handle errors; closing a never-started handle is a no-op. Zero-operative start SHALL be legal. Authoring and connection failures SHALL be named error tuples returned at the causing call site (mirroring the Rust `FlezhaError` variants) — never a raise. `Flezha.SDK.child_spec/1` SHALL accept `namespace:`, `streams:` (a list of StreamOperative modules), and `transport:`, and wrap exactly the create → define → start → await sequence for the host application's supervision tree — it MUST NOT introduce a second lifecycle path. On start the SDK SHALL connect over the selected transport with a handshake carrying the namespace, the declared factories (id + description), and its stable identity, then handle `AssignRunnerId` (storing the session token) and `ConnectSocket` (establishing N primary + N secondary streams through the mode's transport).

#### Scenario: SDK boots and binds
- **WHEN** a host app starts `{Flezha.SDK, namespace: "default", streams: [MyOperative]}` on a node running the Controller
- **THEN** FleetManager lists one active runner advertising `MyOperative`'s factory id, and the runner's primary/secondary stream channels are bound

#### Scenario: Handle form is equivalent to the child spec form
- **WHEN** a caller runs `Flezha.SDK.create_runner("default") |> Flezha.SDK.define_stream(MyOperative) |> Flezha.SDK.start()`
- **THEN** the resulting connection, declared factory set, and FleetManager registration are identical to the child-spec form

#### Scenario: Second start loses the slot at start, not at create
- **WHEN** two handles are created for the same node and both call `start/1`
- **THEN** exactly one wins; the loser receives `{:error, {:already_running, ns}}` having touched no shared state

#### Scenario: Zero-operative start is legal
- **WHEN** a handle with an empty declared set calls `start/1`
- **THEN** the runner connects and registers with zero placeable factories, without error

### Requirement: One SDK core serves two deployment modes
The SDK SHALL support both deployment modes from a single dispatch core, differing only in a client-transport seam: **in-node** (`transport: :native` — the Controller runs in the same BEAM; streams are pid-backed handles) and **standalone** (`transport: :uds | :quic` — the runner is its own BEAM node, on the same machine over UDS or a different machine over QUIC, and the Controller runs elsewhere). In the standalone mode the SDK SHALL speak the existing binary protocol byte-identically to the Rust runner — same `connect_control_channel` handshake framing, same per-runner data-socket paths (UDS) / 2-byte `[stream_type, key]` stream preamble (QUIC), same bounded connect retry, same client environment contract (`FLEZHA_SOCKET_FILES_PATH`, `FLEZHA_QUIC_ADDRESS`, `FLEZHA_CA_CERT_PATH`) — and SHALL require **zero controller-side changes**: the controller cannot distinguish a standalone Elixir runner from a Rust runner. Everything above the transport seam — behaviours, builder, dispatch, ladder, guard, heartbeats, identity — SHALL be shared code with identical semantics in both modes. Distributed Erlang SHALL NOT be a runner transport.

#### Scenario: Standalone runner over UDS
- **WHEN** an Elixir runner node (own OS process) starts with `transport: :uds` against a controller on the same machine
- **THEN** it completes the standard handshake, binds N primary/secondary data sockets on the per-runner paths, receives placements, dispatches, and emits sink outputs — with no controller code path differing from a rust_runner session

#### Scenario: Standalone runner over QUIC from another machine
- **WHEN** an Elixir runner node starts with `transport: :quic` and `FLEZHA_QUIC_ADDRESS` pointing at a remote controller
- **THEN** it connects with TLS verification against `FLEZHA_CA_CERT_PATH`, opens data streams with the `[type, key]` preamble, and operates identically to the UDS case

#### Scenario: Same operative code runs in both modes
- **WHEN** the same StreamOperative module is deployed once in-node (`:native`) and once standalone (`:uds`)
- **THEN** registration, dispatch semantics, outputs, refusals, and heartbeat self-reports are identical; only the transport differs

#### Scenario: Connection loss reconnects as a binding swap
- **WHEN** a standalone runner's control connection breaks and the SDK reconnects
- **THEN** it re-runs the connect handshake with the same stable identity and current declared set, and the controller re-points the existing bindings onto the new session token — no `RunnerReconnected`/`ReassignRunnerId` frames are sent (they are dead)

### Requirement: A standalone runner node boots without becoming a controller
The flezha application SHALL support a runner-only boot: with `:controller_enabled` set `false` (env `FLEZHA_CONTROLLER_ENABLED`, default `true`), `Flezha.Application.start/2` SHALL NOT start the controller supervision tree — no transport servers, no HubManager/ProcessHub, no convergence, no state-path migration, no boot-ready gate — leaving the host application to run only SDK processes via `Flezha.SDK.child_spec/1`. Because a dependency's node never evaluates flezha's `config/*.exs`, the host application SHALL be able to set `config :flezha, controller_enabled: false` itself, and the SDK SHALL NOT depend on any other `:flezha` application-env key being present. The default (`true`) SHALL leave every existing deployment byte-identical. A path-dependency test application (`test/apps/elixir_runner`) SHALL exercise this posture end to end.

#### Scenario: Runner-only node starts lean
- **WHEN** a host app boots flezha with `FLEZHA_CONTROLLER_ENABLED=false` and an SDK child spec with `transport: :quic`
- **THEN** no Transport.Server, HubManager, or convergence process is running on that node, and the SDK connects to the remote controller normally

#### Scenario: Default boot is unchanged
- **WHEN** the application boots without the flag
- **THEN** the controller tree starts exactly as before this change

### Requirement: The SDK presents a durable stable identity on every connect
The SDK SHALL derive and persist a stable identity per the cross-SDK contract: format `"<namespace>-<profile_hash8>-<rand6>"` where the profile hash is order-independent over the sorted declared factory ids; persisted via atomic write at `FLEZHA_RUNNER_ID_PATH` (default `/var/lib/flezha-runner/id`); memoized per boot; presented as `stable_id` on **every** connect, initial and reconnect alike, in both deployment modes. When the persistence path is unavailable the SDK SHALL generate a fresh per-boot identity, log a single `warn`, and run normally — it SHALL NOT refuse to start or crash. The retired `previous_runner_id` field SHALL NOT be sent, and the dead protocol tags 20/21 SHALL NOT be implemented.

#### Scenario: Restart re-presents the same identity and takeover applies
- **WHEN** the SDK supervisor restarts after a crash and reconnects with the persistence path intact
- **THEN** the handshake carries the same `stable_id`, and the controller fences the superseded session, swaps its operative bindings to the new session token, and evicts the old session (newest-wins)

#### Scenario: Missing persistence degrades, never fails
- **WHEN** the SDK boots with no writable `FLEZHA_RUNNER_ID_PATH`
- **THEN** it generates a fresh identity, logs one `warn`, and connects normally; a later restart presents a new identity whose prior binding ages out

### Requirement: The declared factory set is per-runner and mutable after start
Each runner handle SHALL own its declared factory set, starting empty. `define_stream/2` and `remove_stream/2` SHALL be legal both before and after `start/1`; a post-start change SHALL ship `UpdateFactories` (tag 33, full current set, fire-and-forget, no ack) over the control channel, and a reconnect handshake SHALL re-serialize the same per-runner set as the resync of record. Removing a type with live instances SHALL be refused with `{:error, {:operative_type_in_use, factory_id, live_count}}`; removing an undeclared type SHALL be refused naming the id; defining a duplicate factory id SHALL be refused.

#### Scenario: Post-start definition reaches the controller
- **WHEN** `define_stream/2` is called on a started runner
- **THEN** an `UpdateFactories` frame carrying the full declared set is sent and the controller's stored factory set for that runner converges to it (full-set replacement)

#### Scenario: Removing a type with live instances is refused
- **WHEN** `remove_stream/2` targets a type with at least one live instance on this runner
- **THEN** the call returns the in-use error and the declared set is unchanged

### Requirement: Operative and Procedure behaviours mirror the current cross-SDK model
The SDK SHALL define `Flezha.SDK.StreamOperative` (callbacks: `factory_id/0`, `channels/1` receiving the graph builder; optional `description/0`) and `Flezha.SDK.Procedure` (mandatory callbacks: `name/0`, `init_state/1` receiving the entity key or nil, `run/3` receiving input, state, and a ctx, returning `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`; optional callbacks with defaults: `entity_keys/0` → `[]`, `entities_for/1`, `mailbox_capacity/0` → 1024 with env override `FLEZHA_MAILBOX_CAPACITY` and range 1..=1_048_576, `max_entities/0` → 100_000). The ctx SHALL provide `emit/2`, `emit_with_meta/3` (refusing reserved `flz.`-prefixed metadata keys), `now/1` (wall-clock milliseconds evaluated once per invocation), and `entity_key/1`. Emits SHALL accumulate per invocation and flush only on `{:ok, _}` — a failing invocation discards its partial emits. `AddOperative` SHALL instantiate the operative's topology with one process per Entity; `RemOperative` SHALL stop it. Inputs to the same Entity SHALL be processed in arrival order; distinct Entities MAY process concurrently.

#### Scenario: End-to-end dispatch
- **WHEN** the Controller starts an operative on the Elixir runner and a frame is sent to its input channel
- **THEN** the owning Entity's `run/3` executes and sink-bound outputs arrive back at the operative's Overwatch via the secondary stream, serialized by the sink serializer

#### Scenario: Per-Entity ordering
- **WHEN** N inputs with the same entity key are dispatched
- **THEN** the Entity observes them in dispatch order

#### Scenario: Failed invocation discards partial emits
- **WHEN** `run/3` calls `Ctx.emit/2` and then returns `{:error, reason}`
- **THEN** nothing is emitted for that attempt

#### Scenario: Operative removal
- **WHEN** the Controller issues `RemOperative`
- **THEN** the operative's Entity processes terminate and subsequent frames for that operative id are not dispatched

### Requirement: The graph builder carries the v1 subset of the producer-first surface
`channels/1` SHALL author topologies with: `input/2` and `input_with/3` (bytes passthrough, String UTF-8, custom parser; a parse failure rides the entry channel's retry→park ladder), `into/2` and `into_named/3` (unnamed mentions of one Procedure module unify into one node — fan-in; node-identity collisions refuse), edge transforms `map/2`, `filter/2`, `flat_map/2` and edge keying `key_by/2` (fused structurally into one composed closure per consumer edge — the runtime applies at most one transform + one key per edge and never observes a chain), `named/2`, `sink/2` and `sink_with/3` (returning the stream handle — hybrid by default; default serialization bytes/UTF-8/JSON), `sealed/1` (type-level terminator adding no topology), `on_full/2`, `retry_max/2`, `retry_backoff_ms/2`, and fan-out by handle reuse. Entity mode SHALL be derived from the wiring exactly as in the Rust SDK: non-empty `entity_keys/0` → Static; a keyed edge into a keyless procedure → Dynamic; otherwise Single. Registration SHALL accumulate refusals into `{:error, {:invalid_topology, issues}}` (never a raise) using the shared issue names, including `KeyByOnUnkeyableTarget`, `UnkeyedEdgeIntoDynamicProcedure`, `TransformAfterKeyBy`, `DuplicateChannel`, `Cycle`, `UnreachableProcedure`, `NodeIdentityCollision`, `MailboxCapacityZero`/`TooLarge`, `RetryMaxZero`, and `DuplicatePluginSink`. Edge type mismatch is the documented per-language asymmetry: with no type tokens, a mis-typed payload surfaces at dispatch and rides the retry→park ladder. Builder surface belonging to deferred capabilities (`merge`, `route_latest`, `branch`, `codec`, `keyed`, tier declarations) SHALL NOT exist in v1; calling it fails registration with an error naming the module.

#### Scenario: Linear chain with a transform and a key
- **WHEN** an operative declares `c |> input("trades") |> filter(pred) |> key_by(key_fn) |> into(Candle) |> sink("candles")`
- **THEN** registration succeeds with exactly one Procedure node in Dynamic mode, and the entry→Candle edge carries one fused closure applying predicate then key

#### Scenario: Transforms fuse structurally
- **WHEN** three inputs flow through a declared filter chain
- **THEN** the predicate is invoked exactly three times and the routing path applied exactly one composed closure per edge (asserted by counting, not topology inspection)

#### Scenario: Transform after key_by is refused
- **WHEN** a `map/2` is declared after `key_by/2` on the same chain
- **THEN** registration returns `{:error, {:invalid_topology, [TransformAfterKeyBy...]}}`

#### Scenario: Handle reuse fans out
- **WHEN** one stream handle is used to wire two consumers
- **THEN** both consumers receive every value released on that channel

#### Scenario: Deferred builder surface fails loudly
- **WHEN** `channels/1` calls a builder function belonging to a deferred capability
- **THEN** registration fails with an error naming the operative module — never a silent no-op, never a crash of the runner

### Requirement: Bounded Entity mailbox with the cross-SDK overflow default
Each Entity SHALL have a bounded queue (capacity from `mailbox_capacity/0`) with an `OverflowPolicy` of `:drop_nothing`, `:drop_newest`, or `:drop_oldest`. The default SHALL be `:drop_nothing` for **every** edge kind (entry, inter-procedure, plugin-bound) — in lockstep with the Rust SDK builder, the Java SDK builder and the FFI host — and an operative that prefers to shed SHALL say so with `on_full/2`. The retired spelling SHALL NOT be accepted by `on_full/2`, by `Flezha.SDK.Stream`'s type, or by any manifest. Under `:drop_nothing` the routing caller SHALL wait for room (guarded by a monitor on the Entity so a dead Entity cannot wedge it) and SHALL neither drop nor refuse the input; the documented co-tenancy cost is that a delayed primary-stream router delays every operative fed by that stream, exactly as the Rust reader parks. Drops SHALL be observable via telemetry; `blocked_parks` SHALL count episodes (the first wait of a contiguous full period), and the `dispatches` heartbeat counter SHALL increment only on completed dispatches so a wedged Entity population is visible to the existing `runner_pool_wedged` classifier verdict.

The Elixir SDK's mechanism is unchanged by this delta: `Flezha.SDK.Entity.handle_call({:enqueue, …})` already answers immediately when there is room and defers the reply into a `waiters` FIFO when there is not, which is what `:drop_nothing` names.

#### Scenario: Default policy discards nothing
- **WHEN** an Entity queue at capacity receives further inputs under the default policy and the Entity later drains
- **THEN** every input is eventually dispatched in order, nothing is dropped, and `blocked_parks` records one episode

#### Scenario: Declared drop policy sheds
- **WHEN** an Entity queue at capacity receives another input under a declared `on_full(:drop_newest)`
- **THEN** the input is dropped, a drop counter increments, and routing continues serving other Entities

#### Scenario: Wedged population is visible, not silent
- **WHEN** an Entity stays at capacity and never drains while the runner stays connected
- **THEN** subsequent heartbeats carry a non-advancing `dispatches` value and `blocked_parks` stops at one episode

#### Scenario: The retired spelling is refused

<!-- overflow-naming-record:begin -->
- **WHEN** `Flezha.SDK.Channels.on_full/2` is called with `:block`
<!-- overflow-naming-record:end -->
- **THEN** it SHALL raise a `FunctionClauseError` as it does for any other unrecognised policy, and no stream handle SHALL be returned

### Requirement: Failing invocations ride the cross-SDK retry-then-park ladder
A failing `run/3` — error tuple, raise, or exit — SHALL be retried up to the input channel's `retry_max` (default 3 total attempts) with exponential backoff plus jitter from `retry_backoff_ms` (default 50 ms base, capped at 5 s); `{:error, {:non_retryable, reason}}` SHALL park immediately; a raise/exit is retryable by default. On exhaustion the SDK SHALL ship a `park_payload` frame whose body is the postcard `ParkBody` byte-compatible with the pinned cross-language fixture (`park_id`, `origin_offset` absent for the tier-off runner, and a `DlqEntryBody` of channel, error, attempts, payload — payload bytes present for binary/String inputs), SHALL hold the Entity, and SHALL resend every 2 s until the controller's `ParkPayloadAck` (sent only after the DLQ store fsync) matches the `park_id` — the existing controller park path SHALL require zero changes, in both deployment modes. The `ParkBody` encoder SHALL live in `Flezha.State.Wire` beside its decoder, sharing the module's postcard helpers.

#### Scenario: Transient failure recovers within the budget
- **WHEN** an invocation fails twice and succeeds on the third attempt
- **THEN** processing continues, nothing is parked, and retry telemetry records the attempts

#### Scenario: Poison payload parks durably and the Entity resumes
- **WHEN** an invocation fails on every attempt of the budget
- **THEN** a `park_payload` ships, the DLQ store persists the entry under the operative class and procedure, the ack releases the Entity, and the next input dispatches normally

#### Scenario: Park frame is byte-compatible
- **WHEN** the SDK's encoded `ParkBody` is compared against the pinned cross-language fixture bytes
- **THEN** they are identical

### Requirement: Partitioned placement is guarded with the controller's own hash
The SDK SHALL decode `AddOperative`'s `partition_index`/`partition_count`/`key_group_from`/`key_group_to` (zero values = unpartitioned; the shipped range is authoritative, never re-derived). When `partition_count >= 2`: a dynamically computed entity key SHALL be admitted against the owned range **before** any Entity state is created — a miss parks the input with the named contract error; statically declared Entities outside the range SHALL be silently idle (`owns` filter); and a topology with no keyed entry-fed procedure SHALL refuse the bind with `PartitionsWithoutKeyedProcedure` naming `key_by`. When `partition_count < 2` the guard SHALL cost no more than one comparison. The key-group function SHALL be a direct call to `Flezha.Partition.group_of/1` (the controller's pinned FNV-1a-128 implementation), verified against the shared fixture vectors.

#### Scenario: Mis-routed frame parks loudly
- **WHEN** a partition member receives a frame whose key hashes outside its range
- **THEN** the input parks with the contract error naming the key, its group, and the owning partition, and no Entity state is created

#### Scenario: Keyless class refuses partitioned bind
- **WHEN** `partition_count >= 2` arrives for a class with no `key_by` edge and no declared entity keys
- **THEN** the `AddOperative` is refused with `PartitionsWithoutKeyedProcedure`

#### Scenario: Hash agrees with the fixtures
- **WHEN** the SDK's key-group function hashes the shared fixture inputs
- **THEN** it produces `"BTC-USD"→129`, `"ETH-USD"→213`, `"SOL-USD"→60`, `""→141`, `"a"→100`

### Requirement: Liveness self-reporting matches the cross-SDK contract
The SDK SHALL echo `control_ping` on the control channel, answer `Ping` with a `PingResponse` carrying the nonce, and emit `data_heartbeat` frames (empty body) every `FLEZHA_DATA_HEARTBEAT_MS` (default 10 s), jittered into `[base/2, base]`, on each bound secondary stream, with metadata carrying at least `dispatches` (incremented only on completed dispatches — the key the `runner_pool_wedged` verdict requires), `blocked_parks`, and `credit_parked` — the count of credit-park wait iterations, so a runner parked on an exhausted window reads as parked rather than wedged beside a frozen `dispatches`. The contract SHALL hold identically in both deployment modes.

#### Scenario: Heartbeats keep idle channels live
- **WHEN** the runner is connected with zero traffic for several minutes
- **THEN** every secondary StreamChannel's `last_seen` keeps advancing and no stall is flagged

#### Scenario: Wedged dispatch is visible
- **WHEN** dispatch progress halts while the runner process remains alive and fed
- **THEN** consecutive heartbeats carry a non-advancing `dispatches` value, enabling the existing `runner_pool_wedged` classifier verdict

#### Scenario: A credit park is visible as a park
- **WHEN** an Entity is parked on an exhausted credit window across two heartbeats
- **THEN** the second heartbeat's `credit_parked` is greater than the first's while `dispatches` stands still

### Requirement: Declarations and deferred commands follow the scope matrix
`AddOperativeResponse.metadata` SHALL be empty for a class declaring nothing (the legal "declared nothing" encoding), except that a keyless topology SHALL declare `keyed: "false"` so the controller's partitioned-start preflight works, and a class whose smallest `compact_after_bytes/0` differs from the 8 MiB default SHALL declare `compact_after_bytes` with that value. The persistence-family commands — `RestoreInitState`, `CompactEntity`, `EntityState`, `EntityApplyState`, `MigrationSerialization` / `MigrationDeserialization`, `CreditWindowInit`, `CreditGrant` — SHALL be answered for real, by the SDK's own re-expression of the kernel's rules (the `state_persistence` requirement below); `EntityMailboxStatus` SHALL be tolerated with a debug log, as the reference tolerates a pre-credit controller. `WorkflowStop` SHALL be tolerated with a warn (no workflow factory is ever advertised). `RunnerDisconnect` SHALL terminate the connection cleanly.

#### Scenario: Declaration-free response is empty
- **WHEN** an operative with a keyed topology and no declarations binds
- **THEN** its `AddOperativeResponse` carries zero metadata bytes

#### Scenario: Keyless topology declares its keyedness
- **WHEN** an operative whose topology has no keyed procedure binds
- **THEN** its response metadata carries `keyed: "false"`, and a later partitioned start of that class is refused at the controller before any member starts

#### Scenario: A non-default compaction threshold is declared
- **WHEN** a class's only changeful Procedure declares `compact_after_bytes/0` as `1_048_576`
- **THEN** its response metadata carries `compact_after_bytes: "1048576"`, and a class at the default carries no such key

### Requirement: Command codec is isolated for future term-native migration and packaging
All SDK-side control-channel encode/decode SHALL go through a single codec module speaking the `RunnerCommands.*` protobuf structs via the already-compiled `Flezha.Runner.CommandsProto` Protox modules. No SDK or native-transport code outside the codec SHALL reference Protox or `RunnerCommands`, and none SHALL assume control messages are binaries.

#### Scenario: Codec is the only protobuf seam
- **WHEN** the SDK source is audited
- **THEN** Protox/`RunnerCommands` references appear only in the codec module

### Requirement: The SDK is packaging-ready behind named seams
Modules under the SDK namespace SHALL reference controller-application modules only through named seams: the codec module (protobuf), the `Flezha.State.Wire.encode_park_body/1` call (postcard), the `Flezha.Partition.group_of/1` call (key-group hash), `Transport.Server.connect/2` (native mode), and the `flz_nif` client entry points (standalone mode). An automated audit SHALL enforce this boundary from this change onward, so the later extraction into a standalone hex package (`package-elixir-sdk` — app split, own Protox namespace, precompiled NIF distribution) is a swap of seam internals, not a rewrite. The hex publication itself is explicitly deferred to that follow-up.

#### Scenario: Boundary audit passes
- **WHEN** the packaging-boundary audit runs over the SDK source
- **THEN** every controller-application reference resolves to one of the named seams, and any new stray reference fails the audit

### Requirement: Bench harness covers the elixir runtime on all three transports
`mix bench --runtime elixir` SHALL run the `regression`, `entity`, and `dynamic` shapes over the `native` transport (in-process SDK start; one runner, because the runner slot is process-wide) and over `uds`/`quic` (spawning the `test/apps/elixir_runner` app via the same fleet mechanism as the rust/java runners), registering factories `do_nothing`, `entity_stress`, and `dynamic_entity` with the rust_runner topologies, producing the same `Throughput: <n> messages/second` output measured by the unchanged controller-side code. The transport SHALL be selected exactly as today — `scripts/bench.sh -t native|uds|quic` exporting `FLEZHA_TRANSPORT`, read by `mix bench` from the application env — with no new `mix bench` flag. The harness SHALL reject `--runtime rust|java` with the native transport; `scripts/bench.sh -t` SHALL accept `native`. The recorded results SHALL include the same-runtime uds-vs-native comparison (the transport measurement) and the elixir-vs-rust comparison on uds (the runtime measurement). `mix bench.roundtrip --runtime elixir` SHALL run the `identity` and `keyed` workloads at durability `off` through the same fleet mechanism, registering `bench_echo` and `bench_keyed_nopersist` as twins of the rust_runner fixtures (same factory ids, sink names, output bytes and key function), over `uds`, `quic` and `native`, and at durability `state` registering `bench_keyed`, the persistent twin; a round-trip run combining `--runtime elixir` with a durability posture whose capability the Elixir manifest defers SHALL be refused at the task with an error naming that capability, before any runner starts.

#### Scenario: Comparable bench run in-process
- **WHEN** `mix bench --runtime elixir -n 100 -m 1000` runs with the native transport configured
- **THEN** it completes with a `Throughput:` line measured by the unchanged controller-side measurement code

#### Scenario: Same-runtime transport comparison
- **WHEN** the regression shape runs as elixir+uds (port-spawned elixir_runner) and elixir+native on the same host
- **THEN** both produce comparable `Throughput:` lines, isolating the transport saving within one runtime

#### Scenario: Invalid axis combination rejected
- **WHEN** `mix bench --runtime rust` runs with the native transport configured
- **THEN** the task exits with an error explaining the combination is invalid

#### Scenario: Round-trip tier-off run on the elixir runtime
- **WHEN** `mix bench.roundtrip --runtime elixir --workload keyed --durability off` runs with the uds transport configured
- **THEN** the elixir runner serves `bench_keyed_nopersist`, every record is received exactly once, and the report carries the same throughput and latency columns the rust runtime produces

#### Scenario: Round-trip tiered durability refused by name
- **WHEN** `mix bench.roundtrip --runtime elixir --durability on` runs
- **THEN** the task exits before starting a runner with an error naming the deferred `effectively_once` capability and the Elixir SDK guide

#### Scenario: Round-trip persistent run on the elixir runtime
- **WHEN** `mix bench.roundtrip --runtime elixir --workload keyed --durability state` runs with the uds transport configured
- **THEN** the elixir runner serves `bench_keyed`, the controller writes a WAL for its Entity, and every record is received exactly once

### Requirement: A crashed Entity is recovered by its Operative, and the recovery is a requirement

The Operative process SHALL trap exits from the Entity processes it starts, SHALL
drop a dead Entity from its live map on `:EXIT`, and SHALL recreate it on that
key's next input. A crashed Entity SHALL NOT take the Operative down with it, and
SHALL NOT leave the key permanently unroutable.

A recreated Entity of a Procedure that declares no `apply_change/2` starts
fresh: its state is a plain term that nothing persists, so the in-flight value
that ended the previous process and any in-memory state are lost. That is the
documented behaviour of a persistence-free Procedure — but it SHALL be stated,
because the other SDKs' hosts owe the same "a dead worker comes back" property
and this is the reference for it.

A recreated Entity of a changeful Procedure SHALL continue on the seq line and
from the state its last shipped record left: the Operative SHALL keep, per
persistent Entity, the state and `change_seq` as of that Entity's last shipped
record, written by the Entity after its records are on the wire, and SHALL
seed the replacement from it. A replacement that restarted at seq 0 would have
every later record dropped by the controller's writer as a duplicate while the
runner kept dispatching, which is a silent divergence and not a recovery.

A crash SHALL be logged naming the operative, the node reference and the entity
key, so a key crashing on every input is visible rather than appearing as silent
inactivity.

#### Scenario: A crashed Entity does not take its Operative down
- **WHEN** an Entity process exits abnormally
- **THEN** the Operative SHALL remain alive, SHALL drop that Entity from its live map, and SHALL keep serving every other key

#### Scenario: The key is routable again on the next input
- **GIVEN** an Entity that crashed
- **WHEN** the next input for that key arrives
- **THEN** a fresh Entity SHALL be started for it and the input SHALL be dispatched

#### Scenario: A repeatedly crashing key is visible
- **WHEN** an Entity crashes on every input for its key
- **THEN** each crash SHALL be logged naming the operative, node reference and entity key

#### Scenario: A persistent Entity comes back on its own seq line
- **GIVEN** a changeful Entity that has shipped records at seq 0 through 4
- **WHEN** its process is killed untrappably and the next input for its key arrives
- **THEN** the replacement dispatches from the state the seq-4 record left and ships that input's record at seq 5, and the controller's writer appends it

### Requirement: The Elixir runner reports handout liveness on its heartbeat

The SDK SHALL report the same two handout-liveness values every other SDK
reports — the number of dispatches currently in flight and the age in
milliseconds of the oldest — in the data-channel heartbeat's metadata map,
beside the counters it already carries. The values SHALL be maintained in the
existing shared counters structure rather than by a new process or message round
trip, and SHALL be reporting-only: the SDK SHALL take no action of its own based
on them.

#### Scenario: In-flight work is visible to the controller
- **GIVEN** an Operative with one Entity mid-dispatch and the rest idle
- **WHEN** the heartbeat is written
- **THEN** its metadata SHALL carry an in-flight count of one and an oldest-age reflecting that dispatch

#### Scenario: An idle runner reports zero
- **WHEN** no dispatch is in flight
- **THEN** the heartbeat SHALL report an in-flight count of zero and an oldest-age of zero

### Requirement: The Elixir SDK owns a language byte and advertises it

The Elixir SDK SHALL hold the allocated language byte `3` and SHALL carry it on every factory it advertises — in the connect init frame's factory entries and in `UpdateFactories` — in both deployment modes (in-node native and standalone UDS/QUIC). The value SHALL come from one constant in the SDK, not be spelled per call site.

The SDK SHALL stamp the same value at byte 0 of every durable record it ships — change records and snapshots alike — and SHALL refuse to restore a record carrying any other value, per the shared header contract, with the language-mismatch wording the core uses, naming both languages, before the record's format or schema is looked at. The byte is what keeps the Elixir SDK's factory ids — which the Rust and Java test apps also advertise — from crossing runtimes at the controller's placement guard, and what keeps another SDK's bytes out of the Elixir codec.

#### Scenario: The language rides both deployment modes
- **WHEN** the SDK connects in native mode and in standalone mode
- **THEN** each mode's connect init frame SHALL carry `sdk_lang = 3` on every advertised factory entry

#### Scenario: A post-start define carries it too
- **WHEN** the SDK defines a factory after connecting and ships `UpdateFactories`
- **THEN** the new entry SHALL carry `sdk_lang = 3`

#### Scenario: The Elixir runner is not a candidate for another language's factory
- **GIVEN** a namespace where an Elixir runner and a Rust runner both advertise `do_nothing`, established at the Rust language
- **WHEN** an operative of `do_nothing` is placed
- **THEN** the Elixir runner SHALL NOT be a candidate, and the placement SHALL go to the Rust runner

#### Scenario: Every durable record is stamped and a foreign one is refused
- **WHEN** an Elixir Entity ships a change record, and later a `RestoreInitState` for it carries a record stamped with the Rust SDK's byte
- **THEN** the shipped record's byte 0 is `3`, and the restore is refused with the language-mismatch wording naming `elixir (3)` and `rust (1)` — never decoded, never reported as a malformed header

### Requirement: The Elixir SDK records entry deliveries and reports idle episodes in both deployment modes

The Elixir SDK SHALL carry the `entry_idle` capability with its own re-expression of both layers, since it inherits nothing from `flz_core`: one `:atomics` cell per entry-channel name, interned in `:persistent_term` when an Operative resolves its topology and never on the delivery path, stamped by one atomic store in the Operative's input handler after the channel is resolved and before the frame is parsed, on `System.monotonic_time/1` anchored at `:erlang.system_info(:start_time)` so that `0` means never delivered; and an SDK-owned process that holds the registrations, evaluates the episode latch on a `Process.send_after` tick at a quarter of the threshold clamped to `[250 ms, 1 s]`, and invokes the observer under `try/rescue/catch` so a raise, throw or exit becomes one warning. The handle SHALL expose `last_entry_delivery/2` (a `DateTime` derived from the gap, or `nil`) and `on_entry_idle/4` (a fun of arity one, or a module implementing `Flezha.SDK.IdleObserver` with `idle/1` and an optional `resumed/1`), both legal before and after `start/1` and identical in native and standalone modes; `start_link/1` SHALL accept `idle_observers:` for the supervised shape. `test/apps/elixir_runner` SHALL carry the `entry_idle_probe` twin and the manifest SHALL list `entry_idle` under `fixtures`.

#### Scenario: A frame on one channel leaves another's stamp alone
- **WHEN** an operative declares entry channels `idle_input` and `idle_report` fanning into one procedure, and a frame arrives on `idle_report`
- **THEN** `last_entry_delivery/2` for `idle_input` is unchanged and for `idle_report` is at or after the frame

#### Scenario: Registration before start is honoured after it
- **WHEN** `on_entry_idle/4` is called on a handle that has not been started, and the runner is then started in native mode and later in standalone mode
- **THEN** the observer fires once for the first silence past the threshold on the channel it named, in either mode, and once for the frame that ends it

#### Scenario: A raising observer costs the runner nothing
- **WHEN** a registered observer raises inside `idle/1`
- **THEN** the SDK logs one warning naming the channel and the episode, the Entity processes and the tick continue, and the next episode invokes the observer again

### Requirement: The Elixir SDK persists per-Entity state in its own core

The Elixir SDK SHALL carry the `state_persistence` capability by re-expressing the kernel's rules in Elixir, inheriting nothing from `flz_core` and changing nothing in the controller, which SHALL keep being unable to tell an Elixir runner from a Rust one. A Procedure that exports `apply_change/2` is changeful and persisted; `Flezha.SDK.Ctx.apply/3` SHALL be the only recorded mutation path, returning `apply_change(state, change)` and recording the change for the record stream, so that one fold is one push; a Procedure without `apply_change/2` is the reference's `NoChange` and ships nothing. `StreamOperative.persistence_enabled/0` (default `true`) SHALL, when `false`, advance the seq and ship nothing. After a successful dispatch the Entity SHALL ship one `entity_state_change` frame per recorded change — the 16-byte header stamping `sdk_lang = 3`, the SDK's version, the Procedure's schema version and the Erlang External Term Format id `7` allocated in `flz_core::header`, over `Flezha.State.Wire.encode_frame/4` and the instance-keyed identity — at `seq = change_seq` advancing from `0`, in dispatch order on one secondary stream, before the dispatch's output frame, each under the credit window's `acquire`. The SDK SHALL answer `RestoreInitState` by folding the snapshot then the change log in the controller's on-disk framing under the Entity's own serialisation — CRC over header and body, language before format before schema, the core's refusal wordings — re-arming `change_seq` to `max(folded terminal, terminal_seq) + 1` and replying `terminal_seq`; `CompactEntity` by shipping `entity_state_snapshot` at `change_seq − 1` after the Entity's records and replying `snapshot_seq` and the body size, with `{0, 0}` and an error for an unknown Entity; `EntityState` with the Decision-10 JSON shape narrowed by its selector, `flz.change_seq` on the reply frame for one Entity, and the refusals `entity_key_without_procedure`, `procedure_not_found`, `entity_not_found` and `entity_busy` by name; `EntityApplyState`, `MigrationDeserialization` and a non-empty `AddOperative.init_values` by handing each Procedure's JSON to its `state_from_json/1` (default: the object itself); `MigrationSerialization` with the whole-operative JSON; `CreditWindowInit` and `CreditGrant` by arming and replenishing one runner-wide window that parks the shipping Entity, never drops, counts `credit_parked`, and is disarmed on reconnect. On graceful termination — SIGTERM through the BEAM's `init:stop`, `Flezha.SDK.close/1`, `RunnerDisconnect` — the RunnerServer SHALL serialise every live operative, ship one `RunnerShutdownNotification` each and wait at most 2 s in parallel for the acks before closing. `test/apps/elixir_runner` SHALL carry the six reference twins and the manifest SHALL list `state_persistence` under `fixtures`.

#### Scenario: A change is durable on the controller and the state is readable by name
- **WHEN** a `store_input` operative on an Elixir runner receives three inputs
- **THEN** the controller's WAL for `{operative_id, "store_input_procedure", ""}` holds three records at seq 0, 1, 2 stamped `sdk_lang = 3` and `format_id = 7`, and `EntityState` answers `{"store_input_procedure": {"binary_inputs": [], "text_inputs": [...]}}` holding the three inputs

#### Scenario: A rebind restores the accumulated state
- **GIVEN** a `resume_probe` Entity that has folded a running total of 100 and was compacted at seq 7
- **WHEN** the operative is stopped with its state preserved and started again on an Elixir runner
- **THEN** the restore folds the snapshot and the tail, answers `terminal_seq` at or above 7, the next input `+7` emits `107`, and its record ships at `terminal_seq + 1`

#### Scenario: Compaction produces a snapshot behind the records
- **WHEN** the controller's sweep sends `CompactEntity` for an Elixir Entity at `change_seq = 50`
- **THEN** an `entity_state_snapshot` frame arrives at seq 49 after every change record the Entity shipped, the reply carries `snapshot_seq = 49`, and the controller writes the snapshot file and truncates the WAL behind it

#### Scenario: A parked sender resumes on a grant and never drops
- **WHEN** an Elixir Entity overdrives a 64 KiB credit window with 300 KiB of change records
- **THEN** it parks on `acquire` between the controller's fsync-minted grants, every record lands in seq order, the heartbeat's `credit_parked` advances, and the controller counts zero sheds and zero overruns

#### Scenario: SIGTERM ships the freshest state
- **WHEN** an Elixir runner holding a `store_input` operative with three applied inputs receives SIGTERM
- **THEN** the controller's `Overwatch.state.snapshot` for that operative holds the Decision-10 JSON with the three inputs within the 2 s ack window, and the runner exits

#### Scenario: A persistence-free class still ships nothing
- **WHEN** a `persistence_off_counter` operative folds inputs, and separately a Procedure without `apply_change/2` dispatches
- **THEN** the controller writes no WAL record for either, the first advances its seq silently and the second holds no seq at all

