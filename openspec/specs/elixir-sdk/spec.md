# elixir-sdk Specification

## Purpose
TBD - created by archiving change elixir-sdk-native-transport. Update Purpose after archive.
## Requirements
### Requirement: The Elixir runner entry point mirrors the cross-SDK runner-as-handle surface
The SDK SHALL expose `Flezha.SDK.create_runner/2` — `create_runner(namespace, opts)` with `transport: :uds | :quic` defaulting to `:uds` — returning a runner handle carrying its own declared factory set (empty at creation; no connection, no singleton claim, no process spawn), with `register_factory/2` (a relay module of either run, the run read from the module's `run/0`, `:flow` when it defines none; a module that is not a relay answers `{:error, %Flezha.SDK.Error{code: "topology.no_relay_exported"}}` naming the module in its details; the body is `channels/1` for both) and its inverse `unregister_factory/2`, and the lifecycle verbs `start/1`, `await_termination/1`, `run/1` (exactly `start/1` then `await_termination/1`, never an independent implementation), `close/1`, and `namespace/1`. The process-wide active-runner slot SHALL be claimed atomically inside `start/1` — any number of unstarted handles may exist; the loser receives `{:error, %Flezha.SDK.Error{code: "runtime.runner_already_running"}}` naming `ns`; double-start on one handle errors; closing a never-started handle is a no-op. Zero-relay start SHALL be legal. Authoring and connection failures SHALL be named error tuples returned at the causing call site (mirroring the Rust `FlezhaError` variants) — never a raise. `Flezha.SDK.child_spec/1` SHALL accept `namespace:`, `relays:` (a list of relay modules of either run), `transport:` and `namespace_secret:`, and wrap exactly the create → register → start → await sequence for the host application's supervision tree — it MUST NOT introduce a second lifecycle path. On start the SDK SHALL declare every registered class to `flz_core` and start the core's runner, which connects over the selected transport.

#### Scenario: SDK boots and binds
- **WHEN** a host app starts `{Flezha.SDK, namespace: "default", relays: [MyRelay]}` with a controller reachable over UDS
- **THEN** FleetManager lists one active runner advertising `MyRelay`'s factory id, and the runner's primary/secondary stream channels are bound

#### Scenario: Handle form is equivalent to the child spec form
- **WHEN** a caller runs `Flezha.SDK.create_runner("default") |> Flezha.SDK.register_factory(MyRelay) |> Flezha.SDK.start()`
- **THEN** the resulting connection, declared factory set, and FleetManager registration are identical to the child-spec form

#### Scenario: Second start loses the slot at start, not at create
- **WHEN** two handles are created for the same node and both call `start/1`
- **THEN** exactly one wins; the loser receives `{:error, %Flezha.SDK.Error{code: "runtime.runner_already_running"}}` naming `ns` having touched no shared state

#### Scenario: Zero-relay start is legal
- **WHEN** a handle with an empty declared set calls `start/1`
- **THEN** the runner connects and registers with zero placeable factories, without error

#### Scenario: The run is a setting, not the body's name
- **WHEN** a hand-written module declares `@behaviour Flezha.SDK.Relay` with `factory_id/0`, `channels/1` and `run/0` answering `:steps`, and another declares the same two functions and no `run/0`
- **THEN** `register_factory/2` declares the first as a steps relay and the second as a flow relay

#### Scenario: The native transport is gone
- **WHEN** a caller passes `transport: :native`
- **THEN** `create_runner/2` SHALL answer `{:error, %Flezha.SDK.Error{}}` naming the transport and the two it accepts

### Requirement: One SDK core serves two deployment modes
The SDK SHALL reach a controller one way, through `flz_core`'s runner over UDS or QUIC, wherever the controller runs: in the same BEAM, on the same machine, or on another machine. A runner started in the controller's own node SHALL connect over UDS like any other runner, and SHALL present a runner credential when the controller enforces authorization. The controller SHALL NOT be able to distinguish an Elixir runner from any other SDK's runner except by its language byte. Distributed Erlang SHALL NOT be a runner transport.

#### Scenario: Standalone runner over UDS
- **WHEN** an Elixir runner node (own OS process) starts with `transport: :uds` against a controller on the same machine
- **THEN** it completes the standard handshake, binds the primary/secondary data sockets, receives placements, dispatches, and emits outputs, with no controller code path differing from any other runner's session

#### Scenario: Standalone runner over QUIC from another machine
- **WHEN** an Elixir runner node starts with `transport: :quic` and a QUIC address pointing at a remote controller
- **THEN** it connects with TLS verification against the configured CA certificate and operates identically to the UDS case

#### Scenario: A runner in the controller's node uses the same path
- **WHEN** the same relay module is started once in the controller's own BEAM and once in a separate runner node, both over UDS
- **THEN** registration, dispatch, outputs, refusals and activity reads are identical

### Requirement: A standalone runner node boots without becoming a controller
The flezha application SHALL support a runner-only boot: with `:controller_enabled` set `false` (env `FLEZHA_CONTROLLER_ENABLED`, default `true`), `Flezha.Application.start/2` SHALL NOT start the controller supervision tree — no transport servers, no HubManager/ProcessHub, no convergence, no state-path migration, no boot-ready gate — leaving the host application to run only SDK processes via `Flezha.SDK.child_spec/1`. A runner of its own needs none of this: it depends on the `flezha_sdk` package alone, which starts no controller. Because a dependency's node never evaluates flezha's `config/*.exs`, the host application SHALL be able to set `config :flezha, controller_enabled: false` itself, and the SDK SHALL NOT depend on any other `:flezha` application-env key being present. The default (`true`) SHALL leave every existing deployment byte-identical.

#### Scenario: Runner-only node starts lean
- **WHEN** a host app boots flezha with `FLEZHA_CONTROLLER_ENABLED=false` and an SDK child spec with `transport: :quic`
- **THEN** no Transport.Server, HubManager, or convergence process is running on that node, and the SDK connects to the remote controller normally

#### Scenario: Default boot is unchanged
- **WHEN** the application boots without the flag
- **THEN** the controller tree starts exactly as before this change

### Requirement: The declared factory set is per-runner and mutable after start
Each runner handle SHALL own its declared factory set, starting empty. `register_factory/2` and `unregister_factory/2` SHALL be legal both before and after `start/1`. A post-start registration SHALL declare the class to the live core, which serializes it into the connect frame of every later handshake, the resync of record, as it does for every veneer host. Removing a type with live instances SHALL be refused with `{:error, %Flezha.SDK.Error{code: "runtime.relay_type_in_use"}}`, its details naming the factory id and the live count; removing an undeclared type SHALL be refused naming the id; defining a duplicate factory id SHALL be refused.

#### Scenario: Post-start definition reaches the core
- **WHEN** `register_factory/2` is called on a started runner
- **THEN** the class is declared to the core, and the next connect handshake carries it in the runner's factory set

#### Scenario: Removing a type with live instances is refused
- **WHEN** `unregister_factory/2` targets a type with at least one live instance on this runner
- **THEN** the call returns the in-use error and the declared set is unchanged

### Requirement: Relay and Tasklet behaviours mirror the current cross-SDK model
The SDK SHALL define `Flezha.SDK.Relay` (callbacks: `factory_id/0`, `channels/1` receiving the graph builder; optional `description/0`, `max_duration_ms/0`, `run/0`, `durable/0`) and `Flezha.SDK.Tasklet` (mandatory callbacks: `name/0`, `init_state/1` receiving the cell key or nil, `run/3` receiving input, state, and a ctx, returning `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`; optional callbacks with defaults: `cell_keys/0` → `[]`, `cells_for/1`, `mailbox_capacity/0` → 1024 with env override `FLEZHA_MAILBOX_CAPACITY` and range 1..=1_048_576, `max_cells/0` → 100_000 and ENFORCED on a dynamic node — at the cap a new key's input is dropped without a Cell and counted, an existing key's input is unaffected — `cell_idle_ttl_ms/0` → `nil` (no eviction), `schema_version/0` → 1, `min_readable_schema_version/0` → the schema version, `migrate_state/2` and `migrate_change/2` → refuse, `apply_change/2`, `compact_after_bytes/0`, `state_from_json/1`, `clock/0` → `:wall` — the declared clock `now/1` serves: `:wall` (epoch milliseconds at dispatch entry), `:logical` (the Cell's durable change seq, `terminal + 1` after a restore) or `:event_time` (what `event_time_of/1` extracts from the input) — `event_time_of/1`, `on_timer/4` receiving the scheduled timestamp, the kind, the state and a ctx and returning what `run/3` returns, and `max_timers_per_cell/0` → 10_000). The ctx SHALL provide `emit/2`, `emit_with_meta/3` (refusing reserved `flz.`-prefixed metadata keys), `emit_keyed/3` and `emit_keyed_with_meta/4` (the output's natural key on a keyed output), `apply/3`, `now/1` (the declared clock's value, evaluated once per invocation; inside `on_timer/4` it equals the scheduled timestamp under every clock), `cell_key/1`, the timer surface `timer_after/2`, `timer_at_event_time/2`, `cancel_processing_time_timer/2` and `cancel_event_time_timer/2` (the `cell_timers` requirement below), `checkpoint/1` (closes the open checkpoint interval of a class declaring checkpoint acknowledgement — every input folded in since the last close, this one included, is acknowledged by the record that closes it; a no-op on any other class, as the reference's is) and `stable_id/1` (thirty-two lowercase hex characters of the frozen FNV-1a-128 over the Cell's wire identity, the big-endian change seq the invocation started at and a big-endian per-invocation ordinal — the cross-language fixture every SDK pins, so the n-th call of a replayed invocation equals the n-th call of the original). On a node whose output is declared keyed, a keyless emit from a Single Cell SHALL be refused at call time with `{:error, %Flezha.SDK.Error{code: "runtime.key_required_on_keyed_channel"}}` naming the channel and nothing emitted; `emit_keyed` on a node with no keyed output SHALL be refused with `{:error, %Flezha.SDK.Error{code: "runtime.key_not_allowed_on_non_keyed_channel"}}` naming the channel. Emits SHALL accumulate per invocation, with their metadata and key, and flush only on `{:ok, _}` — a failing invocation discards its partial emits. `AddRelay` SHALL instantiate the relay's topology with one process per Cell; `RemRelay` SHALL stop it. Inputs to the same Cell SHALL be processed in arrival order; distinct Cells MAY process concurrently.

#### Scenario: End-to-end dispatch
- **WHEN** the Controller starts a relay on the Elixir runner and a frame is sent to its input channel
- **THEN** the owning Cell's `run/3` executes and output-bound outputs arrive back at the relay's Overwatch via the secondary stream, serialized by the output serializer

#### Scenario: Per-Cell ordering
- **WHEN** N inputs with the same cell key are dispatched
- **THEN** the Cell observes them in dispatch order

#### Scenario: Failed invocation discards partial emits
- **WHEN** `run/3` calls `Ctx.emit/2` and then returns `{:error, reason}`
- **THEN** nothing is emitted for that attempt

#### Scenario: Relay removal
- **WHEN** the Controller issues `RemRelay`
- **THEN** the relay's Cell processes terminate and subsequent frames for that relay id are not dispatched

#### Scenario: The cap on dynamic Cells binds
- **GIVEN** a dynamic node whose Tasklet declares `max_cells/0` as 2
- **WHEN** inputs for three distinct keys arrive
- **THEN** two Cells exist, the third key's input is dropped without a Cell and counted, and a later input for either live key is dispatched normally

#### Scenario: A keyless emit on a keyed output is refused in a Single Cell
- **WHEN** a Single Cell whose node's output is declared `keyed/1` calls `Ctx.emit/2`
- **THEN** the call answers `{:error, %Flezha.SDK.Error{code: "runtime.key_required_on_keyed_channel"}}` naming the channel and nothing is emitted, while the same call in a dynamic Cell emits under that Cell's own key

#### Scenario: The declared clock is what now/1 serves
- **WHEN** a Tasklet declares `logical_clock: true` and its Cell is at change seq 4 when an input is dispatched
- **THEN** `Ctx.now/1` inside `run/3` is 4; for a Tasklet that writes `event_time/1` it is `event_time/1` of that input; for a Tasklet that declares neither it is the epoch millisecond read once at dispatch entry

#### Scenario: A firing reaches on_timer/4 with the scheduled timestamp
- **WHEN** a processing-time timer scheduled at 31000 fires on a Cell whose Tasklet exports `on_timer/4`
- **THEN** `on_timer(31000, :processing_time, state, ctx)` runs with `Ctx.now(ctx) == 31000`, a change it records through a `defchange` recorder is journaled, and a value it emits leaves exactly as one emitted from `run/3`

#### Scenario: stable_id is the cross-language fixture and checkpoint closes the interval
- **WHEN** a ctx built over the identity bytes `instance/op/key` at change seq 42 calls `Ctx.stable_id/1` twice, and a ctx on a checkpoint-acknowledging class calls `Ctx.checkpoint/1`
- **THEN** the two ids are `bbf81fd4955afd1724203700a240e822` then `bbf81fd4965afd1724203700a240e95d` (the values the Rust and Python SDKs pin), and the Cell ships the interval's closing record at that invocation's end

### Requirement: The graph builder carries the v1 subset of the producer-first surface
`channels/1` SHALL author topologies with: `input/2` and `input_with/3` (bytes passthrough, String UTF-8, custom parser; a parse failure is an unreadable input, dead-lettered by the core), `into/2` and `into_named/3` (unnamed mentions of one Tasklet module unify into one node — fan-in; `into_named/3` gives the node its identity), edge transforms `map/2`, `filter/2`, `flat_map/2` and edge keying `key_by/2`, `named/2` (names the channel the node produces on, and nothing else: the tasklet keeps its `name/0`, or the identity `into_named/3` gave it), `keyed/1`, `output/2` and `output_with/3` (returning the stream handle — hybrid by default; default serialization bytes/UTF-8/JSON), `call/3`, `merge/1` with `from/3`, `route_latest/4` and `route_latest_named/5`, `branch/2`, `on_full/2`, `retry_max/2`, `retry_backoff_ms/2`, and fan-out by handle reuse. The builder SHALL record the chains and the SDK SHALL declare them to `flz_core` as the class declaration every veneer host writes. Whether the declaration is valid, the consumer edges, the cell mode of each tasklet and every channel's final name SHALL be the core's answer, and every refusal SHALL carry the core's `topology.*` code with all issues in `details.issues`, in one `{:error, %Flezha.SDK.Error{}}` (never a raise). Defaults the Elixir surface implies SHALL be declared explicitly: a Tasklet with a non-empty `cell_keys/0` and no `cells_for/1` routes to every key, a Tasklet exporting `compensate/3` declares its compensate hook, and the clock is the one `Flezha.SDK.Tasklet.clock/1` answers. `sealed/1` SHALL NOT exist.

#### Scenario: Linear chain with a transform and a key
- **WHEN** a relay declares `c |> input("trades") |> filter(pred) |> key_by(key_fn) |> into(Candle) |> output("candles")`
- **THEN** registration succeeds with exactly one Tasklet node in Dynamic mode, and a value reaching Candle has passed the predicate and carries the key

#### Scenario: Transform after key_by is refused
- **WHEN** a `map/2` is declared after `key_by/2` on the same chain
- **THEN** registration returns `{:error, %Flezha.SDK.Error{code: "topology.transform_after_key_by"}}`

#### Scenario: Handle reuse fans out
- **WHEN** one stream handle is used to wire two consumers
- **THEN** both consumers receive every value released on that channel

#### Scenario: named/2 names the channel only
- **WHEN** a node is wired `|> into(Entry) |> named("hop") |> into(Next)`
- **THEN** the channel between them is named `hop`, and Entry's Cells are identified by `Entry.name/0`

#### Scenario: Two channels merge into one tasklet
- **WHEN** a relay declares `c |> merge() |> from(ticks, &{:tick, &1}) |> from(quotes, &{:quote, &1}) |> key_by(&symbol_of/1) |> into(Detector)`
- **THEN** a value from either entry reaches `run/3` as the mapped term keyed by its symbol

#### Scenario: A whole keyset is declared as routing
- **WHEN** a Tasklet declares `cell_keys/0` answering four keys and no `cells_for/1`
- **THEN** the class registers, and each input reaches all four Cells

### Requirement: The SDK is packaging-ready behind named seams
The SDK SHALL be a mix project developed in its own repository and present here as a submodule at `sdk/elixir`, application `flezha_sdk`, licensed Apache-2.0 and buildable as a hex package, that depends on no module of the controller application. A pure module that both the controller and the SDK use SHALL live once, in the SDK, and be imported by the controller; a module only the controller uses SHALL live in the controller. No boundary audit script SHALL exist: the dependency direction is enforced by the compiler, and the SDK SHALL compile with warnings as errors.

#### Scenario: The package builds alone
- **WHEN** `mix hex.build` runs in `sdk/elixir` with no controller code on the path
- **THEN** it SHALL produce a package, and the SDK SHALL compile with warnings as errors

#### Scenario: The controller builds against the submodule and fetches nothing
- **WHEN** the controller compiles here with the submodule checked out and the network unavailable
- **THEN** it SHALL resolve `flezha_sdk` by path into the submodule and SHALL NOT compile or fetch the SDK's native library

#### Scenario: The controller imports, never copies
- **WHEN** the controller is searched for a second copy of a module the SDK carries
- **THEN** none SHALL be found, and the controller SHALL alias the SDK's module

#### Scenario: The controller holds no SDK runtime module
- **WHEN** the controller's aliases of `Flezha.SDK.*` are listed
- **THEN** every one SHALL name a module the SDK itself still uses

### Requirement: The SDK refuses rather than exiting a caller its own process will not answer

Every call the Elixir SDK makes into one of its own processes SHALL wait a bound named by a row of the one configuration table and SHALL answer a target that does not reply with a refusal the caller can act on, never by exiting that caller. A runner runs inside a host application, so a wedged worker, runner process or local run must not take a process the host supervises with it.

The bounded call SHALL be the package's own `Flezha.SDK.SafeCall`, which the controller imports rather than holding a second copy. Exactly one other SDK file, `Flezha.SDK.Call`, SHALL name it, and the rest of the SDK asks that one.

#### Scenario: A suspended target refuses its caller

- **GIVEN** one of the runner's own processes suspended so that it holds a call and never replies
- **WHEN** the SDK calls into it
- **THEN** the caller SHALL read a refusal and stay alive, rather than exiting or waiting for ever

#### Scenario: The bounded call is the package's own

- **WHEN** the SDK package compiles with no controller code on the path
- **THEN** `Flezha.SDK.SafeCall` SHALL resolve inside it, and no SDK file but `Flezha.SDK.Call` SHALL call it

### Requirement: Bench harness covers the elixir runtime on all three transports
`mix bench --runtime elixir` SHALL run the `regression`, `cell`, and `dynamic` shapes over `uds` and `quic`, spawning the `test/apps/elixir_runner` app via the same fleet mechanism as every other runtime, registering factories `do_nothing`, `cell_stress`, and `dynamic_cell` with the rust_runner topologies, producing the same `Throughput: <n> messages/second` output measured by the unchanged controller-side code. The transport SHALL be selected exactly as for every runtime — `scripts/bench.sh -t uds|quic` exporting `FLEZHA_TRANSPORT` — with no new `mix bench` flag, and no transport SHALL be offered that only one runtime can use. `mix bench.roundtrip --runtime elixir` SHALL run the `identity` and `keyed` workloads at durability `off` through the same fleet mechanism, registering `bench_echo` and `bench_keyed_nopersist` as twins of the rust_runner fixtures (same factory ids, output names, output bytes and key function), over `uds` and `quic`, at durability `state` registering `bench_keyed`, the persistent twin, at durability `on` registering `tier_store` and at durability `checkpoint` registering `bench_checkpoint`, read in `channels/1` as the reference reads them; a round-trip run combining a `--runtime` with a durability posture whose capability that runtime's manifest defers SHALL be refused at the task with an error naming that capability, before any runner starts.

#### Scenario: Comparable bench run
- **WHEN** `mix bench --runtime elixir -n 100 -m 1000` runs with the uds transport configured
- **THEN** it completes with a `Throughput:` line measured by the unchanged controller-side measurement code

#### Scenario: The native axis is gone
- **WHEN** `scripts/bench.sh -t native` runs
- **THEN** it exits with an error naming the transports it accepts

#### Scenario: Round-trip tier-off run on the elixir runtime
- **WHEN** `mix bench.roundtrip --runtime elixir --workload keyed --durability off` runs with the uds transport configured
- **THEN** the elixir runner serves `bench_keyed_nopersist`, every record is received exactly once, and the report carries the same throughput and latency columns the rust runtime produces

#### Scenario: Round-trip tiered durability refused by name
- **WHEN** `mix bench.roundtrip --runtime <sdk> --durability on` runs for an SDK whose manifest lists `effectively_once` under `deferred`
- **THEN** the task exits before starting a runner with an error naming the deferred `effectively_once` capability and that SDK's guide

#### Scenario: Round-trip tiered runs on the elixir runtime
- **WHEN** `mix bench.roundtrip --runtime elixir --workload keyed --durability on` and then `--durability checkpoint` run with the uds transport configured
- **THEN** the elixir runner serves `tier_store` and then `bench_checkpoint`, every record is received exactly once with zero duplicates, and the report carries the same columns the rust runtime produces for the same cases

### Requirement: The Elixir SDK owns a language byte and advertises it

The Elixir SDK SHALL hold the allocated language byte `3` and SHALL declare it on every class it declares to the core, which carries it on every factory entry it advertises. The value SHALL come from one constant in the SDK, not be spelled per call site.

Every durable record the core ships for an Elixir class SHALL carry `3` at byte 0, and a restore of a record carrying any other value SHALL be refused per the shared header contract, with the language-mismatch wording the core uses, naming both languages, before the record's format or schema is looked at.

#### Scenario: The language rides every connect
- **WHEN** an Elixir runner connects over UDS or QUIC
- **THEN** its connect init frame SHALL carry `sdk_lang = 3` on every advertised factory entry

#### Scenario: The Elixir runner is not a candidate for another language's factory
- **GIVEN** a namespace where an Elixir runner and a Rust runner both advertise `do_nothing`, established at the Rust language
- **WHEN** a relay of `do_nothing` is placed
- **THEN** the Elixir runner SHALL NOT be a candidate, and the placement SHALL go to the Rust runner

#### Scenario: Every durable record is stamped and a foreign one is refused
- **WHEN** an Elixir class ships a change record, and later a restore for it carries a record stamped with the Rust SDK's byte
- **THEN** the shipped record's byte 0 is `3`, and the restore is refused with the language-mismatch wording naming `elixir (3)` and `rust (1)`

### Requirement: The Elixir SDK declares schema versions per Tasklet and migrates across them

The Elixir SDK SHALL carry `schema_evolution` through the core. A Tasklet's `schema_version/0` (default 1) and `min_readable_schema_version/0` SHALL be declared to the core, which stamps every record and resolves each record on restore. A record the core hands over for migration SHALL be decoded and given to the Tasklet's `migrate_state/2` (a snapshot) or `migrate_change/2` (a change) with the stamped `from_version` and the DECODED term, and the fold SHALL continue with the term the migration returns; a Tasklet that declares no migration SHALL refuse the record naming the absent migration and the version, and a migration that returns `{:error, reason}` or raises SHALL refuse naming the reason and the version, reseating nothing. `test/apps/elixir_runner` SHALL carry `schema_evolution_v1` and `schema_evolution_v2` as twins of the reference's `evolving_cart` pair.

#### Scenario: State written at v1 comes back through the migration a bumped v2 declares
- **GIVEN** an `evolving_cart` Cell on an Elixir runner that accumulated 7 under `schema_evolution_v1` and was compacted
- **WHEN** the relay is stopped with its state preserved and started under `schema_evolution_v2`
- **THEN** the restore migrates the snapshot and the tail, the state reads `total: 7` with `currency: "EUR"`, and a second restart under v2 migrates nothing

#### Scenario: v1 code meeting v2 state is refused, not decoded
- **WHEN** a Cell written under `schema_evolution_v2` is restarted under `schema_evolution_v1` on an Elixir runner
- **THEN** the restore is refused naming schema version 2 and the running version 1, and the relay is left unbound

#### Scenario: A failing migration reseats nothing
- **WHEN** `migrate_state/2` answers `{:error, "bad shape"}` for a snapshot stamped 1
- **THEN** the restore refuses with a reason carrying `bad shape` and `1`, and the Cell's state is unchanged

### Requirement: An Elixir tasklet requests spawn-or-deliver of a steps instance

The Elixir SDK SHALL let a tasklet request spawn-or-deliver of a steps instance in its
own namespace with one call, under the contract the other SDKs implement
(`docs/topics/core/workflows.md`). `Flezha.SDK.Ctx.spawn_steps/4` SHALL take a steps factory id, a binary
payload and a keyword list of options defaulting to `[]`, whose keys are `id:` (an instance id)
and `if_running:` (`:deliver` or `:error`, `:deliver` when an `id:` is given without it). The call
SHALL make the core's spawn request from the invocation's live activation and return a handle
without waiting for the controller. An `if_running:` given without an `id:` SHALL raise
`ArgumentError` at the call and send nothing.

The handle SHALL resolve, through `Flezha.SDK.Spawn.await/2` or `Flezha.SDK.Spawn.try_result/1`,
which read the core's spawn result, to `{:ok, %{id: id, outcome: :spawned | :delivered | :skipped_on_replay}}`, where `id` is the
instance id the request resolved to, or to `{:error, %Flezha.SDK.Error{}}` carrying the core's code.
A refusal SHALL NOT fault the invocation, its emits or its records. A failure no caller read
SHALL be logged once as a warning. A call from a process other than the invoking worker SHALL be refused.

#### Scenario: A free id spawns and a taken id delivers

- **WHEN** an Elixir flow tasklet calls `spawn_steps(ctx, factory_id, payload, id: "w1")`
  twice, and a connected runner offers the steps factory
- **THEN** the first handle resolves `{:ok, %{id: "w1", outcome: :spawned}}`, the second
  `{:ok, %{id: "w1", outcome: :delivered}}`, and the steps instance receives both payloads as
  inputs

#### Scenario: A call without an id spawns under a generated id

- **WHEN** a tasklet calls `spawn_steps(ctx, "worker", payload)` with no options
- **THEN** the handle resolves `{:ok, %{id: id, outcome: :spawned}}` where `id` begins `worker-` and names the running instance

#### Scenario: if_running without an id raises at the call

- **WHEN** a tasklet calls `spawn_steps(ctx, "worker", payload, if_running: :error)`
- **THEN** the call raises `ArgumentError` naming `if_running` and `id`, and no request leaves the runner

#### Scenario: A refusal leaves the invocation intact

- **WHEN** the controller refuses a spawn, and the tasklet emits a value and returns
  `{:ok, state}`
- **THEN** the emit ships, the invocation is not retried, and the handle resolves
  `{:error, %Flezha.SDK.Error{}}` carrying the controller's own code

### Requirement: Socket mode reaches the controller through the SDK's own NIF

The SDK SHALL reach the controller through a Rustler crate of its own, Apache-2.0, that binds `flz_core_ffi::veneer` and SHALL NOT load `flz_nif`. The crate SHALL depend on `flz_core_ffi` at the core's release tag, SHALL be distributed precompiled for the five supported triples through `rustler_precompiled` with a checksum the package carries, built by the core release job, and SHALL build from source when the checksum file names no artefact for the host.

#### Scenario: A runner loads the precompiled NIF
- **WHEN** a runner starts on a supported triple with no Rust toolchain
- **THEN** the SDK SHALL load the precompiled library, verify it against the checksum file, and connect

#### Scenario: The controller's NIF is untouched
- **WHEN** `flz_nif` is inspected after the change
- **THEN** it SHALL hold the server half only and SHALL keep `publish = false`

### Requirement: The Elixir SDK authors with `use` macros that expand to its behaviours
The SDK SHALL provide `use Flezha.SDK.Tasklet` and `use Flezha.SDK.Relay`. Each SHALL set the module's `@behaviour` and, for every option written, define the zero-arity callback of the same name returning the option's value, and SHALL define nothing for an option not written, so every default stays resolved by `function_exported?` as it is for a hand-written module. `use Flezha.SDK.Tasklet` SHALL require `name` and accept `cell_keys`, `mailbox_capacity`, `max_cells`, `idle_ttl_seconds`, `compact_after_bytes`, `schema_version`, `min_readable_schema_version`, `logical_clock` and `max_timers_per_cell`, plus the short-form options; `idle_ttl_seconds` SHALL be a positive integer and `logical_clock` a boolean, any other value being a `CompileError` naming the option; the retired options `cell_idle_ttl_ms` and `clock`, and a `clock/0`, `event_time_of/1` or `cell_idle_ttl_ms/0` the module defines with `def` or `defp`, SHALL each be a `CompileError` naming what replaced it; it SHALL import `Flezha.SDK.Ctx`'s authoring functions except `apply/3`, SHALL remove any `Flezha.SDK.Channels` import the module inherits lexically, and SHALL define `init_state/1` returning `nil` at `@before_compile` when the module defines neither a `state` block nor its own `init_state/1`. `use Flezha.SDK.Relay` SHALL require `factory_id` and accept `description`, `max_duration_ms`, `run` and `durable`; under `run: {:steps, ...}` `each` SHALL accept only `:once`, `:per_input` and `:chained`, and `on_failure` only `:undo_all` and `:keep_progress`, any other value, or a setting under a parent that does not carry it, being a `CompileError` naming it. The relay macro SHALL import `Flezha.SDK.Channels`; a tasklet of either run ends its instance with `complete/2`, which `use Flezha.SDK.Tasklet` imports with the rest of `Flezha.SDK.Ctx`. An unknown option, or a missing required one, SHALL be a `CompileError` naming it. Callbacks that take arguments stay `def`s the author writes. A module that `use`s `Flezha.SDK.Tasklet`, or a short-form relay module, that writes `event_time/1` and declares `logical_clock: true` SHALL fail to compile with a `CompileError` naming both and the module the author wrote. A module written with `@behaviour` and hand-written callbacks is the reference form: it SHALL keep registering and running, with the clock and idle TTL read from its callbacks, and SHALL be documented on the Elixir SDK reference page only, never in the guides.

#### Scenario: A written option is a callback, an unwritten one is not

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "position", max_cells: 50_000`
- **THEN** it exports `name/0` returning `"position"` and `max_cells/0` returning 50_000, and `function_exported?(module, :mailbox_capacity, 0)` is false, so `FLEZHA_MAILBOX_CAPACITY` still decides its mailbox

#### Scenario: A stateless tasklet needs no init_state

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "echo"` and only `run/3`
- **THEN** it compiles, `init_state(nil)` answers `nil`, and a flow relay wiring it into an output echoes its input

#### Scenario: A nested tasklet does not see the relay's builder

- **WHEN** a module `use`s `Flezha.SDK.Relay` and nests a module that `use`s `Flezha.SDK.Tasklet` and defines its own `input/0`
- **THEN** both compile, and the nested module's `input/0` does not conflict with `Flezha.SDK.Channels.input/2`

#### Scenario: A misspelt option fails to compile

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "p", max_entites: 10`
- **THEN** compilation fails with a `CompileError` naming `max_entites`

#### Scenario: A retired steps relay option fails to compile

- **WHEN** a module writes `use Flezha.SDK.Relay, factory_id: "w", retry_attempts: 5` or `use Flezha.SDK.Relay, factory_id: "w", wake_after_ms: 5000`
- **THEN** compilation fails with a `CompileError` naming the option, as for any unknown option

#### Scenario: The behaviour form is unchanged

- **WHEN** a module implements `Flezha.SDK.Tasklet` with `@behaviour` and hand-written callbacks, including `apply_change/2`, `idle_ttl_seconds/0` and `event_time/1`
- **THEN** it registers and runs, its changes fold through its own `apply_change/2`, its Cells are evicted after the declared seconds, and `Ctx.now/1` serves its event time

#### Scenario: complete/2 is called without its module

- **WHEN** a tasklet of a steps relay that `use`s `Flezha.SDK.Tasklet` calls `complete(ctx, "done")` in `run/3`
- **THEN** it compiles with no warning and the instance completes with the result `"done"`, exactly as a call to `Flezha.SDK.Ctx.complete/2` would

#### Scenario: The idle TTL is a whole number of seconds

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "session", idle_ttl_seconds: 1`, and another writes `idle_ttl_seconds: 0.2`
- **THEN** the first exports `idle_ttl_seconds/0` returning 1 and its idle dynamic Cells are evicted after one second, and the second fails to compile with a `CompileError` naming `idle_ttl_seconds`

#### Scenario: Two clocks do not compile

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "quiet", logical_clock: true` and defines `event_time/1`, or a short-form relay module does the same with `logical_clock: true` on its relay macro
- **THEN** compilation fails with a `CompileError` naming `event_time/1`, `logical_clock` and the module the author wrote

#### Scenario: A retired option or callback names what replaced it

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "p", clock: :logical`, or defines `event_time_of/1` beside `use Flezha.SDK.Tasklet`, or a hand-written `@behaviour Flezha.SDK.Tasklet` module exports `clock/0`
- **THEN** the first two fail to compile naming `logical_clock: true` and `event_time/1` respectively, and registering a relay that wires the third is refused with `{:RetiredCallback, name, "clock/0", replacement}`

#### Scenario: A logical clock on a changeless tasklet still compiles

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "nochange", logical_clock: true` and declares no `defchange`
- **THEN** it compiles, and registering a relay that wires it is refused with `topology.unservable_clock` naming `nochange`

### Requirement: The single-tasklet short form is three `use` options
The short form SHALL be declared with `use Flezha.SDK.Relay`, the same macro that declares a full-form relay, and SHALL NOT be declared with `use Flezha.SDK.Tasklet`, which SHALL accept none of `run`, `output` or `input` and SHALL raise a `CompileError` naming the option and the relay macro when one is given. The relay macro SHALL accept `output` and `input` beside the `factory_id` it already requires. How it runs is the `run` setting, written as on a full form.

A module SHALL be a short form exactly when it defines `run/3` and no `channels/1`, the one topology body every relay shares. Defining both SHALL be a `CompileError` beginning `topology.short_form_with_topology`, and defining neither SHALL be a `CompileError` naming the missing body; both SHALL be decided at `@before_compile`, where the module's definitions are known. A short form MAY write the class settings (`run`, `durable`, `max_duration_ms`, `description`), which the macro defines exactly as it does for a full form.

With `output`, the macro SHALL generate `factory_id/0` equal to the `factory_id` option and `channels/1` wiring an entry channel named `"input"` (or the `input` value) into a generated nested tasklet module and that module to the `output` plugin channel; with no `output`, it SHALL wire the entry channel into the tasklet module and declare no plugin channel. The nested module SHALL implement `Flezha.SDK.Tasklet`, SHALL answer the `factory_id` verbatim from `name/0`, and SHALL delegate `run/3` to the declaring module, so the expansion is a separate tasklet and a separate relay rather than one module in both roles. Every other relay callback SHALL be left undefined so its default applies, and a steps short form SHALL be a `once` steps relay unless it writes `each:`. The module SHALL be registered as any relay is: listed under `relays:` in the child spec, or passed to `register_factory/2`.

The Elixir SDK cannot see a tasklet's output type, owes no `topology.output_without_type`, and SHALL NOT list `shorthand_typed_output` in its manifest. A duplicate factory id SHALL be refused as `{:error, %Flezha.SDK.Error{code: "topology.duplicate_factory_id"}}` naming the id in its details.

#### Scenario: A relay module with a run body is a whole flow relay

- **WHEN** a module writes `use Flezha.SDK.Relay, factory_id: "upcase", output: "out"` and `run/3`, and the child spec lists it under `relays:`
- **THEN** the runner declares a flow relay `upcase` with entry channel `input` and output `out`, backed by a generated nested tasklet whose `name/0` is `upcase`

#### Scenario: A relay module with a run body is a whole steps relay

- **WHEN** a module writes `use Flezha.SDK.Relay, factory_id: "place", output: "placed", input: "order"` and `run/3`, is listed under `relays:`, an instance is started, and one message is sent on `order`
- **THEN** `run/3` receives the message on `order`, what it emits reaches `placed`, and the instance completes

#### Scenario: A short-form steps relay declares its loop mode

- **WHEN** a module writes `use Flezha.SDK.Relay, factory_id: "chat", output: "replies", each: :per_input` and `run/3`, and two messages reach its entry channel
- **THEN** the module exports `each/0` returning `:per_input`, `run/3` runs once per message, and the instance does not complete after the first

#### Scenario: A short form with no output is declared

- **WHEN** a module writes `use Flezha.SDK.Relay, factory_id: "counter", input: "ticks"`, a `state do ... end` block, a `defchange` and `run/3`
- **THEN** the relay declares `ticks` into the generated tasklet module and no plugin channel, and the `defchange` recorder journals as it does on any tasklet

#### Scenario: An input needs no output

- **WHEN** a module writes `use Flezha.SDK.Relay, factory_id: "counter", input: "ticks"` and `run/3`
- **THEN** it compiles and the entry channel is named `ticks`

#### Scenario: register reads a short form's run

- **WHEN** a module written with `use Flezha.SDK.Relay`, `run: :steps` and an `output` is passed to `register_factory/2`
- **THEN** it is declared as a steps relay, because the macro defined `run/0` from the option, and its generated body is `channels/1` as a flow relay's is

#### Scenario: The generated tasklet is a module of its own

- **WHEN** a short form module `Upcase` is compiled
- **THEN** a nested module implementing `Flezha.SDK.Tasklet` exists whose `run/3` delegates to `Upcase.run/3`, and `Upcase` itself implements no tasklet callback

#### Scenario: A run body beside a channels body does not compile

- **WHEN** a module writes `use Flezha.SDK.Relay, factory_id: "upcase", output: "out"` and defines both `run/3` and `channels/1`
- **THEN** compilation fails with a `CompileError` whose message begins `topology.short_form_with_topology`

#### Scenario: An output on a tasklet does not compile

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "upcase", output: "out"`
- **THEN** compilation fails with a `CompileError` naming `output` and `Flezha.SDK.Relay`

#### Scenario: A relay with neither body does not compile

- **WHEN** a module writes `use Flezha.SDK.Relay, factory_id: "upcase"` and defines neither `run/3` nor `channels/1`
- **THEN** compilation fails naming the missing body

#### Scenario: The behaviour form keeps working

- **WHEN** a module writes `@behaviour Flezha.SDK.Relay` with hand-written `factory_id/0` and `channels/1`
- **THEN** it registers and runs unchanged, and owes no short-form refusal

### Requirement: The Elixir SDK announces each class's run and refuses a misplaced setting

Every class the SDK declares to the core SHALL carry its settings tree as the module writes it: `run/0`, `durable/0`, `description/0` and `max_duration_ms/0`. `use Flezha.SDK.Relay` SHALL refuse a setting written under a parent that does not carry it with a `CompileError` naming the setting and the parent, and a value outside its choice naming the value; a module that writes `run/0` or `durable/0` by hand SHALL meet the core's check at `register_factory/2`, as `topology.unknown_field` or `topology.invalid_setting`, carried in the same `{:error, %Flezha.SDK.Error{}}` as its other refusals are. `Flezha.SDK.Ctx.timer_after/2`, `timer_at_event_time/2`, `cancel_processing_time_timer/2` and `cancel_event_time_timer/2` SHALL answer inside a steps tasklet exactly as inside a flow tasklet, and `on_timer/4` SHALL run on a firing in both runs.

#### Scenario: The handshake carries the run

- **WHEN** a runner starts with a flow relay module and a steps relay module under `relays:`
- **THEN** its handshake's factory entries carry `"run" => "flow"` and `"run" => "steps"` respectively

#### Scenario: The tier under a steps run does not compile

- **WHEN** a module writes `run: {:steps, exactly: :effectively_once}` in its `use`
- **THEN** compilation fails with a `CompileError` naming `exactly` and `steps`

#### Scenario: A hand-written run outside its set is refused

- **WHEN** a module declaring `@behaviour Flezha.SDK.Relay` defines `run/0` returning `{:steps, each: :forever}`
- **THEN** `register_factory/2` answers `{:error, %Flezha.SDK.Error{code: "topology.invalid_setting"}}` whose details name `each` and `forever`

#### Scenario: A timer inside a steps tasklet fires

- **WHEN** a tasklet of a running `:per_input` steps relay calls `Flezha.SDK.Ctx.timer_after(ctx, 300)` in `run/3`
- **THEN** the call answers `{:ok, at}`, the same call inside a flow tasklet answers the same, and `on_timer/4` runs on that Cell with `fired_at == at`

### Requirement: A `state` block declares defaults, and `defchange` is the one way to change state
`use Flezha.SDK.Tasklet` SHALL make `state do … end` and `defchange` available. Inside `state`, `field name, type, default: value` SHALL declare a field, `type` being one of `:integer`, `:float`, `:string`, `:boolean`, `:list`, `:map` or `:term`. The block SHALL generate `init_state/1` returning the map of field defaults, overridable by the author, and `state_from_json/1` reading each field's string key and falling back to its default. It SHALL generate no change: no `apply_change/2` clause and no recorder. `defchange name(state, args…) do body end` SHALL be the one way to change state in the `use` form, in a module with or without a `state` block: it SHALL generate the clause `apply_change(state, {:name, args…})` evaluating `body`, and the recorder `name(state, ctx, args…)`, which records the change through `Flezha.SDK.Ctx.apply/3` and answers the new state, so a mutation spanning several fields is one journal record. A module with at least one `defchange` SHALL export `apply_change/2` and be changeful; a module with none SHALL export no `apply_change/2`, whether or not it has a `state` block. A module that `use`s `Flezha.SDK.Tasklet`, or a short-form relay module, that defines `apply_change/2` itself SHALL fail to compile with a `CompileError` naming `apply_change/2` and directing the author to `defchange`; the delegation the short form generates in its nested tasklet module is not the author's definition and SHALL NOT be refused. A module that has a `state` block and also defines `state_from_json/1` itself SHALL fail to compile, naming the function and the block.

#### Scenario: A named change is one record for several fields

- **WHEN** a tasklet declares `defchange record_fill(state, qty, id) do %{state | qty: state.qty + qty, fill_ids: [id | state.fill_ids]} end` and `run/3` calls `record_fill(state, ctx, 3, "f1")`
- **THEN** exactly one change `{:record_fill, 3, "f1"}` is recorded, and a restore reproduces both fields

#### Scenario: JSON state is read by field name

- **WHEN** the controller hands the tasklet the JSON object `{"qty": 7}` through `init_values`
- **THEN** `state_from_json/1` answers the defaults with `qty` set to 7

#### Scenario: A state block generates no recorder

- **WHEN** a tasklet declares `state do field :qty, :integer, default: 0; field :seen, :list, default: [] end` and no `defchange`
- **THEN** it exports `init_state/1` and `state_from_json/1`, and exports no `apply_change/2`, no `set_qty/3` and no `push_seen/3`

#### Scenario: defchange works without a state block

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "counter"`, its own `init_state/1` returning 0, `defchange add(count, n) do count + n end`, and `run/3` answering `{:ok, add(state, ctx, 1)}`
- **THEN** it compiles, one change `{:add, 1}` is recorded per input, and a restore folds the log back to the count

#### Scenario: A hand-written apply_change does not compile in the use form

- **WHEN** a module that `use`s `Flezha.SDK.Tasklet` defines `apply_change/2`, with or without a `state` block or a `defchange`
- **THEN** compilation fails with a `CompileError` naming `apply_change/2` and `defchange`

### Requirement: The Elixir SDK is a veneer host over flz_core

The Elixir SDK SHALL run every relay through `flz_core` by way of `flz_core_ffi::veneer`, as the Rust, Java, Python, Node, Ruby and PHP SDKs do, and SHALL carry no runtime of its own. Placement, Cells and their mailboxes, the record stream, restore folds, compaction, sleep, timers and clocks, the steps driver, the retry and reconnect ladders, the effectively-once tier, late-input correction, dynamic Cell eviction and caps, spawn handles, parts, recorded effects, local sends, liveness and the activity read SHALL be the core's. The host SHALL answer each activation the core hands it:

- work items by running `run/3` or `on_timer/4`;
- `restore_fold` by folding the snapshot and the changes through `apply_change/2`, migrating with `migrate_state/2` and `migrate_change/2` when a record's stamped version asks for it;
- `snapshot_request` with the encoded state;
- `live_state_read` and `migration_serialize` with the state as JSON;
- `apply_state` through `state_from_json/1`;
- `key_request` with each payload's keys and event time;
- `compensate_run` through `compensate/3`.

A Cell's live state and its pending timer set SHALL be held by the host, keyed by relay, tasklet and Cell key. State snapshots and change bodies SHALL be Erlang External Term Format (format id `7`), and a payload between two tasklets SHALL be ETF.

#### Scenario: The same relays pass the same e2e matrix

- **WHEN** the conformance run executes against `FLEZHA_E2E_RUNTIME=elixir`
- **THEN** the manifest SHALL defer no capability, and every `test/system` module SHALL pass as it does for the reference runtime

#### Scenario: Activity totals come from the core

- **WHEN** the activity of a relay hosted by an Elixir runner is read
- **THEN** every tasklet row SHALL carry the admitted, blocked and dropped totals, and no Elixir code SHALL count them

#### Scenario: No runtime module remains

- **WHEN** `sdk/elixir/lib` is searched for a Cell process, a relay server, a steps driver, a record framer or a topology check
- **THEN** none SHALL be found

### Requirement: The Elixir SDK dispatches on a pool of workers fed by native poll threads

Each worker of the Elixir SDK SHALL be an Elixir process paired with one thread the native library owns. The thread SHALL block in the core's poll and hand each activation to its worker as a message. The worker SHALL run the user code and answer through the core's complete. A thread SHALL poll again only after its worker completed. No call that parks for an activation SHALL run on a BEAM scheduler, dirty or normal. The pool SHALL start at a base size and grow on the core's starvation signal up to a ceiling, both read through `Flezha.SDK.Knob`.

#### Scenario: A parked poll holds no VM thread

- **WHEN** an idle runner with a pool of eight workers waits for work
- **THEN** no dirty or normal scheduler SHALL be occupied by a poll

#### Scenario: A tasklet blocked inside run grows the pool

- **WHEN** every worker is held by a tasklet waiting on a spawn answer and the core reports starvation
- **THEN** the pool SHALL add a worker, up to its ceiling, and new work SHALL be dispatched

### Requirement: One runner or one local run is live in a BEAM at a time

Because the core's activation queue is process-wide, the Elixir SDK SHALL hold one live runner or local run per BEAM, and SHALL refuse a second as `runtime.core_in_use` until the first has stopped and its context is freed. A runner SHALL run once per BEAM: once it has stopped or crashed, a later start in the node SHALL be refused as the core refuses it, so a supervisor's restart reaches the node rather than serving nothing.

#### Scenario: A second local run is refused while one is live

- **WHEN** a local run is live and another local run is started in the same BEAM
- **THEN** the second SHALL be refused with `runtime.core_in_use`, and SHALL start once the first has stopped

### Requirement: The Elixir SDK hands its connection settings to the core explicitly

The SDK SHALL read its connection settings through `Flezha.SDK.Knob`, which reads the OS environment first and the application env second: the socket directory, the transport, the QUIC addresses, the CA certificate, the client certificate and key, and the namespace secret. The SDK SHALL pass them to the core through the core's settings-taking start entry. It SHALL NOT write the process environment. `create_runner/2` and `child_spec/1` SHALL accept `namespace_secret:` for a runner whose controller enforces authorization.

#### Scenario: Application config reaches the core

- **WHEN** a host application sets `config :flezha_sdk, socket_files_path: dir` and starts a runner
- **THEN** the runner SHALL dial the controller socket in `dir`

#### Scenario: A runner in the controller's node finds that controller

- **WHEN** the controller and a runner start in one BEAM with the same socket directory configured
- **THEN** the runner SHALL connect to that controller over UDS, and SHALL NOT dial any other controller on the host

