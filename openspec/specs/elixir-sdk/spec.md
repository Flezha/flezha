# elixir-sdk Specification

## Purpose
TBD - created by archiving change elixir-sdk-native-transport. Update Purpose after archive.
## Requirements
### Requirement: The Elixir runner entry point mirrors the cross-SDK runner-as-handle surface
The SDK SHALL expose `Flezha.SDK.create_runner/2` — `create_runner(namespace, opts)` with `transport: :native | :uds | :quic` defaulting to `:native` — returning a runner handle carrying its own declared factory set (empty at creation; no connection, no singleton claim, no process spawn), with `register_factory/2` (an operator module of either run, the run read from the module's `run/0`, `:flow` when it defines none; a module that is not an operator answers `{:error, %Flezha.SDK.Error{code: "topology.no_operator_exported"}}` naming the module in its details; the body is `channels/1` for both) and its inverse `unregister_factory/2`, and the lifecycle verbs `start/1`, `await_termination/1`, `run/1` (exactly `start/1` then `await_termination/1`, never an independent implementation), `close/1`, and `namespace/1`. The process-wide active-runner slot SHALL be claimed atomically inside `start/1` — any number of unstarted handles may exist; the loser receives `{:error, %Flezha.SDK.Error{code: "runtime.runner_already_running"}}` naming `ns`; double-start on one handle errors; closing a never-started handle is a no-op. Zero-operator start SHALL be legal. Authoring and connection failures SHALL be named error tuples returned at the causing call site (mirroring the Rust `FlezhaError` variants) — never a raise. `Flezha.SDK.child_spec/1` SHALL accept `namespace:`, `operators:` (a list of operator modules of either run), and `transport:`, and wrap exactly the create → register → start → await sequence for the host application's supervision tree — it MUST NOT introduce a second lifecycle path. On start the SDK SHALL connect over the selected transport with a handshake carrying the namespace, the declared factories (id, description, language and run), and its stable identity, then handle `AssignRunnerId` (storing the session token) and `ConnectSocket` (establishing N primary + N secondary streams through the mode's transport).

#### Scenario: SDK boots and binds
- **WHEN** a host app starts `{Flezha.SDK, namespace: "default", operators: [MyOperator]}` on a node running the Controller
- **THEN** FleetManager lists one active runner advertising `MyOperator`'s factory id, and the runner's primary/secondary stream channels are bound

#### Scenario: Handle form is equivalent to the child spec form
- **WHEN** a caller runs `Flezha.SDK.create_runner("default") |> Flezha.SDK.register_factory(MyOperator) |> Flezha.SDK.start()`
- **THEN** the resulting connection, declared factory set, and FleetManager registration are identical to the child-spec form

#### Scenario: Second start loses the slot at start, not at create
- **WHEN** two handles are created for the same node and both call `start/1`
- **THEN** exactly one wins; the loser receives `{:error, %Flezha.SDK.Error{code: "runtime.runner_already_running"}}` naming `ns` having touched no shared state

#### Scenario: Zero-operator start is legal
- **WHEN** a handle with an empty declared set calls `start/1`
- **THEN** the runner connects and registers with zero placeable factories, without error

#### Scenario: The run is a setting, not the body's name
- **WHEN** a hand-written module declares `@behaviour Flezha.SDK.Operator` with `factory_id/0`, `channels/1` and `run/0` answering `:steps`, and another declares the same two functions and no `run/0`
- **THEN** `register_factory/2` declares the first as a steps operator and the second as a flow operator

### Requirement: One SDK core serves two deployment modes
The SDK SHALL support both deployment modes from a single dispatch core, differing only in a client-transport seam: **in-node** (`transport: :native` — the Controller runs in the same BEAM; streams are pid-backed handles) and **standalone** (`transport: :uds | :quic` — the runner is its own BEAM node, on the same machine over UDS or a different machine over QUIC, and the Controller runs elsewhere). In the standalone mode the SDK SHALL speak the existing binary protocol byte-identically to the Rust runner — same `connect_control_channel` handshake framing, same per-runner data-socket paths (UDS) / 2-byte `[stream_type, key]` stream preamble (QUIC), same bounded connect retry, same client environment contract (`FLEZHA_SOCKET_FILES_PATH`, `FLEZHA_QUIC_ADDRESS`, `FLEZHA_CA_CERT_PATH`) — and SHALL require **zero controller-side changes**: the controller cannot distinguish a standalone Elixir runner from a Rust runner. Everything above the transport seam — behaviours, builder, dispatch, ladder, guard, heartbeats, identity — SHALL be shared code with identical semantics in both modes. Distributed Erlang SHALL NOT be a runner transport.

#### Scenario: Standalone runner over UDS
- **WHEN** an Elixir runner node (own OS process) starts with `transport: :uds` against a controller on the same machine
- **THEN** it completes the standard handshake, binds N primary/secondary data sockets on the per-runner paths, receives placements, dispatches, and emits sink outputs — with no controller code path differing from a rust_runner session

#### Scenario: Standalone runner over QUIC from another machine
- **WHEN** an Elixir runner node starts with `transport: :quic` and `FLEZHA_QUIC_ADDRESS` pointing at a remote controller
- **THEN** it connects with TLS verification against `FLEZHA_CA_CERT_PATH`, opens data streams with the `[type, key]` preamble, and operates identically to the UDS case

#### Scenario: Same operator code runs in both modes
- **WHEN** the same Operator module is deployed once in-node (`:native`) and once standalone (`:uds`)
- **THEN** registration, dispatch semantics, outputs, refusals, and heartbeat self-reports are identical; only the transport differs

#### Scenario: Connection loss reconnects as a binding swap
- **WHEN** a standalone runner's control connection breaks and the SDK reconnects
- **THEN** it re-runs the connect handshake with the same stable identity and current declared set, and the controller re-points the existing bindings onto the new session token — no `RunnerReconnected`/`ReassignRunnerId` frames are sent (they are dead)

### Requirement: A standalone runner node boots without becoming a controller
The flezha application SHALL support a runner-only boot: with `:controller_enabled` set `false` (env `FLEZHA_CONTROLLER_ENABLED`, default `true`), `Flezha.Application.start/2` SHALL NOT start the controller supervision tree — no transport servers, no HubManager/ProcessHub, no convergence, no state-path migration, no boot-ready gate — leaving the host application to run only SDK processes via `Flezha.SDK.child_spec/1`. A runner of its own needs none of this: it depends on the `flezha_sdk` package alone, which starts no controller. Because a dependency's node never evaluates flezha's `config/*.exs`, the host application SHALL be able to set `config :flezha, controller_enabled: false` itself, and the SDK SHALL NOT depend on any other `:flezha` application-env key being present. The default (`true`) SHALL leave every existing deployment byte-identical.

#### Scenario: Runner-only node starts lean
- **WHEN** a host app boots flezha with `FLEZHA_CONTROLLER_ENABLED=false` and an SDK child spec with `transport: :quic`
- **THEN** no Transport.Server, HubManager, or convergence process is running on that node, and the SDK connects to the remote controller normally

#### Scenario: Default boot is unchanged
- **WHEN** the application boots without the flag
- **THEN** the controller tree starts exactly as before this change

### Requirement: The SDK presents a durable stable identity on every connect
The SDK SHALL derive and persist a stable identity per the cross-SDK contract: format `"<namespace>-<profile_hash8>-<rand6>"` where the profile hash is order-independent over the sorted declared factory ids; persisted via atomic write at `FLEZHA_RUNNER_ID_PATH` (default `/var/lib/flezha-runner/id`); memoized per boot; presented as `stable_id` on **every** connect, initial and reconnect alike, in both deployment modes. Beside it the SDK SHALL present an `incarnation`: a random 128-bit value as lowercase hex, minted when the runner server starts and held in its state, never persisted, so a reconnect from the same server presents the same one and a server the supervisor restarted, whose operators were linked to it and died with it, presents a new one. When the persistence path is unavailable the SDK SHALL generate a fresh per-boot identity, log a single `warn`, and run normally — it SHALL NOT refuse to start or crash. The retired `previous_runner_id` field SHALL NOT be sent, and the dead protocol tags 20/21 SHALL NOT be implemented.

#### Scenario: Restart re-presents the same identity and takeover applies
- **WHEN** the SDK supervisor restarts after a crash and reconnects with the persistence path intact
- **THEN** the handshake carries the same `stable_id` and a new `incarnation`, and the controller fences the superseded session, evicts it, and binds each of its operators onto the new session through the one bind path, so the restarted server is told `AddOperator` for each with its resume state (newest-wins)

#### Scenario: A reconnect from the same server presents the same incarnation
- **WHEN** the control connection drops and the same runner server reconnects
- **THEN** the handshake carries the same `stable_id` and the same `incarnation`, and the controller re-points the existing bindings onto the new session token without sending `AddOperator` for an operator the server still runs

#### Scenario: Missing persistence degrades, never fails
- **WHEN** the SDK boots with no writable `FLEZHA_RUNNER_ID_PATH`
- **THEN** it generates a fresh identity, logs one `warn`, and connects normally; a later restart presents a new identity whose prior binding ages out

### Requirement: The declared factory set is per-runner and mutable after start
Each runner handle SHALL own its declared factory set, starting empty. `register_factory/2` and `unregister_factory/2` SHALL be legal both before and after `start/1`; a post-start change SHALL ship `UpdateFactories` (tag 33, full current set, fire-and-forget, no ack) over the control channel, and a reconnect handshake SHALL re-serialize the same per-runner set as the resync of record. Removing a type with live instances SHALL be refused with `{:error, %Flezha.SDK.Error{code: "runtime.operator_type_in_use"}}`, its details naming the factory id and the live count; removing an undeclared type SHALL be refused naming the id; defining a duplicate factory id SHALL be refused.

#### Scenario: Post-start definition reaches the controller
- **WHEN** `register_factory/2` is called on a started runner
- **THEN** an `UpdateFactories` frame carrying the full declared set is sent and the controller's stored factory set for that runner converges to it (full-set replacement)

#### Scenario: Removing a type with live instances is refused
- **WHEN** `unregister_factory/2` targets a type with at least one live instance on this runner
- **THEN** the call returns the in-use error and the declared set is unchanged

### Requirement: Operator and Tasklet behaviours mirror the current cross-SDK model
The SDK SHALL define `Flezha.SDK.Operator` (callbacks: `factory_id/0`, `channels/1` receiving the graph builder; optional `description/0`, `max_duration_ms/0`, `run/0`, `durable/0`) and `Flezha.SDK.Tasklet` (mandatory callbacks: `name/0`, `init_state/1` receiving the cell key or nil, `run/3` receiving input, state, and a ctx, returning `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`; optional callbacks with defaults: `cell_keys/0` → `[]`, `cells_for/1`, `mailbox_capacity/0` → 1024 with env override `FLEZHA_MAILBOX_CAPACITY` and range 1..=1_048_576, `max_cells/0` → 100_000 and ENFORCED on a dynamic node — at the cap a new key's input is dropped without a Cell and counted, an existing key's input is unaffected — `cell_idle_ttl_ms/0` → `nil` (no eviction), `schema_version/0` → 1, `min_readable_schema_version/0` → the schema version, `migrate_state/2` and `migrate_change/2` → refuse, `apply_change/2`, `compact_after_bytes/0`, `state_from_json/1`, `clock/0` → `:wall` — the declared clock `now/1` serves: `:wall` (epoch milliseconds at dispatch entry), `:logical` (the Cell's durable change seq, `terminal + 1` after a restore) or `:event_time` (what `event_time_of/1` extracts from the input) — `event_time_of/1`, `on_timer/4` receiving the scheduled timestamp, the kind, the state and a ctx and returning what `run/3` returns, and `max_timers_per_cell/0` → 10_000). The ctx SHALL provide `emit/2`, `emit_with_meta/3` (refusing reserved `flz.`-prefixed metadata keys), `emit_keyed/3` and `emit_keyed_with_meta/4` (the output's natural key on a keyed sink), `apply/3`, `now/1` (the declared clock's value, evaluated once per invocation; inside `on_timer/4` it equals the scheduled timestamp under every clock), `cell_key/1`, the timer surface `timer_after/2`, `timer_at_event_time/2`, `cancel_processing_time_timer/2` and `cancel_event_time_timer/2` (the `cell_timers` requirement below), `checkpoint/1` (closes the open checkpoint interval of a class declaring checkpoint acknowledgement — every input folded in since the last close, this one included, is acknowledged by the record that closes it; a no-op on any other class, as the reference's is) and `stable_id/1` (thirty-two lowercase hex characters of the frozen FNV-1a-128 over the Cell's wire identity, the big-endian change seq the invocation started at and a big-endian per-invocation ordinal — the cross-language fixture every SDK pins, so the n-th call of a replayed invocation equals the n-th call of the original). On a node whose sink is declared keyed, a keyless emit from a Single Cell SHALL be refused at call time with `{:error, %Flezha.SDK.Error{code: "runtime.key_required_on_keyed_channel"}}` naming the channel and nothing emitted; `emit_keyed` on a node with no keyed sink SHALL be refused with `{:error, %Flezha.SDK.Error{code: "runtime.key_not_allowed_on_non_keyed_channel"}}` naming the channel. Emits SHALL accumulate per invocation, with their metadata and key, and flush only on `{:ok, _}` — a failing invocation discards its partial emits. `AddOperator` SHALL instantiate the operator's topology with one process per Cell; `RemOperator` SHALL stop it. Inputs to the same Cell SHALL be processed in arrival order; distinct Cells MAY process concurrently.

#### Scenario: End-to-end dispatch
- **WHEN** the Controller starts an operator on the Elixir runner and a frame is sent to its input channel
- **THEN** the owning Cell's `run/3` executes and sink-bound outputs arrive back at the operator's Overwatch via the secondary stream, serialized by the sink serializer

#### Scenario: Per-Cell ordering
- **WHEN** N inputs with the same cell key are dispatched
- **THEN** the Cell observes them in dispatch order

#### Scenario: Failed invocation discards partial emits
- **WHEN** `run/3` calls `Ctx.emit/2` and then returns `{:error, reason}`
- **THEN** nothing is emitted for that attempt

#### Scenario: Operator removal
- **WHEN** the Controller issues `RemOperator`
- **THEN** the operator's Cell processes terminate and subsequent frames for that operator id are not dispatched

#### Scenario: The cap on dynamic Cells binds
- **GIVEN** a dynamic node whose Tasklet declares `max_cells/0` as 2
- **WHEN** inputs for three distinct keys arrive
- **THEN** two Cells exist, the third key's input is dropped without a Cell and counted, and a later input for either live key is dispatched normally

#### Scenario: A keyless emit on a keyed sink is refused in a Single Cell
- **WHEN** a Single Cell whose node's sink is declared `keyed/1` calls `Ctx.emit/2`
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
`channels/1` SHALL author topologies with: `input/2` and `input_with/3` (bytes passthrough, String UTF-8, custom parser; a parse failure rides the entry channel's retry→park ladder), `into/2` and `into_named/3` (unnamed mentions of one Tasklet module unify into one node — fan-in; node-identity collisions refuse), edge transforms `map/2`, `filter/2`, `flat_map/2` and edge keying `key_by/2` (fused structurally into one composed closure per consumer edge — the runtime applies at most one transform + one key per edge and never observes a chain), `named/2`, `keyed/1` (declares output identity on the sink the handle reaches — the reference's `.keyed()`; on an entry handle, or on a handle that reaches `into/2` before any sink, registration refuses with `topology.keyed_on_entry` / `topology.keyed_channel_without_plugin_wire`), `sink/2` and `sink_with/3` (returning the stream handle — hybrid by default; default serialization bytes/UTF-8/JSON), `on_full/2`, `retry_max/2`, `retry_backoff_ms/2`, and fan-out by handle reuse. Every plugin frame SHALL carry `channel` = the producing node's identity — the tasklet's `name/0`, or the name `named/2` gave the node — never the plugin's name, so a `named/2` survives the plugin terminator and a forward target resolves its entry channel by the name the author declared; an entry routed straight to a sink carries the entry's name. User metadata from `emit_with_meta/3` SHALL ride the frame. Cell mode SHALL be derived from the wiring exactly as in the Rust SDK: non-empty `cell_keys/0` → Static; a keyed edge into a keyless tasklet → Dynamic; otherwise Single. Registration SHALL accumulate refusals into one `{:error, %Flezha.SDK.Error{}}` (never a raise), its `details.issues` every issue, using the shared issue codes, including `topology.key_by_on_unkeyable_target`, `topology.unkeyed_edge_into_dynamic_tasklet`, `topology.transform_after_key_by`, `topology.duplicate_channel`, `topology.cycle`, `topology.unreachable_tasklet`, `topology.node_identity_collision`, `topology.mailbox_capacity_zero`/`topology.mailbox_capacity_too_large`, `topology.retry_max_zero`, `topology.duplicate_plugin_sink`, `topology.keyed_on_entry`, `topology.keyed_channel_without_plugin_wire`, `topology.unservable_clock` (a `:logical` clock on a Tasklet without `apply_change/2` — the reference's build refusal), `topology.two_clocks` (a hand-written Tasklet exporting `event_time/1` whose `logical_clock/0` answers `true`), `topology.retired_callback` (a callback that no longer exists, naming its replacement), and the tier's — `topology.checkpoint_every_zero`, `topology.checkpoint_max_delay_zero`, `topology.unkeyed_channel_under_effectively_once` (naming the sink's channel) and `topology.dropping_policy_under_effectively_once` (naming the node's inbound channel and the policy) — the reference's names for the same contradictions. Edge type mismatch is the documented per-language asymmetry: with no type tokens, a mis-typed payload surfaces at dispatch and rides the retry→park ladder. The tier, its checkpoint and its correction window SHALL be members of the class's `run` tree (`{:flow, exactly: {:effectively_once, ack: ..., correction_ms: ms, beyond_window: ...}}`), never builder calls, so none can be written without the tier or under a steps run; a `beyond_window` with no `correction_ms` is a zero window and therefore a refusal, exactly as the shared core's tree is. Registration SHALL refuse with the reference's names: `topology.correction_without_event_time` (no Tasklet in the class declares an `:event_time` clock, naming the first Tasklet and the clock it declares) and `topology.correction_window_zero` (`correction_ms: 0`, or a `beyond_window` with no window). A shedding overflow policy under a declared window SHALL be refused by the tier's existing `topology.dropping_policy_under_effectively_once`, because a window requires the tier — no second check is added for a program the first already refuses. `merge/1` SHALL return a `%Flezha.SDK.Merge{}` handle that the same `Flezha.SDK.Channels` functions carry forward: `from/3` (a source stream and its mapping), `key_by/2` (declared once, on the merge) and `into/2` / `into_named/3`, each dispatching on the handle it receives, so one import of `Flezha.SDK.Channels` writes a pipe with a merge and `Flezha.SDK.Merge` exports none of the four, with the `multi-input-tasklets` semantics: each mapping composes after that source's own transforms, the key is applied to the mapped value on every source's edge, and the merged node's output stream chains like any other; `Topology.build/1` judges the merge groups. `route_latest/4` and `route_latest_named/5` SHALL expand to a merge into an SDK-packaged single-cell router tasklet with journaled `{document, version, delivered}` state, as the reference's do. `branch` SHALL NOT exist in v1 (fan-out is handle reuse); calling it fails registration with an error naming the module. `sealed/1` SHALL NOT exist: `sdk-graph-authoring` gives `sealed` only to the SDKs whose type system can enforce it, and an identity that seals nothing is a misleading surface.

#### Scenario: Linear chain with a transform and a key
- **WHEN** an operator declares `c |> input("trades") |> filter(pred) |> key_by(key_fn) |> into(Candle) |> sink("candles")`
- **THEN** registration succeeds with exactly one Tasklet node in Dynamic mode, and the entry→Candle edge carries one fused closure applying predicate then key

#### Scenario: Transforms fuse structurally
- **WHEN** three inputs flow through a declared filter chain
- **THEN** the predicate is invoked exactly three times and the routing path applied exactly one composed closure per edge (asserted by counting, not topology inspection)

#### Scenario: Transform after key_by is refused
- **WHEN** a `map/2` is declared after `key_by/2` on the same chain
- **THEN** registration returns `{:error, %Flezha.SDK.Error{code: "topology.transform_after_key_by"}}`

#### Scenario: Handle reuse fans out
- **WHEN** one stream handle is used to wire two consumers
- **THEN** both consumers receive every value released on that channel

#### Scenario: branch fans out like handle reuse
- **WHEN** an operator declares `placed |> branch([&into(&1, ConfirmFill), &into(&1, RecordOrder)])`
- **THEN** registration succeeds with the topology two `into/2` calls off the held `placed` handle build, and the pipe continues on `placed`

#### Scenario: Deferred builder surface fails loudly
- **WHEN** `channels/1` calls a builder function the SDK does not carry
- **THEN** registration fails with an error naming the operator module, never a silent no-op, never a crash of the runner

#### Scenario: Two channels merge into one tasklet
- **WHEN** an operator declares `c |> merge() |> from(ticks, &{:tick, &1}) |> from(quotes, &{:quote, &1}) |> key_by(&symbol_of/1) |> into(Detector)`
- **THEN** registration succeeds with one Detector node fed by two consumer edges, each edge's fused closure applying that source's mapping and then the merge's key, and a value from either entry reaches `run/3` as the mapped term keyed by its symbol

#### Scenario: A merge is judged by the reference names
- **WHEN** a merge declares one source, or the same stream twice, or a source whose channel is already wired into the same node by `into/2`
- **THEN** registration returns `{:error, %Flezha.SDK.Error{}}` whose code is, respectively, `topology.merge_too_few_sources`, `topology.duplicate_merge_source` and `topology.edge_already_wired` respectively, with the reference sentences

#### Scenario: The router delivers on a key's own traffic
- **WHEN** `route_latest(c, config, trades, &(&1.symbol))` is declared and a unit test feeds a document, trades for `"A"` and `"B"`, the same document, then a trade for `"A"`
- **THEN** exactly one `{"A", document}` and one `{"B", document}` are emitted, the repeated document is inert, and the node's identity is `route_latest`

#### Scenario: The frame carries the node's channel name, and `named/2` survives the sink
- **WHEN** a node named `"e"` is wired `|> named("e") |> sink("out")` and its Cell emits
- **THEN** the plugin frame's metadata carries `channel: "e"`, and an unnamed node's frame carries its tasklet's `name/0`

#### Scenario: A keyed handle with no plugin sink is refused
- **WHEN** `channels/1` declares `keyed/1` on a handle and then routes it with `into/2` before any sink
- **THEN** registration returns `{:error, %Flezha.SDK.Error{code: "topology.keyed_channel_without_plugin_wire"}}`

#### Scenario: A clock the runtime cannot serve is refused at registration
- **WHEN** a Tasklet without `apply_change/2` answers `logical_clock/0` with `true`, or a hand-written Tasklet exports `event_time/1` and also answers `logical_clock/0` with `true`
- **THEN** registration returns `{:error, %Flezha.SDK.Error{code: "topology.unservable_clock"}}` for the first and `topology.two_clocks` for the second, naming the tasklet, and no Cell is started

#### Scenario: The tier declarations register, compose and refuse by the reference's names
- **WHEN** a class declares `run: {:flow, exactly: :effectively_once}` with a keyed sink; another declares `{:effectively_once, ack: {:checkpoint, every: 4, outputs: :at_checkpoint, max_delay_ms: 250}}`; a third writes `ack` under `:at_least_once`; a fourth declares the tier over an unkeyed sink; a fifth declares the tier over an entry with `on_full(:drop_newest)`
- **THEN** the first two register with the tier and the checkpoint policy carrying all three members, the third fails to compile naming `ack` and `at_least_once`, and the last two are refused with `topology.unkeyed_channel_under_effectively_once` naming the sink's channel and `topology.dropping_policy_under_effectively_once` naming the channel and `drop_newest`

#### Scenario: The correction declarations register, compose and refuse by the reference's names
- **WHEN** a class declares `{:effectively_once, correction_ms: 600_000}` and a Tasklet with an `:event_time` clock; a second adds `beyond_window: :drop` beside it; a third writes `correction_ms` under a steps run; a fourth declares the window over a `:wall`-clocked Tasklet; a fifth declares `correction_ms: 0` and a sixth only `beyond_window: :drop`
- **THEN** the first two register with the window and the beyond-window policy carried whole, the third fails to compile naming `correction_ms` and `steps`, and the last three are refused with `topology.correction_without_event_time` naming the Tasklet and its clock, and `topology.correction_window_zero` twice

### Requirement: Bounded Cell mailbox with the cross-SDK overflow default
Each Cell SHALL have a bounded queue (capacity from `mailbox_capacity/0`) with an `OverflowPolicy` of `:drop_nothing`, `:drop_newest`, or `:drop_oldest`. The default SHALL be `:drop_nothing` for **every** edge kind (entry, inter-tasklet, plugin-bound) — in lockstep with the Rust SDK builder, the Java SDK builder and the FFI host — and an operator that prefers to shed SHALL say so with `on_full/2`. The retired spelling SHALL NOT be accepted by `on_full/2`, by `Flezha.SDK.Stream`'s type, or by any manifest. Under `:drop_nothing` the routing caller SHALL wait for room (guarded by a monitor on the Cell so a dead Cell cannot wedge it) and SHALL neither drop nor refuse the input; the documented co-tenancy cost is that a delayed primary-stream router delays every operator fed by that stream, exactly as the Rust reader parks. Drops SHALL be observable via telemetry; `blocked_parks` SHALL count episodes (the first wait of a contiguous full period), and the `dispatches` heartbeat counter SHALL increment only on completed dispatches so a wedged Cell population is visible to the existing `runner_pool_wedged` classifier verdict.

In the Elixir SDK a `:drop_nothing` push that takes a free slot from the Cell's `Flezha.SDK.Cell.Room` is a cast, so the caller does not wait on a Cell that is running a tasklet; one that finds no free slot is a call, and `Flezha.SDK.Cell.handle_call({:enqueue, …})` defers its reply into a `waiters` FIFO that each dequeued input's freed slot is handed to in order, which is what `:drop_nothing` names.

#### Scenario: Default policy discards nothing
- **WHEN** a Cell queue at capacity receives further inputs under the default policy and the Cell later drains
- **THEN** every input is eventually dispatched in order, nothing is dropped, and `blocked_parks` records one episode

#### Scenario: Declared drop policy sheds
- **WHEN** a Cell queue at capacity receives another input under a declared `on_full(:drop_newest)`
- **THEN** the input is dropped, a drop counter increments, and routing continues serving other Cells

#### Scenario: Wedged population is visible, not silent
- **WHEN** a Cell stays at capacity and never drains while the runner stays connected
- **THEN** subsequent heartbeats carry a non-advancing `dispatches` value and `blocked_parks` stops at one episode

#### Scenario: The retired spelling is refused

<!-- overflow-naming-record:begin -->
- **WHEN** `Flezha.SDK.Channels.on_full/2` is called with `:block`
<!-- overflow-naming-record:end -->
- **THEN** it SHALL raise a `FunctionClauseError` as it does for any other unrecognised policy, and no stream handle SHALL be returned

### Requirement: Failing invocations ride the cross-SDK retry-then-park ladder
A failing `run/3` — error tuple, raise, or exit — SHALL be retried up to the input channel's `retry_max` (default 3 total attempts) with exponential backoff plus jitter from `retry_backoff_ms` (default 50 ms base, capped at 5 s); `{:error, {:non_retryable, reason}}` SHALL park immediately; a raise/exit is retryable by default. Outside the effectively-once tier, on exhaustion the SDK SHALL ship a `park_payload` frame whose body is the postcard `ParkBody` byte-compatible with the pinned cross-language fixture (`park_id`, `origin_offset` absent for the tier-off runner, and a `DlqEntryBody` of channel, error, attempts, payload — payload bytes present for binary/String inputs), SHALL hold the Cell, and SHALL resend every 2 s until the controller's `ParkPayloadAck` (sent only after the DLQ store fsync) matches the `park_id` — the existing controller park path SHALL require zero changes, in both deployment modes. The `ParkBody` encoder SHALL live in `Flezha.SDK.Wire` beside its decoder, sharing the module's postcard helpers. Under the tier the park SHALL ride the invocation's effect record as its `dlq` entry (channel, error, attempts, payload) beside the poison input's consumed offset, closing any open checkpoint interval, and SHALL hold nothing — no `park_payload` frame, no ack wait: the next queued input dispatches at once, the reference's tier-on park. A parked firing's removal SHALL ride the same record's timer ops.

#### Scenario: Transient failure recovers within the budget
- **WHEN** an invocation fails twice and succeeds on the third attempt
- **THEN** processing continues, nothing is parked, and retry telemetry records the attempts

#### Scenario: Poison payload parks durably and the Cell resumes
- **WHEN** an invocation fails on every attempt of the budget
- **THEN** a `park_payload` ships, the DLQ store persists the entry under the operator class and tasklet, the ack releases the Cell, and the next input dispatches normally

#### Scenario: Park frame is byte-compatible
- **WHEN** the SDK's encoded `ParkBody` is compared against the pinned cross-language fixture bytes
- **THEN** they are identical

#### Scenario: A tier-on park rides the effect record and holds nothing
- **WHEN** an invocation on a class declaring `exactly: :effectively_once` fails on every attempt of the budget at consumed offset 7, with an input queued behind it
- **THEN** one `cell_effect_record` ships carrying no change, the `dlq` entry with the error and the payload bytes, and consumed offset 7; no `park_payload` frame ships; and the queued input dispatches without any ack

### Requirement: Partitioned placement is guarded with the controller's own hash
The SDK SHALL decode `AddOperator`'s `partition_index`/`partition_count`/`key_group_from`/`key_group_to` (zero values = unpartitioned; the shipped range is authoritative, never re-derived), for an instance of either run: the steps driver SHALL hold the range as the flow operator does, and the Cell set the two share SHALL apply the guard on it. When `partition_count >= 2`: a dynamically computed cell key SHALL be admitted against the owned range **before** any Cell state is created — a miss parks the input with the named contract error, and on a steps operator member opens no pass; statically declared Cells outside the range SHALL be silently idle (`owns` filter); a topology with no keyed entry-fed tasklet SHALL refuse the bind with `topology.partitions_without_keyed_tasklet` naming `key_by`, for a flow class and a steps class alike; and a steps class whose loop mode is `once` or `chained` SHALL refuse the bind with `topology.partitions_without_per_input_loop` naming `each per_input`. A steps class's response metadata SHALL declare `each` with its declared value. When `partition_count < 2` the guard SHALL cost no more than one comparison. The key-group function SHALL be `Flezha.SDK.KeyGroup.group_of/1`, the pinned FNV-1a-128 the controller's range assignment calls too, verified against the shared fixture vectors.

#### Scenario: Mis-routed frame parks loudly
- **WHEN** a partition member receives a frame whose key hashes outside its range
- **THEN** the input parks with the contract error naming the key, its group, and the owning partition, and no Cell state is created

#### Scenario: A steps operator member parks a mis-routed message and opens no pass
- **WHEN** a partition member of a `per_input` steps operator receives a message whose `key_by` result hashes outside its range
- **THEN** the message parks with the contract error through the shared pre-dispatch park, no Cell is minted, no stage is dispatched and the loop waits for its next message

#### Scenario: Keyless class refuses partitioned bind
- **WHEN** `partition_count >= 2` arrives for a class with no `key_by` edge and no declared cell keys
- **THEN** the `AddOperator` is refused with `topology.partitions_without_keyed_tasklet`

#### Scenario: A once steps operator refuses partitioned bind
- **WHEN** `partition_count >= 2` arrives for a steps class declaring `each: :once`
- **THEN** the `AddOperator` is refused with `topology.partitions_without_per_input_loop`, and a `per_input` class of the same shape binds and declares `each: "per_input"`

#### Scenario: Hash agrees with the fixtures
- **WHEN** the SDK's key-group function hashes the shared fixture inputs
- **THEN** it produces `"BTC-USD"→129`, `"ETH-USD"→213`, `"SOL-USD"→60`, `""→141`, `"a"→100`

### Requirement: Liveness self-reporting matches the cross-SDK contract
The SDK SHALL echo `control_ping` on the control channel, answer `Ping` with a `PingResponse` carrying the nonce, and emit `data_heartbeat` frames (empty body) every `FLEZHA_DATA_HEARTBEAT_MS` (default 10 s), jittered into `[base/2, base]`, on each bound secondary stream, with metadata carrying **exactly the key set the shared core's heartbeat builds** — not a subset of it. The keys are `dispatches` (incremented only on completed dispatches — the key the `runner_pool_wedged` verdict requires), `blocked_parks`, `credit_parked` — the count of credit-park wait iterations, so a runner parked on an exhausted window reads as parked rather than wedged beside a frozen `dispatches` — `drops_newest` and `drops_oldest`, `inflight` and `inflight_oldest_ms`, `pending_pushes` and `pending_push_oldest_ms`, `restore_discarded_inputs` (inputs a rebind threw away, the user's only handle on the size of that loss), and `timers_armed` (the processing-time timers the runner's Cells hold armed on the BEAM's timer service) and `timers_fired` (firings completed, monotonic): the two timer keys every other SDK's heartbeat carries. The contract SHALL hold identically in both deployment modes.

This SDK's core is its own and inherits nothing from `flz_core`, so the key set
is a thing it can drift on and the other runtimes cannot. The metadata map SHALL
therefore be built by walking the key list the SDK declares from its own counter
table, which the conformance gate holds equal to the set the controller mirrors,
rather than written as a literal, and a key it cannot yet report SHALL be given a
counter rather than a literal zero — a reader cannot tell an absent key from a
genuinely zero one.

#### Scenario: Heartbeats keep idle channels live
- **WHEN** the runner is connected with zero traffic for several minutes
- **THEN** every secondary StreamChannel's `last_seen` keeps advancing and no stall is flagged

#### Scenario: Wedged dispatch is visible
- **WHEN** dispatch progress halts while the runner process remains alive and fed
- **THEN** consecutive heartbeats carry a non-advancing `dispatches` value, enabling the existing `runner_pool_wedged` classifier verdict

#### Scenario: A credit park is visible as a park
- **WHEN** a Cell is parked on an exhausted credit window across two heartbeats
- **THEN** the second heartbeat's `credit_parked` is greater than the first's while `dispatches` stands still

#### Scenario: Timer occupancy and firings ride the heartbeat
- **WHEN** a Cell registers two processing-time timers and later one of them fires
- **THEN** a heartbeat after the registrations carries `timers_armed` = 2, and one after the firing carries `timers_armed` = 1 and `timers_fired` = 1

#### Scenario: Discarded inputs ride the heartbeat
- **WHEN** a rebind discards inputs the runner had not yet consumed
- **THEN** the next heartbeat carries `restore_discarded_inputs` advanced by the number discarded

#### Scenario: The key set equals the shared core's
- **WHEN** this SDK's heartbeat metadata keys are compared with the key set `flz_core`'s heartbeat builds
- **THEN** the two sets SHALL be equal, and a key present in one and not the other SHALL fail the cross-SDK gate

### Requirement: Declarations and deferred commands follow the scope matrix
`AddOperatorResponse.metadata` SHALL be empty for a class declaring nothing (the legal "declared nothing" encoding), except that a keyless topology SHALL declare `keyed: "false"` so the controller's partitioned-start preflight works, a class whose smallest `compact_after_bytes/0` differs from the 8 MiB default SHALL declare `compact_after_bytes` with that value, every class SHALL declare `run` and `durable` (and `sleep_after_ms` when it sleeps), a class on the effectively-once tier SHALL declare `exactly: "effectively_once"`, and one declaring checkpoint acknowledgement SHALL declare `ack: "checkpoint"` — and a class declaring `correction_ms` SHALL declare `correction_ms` with the declared milliseconds and `beyond_window` with `process_as_arrival` or `drop` — both keys whenever a window is declared, as the reference ships them, so an undeclared policy reaches the controller as the default rather than as an absence — the four keys the reference ships and the goldens `tier_store.json`, `checkpoint_batched_store.json` and `candle.json` pin; the cadence, the held outputs and the delay stay SDK-side and reach no metadata. The persistence-family commands — `RestoreInitState`, `CompactCell`, `CellState`, `CellApplyState`, `MigrationSerialization` / `MigrationDeserialization`, `CreditWindowInit`, `CreditGrant` — SHALL be answered for real, by the SDK's own re-expression of the kernel's rules (the `state_persistence` requirement below); `CellMailboxStatus` SHALL be tolerated with a debug log, as the reference tolerates a pre-credit controller. `StepsStop` SHALL stop the named steps instance, and one naming an unknown operator SHALL be acknowledged with a warn. `RunnerDisconnect` SHALL terminate the connection cleanly: a terminal one runs the graceful shutdown and ends the VM with the terminal status; a non-terminal one SHALL stop the runner in place — the reference's `Stopped` — leaving the controller to take the session down, reconnecting into nothing and keeping the process, so a `:permanent` supervisor has nothing to restart into a second registration.

#### Scenario: Declaration-free response is empty
- **WHEN** an operator with a keyed topology and no declarations binds
- **THEN** its `AddOperatorResponse` carries zero metadata bytes

#### Scenario: Keyless topology declares its keyedness
- **WHEN** an operator whose topology has no keyed tasklet binds
- **THEN** its response metadata carries `keyed: "false"`, and a later partitioned start of that class is refused at the controller before any member starts

#### Scenario: A non-default compaction threshold is declared
- **WHEN** a class's only changeful Tasklet declares `compact_after_bytes/0` as `1_048_576`
- **THEN** its response metadata carries `compact_after_bytes: "1048576"`, and a class at the default carries no such key

#### Scenario: A non-terminal RunnerDisconnect stops the runner in place
- **GIVEN** an Elixir runner under a `:permanent` supervisor, registered with the controller
- **WHEN** the controller sends a non-terminal `RunnerDisconnect`
- **THEN** the runner acks, the controller's session for it ends, the same process stays alive with no reconnect attempt, and the namespace lists no runner afterwards

#### Scenario: The tier keys match the goldens
- **WHEN** `tier_store` and `checkpoint_batched_store` bind on an Elixir runner
- **THEN** the first's response metadata is exactly `effectively_once: "true", keyed: "false"` and the second's exactly `ack_granularity: "checkpoint", effectively_once: "true", keyed: "false"` — the committed goldens, key set and values

#### Scenario: The correction keys match the golden
- **WHEN** `candle` — a class declaring `{:flow, exactly: {:effectively_once, correction_ms: 600_000}}` and no beyond-window policy — binds on an Elixir runner
- **THEN** its response metadata is exactly `beyond_window: "process_as_arrival", correction_ms: "600000", durable: "local", exactly: "effectively_once", keyed: "false", run: "flow"` — the committed golden `test/fixtures/declarations/candle.json`, key set and values

### Requirement: Command codec is isolated for future term-native migration and packaging
All SDK-side control-channel encode/decode SHALL go through a single codec module speaking the `RunnerCommands.*` protobuf structs via the already-compiled `Flezha.Runner.CommandsProto` Protox modules. No SDK or native-transport code outside the codec SHALL reference Protox or `RunnerCommands`, and none SHALL assume control messages are binaries.

#### Scenario: Codec is the only protobuf seam
- **WHEN** the SDK source is audited
- **THEN** Protox/`RunnerCommands` references appear only in the codec module

### Requirement: The SDK is packaging-ready behind named seams
The SDK SHALL be a mix project developed in its own repository and present here as a submodule at `sdk/elixir`, application `flezha_sdk`, licensed Apache-2.0 and buildable as a hex package, that depends on no module of the controller application. The controller SHALL depend on the SDK, and every pure module both need (the partition hash, the record frame's pure half, the postcard codec, the bounded call, the counter-table macro, the in-node stream) SHALL live once, in the SDK, and be imported by the controller. The SDK's protobuf codec SHALL be generated into the SDK's own namespace from the shared schema. No boundary audit script SHALL exist: the dependency direction is enforced by the compiler, and the SDK SHALL compile with warnings as errors.

#### Scenario: The package builds alone
- **WHEN** `mix hex.build` runs in `sdk/elixir` with no controller code on the path
- **THEN** it SHALL produce a package, and the SDK SHALL compile with warnings as errors

#### Scenario: The controller builds against the submodule and fetches nothing
- **WHEN** the controller compiles here with the submodule checked out and the network unavailable
- **THEN** it SHALL resolve `flezha_sdk` by path into the submodule and SHALL NOT compile or fetch the SDK's socket NIF

#### Scenario: The controller imports, never copies
- **WHEN** the controller is searched for a second copy of a module the SDK carries
- **THEN** none SHALL be found, and the controller SHALL alias the SDK's module

### Requirement: The SDK refuses rather than exiting a caller its own process will not answer

Every call the Elixir SDK makes into one of its own processes SHALL wait a bound named by a row of the one configuration table and SHALL answer a target that does not reply with a refusal the caller can act on, never by exiting that caller. A runner runs inside a host application, so a wedged Cell, Operator, RunnerServer or local run must not take a process the host supervises with it.

Three calls SHALL wait with no bound at all, because there the wait is the protocol and the caller is one of the runner's own processes: the `:drop_nothing` enqueue, whose guarantee is that an input is never dropped and never refused; the credit window's acquire, which parks a sender until a grant covers its frame; and the Cell rebind, whose caller replays that Cell's outputs after it returns and so must not proceed before its records reach the wire. None SHALL be given a bound, because a caller that abandoned one would lose an input, ship past a window the Controller is metering, or release outputs ahead of their own records.

A refusal SHALL be phrased in the words the surface it reaches already uses: the live-read and restore commands SHALL answer the `cell_busy` token the Controller matches as a prefix.

A refusal is not a cancellation — the request stays in the target's mailbox and the work still runs — so a call whose duration grows with the state or the payload SHALL take a bound derived from the Controller surface that awaits its response, never the flat control bound. The Cell fold, the snapshot and the state apply SHALL each sit strictly inside the restore await, the compaction bound and the control reply bound respectively, so that the SDK never reports a failure while the Controller is still waiting for the answer.

The bounded call SHALL be the package's own `Flezha.SDK.SafeCall`, which the controller imports rather than holding a second copy. Exactly one other SDK file, `Flezha.SDK.Call`, SHALL name it, and the rest of the SDK asks that one.

#### Scenario: A suspended target refuses its caller

- **GIVEN** one of the runner's own processes suspended so that it holds a call and never replies
- **WHEN** the SDK calls into it
- **THEN** the caller SHALL read a refusal and stay alive, rather than exiting or waiting for ever

#### Scenario: The protocol waits still wait

- **GIVEN** a suspended Cell and a suspended credit window
- **WHEN** an input is enqueued under `:drop_nothing`, a record acquires credit, and the Cell is rebound
- **THEN** all three callers SHALL still be waiting, and no other call the SDK makes SHALL be written without a bound

#### Scenario: The bounded call is the package's own

- **WHEN** the SDK package compiles with no controller code on the path
- **THEN** `Flezha.SDK.SafeCall` SHALL resolve inside it, and no SDK file but `Flezha.SDK.Call` SHALL call it

### Requirement: Bench harness covers the elixir runtime on all three transports
`mix bench --runtime elixir` SHALL run the `regression`, `cell`, and `dynamic` shapes over the `native` transport (in-process SDK start; one runner, because the runner slot is process-wide) and over `uds`/`quic` (spawning the `test/apps/elixir_runner` app via the same fleet mechanism as the rust/java runners), registering factories `do_nothing`, `cell_stress`, and `dynamic_cell` with the rust_runner topologies, producing the same `Throughput: <n> messages/second` output measured by the unchanged controller-side code. The transport SHALL be selected exactly as today — `scripts/bench.sh -t native|uds|quic` exporting `FLEZHA_TRANSPORT`, read by `mix bench` from the application env — with no new `mix bench` flag. The harness SHALL reject `--runtime rust|java` with the native transport; `scripts/bench.sh -t` SHALL accept `native`. The recorded results SHALL include the same-runtime uds-vs-native comparison (the transport measurement) and the elixir-vs-rust comparison on uds (the runtime measurement). `mix bench.roundtrip --runtime elixir` SHALL run the `identity` and `keyed` workloads at durability `off` through the same fleet mechanism, registering `bench_echo` and `bench_keyed_nopersist` as twins of the rust_runner fixtures (same factory ids, sink names, output bytes and key function), over `uds`, `quic` and `native`, at durability `state` registering `bench_keyed`, the persistent twin, at durability `on` registering `tier_store` (the same upsert under `exactly: :effectively_once` with the reference's 300 ms retry backoff) and at durability `checkpoint` registering `bench_checkpoint` (the upsert acknowledging every `FLEZHA_BENCH_CHECKPOINT_EVERY` inputs, holding its outputs when `FLEZHA_BENCH_CHECKPOINT_OUTPUTS` is `at_checkpoint` for `FLEZHA_BENCH_CHECKPOINT_DELAY_MS`, read in `channels/1` as the reference reads them); a round-trip run combining a `--runtime` with a durability posture whose capability that runtime's manifest defers SHALL be refused at the task with an error naming that capability, before any runner starts.

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
- **WHEN** `mix bench.roundtrip --runtime <sdk> --durability on` runs for an SDK whose manifest lists `effectively_once` under `deferred`
- **THEN** the task exits before starting a runner with an error naming the deferred `effectively_once` capability and that SDK's guide

#### Scenario: Round-trip persistent run on the elixir runtime
- **WHEN** `mix bench.roundtrip --runtime elixir --workload keyed --durability state` runs with the uds transport configured
- **THEN** the elixir runner serves `bench_keyed`, the controller writes a WAL for its Cell, and every record is received exactly once

#### Scenario: Round-trip tiered runs on the elixir runtime
- **WHEN** `mix bench.roundtrip --runtime elixir --workload keyed --durability on` and then `--durability checkpoint` run with the uds transport configured
- **THEN** the elixir runner serves `tier_store` and then `bench_checkpoint`, every record is received exactly once with zero duplicates, and the report carries the same columns the rust runtime produces for the same cases

### Requirement: A crashed Cell is recovered by its Operator, and the recovery is a requirement

The Operator process SHALL monitor the Cell processes it starts, SHALL drop a dead Cell from its
live map on its `:DOWN`, and SHALL recreate it on that key's next input. A crashed Cell SHALL NOT
leave the key permanently unroutable, and SHALL NOT take the Operator down with it except in the
one case below, where recovering it in place would lose input.

When a Cell exits abnormally (any reason but `:normal`, `:shutdown` or `{:shutdown, _}`) while the
instance's processed-prefix ledger holds any input offset, the Operator SHALL NOT recreate it: it
SHALL stop with the reason `{:cell_exited, reason}`, so that the runner reports the instance and the
controller rebinds it and re-forwards its input log above the confirmed floor. A held offset is an
input a Cell took and has not finished, and only durable input carries one, so this case arises
only where the log can deliver the input again. A push that finds its Cell dead SHALL do the same
when the ledger still holds an offset once the push's own hold is released, rather than mint a
replacement that could take a later input and raise its floor past the lost ones. Before it exits
abnormally, for this reason or any other, the Operator SHALL end every Cell it holds and wait for
each to be gone, bounded by `sdk_shutdown_ack_ms`.

A recreated Cell of a Tasklet that declares no `apply_change/2` starts
fresh: its state is a plain term that nothing persists, so the in-flight value
that ended the previous process and any in-memory state are lost. That is the
documented behaviour of a persistence-free Tasklet, but it SHALL be stated,
because the other SDKs' hosts owe the same "a dead worker comes back" property
and this is the reference for it.

A recreated Cell of a changeful Tasklet SHALL continue on the seq line and
from the state its last shipped record left: the Operator SHALL keep, per
persistent Cell, the state and `change_seq` as of that Cell's last shipped
record, written by the Cell after its records are on the wire, and SHALL
seed the replacement from it. A replacement that restarted at seq 0 would have
every later record dropped by the controller's writer as a duplicate while the
runner kept dispatching, which is a silent divergence and not a recovery.

A crash SHALL be logged naming the operator, the node reference and the cell
key, so a key crashing on every input is visible rather than appearing as silent
inactivity.

#### Scenario: A crashed Cell does not take its Operator down
- **GIVEN** an instance whose processed-prefix ledger holds no input offset
- **WHEN** a Cell process exits abnormally
- **THEN** the Operator SHALL remain alive, SHALL drop that Cell from its live map, and SHALL keep serving every other key

#### Scenario: The key is routable again on the next input
- **GIVEN** a Cell that crashed
- **WHEN** the next input for that key arrives
- **THEN** a fresh Cell SHALL be started for it and the input SHALL be dispatched

#### Scenario: A repeatedly crashing key is visible
- **WHEN** a Cell crashes on every input for its key
- **THEN** each crash SHALL be logged naming the operator, node reference and cell key

#### Scenario: A persistent Cell comes back on its own seq line
- **GIVEN** a changeful Cell that has shipped records at seq 0 through 4, in an instance whose processed-prefix ledger holds no input offset
- **WHEN** its process is killed untrappably and the next input for its key arrives
- **THEN** the replacement dispatches from the state the seq-4 record left and ships that input's record at seq 5, and the controller's writer appends it

#### Scenario: A Cell that dies with unfinished input ends its Operator
- **GIVEN** an instance whose input carries offsets and a Cell running the input at offset 10 with the inputs at 11 and 12 pushed behind it
- **WHEN** the Cell is killed untrappably, or exits through a process linked into it
- **THEN** the Operator SHALL stop with `{:cell_exited, reason}`, no replacement Cell SHALL run any input, and the crash SHALL be logged naming the operator, node reference and cell key

#### Scenario: A push to a dead Cell does not mint a replacement over held input
- **GIVEN** an instance whose input carries offsets, a Cell that has died holding the input at offset 10, and an Operator that has not yet read its `:DOWN`
- **WHEN** the Operator pushes the input at offset 11 to that Cell and the push finds it dead
- **THEN** the Operator SHALL stop with `{:cell_exited, reason}` rather than mint a replacement for the key

#### Scenario: An Operator's Cells are gone before its exit is seen
- **WHEN** an Operator stops abnormally
- **THEN** every Cell it held SHALL have exited before the Operator's own exit reaches the runner

### Requirement: The Elixir runner reports handout liveness on its heartbeat

The SDK SHALL report the same two handout-liveness values every other SDK
reports — the number of dispatches currently in flight and the age in
milliseconds of the oldest — in the data-channel heartbeat's metadata map,
beside the counters it already carries. The values SHALL be maintained in the
existing shared counters structure rather than by a new process or message round
trip, and SHALL be reporting-only: the SDK SHALL take no action of its own based
on them.

#### Scenario: In-flight work is visible to the controller
- **GIVEN** an Operator with one Cell mid-dispatch and the rest idle
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
- **WHEN** an operator of `do_nothing` is placed
- **THEN** the Elixir runner SHALL NOT be a candidate, and the placement SHALL go to the Rust runner

#### Scenario: Every durable record is stamped and a foreign one is refused
- **WHEN** an Elixir Cell ships a change record, and later a `RestoreInitState` for it carries a record stamped with the Rust SDK's byte
- **THEN** the shipped record's byte 0 is `3`, and the restore is refused with the language-mismatch wording naming `elixir (3)` and `rust (1)` — never decoded, never reported as a malformed header

### Requirement: The Elixir SDK persists per-Cell state in its own core

The Elixir SDK SHALL carry the `state_persistence` capability by re-expressing the kernel's rules in Elixir, inheriting nothing from `flz_core` and changing nothing in the controller, which SHALL keep being unable to tell an Elixir runner from a Rust one. A Tasklet that exports `apply_change/2` is changeful and persisted; `Flezha.SDK.Ctx.apply/3` SHALL be the only recorded mutation path, returning `apply_change(state, change)` and recording the change for the record stream, so that one fold is one push; a Tasklet without `apply_change/2` is the reference's `NoChange` and ships nothing. An operator's `durable/0` (default `:local`) SHALL, when `false`, advance the seq and ship nothing. After a successful dispatch the Cell SHALL ship one `cell_state_change` frame per recorded change — the 16-byte header stamping `sdk_lang = 3`, the SDK's version, the Tasklet's schema version and the Erlang External Term Format id `7` allocated in `flz_core::header`, over `Flezha.SDK.Wire.encode_frame/4` and the instance-keyed identity — at `seq = change_seq` advancing from `0`, in dispatch order on one secondary stream, before the dispatch's output frame, each under the credit window's `acquire`; a dispatch that committed timer ops SHALL ship them after its change records as one further `cell_state_change` frame at the next seq, stamped `format_id = 3` over the postcard `Vec<TimerOp>` body every SDK writes, so the controller stores it as any change record and never decodes it; a class declaring the tier SHALL ship one `cell_effect_record` per effectful invocation in place of those records (the effectively-once requirement below). The SDK SHALL answer `RestoreInitState` by folding the snapshot then the change log in the controller's on-disk framing under the Cell's own serialisation — CRC over header and body, language before format before schema, the core's refusal wordings — folding a `format_id = 3` record into the pending-timer set rather than the state, and a `format_id = 2` effect record's enclosed change bodies (each under the enclosing record's version decision), timer ops, consumed offset and event time into the state, the set, the consumed floor and the event-time high-water, re-arming `change_seq` to `max(folded terminal, terminal_seq) + 1`, re-arming the folded processing-time timers, and replying `terminal_seq`; `CompactCell` by shipping `cell_state_snapshot` at `change_seq − 1` after the Cell's records — followed, when the Cell holds pending timers, by one `format_id = 3` record re-stating the whole pending set at the next seq, so the truncation the snapshot authorises cannot take the timers with it — and replying `snapshot_seq` and the body size, with `{0, 0}` and an error for an unknown Cell; `CellState` with the Decision-10 JSON shape narrowed by its selector, `flz.change_seq` on the reply frame for one Cell, and the refusals `cell_key_without_tasklet`, `tasklet_not_found`, `cell_not_found` and `cell_busy` by name; `CellApplyState`, `MigrationDeserialization` and a non-empty `AddOperator.init_values` by handing each Tasklet's JSON to its `state_from_json/1` (default: the object itself); `MigrationSerialization` with the whole-operator JSON; `CreditWindowInit` and `CreditGrant` by arming and replenishing one runner-wide window that parks the shipping Cell, never drops, counts `credit_parked`, and is disarmed on reconnect. On graceful termination — SIGTERM through the BEAM's `init:stop`, `Flezha.SDK.close/1`, `RunnerDisconnect` — the RunnerServer SHALL serialise every live operator, ship one `RunnerShutdownNotification` each and wait at most 2 s in parallel for the acks before closing. `test/apps/elixir_runner` SHALL carry the six reference twins and the manifest SHALL list `state_persistence` under `fixtures`.

#### Scenario: A change is durable on the controller and the state is readable by name
- **WHEN** a `store_input` operator on an Elixir runner receives three inputs
- **THEN** the controller's WAL for `{operator_id, "store_input_tasklet", ""}` holds three records at seq 0, 1, 2 stamped `sdk_lang = 3` and `format_id = 7`, and `CellState` answers `{"store_input_tasklet": {"binary_inputs": [], "text_inputs": [...]}}` holding the three inputs

#### Scenario: A rebind restores the accumulated state
- **GIVEN** a `resume_probe` Cell that has folded a running total of 100 and was compacted at seq 7
- **WHEN** the operator is stopped with its state preserved and started again on an Elixir runner
- **THEN** the restore folds the snapshot and the tail, answers `terminal_seq` at or above 7, the next input `+7` emits `107`, and its record ships at `terminal_seq + 1`

#### Scenario: Compaction produces a snapshot behind the records
- **WHEN** the controller's sweep sends `CompactCell` for an Elixir Cell at `change_seq = 50`
- **THEN** a `cell_state_snapshot` frame arrives at seq 49 after every change record the Cell shipped, the reply carries `snapshot_seq = 49`, and the controller writes the snapshot file and truncates the WAL behind it

#### Scenario: A parked sender resumes on a grant and never drops
- **WHEN** an Elixir Cell overdrives a 64 KiB credit window with 300 KiB of change records
- **THEN** it parks on `acquire` between the controller's fsync-minted grants, every record lands in seq order, the heartbeat's `credit_parked` advances, and the controller counts zero sheds and zero overruns

#### Scenario: SIGTERM ships the freshest state
- **WHEN** an Elixir runner holding a `store_input` operator with three applied inputs receives SIGTERM
- **THEN** the controller's `Overwatch.state.snapshot` for that operator holds the Decision-10 JSON with the three inputs within the 2 s ack window, and the runner exits

#### Scenario: A persistence-free class still ships nothing
- **WHEN** a `persistence_off_counter` operator folds inputs, and separately a Tasklet without `apply_change/2` dispatches
- **THEN** the controller writes no WAL record for either, the first advances its seq silently and the second holds no seq at all

#### Scenario: Timer ops are records in the Cell's own log, and a compaction re-states them
- **GIVEN** an Elixir Cell whose log holds two change records and one `format_id = 3` record registering a processing-time timer, at `change_seq = 3`
- **WHEN** the controller's sweep sends `CompactCell`
- **THEN** the snapshot ships at seq 2, a `format_id = 3` record registering the timer ships at seq 3 behind it, the reply carries `snapshot_seq = 2`, and a restore over the truncated log rebuilds the pending set from that record

#### Scenario: An effect record folds on restore
- **GIVEN** an Elixir Cell's log holding a change record, then a `format_id = 2` effect record enclosing two change bodies, one timer registration and consumed offset 9 at event time 400
- **WHEN** the Cell is restored over it
- **THEN** the state holds all three changes in seq order, the pending set holds the timer, the consumed floor is 9 and the event-time high-water 400, and the terminal is the effect record's seq

### Requirement: The Elixir SDK mints the identity stamps on keyed plugin output

The Elixir SDK SHALL carry `keyed_channels` in its own core: every plugin frame from a sink declared `keyed/1` SHALL carry `flz.cell` (`<operator id>/<tasklet name>/<cell key or empty>`, the label the shared core derives for the same Cell), `flz.seq` (`<change_seq at handout>.<emit ordinal>`, the seq the invocation started at, before its own changes advanced it) and `flz.key` (the emit's key, else the sink route's `key_by` over the value, else the emitting Cell's own key), appended after the user's metadata so a stamp cannot be clobbered. The ordinal SHALL start at zero for each work item, SHALL advance on every plugin-targeted output the invocation produces — stamped or not, each output of a `flat_map` counted — and SHALL NOT advance for an inter-tasklet output. A sink not declared keyed SHALL carry no `flz.` key. Under the effectively-once tier the Cell SHALL capture each sink output into its invocation's effect record unstamped — the producing node's channel name, the resolved key, the user's metadata key-sorted with any `channel` key dropped, and the serialised payload — through the same sink leaf the router uses, and the router SHALL deliver no sink frame for that class; the controller derives `flz.cell`, `flz.seq` and `flz.epoch` from the durable record at flush, as it does for every other SDK. `test/apps/elixir_runner`'s `partition_relay` SHALL declare its sink keyed and name its channel `input`, and the manifest SHALL list `keyed_channels` under `fixtures` with the reference's id.

#### Scenario: A keyed sink's frame carries the three stamps after user metadata
- **WHEN** a dynamic Cell keyed `"k1"` at change_seq 4 calls `Ctx.emit_with_meta/3` with `%{"tag" => "a"}` on a node whose sink is keyed
- **THEN** the frame carries `tag: "a"`, `flz.cell: "op-1/counter/k1"`, `flz.seq: "4.0"` and `flz.key: "k1"`, and a second emit in the same invocation carries `flz.seq: "4.1"`

#### Scenario: A fresh work item restarts the ordinal, and an inter-tasklet output does not advance it
- **WHEN** the next input for the same Cell emits once on the keyed sink after emitting once towards a consumer node
- **THEN** its plugin frame carries ordinal `0`

#### Scenario: A keyed output forwards to the partition of the target group that owns its key
- **WHEN** `partition_relay` on an Elixir runner, partitioned three ways, feeds a partitioned `dynamic_cell` through `operator_forward`
- **THEN** every key lands on the downstream member owning its key group and nowhere else, because the frame carries `flz.key` and `channel: "input"`

#### Scenario: The smoke golden pins the stamp keys
- **WHEN** `partition_relay` on an Elixir runner emits under a sink with `include_metadata`
- **THEN** the sorted `flz.*` keys of the frame equal `keyed_output_stamps.json`

#### Scenario: No stamps under the tier
- **WHEN** a class declaring `exactly: :effectively_once` emits with `%{"tag" => "a"}` on its keyed sink
- **THEN** no output frame leaves on the primary stream, and the effect record's output carries channel `sink`, the key, metadata `[{"tag", "a"}]` and the payload with no `flz.` key

### Requirement: The Elixir SDK evicts idle dynamic Cells and caps them

The Elixir SDK SHALL carry `dynamic_cell` in its own core, for both runs through one module. A dynamic node whose Tasklet declares `idle_ttl_seconds/0` SHALL be swept by one `:reap` tick in the process that owns its Cells, the flow instance's or the steps Driver's, at `FLEZHA_REAP_INTERVAL_MS`, else `min(ttl / 4, 30 s)` and at least 1 ms, the TTL being the declared seconds; a node declaring none SHALL never be swept and no tick SHALL be scheduled. The sweep SHALL run under the owner's own routing serialisation (the drain gate: no push can be routed while it runs) and SHALL evict a Cell only when it has been idle past the TTL with an empty queue, no dispatch in flight, no held park and no deferred push, skipping a busy one until the next sweep. On a steps instance the sweep SHALL additionally run only while the loop is waiting for its next message, and SHALL skip a Cell named by a completed stage the class's `on_failure` would still undo whose tasklet declares `compensate/3`, the same plan the unwind reads. Eviction SHALL be destructive: the Cell SHALL ship `delete_cell_state` on its own record stream when the class persists (behind its records by construction), SHALL clear its slot so a successor is a cold start from `init_state/1`, and SHALL stop; the owner SHALL drop it from the live map and remember the key in a trace of at most 1 024 keys. `Operator.lookup/4` SHALL answer `:cell_evicted` for a traced key that is not live, and the live read SHALL spell the shared core's `cell_evicted:` wording for it, distinguishably from `cell_not_found`. `max_cells/0` SHALL bind at `ensure_cell/3` as the behaviours requirement states, for a steps operator's keyed tasklet as for a flow operator's. Creation, eviction and capped drops SHALL be reported as one `[:flezha, :sdk, :dynamic]` telemetry event carrying the kind. `test/apps/elixir_runner`'s `dynamic_cell` SHALL journal its push and read its TTL from `DYN_TEST_TTL_MS`, rounded up to whole seconds, and the manifest SHALL list `dynamic_cell` under `fixtures` with the reference's id.

#### Scenario: An idle Cell is evicted destructively on the sweep
- **GIVEN** a persistent dynamic node with a TTL and a Cell that shipped a record at seq 0 and has been idle past the TTL
- **WHEN** the sweep runs
- **THEN** a `delete_cell_state` frame for that identity follows the record on the same stream, the Cell's slot is gone, `lookup/3` answers `{:error, :cell_evicted}`, and the next input for the key starts a fresh Cell at seq 0 from `init_state/1`

#### Scenario: A busy or recently pushed Cell is kept
- **WHEN** the sweep meets a Cell mid-dispatch, or one pushed within the TTL
- **THEN** neither is evicted on that sweep and both are swept again on the next

#### Scenario: A steps operator's keyed Cell is swept only between passes and outside the rollback's reach
- **WHEN** a `:per_input` steps operator's keyed compensable tasklet ran for `"a"` in a committed iteration, the loop is waiting for a message, and `"a"`'s Cell is idle past its TTL
- **THEN** under `:undo_all` the sweep keeps it and under `:keep_progress` the sweep evicts it, and in either mode no sweep evicts anything while a pass is running

#### Scenario: A live dynamic Cell is readable by name, and an evicted key is refused by name
- **WHEN** `CellState` names a live key of a `dynamic_cell` operator on an Elixir runner, then names a key the reaper evicted, then names a key never seen
- **THEN** the first answers that key's state alone, the second refuses with `cell_evicted`, the third with `cell_not_found`

#### Scenario: No TTL, no tick
- **WHEN** an Operator's dynamic nodes declare no `idle_ttl_seconds/0`
- **THEN** no `:reap` message is ever scheduled and its Cells live for the operator's lifetime

### Requirement: The Elixir SDK declares schema versions per Tasklet and migrates across them

The Elixir SDK SHALL carry `schema_evolution` in its own core. A Tasklet's `schema_version/0` (default 1) SHALL be stamped on every record it ships and resolved per record on restore against its `min_readable_schema_version/0` (default: the version; a floor above the version reads the version only): equal decodes, between the floor and the version migrates, above the version or below the floor refuses with the shared core's wordings. A migrating record SHALL be handed to the Tasklet's `migrate_state/2` (a snapshot) or `migrate_change/2` (a change) with the stamped `from_version` and the DECODED term — this SDK's body codec is the language's own term format, so the author receives the term, not bytes — and the fold SHALL continue with the running version's term the migration returns; a Tasklet that declares no migration SHALL refuse the record naming the absent migration and the version, and a migration that returns `{:error, reason}` or raises SHALL refuse naming the reason and the version, reseating nothing. A restore that migrated any record SHALL answer `migrated` true, so the controller's compaction rewrites the baseline at the running version. `test/apps/elixir_runner` SHALL carry `schema_evolution_v1` and `schema_evolution_v2` as twins of the reference's `evolving_cart` pair, and the manifest SHALL list `schema_evolution` under `fixtures` with the reference's two ids.

#### Scenario: State written at v1 comes back through the migration a bumped v2 declares
- **GIVEN** an `evolving_cart` Cell on an Elixir runner that accumulated 7 under `schema_evolution_v1` and was compacted
- **WHEN** the operator is stopped with its state preserved and started under `schema_evolution_v2`
- **THEN** the restore migrates the snapshot and the tail, the state reads `total: 7` with `currency: "EUR"`, the controller observes the migration and compacts, and a second restart under v2 migrates nothing

#### Scenario: v1 code meeting v2 state is refused, not decoded
- **WHEN** a Cell written under `schema_evolution_v2` is restarted under `schema_evolution_v1` on an Elixir runner
- **THEN** the restore is refused with `schema_version 2 is above the running tasklet's 1` and the operator is left unbound

#### Scenario: A floor without a migration fails at the first old record
- **WHEN** a Tasklet at version 2 declaring a floor of 1 and no `migrate_change/2` folds a record stamped 1
- **THEN** the fold refuses naming the absent migration and version 1, and the Cell keeps the state it had

#### Scenario: A failing migration reseats nothing
- **WHEN** `migrate_state/2` answers `{:error, "bad shape"}` for a snapshot stamped 1
- **THEN** the restore refuses with a reason carrying `bad shape` and `1`, and the Cell's state and seq are unchanged

### Requirement: The Elixir SDK carries per-Cell timers and the three clock kinds in its own core

The Elixir SDK SHALL carry the `cell_timers` capability by re-expressing the kernel's rules in Elixir, inheriting nothing from `flz_core` and changing nothing in the controller, which SHALL keep being unable to tell an Elixir runner from a Rust one. `Flezha.SDK.Ctx.timer_after/2` (milliseconds, anchored on `now/1`) and `timer_at_event_time/2` SHALL answer `{:ok, at}` with the absolute scheduled timestamp (the handle `cancel_processing_time_timer/2` and `cancel_event_time_timer/2` take, each answering whether a timer was pending) and SHALL refuse `{:error, {:event_time_timer_without_event_time_clock, tasklet, clock}}` under a clock other than `:event_time` and `{:error, {:timer_cap_exceeded, tasklet, max}}` past `max_timers_per_cell/0`, registering nothing on a refusal. Pending timers SHALL be one set per kind: a duplicate registration is a no-op that produces no op. An invocation's registrations and cancellations SHALL accumulate as ops over a copy of the pending set and commit only when the invocation completes (a firing's removal is op 0 and is committed on a park as well), and the committed ops SHALL ship after the dispatch's change records as one `format_id = 3` record when the class persists; a Tasklet without `apply_change/2` holds its timers in memory only, as the shared core's host path does. A processing-time timer SHALL be armed on the BEAM's timer service by the Cell process and fire through the Cell's own queue in ascending scheduled order, serialised against `run/3` by the same drain; an event-time timer SHALL fire when an input's `event_time/1` advances the Cell's event-time high-water to or past it, every due one in ascending order before that input, and never on a Cell nothing arrives at. `on_timer/4` SHALL receive the scheduled timestamp and the kind, `now/1` SHALL equal that timestamp inside the firing under every clock, a firing whose timer is no longer pending SHALL fire nothing, and a firing that fails SHALL ride the input channel's retry ladder and park exactly as an input does, its removal committed either way. A restore SHALL rebuild the pending set from the change log and re-arm the processing-time timers, overdue ones firing at once in ascending order; a compaction SHALL re-state the pending set above the snapshot's seq; the replacement of a Cell killed untrappably SHALL re-arm from the slot; eviction SHALL discard the Cell's timers with its state. Registrations, firings, cancellations and refusals SHALL be reported as one `[:flezha, :sdk, :timers]` telemetry event carrying the kind and the tasklet (this SDK's per-Tasklet timer counters), and the heartbeat SHALL carry `timers_armed` and `timers_fired`. `test/apps/elixir_runner` SHALL carry `session_timeout` as the reference's twin, and the manifest SHALL list `cell_timers` under `fixtures` with that id.

#### Scenario: A quiet session is closed by its own idle timer
- **WHEN** a `session_timeout` operator on an Elixir runner receives one input and nothing else for `TIMER_TEST_TIMEOUT_MS`
- **THEN** `on_timer/4` runs with no further input, its change is journaled, `CellState` reads `closed_at` holding the scheduled timestamp, and `closed@<at>` leaves on `test_kafka`

#### Scenario: A pending timer survives a re-seat from disk
- **WHEN** the Cell is rebuilt through `AddOperator` and `RestoreInitState` while its timer is still pending
- **THEN** the fold rebuilds the pending set from the `format_id = 3` record the first run shipped, the timer is re-armed on the fresh Cell, and it fires with no new input

#### Scenario: Crossing the high-water fires in order, before the input
- **WHEN** a Cell whose Tasklet writes `event_time/1` holds event-time timers at 100, 200 and 300, its high-water is 50, and an input with event time 250 arrives
- **THEN** `on_timer/4` runs for 100, then for 200, then `run/3` runs for the input; 300 stays pending, and a quiet period afterwards fires nothing however long it lasts

#### Scenario: A firing is serialised against run, and a poison firing parks once
- **WHEN** a processing-time timer comes due while the Cell is inside `run/3`, and separately `on_timer/4` fails past the retry ladder
- **THEN** the first `on_timer/4` runs only after that invocation returns and after any input queued ahead of the due message, and the second parks with the fired timer's removal committed, so it does not fire again after the park is released

#### Scenario: The pending set is bounded and a wrong-clock registration is refused
- **WHEN** a Cell holding `max_timers_per_cell/0` pending timers registers one more, and a `:wall` Tasklet calls `timer_at_event_time/2`
- **THEN** the first answers `{:error, {:timer_cap_exceeded, name, max}}` with the set unchanged, the second `{:error, {:event_time_timer_without_event_time_clock, name, :wall}}` with nothing registered, and both are reported as `:refused`

#### Scenario: Overdue timers fire ascending after a restore, and a stale due message fires nothing
- **WHEN** a restore rebuilds processing-time timers at 100 and 200 that are both in the past, and separately a timer is cancelled after its due message was armed
- **THEN** `on_timer/4` runs for 100 then 200 with no input, and nothing runs for the cancelled one

#### Scenario: Eviction discards timers, and a replacement re-arms from the slot
- **WHEN** a dynamic Cell holding a pending timer is idle-evicted, and separately a Cell holding one is killed untrappably
- **THEN** nothing fires for the evicted one and its key's next input cold-starts with no pending timers, while the killed one's replacement seeds the set from the slot and its timer still fires

### Requirement: The Elixir SDK carries the effectively-once tier in its own core

The Elixir SDK SHALL carry the `effectively_once` capability by re-expressing the kernel's rules in Elixir, inheriting nothing from `flz_core` and changing nothing in the controller, which SHALL keep being unable to tell an Elixir runner from a Rust one. A class declaring `exactly: :effectively_once` SHALL, for every effectful invocation — one that applied a change, emitted, committed a timer op or parked — ship ONE `cell_effect_record` frame at the next seq in place of its change records: the 16-byte header stamped `format_id = 2` over the postcard `EffectRecordBody` `Flezha.SDK.Wire.encode_effect_body/1` writes, enclosing the changes as External Term Format bodies, the captured sink outputs, the input's `flz.offset` as `consumed_offset` (absent for a firing), the park as `dlq`, the declared clock's value as `event_time` and the committed timer ops, under the credit window's `acquire`; an invocation with no effect SHALL ship nothing unless it parked. The Cell SHALL read the offset the router threads from the frame's `flz.offset` metadata onto its queue item, SHALL skip an input whose offset is at or below its consumed floor as a completed dispatch, and SHALL hand an inter-tasklet emit the offset of the input that produced it. Under checkpoint acknowledgement the Cell SHALL keep one staged interval — the changes, the timer ops, the offset of the last input folded, the input count, when it opened, and the outputs when `outputs: :at_checkpoint` is declared — that ships as one closing record carrying the interval's last offset when `Ctx.checkpoint/1` was called, when `every` inputs have folded in, when a park closes it, or when `max_delay_ms` has elapsed since it opened (a close message from the BEAM's timer service that rides the Cell's own queue and runs no author code, a stale one finding nothing open); a mid-interval as-produced emit SHALL ship in a record acknowledging nothing; a restore SHALL discard the open interval and rebuild the consumed floor and the event-time high-water from the log's effect records; the replacement of a Cell killed untrappably SHALL re-hold the open interval and the floor from the slot. A compaction SHALL re-state pending timers as a `format_id = 3` record under the tier as it does outside it. Held effect records of an invocation with inter-tasklet outputs (the `effectively-once-output` requirement on edge outputs) SHALL ship in seq order once the router reports the pushes admitted. `test/apps/elixir_runner` SHALL carry `tier_store`, `checkpoint_store`, `checkpoint_batched_store` and `bench_checkpoint` as the reference's twins, and the manifest SHALL list `effectively_once` under `fixtures` with those four ids.

#### Scenario: An effectful invocation ships one effect record and no output frame
- **WHEN** a `tier_store` Cell on an Elixir runner receives `k1:a` at `flz.offset` 3
- **THEN** one `cell_effect_record` frame ships at its seq, enclosing the `{:set, "k1", "a"}` body, one output on channel `sink` keyed `k1` with payload `k1=a`, consumed offset 3, and the wall clock's value as event time; no `cell_state_change` and no output frame leave

#### Scenario: An open interval acknowledges nothing and closes on the checkpoint
- **WHEN** a `checkpoint_store` Cell folds `k1:a` and `k2:a` at offsets 0 and 1, then `commit:1` at offset 2
- **THEN** the first two invocations ship records carrying their outputs and no offset, and the third ships one record carrying both changes and consumed offset 2

#### Scenario: A declared cadence and the delay close the interval without a checkpoint call
- **WHEN** a class declaring `ack: {:checkpoint, every: 3}` folds three inputs, and a class declaring `ack: {:checkpoint, outputs: :at_checkpoint, max_delay_ms: 20}` folds one input and receives nothing more
- **THEN** the first ships one record at the third input carrying all three changes and the third offset, and the second ships its held output within the delay in a record carrying its offset

#### Scenario: Held outputs ride the closing record as one batch
- **WHEN** a `checkpoint_batched_store` Cell folds three emitting inputs then `commit:1`
- **THEN** no output leaves before the commit, and the closing record's `outputs` holds the three in production order under one seq

#### Scenario: A restore discards the open interval and rebuilds the floor
- **GIVEN** a checkpoint Cell holding an open interval of two changes whose log's last effect record consumed offset 5
- **WHEN** it is restored over that log and the controller re-forwards offsets 4 and 6
- **THEN** the interval's changes are gone from the state, offset 4 is skipped as a completed dispatch, offset 6 dispatches, and the next close acknowledges 6

#### Scenario: A replacement re-holds the open interval from the slot
- **WHEN** a checkpoint Cell holding two staged changes is killed untrappably and its replacement receives `commit:1`
- **THEN** the closing record carries the two staged changes and the commit's offset at the seq the slot held

#### Scenario: A firing ships an effect record with no offset
- **WHEN** a processing-time timer fires on a class declaring the tier and `on_timer/4` applies a change and emits
- **THEN** one effect record carries the change, the output, the fired timer's removal and no consumed offset

#### Scenario: The tier scenarios pass under the Elixir runtime
- **WHEN** `FLEZHA_E2E_RUNTIME=elixir mix test --include system` runs the eight modules tagged `capability: :effectively_once` and the smoke tier's two tier goldens
- **THEN** every scenario runs and passes on `uds` and the crash matrix on `quic`, the Postgres-gated ones with the `flezha_postgres_sink` container started by name, and `effectively_once` sits in `@required_capabilities`

### Requirement: The Elixir SDK carries late-input correction in its own core
The Elixir SDK SHALL implement late-input correction in its own runner core, which inherits nothing from `flz_core`, against the rules of `effectively-once-correction` rather than its Rust. A class MAY declare `correction_ms` and `beyond_window` under `exactly: {:effectively_once, ...}` in its `run`; the declaration SHALL require at least one Tasklet declaring an `:event_time` clock, SHALL refuse a zero window, and SHALL reach the controller as `correction_ms` and `beyond_window` on `AddOperatorResponse.metadata`. Each Cell SHALL classify every input against the event-time high-water it already tracks and restores, BEFORE advancing it: at or ahead of the high-water, or with no high-water yet, the input SHALL be dispatched and the high-water advanced; behind it by no more than the declared window the input SHALL NOT be dispatched and one `correction_request` frame SHALL be sent on the Cell's own state stream carrying the target event time alone, with the Cell identity on the frame envelope, a seq of zero and a zero record header; behind it by more than the window the declared policy SHALL decide — `process_as_arrival` (the default) dispatches it and `drop` does not — and in NEITHER case SHALL the high-water advance. A Cell with no state stream SHALL dispatch a correctable input as an arrival rather than withhold it for a correction it could not request. An input the controller marked `flz.replay` SHALL be buffered by its Cell rather than dispatched, and the payload-less `flz.replay_end` broadcast SHALL be intercepted before any channel is resolved and delivered to EVERY Cell of the operator, static and dynamic. The close SHALL be placed on each Cell's own queue behind the members that preceded it rather than executed on arrival, so a close can never overtake a member it was meant to include; on reaching it the Cell SHALL order the buffered members by extracted event time, ties broken by ascending input-log offset, and dispatch them ahead of anything enqueued behind the close. A replayed member SHALL advance the high-water and SHALL NOT be re-classified. A `RestoreInitState` SHALL discard a buffered slice. The SDK SHALL write no correction journal, no crash marker, no new control message and no second replay path: the correction is the existing tier restore-and-replay with a rewound baseline and an event-time-ordered slice.

#### Scenario: A late input inside the window is withheld and asks for a correction
- **WHEN** a Cell of a class declaring `correction_ms: 600_000` has dispatched an input at event time 300 000 and then receives one at event time 299 000
- **THEN** the second input is not dispatched, exactly one `correction_request` frame is sent on the Cell's state stream whose decoded body is 299 000, and the Cell's high-water is unchanged

#### Scenario: A beyond-window input follows the policy and never moves the high-water
- **WHEN** a Cell with high-water 1 000 000 and a 600 000 ms window receives an input at event time 1 000
- **THEN** under the default `process_as_arrival` it is dispatched and the high-water stays 1 000 000, and under a declared `:drop` it is neither dispatched nor recorded, with the high-water likewise unchanged

#### Scenario: A replayed slice is held to its broadcast and folded in event-time order
- **WHEN** the controller re-forwards three inputs marked `flz.replay` with event times 300, 100 and 200 at ascending input-log offsets and then broadcasts `flz.replay_end`
- **THEN** none of the three is dispatched on arrival, and on the broadcast they are dispatched in the order 100, 200, 300

#### Scenario: A live input behind the broadcast folds after the whole slice
- **WHEN** an input that carries no replay mark is enqueued after the last slice member and before the end-of-slice broadcast
- **THEN** it is dispatched after every member of the sorted slice, never between two of them

#### Scenario: The broadcast reaches every Cell, including one that buffered nothing
- **WHEN** an operator holds two Cells, only one of which received slice members, and the `flz.replay_end` frame arrives
- **THEN** the Cell with members flushes them in event-time order and the Cell with none continues normally, neither stalling nor erroring

#### Scenario: A restore discards a buffered slice
- **WHEN** a Cell holding buffered slice members is sent `RestoreInitState`
- **THEN** the buffer is empty afterwards, and the members return only if the controller re-forwards them from the rewound watermark

#### Scenario: A replayed member is not re-classified as late
- **WHEN** a slice member whose event time is behind the Cell's high-water is dispatched from the ordered flush
- **THEN** it runs, it advances the high-water, and no `correction_request` is sent for it

### Requirement: The Elixir SDK SHALL hold a failed plugin-bound output rather than logging it

A plugin-bound output whose write fails SHALL be held and retried on the next
bound stream, on the same terms the FFI hosts hold theirs. Logging the failure and
returning SHALL NOT be the whole handling, because the caller has no way to learn
the output was lost and no counter records it.

#### Scenario: An emit fails while the data stream is down

- **WHEN** the Elixir SDK's `emit_output/3` cannot write a plugin-bound output
- **THEN** the output SHALL be held for replay and SHALL NOT be discarded after a
  log line

#### Scenario: The hold is bounded and says so when it overflows

- **WHEN** more outputs are held than the bound allows
- **THEN** the SDK SHALL count what it sheds, so a silent loss cannot be mistaken
  for a delivery

### Requirement: The Elixir SDK authors and drives steps operators in its own core

The Elixir SDK SHALL carry a `Flezha.SDK.Operator` behaviour whose class declares
a structured DAG in `channels/1`, the same callback and builder token a flow class uses, and no one-function
`run/2` body, plus the settings tree's `run: {:steps, each: ..., on_failure: ...}` (`each`
`:once` | `:per_input` | `:chained`, `on_failure` `:undo_all` | `:keep_progress`), `durable`
and `max_duration_ms`, each reading a default when the class is silent. There SHALL be no `retry_attempts/0`, `retry_backoff_ms/0`,
`barrier_retry_attempts/0` or `wake_after_ms/0`: a stage's ladder is bounded by the edge's `retry_max/2` and
`retry_backoff_ms/2` as a flow operator's is, a barrier is re-sent a fixed number of times read
from `Flezha.SDK.Knob`, and a sleeping instance's wake deadline is the earliest pending processing-time
timer its tasklets registered. `Flezha.SDK.register_factory/2` SHALL register such a class onto the runner handle
exactly as it registers a flow class, and the runner SHALL announce the class's run with its
factory id, so the controller places by factory id and knows the run before it sends `AddOperator`.

Registration SHALL accumulate refusals into one `{:error, %Flezha.SDK.Error{}}` (never a
raise), its code the first issue's and its `details.issues` every issue, using the reference's codes: `topology.no_steps_body` (no `channels/1`), `topology.no_entry_channel` and
`topology.branching_chained_loop`. `EntryRoot`
and `MergeFromEntryChannel` SHALL NOT exist: an entry channel may feed several tasklets, a class
may declare several entry channels, each rooting its own chain, and an entry channel may be a
merge source, because every message carries the channel it arrived on. `DynamicCells` SHALL NOT
exist: a keyed edge into a keyless steps tasklet makes it dynamic exactly as it makes a flow operator's
tasklet dynamic, and its Cells are minted per key on dispatch. `ZeroRetryBackoff`
SHALL NOT exist, the knob it judged being gone; `topology.retry_max_zero` on an edge is the shared topology's
refusal for both runs. A refusal the shared
topology already raises SHALL be carried through as its own issue, with its own code, rather than restated,
and the reference's `topology.cycle`, `topology.unreachable_tasklet` and `topology.multiple_output_channels` SHALL NOT exist here:
the first two are unreachable because the shared topology refuses a cycle first and a node
exists only where an `into` created it, and the last because a node owns exactly one output
channel. A missing inter-tasklet codec SHALL be named by no SDK, this one included: every
inter-tasklet value has a serialization everywhere, so there is nothing for an author to
declare and nothing to refuse.

A structured class SHALL drive its DAG frontier by frontier, over the chain of the pass's origin:
the entry channel the pass's message arrived on, or the tasklet whose timer's firing opened the pass.
Each stage SHALL commit as soon as its Cells have drained, whatever its siblings are still doing,
one `TaskCommit` in flight at a time. A tasklet becomes runnable only once every producer feeding it in
that chain has committed, so a fan-out interleaves independently, a fan-in is dispatched exactly
once with the accumulated batch of this pass's producers, and a producer of another chain is
never waited for. A message SHALL enter through the same `resolve_channel`, `parse` and park
functions the flow operator's `{:input, ...}` handler uses, so an undeclared channel name and
an unreadable payload park as a flow operator's do and start no pass. A stage's Cells SHALL be the
flow operator's Cell set, held through the one module `Flezha.SDK.Operator` holds it
through: single and static Cells prewarmed at start, a keyed Cell minted on the first value of
a pass addressed to its key, every value reaching its Cells through the same per-mode target
resolution the flow operator routes with, one barrier queued behind each Cell the stage
handed a value to, and the stage done when those reply; a Cell that exits before replying SHALL
fail its stage as an exhausted ladder does, and an undo's Cell that exits SHALL count as a failed
hook, the rest of the plan still running. Every stage Cell SHALL be started with
the same ctx facts a flow Cell gets from its node's sink routes (`channel`, `keyed_sink?`,
`key_required?`), so `Flezha.SDK.Ctx.emit_keyed/3` and `emit_with_meta/3` answer in a steps operator
tasklet exactly as in a flow tasklet. A durable class's Cell SHALL capture each sink output
into its invocation's effect record, unstamped, through the same sink leaf and the same record
path a tier flow operator Cell uses, and SHALL ship the record on its own record stream before the
stage's barrier; the driver SHALL keep no outbox of its own, and a class that journals nothing
SHALL route its sink outputs to the wire with their channel, key and metadata as a
non-persistent flow Cell does. A stage's record SHALL name the Cells
it dispatched to, by identity, whether or not they emitted, each with the seq of its last effect
record in the stage and the sink channels it wrote on; its `TaskCommit.cells_ran`, its
intent markers and its undo SHALL read that record, so the undo runs on exactly the Cells that
ran and on no declared Cell that received nothing. A restore naming a keyed Cell SHALL
materialise it through the same lookup a flow operator's restore uses, refusing past the cap, and the
driver SHALL count every restore frame the bind owes as landed whether or not a Cell took it,
after the frame's fold so the gate never opens over unapplied state, and a frame no Cell takes
never leaves it waiting. A stage's Cell SHALL hand its due
timer firings to the driver rather than its own queue, and the driver SHALL be the gate: a firing
while the driver waits opens a pass at that tasklet, dispatched to that one Cell with a barrier
behind it; a firing whose stage is in flight joins it with one more barrier; any other firing
waits in the driver's queue, in arrival order with messages, and is served at the next boundary
before the loop mode's own continuation. A firing the driver dispatched SHALL keep its removal
out of the Cell's records and its timestamp in the Cell's pending set, un-armed, until the driver
releases it on the stage's acknowledgement, and the driver SHALL drop from a rebound Cell's
restored timers every firing the journal records committed. At a park the driver SHALL read
each Cell's earliest pending processing-time timer and ship the minimum as the wake deadline. Each stage
SHALL be committed durably before the next begins, and the class's leaf and compensable positions SHALL be
declared to the controller in the `AddOperator` response.

#### Scenario: A structured class is refused for every fault at once

- **WHEN** a steps class declares no tasklets and no entry channel
- **THEN** registration answers `{:error, %Flezha.SDK.Error{}}` whose `details.issues` name
  `topology.no_steps_body` and every other fault together, not the first alone

#### Scenario: A stage's Cells are the SDK's own, not a second mechanism

- **WHEN** a steps stage dispatches a batch
- **THEN** it runs on `Flezha.SDK.Cell` processes with the same state, records, timers and
  retry ladder a flow operator's Cells have, the ladder's bounds read from the consuming
  edge's `retry_max` / `retry_backoff_ms` exactly as a flow operator Cell reads them, differing only in
  that an exhausted ladder fails the tasklet instead of parking the input to the dead-letter queue
  and that a due firing is handed to the driver, which decides when it runs

#### Scenario: A stage whose Cell dies ends the run instead of waiting for it

- **WHEN** a stage's Cell is killed while its barrier is still queued behind the running input
- **THEN** the driver fails that stage, the run unwinds and ships `OperatorCompleted` with status
  `error` naming the tasklet, and a later stage never runs

#### Scenario: A step commits while its sibling still runs

- **WHEN** a pass fans out to two tasklets, one that finishes at once and one still running
- **THEN** the finished one's `TaskCommit` is sent before the running one ends, so a crash then
  re-runs only the one that had not committed

#### Scenario: A keyed steps tasklet mints its Cells on dispatch

- **WHEN** a steps class declares `input("in") |> into(Split) |> key_by(&key/1) |> into(Keyed)`
  and a pass releases values keyed `"a"` and `"b"` from `Split`
- **THEN** registration accepted the class, two `Keyed` Cells exist only after the release,
  `lookup/4` finds each by its key, the stage's `TaskCommit` names both identities, and a
  third value keyed `"a"` in the same pass reaches the existing Cell

#### Scenario: A Cell that emitted nothing is still recorded and undone

- **WHEN** a compensable tasklet runs in two Cells of which one emits nothing, and a later
  tasklet fails
- **THEN** the stage's `TaskCommit.cells_ran` names both, and `compensate/3` runs in both,
  while a third declared Cell of that tasklet that received no value is neither named nor
  compensated

#### Scenario: A steps tasklet emits to a keyed sink as a flow tasklet does

- **WHEN** a durable steps tasklet wired `into(Emitter) |> named("out") |> keyed() |> sink()` calls `Ctx.emit_keyed(ctx, "k", value)` and then `Ctx.emit(ctx, value)`
- **THEN** the first answers `:ok` and the second `{:error, %Flezha.SDK.Error{code: "runtime.key_required_on_keyed_channel"}}` naming `"out"`, exactly as under `use Flezha.SDK.Operator`

#### Scenario: A stage's sink output rides its Cell's record and waits for the commit

- **WHEN** a tasklet on a sink channel emits with `%{"tag" => "a"}` while the class journals
- **THEN** the Cell's effect record for that invocation carries channel `out`, the key, metadata `[{"tag", "a"}]` and the payload with no `flz.` key, the record is on the Cell's stream before the stage's `TaskCommit`, the `TaskCommit` names that record's seq for the Cell, and the sink receives the frame with `flz.cell`, `flz.seq`, `flz.key` and `tag` only after the controller acks the commit

#### Scenario: A class that journals nothing sends its output to the wire

- **WHEN** a steps class with `durable: false` emits on its sink
- **THEN** the frame leaves on the wire with `channel`, the key and the metadata during the pass, and nothing of it is dropped with the iteration

#### Scenario: A tasklet ends its steps operator

- **WHEN** a tasklet of a `:per_input` class calls `Flezha.SDK.Ctx.complete(ctx, result)` while handling a message
- **THEN** the iteration is committed durably, and the instance then completes with `result`
  instead of waiting for the next message

#### Scenario: A message names the chain it starts

- **WHEN** a class declares entries `a` and `b`, each into its own tasklet, and a message arrives naming `b`
- **THEN** `b`'s tasklet runs over the parsed message and `a`'s does not, and a join fed by both waits only for the producer in `b`'s chain

#### Scenario: An unreadable message parks and starts no pass

- **WHEN** a `:once` class whose entry parser raises receives a message the parser rejects
- **THEN** the message is parked to the dead-letter queue as the flow operator parks one, no stage is dispatched, and the instance keeps waiting for its message

#### Scenario: A firing names the chain it starts

- **WHEN** a `:per_input` class `in -> tick -> relay -> out` waits for a message and `tick`'s Cell reports a due firing
- **THEN** the driver dispatches the firing to that Cell with a barrier, `on_timer/4` runs there, `relay` runs over what it emitted, `tick`'s `TaskCommit` carries the trigger, and the iteration commits

#### Scenario: A firing during a pass waits for the boundary

- **WHEN** `tick`'s Cell reports a due firing while the driver is committing `relay`
- **THEN** the firing waits in the driver's queue and opens the next pass after the iteration's acknowledgement, ahead of a message that arrived later

#### Scenario: The park names the earliest timer

- **WHEN** a `:per_input` instance's Cells hold processing-time timers at 90 000 and 60 000 when the park-idle window elapses
- **THEN** the driver ships `IterPark` with `wake_deadline_ms` 60 000

### Requirement: One plugin sink per channel and plugin

`sink/2` and `sink_with/3` SHALL refuse a second sink declared on the same channel to the same
plugin with `topology.duplicate_plugin_sink`, naming both. Neither half alone is the duplicate: one channel
MAY reach several plugins — a keyed wire beside a plain one — and one plugin MAY be fed by
several channels, which is how a chain reports every stage to one place with a different encoder
each time. This is a deliberate departure from the reference, whose `sink` closes its channel
and therefore refuses per channel alone.

#### Scenario: The same channel and plugin twice is refused

- **WHEN** a class declares `sink(s, "out")` twice on one handle
- **THEN** registration fails with `topology.duplicate_plugin_sink` naming the channel and the plugin

#### Scenario: A plugin fed by several channels is not a duplicate

- **WHEN** two named channels of one class each declare a sink to the plugin `"branch_sink"`
  with different encoders
- **THEN** registration succeeds and each channel's frames carry its own encoder's bytes

### Requirement: The Elixir SDK SHALL hold a failed change record rather than losing it

A change record whose write to the secondary stream fails SHALL be held and
replayed on the next bound stream, on the same terms the SDK holds a failed
plugin-bound output. Losing it SHALL NOT be the whole handling, because an output
the SDK does replay can otherwise reach the controller with no record of the
change that produced it.

The same SHALL hold for every frame the SDK writes to that stream: the delete an
evicted Cell ships, the records a Cell still holds when it stops, and the
steps driver's outputs and records written through the runner. A hold that
dies with the process that owned it is not a hold, so frames a Cell holds
when it stops in an orderly way SHALL pass to a holder that outlives it and be
replayed ahead of anything its replacement writes.

#### Scenario: A record's write fails while the data stream is down

- **WHEN** a Cell's `cell_state_change` frame cannot be written
- **THEN** the record SHALL be held for replay and SHALL NOT be discarded

#### Scenario: A replay keeps records ahead of the outputs of the same dispatch

- **WHEN** held records and held outputs of one dispatch are replayed after a rebind
- **THEN** the dispatch's records SHALL reach the stream before its output frame,
  as they do on a live stream

#### Scenario: The hold is bounded and counts what it sheds

- **WHEN** more records are held than the bound allows
- **THEN** the SDK SHALL count what it sheds where every drop-oldest on this wire
  is counted

#### Scenario: An eviction with no stream still reaches the controller

- **WHEN** a Cell is evicted while no secondary stream is bound
- **THEN** its `delete_cell_state` frame SHALL be replayed on the next bound
  stream rather than lost with the process

#### Scenario: A Cell that stops while holding hands its frames on

- **WHEN** a Cell with held records is evicted before a stream is bound
- **THEN** after the rebind those records SHALL reach the stream, ahead of the
  output of the dispatch that produced them and ahead of anything a replacement
  Cell writes

#### Scenario: A steps operator write the runner cannot ship is held

- **WHEN** the steps driver's output or record is cast to the runner with no
  secondary stream bound, or the write is refused
- **THEN** the frame SHALL be held and replayed on the next bound stream, and
  SHALL NOT be dropped behind a log line

### Requirement: The Elixir SDK re-expresses the reference check from the reference fixture

`Flezha.SDK.Topology.build/1` SHALL judge a topology by the same three stages the core's
check runs — the ordered links of each chain, the derivation, the topology and class
facts — over a record of the same shape, refusing with the reference names and sentences
as `{:error, %Flezha.SDK.Error{}}` carrying every issue in `details.issues`, since the Elixir SDK inherits nothing from
`flz_core`. It SHALL carry every reference name except `topology.branch_too_few_arms`, whose surface (`branch`)
the v1 subset does not author and which the fixture rows name under `needs` and the
conformance run skips by name; `topology.merge_too_few_sources` and `topology.duplicate_merge_source` SHALL be
judged by the same merge-group walk the core's `chains.rs` runs, over the merge links the
builder records. The single checkpoint implication site SHALL remain
`Flezha.SDK.Channels`, proven by the same fixture rows.

#### Scenario: A keyed channel without a plugin wire is refused

- **WHEN** a channel is declared `keyed` and no `sink` closes it
- **THEN** `build/1` returns `{:error, %Flezha.SDK.Error{code: "topology.keyed_channel_without_plugin_wire"}}`

#### Scenario: A merge row refuses with the reference name

- **WHEN** the Elixir interpreter reaches `merge_one_source` or `merge_same_twice`
- **THEN** it authors the row through `Channels.merge/1` and `Flezha.SDK.Merge`, `build/1` refuses with `topology.merge_too_few_sources` or `topology.duplicate_merge_source`, no row is skipped for `merge`, and the manifest's `topology_surfaces` lists `merge`

### Requirement: The Elixir SDK SHALL declare the guarantee class of every frame it writes

Every frame the Elixir SDK writes to a stream SHALL carry a declared guarantee
class, and the class SHALL decide what happens when the write is refused. A
writer SHALL name the frame's payload id and SHALL NOT choose a policy, so that
adding a frame kind is adding a row rather than adding a behaviour. The classes
are `:control` (a control frame — refused to its caller, or counted where the
caller is a cast, and never held, because a reply re-driven after its waiter
timed out is a stale answer), `:durable` (a change record, a snapshot, a delete,
a park payload or a shutdown state ship — held in the bounded ring, drop-oldest
counted, replayed in write order on the next bound stream) and `:output` (a
plugin or steps operator emit — held already framed, so a keyed output's channel and
its identity stamps survive the replay). A payload id the table does not name
SHALL be refused at the call rather than given a default.

The data heartbeat SHALL NOT be held. It reports a moment that has passed, so
replaying it after a rebind is worse than losing it; its write verdict SHALL
instead feed a reconnect ladder on the same terms the shared core's heartbeat
does, and a heartbeat is delivered when any bound secondary took it.

#### Scenario: A durable frame refused by a dead stream is replayed

- **WHEN** the SDK writes a frame of class `:durable` and the write is refused,
  or no stream is bound
- **THEN** the frame SHALL be held and replayed on the next bound stream, and a
  shed at the bound SHALL be counted where every drop-oldest on this wire is counted

#### Scenario: A control frame is refused rather than held

- **WHEN** the SDK writes a frame of class `:control` and the write is refused
- **THEN** the refusal SHALL reach the caller, or be counted where the caller is
  a cast, and the frame SHALL NOT enter the hold

#### Scenario: An undelivered heartbeat asks for a reconnect

- **WHEN** no bound secondary takes the SDK's `data_heartbeat`
- **THEN** the miss SHALL feed the reconnect ladder rather than being discarded,
  and a runner with one live and one dead secondary SHALL NOT reconnect

#### Scenario: A frame the table does not name is refused

- **WHEN** a write names a payload id the class table does not declare
- **THEN** the SDK SHALL refuse the write rather than apply a default policy

### Requirement: The Elixir SDK SHALL read the durable frame through the shared frame seam

The Elixir SDK SHALL decode the durable record framing through its frame module, `Flezha.SDK.RecordFrame`, rather than through a walker of its own, and SHALL NOT hold its own copy of the header length, the language-name table, the format identifiers or the framing crc32.

The Elixir SDK is its own core and inherits nothing from `flz_core`; this requirement is about one Elixir module the SDK and the controller share, not about linking the kernel. The frame module is pure: it takes binaries, owns no process and no Controller state, and the controller's walks over a record log on a device read through it.

The SDK's answer to a record whose crc32 fails SHALL be the refusal the frame seam defines, not a second refusal phrased in the SDK's own words. That refusal names the byte offset the record failed at and the last `seq` that verified before it; the SDK SHALL NOT restate it with a `seq` of its own.

#### Scenario: The SDK holds no second copy of the frame

- **WHEN** the Elixir SDK's record module is read
- **THEN** it SHALL contain no header-length constant, no language-name table, no format-identifier literal and no crc32 expression of its own

#### Scenario: The controller reads a device through the SDK's frame

- **WHEN** the controller's record-log walk decodes a record read from a file
- **THEN** it SHALL reach `Flezha.SDK.RecordFrame`'s decoder and refusal, and SHALL hold no framing of its own

#### Scenario: The SDK refuses a corrupt record the way the seam does

- **GIVEN** a restore payload holding a record whose crc32 does not verify
- **WHEN** the Elixir SDK folds it
- **THEN** it SHALL refuse with the seam's refusal, naming the byte offset and the last `seq` that verified, and SHALL NOT return a partial state as success

### Requirement: The Elixir SDK SHALL report what a restore does to queued input

The Elixir SDK SHALL carry the restore-time queued-input outcome on its heartbeat, under the same key every other runtime uses, so that the fate of queued input is readable for an Elixir-hosted Operator as it is for every other host.

The Controller already mirrors that key into its liveness counters and its observability source, and the kernel already emits it; the Elixir SDK is the only runtime that carries neither the counter nor the key. A key that one runtime pays for on the wire and another omits leaves an operations surface that is correct for six SDKs and blank for the seventh.

If the Elixir SDK discards no queued input on restore, it SHALL still declare the key and report zero, and the reason SHALL be stated where the key is declared. A key reported as absent and a key reported as zero are different answers to a user, and only the second is honest about a runtime that was asked.

#### Scenario: The heartbeat carries the key

- **WHEN** an Elixir-hosted runner sends a data heartbeat
- **THEN** the restore-time discard count SHALL be among its keys, and the Controller's mirror SHALL accept it without a slot mismatch

#### Scenario: A discard is visible to a user

- **GIVEN** an Elixir-hosted Cell holding queued input when a restore lands
- **WHEN** the restore discards that input
- **THEN** the count SHALL rise on the operations surface that reports it for every other runtime

#### Scenario: A runtime that discards nothing says so

- **WHEN** the Elixir SDK performs a restore that discards no queued input
- **THEN** it SHALL report zero rather than omitting the key, so the surface distinguishes "nothing was discarded" from "nothing was measured"

### Requirement: An Elixir tasklet requests spawn-or-deliver of a steps instance

The Elixir SDK SHALL let a tasklet request spawn-or-deliver of a steps instance in its
own namespace with one call, under the contract the other SDKs implement
(`docs/sdk/tasklet_requested_spawn.md`). `Flezha.SDK.Ctx.spawn_steps/4` SHALL take a steps factory id, a binary
payload and a keyword list of options defaulting to `[]`, whose keys are `id:` (an instance id)
and `if_running:` (`:deliver` or `:error`, `:deliver` when an `id:` is given without it). The call
SHALL send `SpawnStepsRequest` on the runner's control plane, with an empty instance id when no
`id:` was given, and return a handle without waiting for the controller. An `if_running:` given
without an `id:` SHALL raise `ArgumentError` at the call and send nothing.

The handle SHALL resolve, through `Flezha.SDK.Spawn.await/2` or `Flezha.SDK.Spawn.try_result/1`,
to `{:ok, %{id: id, outcome: :spawned | :delivered | :skipped_on_replay}}`, where `id` is the
instance id the request resolved to (the caller's, or the one the controller generated), or to
`{:error, %Flezha.SDK.Error{}}`, whose code is `operator.already_running`, `operator.factory_unknown`,
`operator.factory_not_steps`, the controller's own code for any other refusal, `spawn.timeout` or
`spawn.unavailable`.
A refusal SHALL NOT fault the invocation, its emits or its records. A failure no caller read
SHALL be logged once as a warning. An invocation of a replayed input SHALL resolve
`{:ok, %{id: id, outcome: :skipped_on_replay}}` with the caller's id, or `""` when no `id:` was
given, and send nothing. A context with no runner, or a runner with no control
plane, SHALL resolve `spawn.unavailable` at once and send nothing. A request the controller never
answers SHALL resolve `spawn.timeout` after the runner's reply deadline, and the runner SHALL keep
no waiter for it afterwards. The handle SHALL be read inside the invocation that made it; the
SDK MAY drop a reply that arrives after the invocation returned, and the warning is what
reaches such a caller. The request SHALL be written as a `:control` frame.

#### Scenario: A free id spawns and a taken id delivers

- **WHEN** an Elixir flow tasklet calls `spawn_steps(ctx, factory_id, payload, id: "w1")`
  twice, and a connected runner offers the steps factory
- **THEN** the first handle resolves `{:ok, %{id: "w1", outcome: :spawned}}`, the second
  `{:ok, %{id: "w1", outcome: :delivered}}`, and the steps instance receives both payloads as
  inputs

#### Scenario: A call without an id spawns under a generated id

- **WHEN** a tasklet calls `spawn_steps(ctx, "worker", payload)` with no options
- **THEN** the request carries an empty instance id, and the handle resolves
  `{:ok, %{id: id, outcome: :spawned}}` where `id` begins `worker-` and names the running instance

#### Scenario: The error policy refuses a running id

- **WHEN** a tasklet calls `spawn_steps` with `id: instance_id, if_running: :error` for an
  instance id that is running
- **THEN** the handle resolves `{:error, %Flezha.SDK.Error{code: "operator.already_running"}}`, its details naming the instance id

#### Scenario: if_running without an id raises at the call

- **WHEN** a tasklet calls `spawn_steps(ctx, "worker", payload, if_running: :error)`
- **THEN** the call raises `ArgumentError` naming `if_running` and `id`, and no
  `SpawnStepsRequest` leaves the runner

#### Scenario: The call returns before the controller answers

- **WHEN** a tasklet calls `spawn_steps` and the controller has not answered yet
- **THEN** the call has already returned a handle, and `try_result/1` answers `:pending`

#### Scenario: A refusal leaves the invocation intact

- **WHEN** the controller refuses a spawn, and the tasklet emits a value and returns
  `{:ok, state}`
- **THEN** the emit ships, the invocation is not retried, and the handle resolves
  `{:error, %Flezha.SDK.Error{}}` carrying the controller's own code

#### Scenario: An unread failure is logged once

- **WHEN** a spawn fails and no caller reads its handle
- **THEN** exactly one warning names the instance id (empty when none was given), the factory id
  and the reason

#### Scenario: A replayed input issues nothing

- **WHEN** a correction-declaring class re-runs a replayed slice and its tasklet calls
  `spawn_steps` with no `id:`
- **THEN** the handle resolves `{:ok, %{id: "", outcome: :skipped_on_replay}}` and no
  `SpawnStepsRequest` leaves the runner

#### Scenario: A context with no runner is unavailable

- **WHEN** a tasklet runs under a context that carries no runner, or under a runner whose
  session is down, and calls `spawn_steps`
- **THEN** the handle resolves `{:error, %Flezha.SDK.Error{code: "spawn.unavailable"}}` at once, and nothing is sent or kept

#### Scenario: An unanswered request times out and is forgotten

- **WHEN** the request is written and no `SpawnStepsResponse` arrives within the reply
  deadline
- **THEN** the handle resolves `{:error, %Flezha.SDK.Error{code: "spawn.timeout"}}`, an unread one is logged once, and the
  runner holds no waiter for the request

#### Scenario: The request is a control frame

- **WHEN** the runner writes a `SpawnStepsRequest`
- **THEN** `Hold` classes it `:control`, and the runner server does not raise

### Requirement: The Elixir per-input loop waits for its inbox as the shared core does

A `:per_input` steps loop in the Elixir SDK SHALL seed every iteration, the first included,
from the next input in its inbox and never from the spawn args. With nothing queued it SHALL
wait, for up to 60 seconds while a rehydrating bind still owes it inputs and otherwise for
its class's `sleep_after`, and SHALL park only when that wait ends with nothing to consume and
the class sleeps (its `durable` is `:local` or `:remote` with a `sleep_after` that is not
`false`). A class kept in memory (`durable: false`) SHALL wait until an input or a stop arrives,
because it cannot sleep, and a `sleep_after` written under `false` SHALL fail to compile naming
it. A stop or the duration cap reached during the wait SHALL complete
the run as `cancelled`. An iteration SHALL commit the durable input-log offset its input
carried in `flz.offset`, and SHALL commit no offset when its input carried none; the SDK SHALL
read no other offset key.

A flow operator in the Elixir SDK whose class sleeps SHALL sleep the
same way, re-expressed in its own core: its instance process SHALL arm one idle timer at bind
(after any owed input tail has arrived), SHALL ask its Cells the idle question when it fires
(mailbox empty, nothing in flight, buffer flushed, no timer armed) and re-arm from the latest
activity when any Cell is busy, and, idle, SHALL ship `IterPark` with iteration `0` and stop
its Cells; the class SHALL announce `durable` and `sleep_after_ms` in its declarations for
the controller.

#### Scenario: The first iteration consumes the first input

- **WHEN** a per-input loop is started with spawn args and one input then arrives
- **THEN** its first committed iteration ran over that input, and no iteration ran over the args

#### Scenario: An empty inbox parks only after the idle window

- **WHEN** a journaling per-input loop finds its inbox empty and no input arrives within the idle
  window
- **THEN** it ships `IterPark` and no `OperatorCompleted`

#### Scenario: An owed tail is waited for past the idle window

- **WHEN** a rehydrated loop still owes an input and its inbox is empty
- **THEN** it waits for that input rather than parking at the idle window or completing

#### Scenario: A loop that journals nothing waits instead of parking

- **WHEN** a per-input loop whose class's persistence is off finds its inbox empty
- **THEN** it ships no `IterPark`, runs the next input that arrives, and a stop completes it as
  `cancelled`

#### Scenario: A sleeping flow class parks its instance

- **WHEN** a flow class sleeps (`durable: {:local, sleep_after: ms}`) and an instance of it receives nothing for that interval with every Cell drained
- **THEN** the instance ships `IterPark` with iteration `0`, stops its Cells, and the next message the controller forwards after the wake reaches a Cell restored to its state before the sleep

#### Scenario: A sleep interval on a class kept in memory does not compile

- **WHEN** a module writes `durable: {false, sleep_after: 1_000}` in its `use`
- **THEN** compilation fails with a `CompileError` naming `sleep_after` and `false`, and nothing is declared

#### Scenario: An iteration commits its input's offset

- **WHEN** an input stamped `flz.offset` `7` is consumed
- **THEN** that iteration's `IterCommit` carries consumed offset `7`

#### Scenario: An unjournaled input commits no offset

- **WHEN** an input carrying no `flz.offset` is consumed by a class that journals nothing
- **THEN** that iteration's `IterCommit` carries no consumed offset

#### Scenario: A once class waits for its message

- **WHEN** a `:once` class is started with no message
- **THEN** it dispatches no stage, parks after the idle window when it journals, waits when it does not, and runs its one pass over the first message that arrives

#### Scenario: A carry-fed pass commits no offset

- **WHEN** a journaling `:chained` loop's first pass consumes the input at `flz.offset` `0` and two carry-fed passes follow
- **THEN** the three `IterCommit`s carry consumed offsets `0`, none and none

### Requirement: Socket mode reaches the controller through the SDK's own NIF

The SDK SHALL dial UDS and QUIC through a Rustler crate of its own, Apache-2.0, over `flz_transport`'s client, and SHALL NOT load `flz_nif`. The crate SHALL be distributed precompiled for the five supported triples through `rustler_precompiled` with a checksum the package carries, built by the core release job, and SHALL build from source when the checksum file names no artefact for the host.

#### Scenario: A standalone runner loads the precompiled NIF
- **WHEN** a runner starts on a supported triple with no Rust toolchain
- **THEN** the SDK SHALL load the precompiled client NIF, verify it against the checksum file, and connect

#### Scenario: The controller's NIF is untouched
- **WHEN** `flz_nif` is inspected after the change
- **THEN** it SHALL hold the server half only and SHALL keep `publish = false`

### Requirement: Native mode is a behaviour the controller registers

The SDK SHALL declare a connect behaviour for in-node runners and SHALL call the implementation set in its application env, refusing by name when none is set. The controller SHALL implement the behaviour with its transport server and set it at boot. The SDK SHALL NOT name a controller module.

#### Scenario: An in-node runner connects through the registered implementation
- **WHEN** the controller boots and an operator starts on the in-node runner
- **THEN** the SDK's `:native` connect SHALL reach the controller through the registered behaviour and dispatch SHALL proceed as before

#### Scenario: A standalone runner has no native mode
- **WHEN** a runner outside the controller's VM asks for `:native`
- **THEN** the SDK SHALL refuse naming the behaviour and the modes it does support

### Requirement: The Elixir SDK authors with `use` macros that expand to its behaviours
The SDK SHALL provide `use Flezha.SDK.Tasklet` and `use Flezha.SDK.Operator`. Each SHALL set the module's `@behaviour` and, for every option written, define the zero-arity callback of the same name returning the option's value, and SHALL define nothing for an option not written, so every default stays resolved by `function_exported?` as it is for a hand-written module. `use Flezha.SDK.Tasklet` SHALL require `name` and accept `cell_keys`, `mailbox_capacity`, `max_cells`, `idle_ttl_seconds`, `compact_after_bytes`, `schema_version`, `min_readable_schema_version`, `logical_clock` and `max_timers_per_cell`, plus the short-form options; `idle_ttl_seconds` SHALL be a positive integer and `logical_clock` a boolean, any other value being a `CompileError` naming the option; the retired options `cell_idle_ttl_ms` and `clock`, and a `clock/0`, `event_time_of/1` or `cell_idle_ttl_ms/0` the module defines with `def` or `defp`, SHALL each be a `CompileError` naming what replaced it; it SHALL import `Flezha.SDK.Ctx`'s authoring functions except `apply/3`, SHALL remove any `Flezha.SDK.Channels` import the module inherits lexically, and SHALL define `init_state/1` returning `nil` at `@before_compile` when the module defines neither a `state` block nor its own `init_state/1`. `use Flezha.SDK.Operator` SHALL require `factory_id` and accept `description`, `max_duration_ms`, `run` and `durable`; under `run: {:steps, ...}` `each` SHALL accept only `:once`, `:per_input` and `:chained`, and `on_failure` only `:undo_all` and `:keep_progress`, any other value, or a setting under a parent that does not carry it, being a `CompileError` naming it. The operator macro SHALL import `Flezha.SDK.Channels`; a tasklet of either run ends its instance with `complete/2`, which `use Flezha.SDK.Tasklet` imports with the rest of `Flezha.SDK.Ctx`. An unknown option, or a missing required one, SHALL be a `CompileError` naming it. Callbacks that take arguments stay `def`s the author writes. A module that `use`s `Flezha.SDK.Tasklet`, or a short-form operator module, that writes `event_time/1` and declares `logical_clock: true` SHALL fail to compile with a `CompileError` naming both and the module the author wrote. A module written with `@behaviour` and hand-written callbacks is the reference form: it SHALL keep registering and running, with the clock and idle TTL read from its callbacks, and SHALL be documented on the Elixir SDK reference page only, never in the guides.

#### Scenario: A written option is a callback, an unwritten one is not

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "position", max_cells: 50_000`
- **THEN** it exports `name/0` returning `"position"` and `max_cells/0` returning 50_000, and `function_exported?(module, :mailbox_capacity, 0)` is false, so `FLEZHA_MAILBOX_CAPACITY` still decides its mailbox

#### Scenario: A stateless tasklet needs no init_state

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "echo"` and only `run/3`
- **THEN** it compiles, `init_state(nil)` answers `nil`, and a flow operator wiring it into a sink echoes its input

#### Scenario: A nested tasklet does not see the operator's builder

- **WHEN** a module `use`s `Flezha.SDK.Operator` and nests a module that `use`s `Flezha.SDK.Tasklet` and defines its own `input/0`
- **THEN** both compile, and the nested module's `input/0` does not conflict with `Flezha.SDK.Channels.input/2`

#### Scenario: A misspelt option fails to compile

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "p", max_entites: 10`
- **THEN** compilation fails with a `CompileError` naming `max_entites`

#### Scenario: A retired steps operator option fails to compile

- **WHEN** a module writes `use Flezha.SDK.Operator, factory_id: "w", retry_attempts: 5` or `use Flezha.SDK.Operator, factory_id: "w", wake_after_ms: 5000`
- **THEN** compilation fails with a `CompileError` naming the option, as for any unknown option

#### Scenario: The behaviour form is unchanged

- **WHEN** a module implements `Flezha.SDK.Tasklet` with `@behaviour` and hand-written callbacks, including `apply_change/2`, `idle_ttl_seconds/0` and `event_time/1`
- **THEN** it registers and runs, its changes fold through its own `apply_change/2`, its Cells are evicted after the declared seconds, and `Ctx.now/1` serves its event time

#### Scenario: complete/2 is called without its module

- **WHEN** a tasklet of a steps operator that `use`s `Flezha.SDK.Tasklet` calls `complete(ctx, "done")` in `run/3`
- **THEN** it compiles with no warning and the instance completes with the result `"done"`, exactly as a call to `Flezha.SDK.Ctx.complete/2` would

#### Scenario: The idle TTL is a whole number of seconds

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "session", idle_ttl_seconds: 1`, and another writes `idle_ttl_seconds: 0.2`
- **THEN** the first exports `idle_ttl_seconds/0` returning 1 and its idle dynamic Cells are evicted after one second, and the second fails to compile with a `CompileError` naming `idle_ttl_seconds`

#### Scenario: Two clocks do not compile

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "quiet", logical_clock: true` and defines `event_time/1`, or a short-form operator module does the same with `logical_clock: true` on its operator macro
- **THEN** compilation fails with a `CompileError` naming `event_time/1`, `logical_clock` and the module the author wrote

#### Scenario: A retired option or callback names what replaced it

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "p", clock: :logical`, or defines `event_time_of/1` beside `use Flezha.SDK.Tasklet`, or a hand-written `@behaviour Flezha.SDK.Tasklet` module exports `clock/0`
- **THEN** the first two fail to compile naming `logical_clock: true` and `event_time/1` respectively, and registering an operator that wires the third is refused with `{:RetiredCallback, name, "clock/0", replacement}`

#### Scenario: A logical clock on a changeless tasklet still compiles

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "nochange", logical_clock: true` and declares no `defchange`
- **THEN** it compiles, and registering an operator that wires it is refused with `topology.unservable_clock` naming `nochange`

### Requirement: The single-tasklet short form is three `use` options
The short form SHALL be declared with `use Flezha.SDK.Operator`, the same macro that declares a full-form operator, and SHALL NOT be declared with `use Flezha.SDK.Tasklet`, which SHALL accept none of `run`, `sink` or `input` and SHALL raise a `CompileError` naming the option and the operator macro when one is given. The operator macro SHALL accept `sink` and `input` beside the `factory_id` it already requires. How it runs is the `run` setting, written as on a full form.

A module SHALL be a short form exactly when it defines `run/3` and no `channels/1`, the one topology body every operator shares. Defining both SHALL be a `CompileError` beginning `topology.short_form_with_topology`, and defining neither SHALL be a `CompileError` naming the missing body; both SHALL be decided at `@before_compile`, where the module's definitions are known. A short form MAY write the class settings (`run`, `durable`, `max_duration_ms`, `description`), which the macro defines exactly as it does for a full form.

With `sink`, the macro SHALL generate `factory_id/0` equal to the `factory_id` option and `channels/1` wiring an entry channel named `"input"` (or the `input` value) into a generated nested tasklet module and that module to the `sink` plugin channel; with no `sink`, it SHALL wire the entry channel into the tasklet module and declare no plugin channel. The nested module SHALL implement `Flezha.SDK.Tasklet`, SHALL answer the `factory_id` verbatim from `name/0`, and SHALL delegate `run/3` to the declaring module, so the expansion is a separate tasklet and a separate operator rather than one module in both roles. Every other operator callback SHALL be left undefined so its default applies, and a steps short form SHALL be a `once` steps operator unless it writes `each:`. The module SHALL be registered as any operator is: listed under `operators:` in the child spec, or passed to `register_factory/2`.

The Elixir SDK cannot see a tasklet's output type, owes no `topology.sink_without_output`, and SHALL NOT list `shorthand_typed_output` in its manifest. A duplicate factory id SHALL be refused as `{:error, %Flezha.SDK.Error{code: "topology.duplicate_factory_id"}}` naming the id in its details.

#### Scenario: An operator module with a run body is a whole flow operator

- **WHEN** a module writes `use Flezha.SDK.Operator, factory_id: "upcase", sink: "out"` and `run/3`, and the child spec lists it under `operators:`
- **THEN** the runner declares a flow operator `upcase` with entry channel `input` and sink `out`, backed by a generated nested tasklet whose `name/0` is `upcase`

#### Scenario: An operator module with a run body is a whole steps operator

- **WHEN** a module writes `use Flezha.SDK.Operator, factory_id: "place", sink: "placed", input: "order"` and `run/3`, is listed under `operators:`, an instance is started, and one message is sent on `order`
- **THEN** `run/3` receives the message on `order`, what it emits reaches `placed`, and the instance completes

#### Scenario: A short-form steps operator declares its loop mode

- **WHEN** a module writes `use Flezha.SDK.Operator, factory_id: "chat", sink: "replies", each: :per_input` and `run/3`, and two messages reach its entry channel
- **THEN** the module exports `each/0` returning `:per_input`, `run/3` runs once per message, and the instance does not complete after the first

#### Scenario: A short form with no sink is declared

- **WHEN** a module writes `use Flezha.SDK.Operator, factory_id: "counter", input: "ticks"`, a `state do ... end` block, a `defchange` and `run/3`
- **THEN** the operator declares `ticks` into the generated tasklet module and no plugin channel, and the `defchange` recorder journals as it does on any tasklet

#### Scenario: An input needs no sink

- **WHEN** a module writes `use Flezha.SDK.Operator, factory_id: "counter", input: "ticks"` and `run/3`
- **THEN** it compiles and the entry channel is named `ticks`

#### Scenario: register reads a short form's run

- **WHEN** a module written with `use Flezha.SDK.Operator`, `run: :steps` and a `sink` is passed to `register_factory/2`
- **THEN** it is declared as a steps operator, because the macro defined `run/0` from the option, and its generated body is `channels/1` as a flow operator's is

#### Scenario: The generated tasklet is a module of its own

- **WHEN** a short form module `Upcase` is compiled
- **THEN** a nested module implementing `Flezha.SDK.Tasklet` exists whose `run/3` delegates to `Upcase.run/3`, and `Upcase` itself implements no tasklet callback

#### Scenario: A run body beside a channels body does not compile

- **WHEN** a module writes `use Flezha.SDK.Operator, factory_id: "upcase", sink: "out"` and defines both `run/3` and `channels/1`
- **THEN** compilation fails with a `CompileError` whose message begins `topology.short_form_with_topology`

#### Scenario: A sink on a tasklet does not compile

- **WHEN** a module writes `use Flezha.SDK.Tasklet, name: "upcase", sink: "out"`
- **THEN** compilation fails with a `CompileError` naming `sink` and `Flezha.SDK.Operator`

#### Scenario: An operator with neither body does not compile

- **WHEN** a module writes `use Flezha.SDK.Operator, factory_id: "upcase"` and defines neither `run/3` nor `channels/1`
- **THEN** compilation fails naming the missing body

#### Scenario: The behaviour form keeps working

- **WHEN** a module writes `@behaviour Flezha.SDK.Operator` with hand-written `factory_id/0` and `channels/1`
- **THEN** it registers and runs unchanged, and owes no short-form refusal

### Requirement: The Elixir SDK announces each class's run and refuses a misplaced setting

The handshake's factory entries and every `FactoryDecl` the SDK ships in `UpdateFactories` SHALL carry `run`, `"flow"` or `"steps"`, with `each` and `on_failure` for a steps class, read from the module's `run/0`, for a class registered before start and for one defined after it alike. `use Flezha.SDK.Operator` SHALL refuse a setting written under a parent that does not carry it with a `CompileError` naming the setting and the parent, and a value outside its choice naming the value; a module that writes `run/0` or `durable/0` by hand SHALL meet the same check at `register_factory/2`, as `topology.unknown_field` or `topology.invalid_setting`, carried in the same `{:error, %Flezha.SDK.Error{}}` as its other refusals are. `Flezha.SDK.Declarations.for/1` SHALL declare no `effectively_once` for a steps operator, because none can be written, and no `wake_after_ms` for any class. A steps stage's Cells SHALL be started with the same timer policy and clock a flow operator's are, so `Flezha.SDK.Ctx.timer_after/2`, `timer_at_event_time/2`, `cancel_processing_time_timer/2` and `cancel_event_time_timer/2` answer inside a steps tasklet exactly as inside a flow tasklet, and `on_timer/4` runs on a firing in both runs.

#### Scenario: The handshake carries the run

- **WHEN** a runner starts with a flow operator module and a steps operator module under `operators:`
- **THEN** its handshake's `factories` entries carry `"run" => "flow"` and `"run" => "steps"` respectively, and a module defined after start ships an `UpdateFactories` whose `FactoryDecl` carries the same

#### Scenario: The tier under a steps run does not compile

- **WHEN** a module writes `run: {:steps, exactly: :effectively_once}` in its `use`
- **THEN** compilation fails with a `CompileError` naming `exactly` and `steps`

#### Scenario: A hand-written run outside its set is refused

- **WHEN** a module declaring `@behaviour Flezha.SDK.Operator` defines `run/0` returning `{:steps, each: :forever}`
- **THEN** `register_factory/2` answers `{:error, %Flezha.SDK.Error{code: "topology.invalid_setting"}}` whose details name `each` and `forever`

#### Scenario: A timer inside a steps tasklet fires

- **WHEN** a tasklet of a running `:per_input` steps operator calls `Flezha.SDK.Ctx.timer_after(ctx, 300)` in `run/3`
- **THEN** the call answers `{:ok, at}`, the same call inside a flow tasklet answers the same, and `on_timer/4` runs on that Cell with `fired_at == at`, opening a pass at that tasklet

### Requirement: A `state` block declares defaults, and `defchange` is the one way to change state
`use Flezha.SDK.Tasklet` SHALL make `state do … end` and `defchange` available. Inside `state`, `field name, type, default: value` SHALL declare a field, `type` being one of `:integer`, `:float`, `:string`, `:boolean`, `:list`, `:map` or `:term`. The block SHALL generate `init_state/1` returning the map of field defaults, overridable by the author, and `state_from_json/1` reading each field's string key and falling back to its default. It SHALL generate no change: no `apply_change/2` clause and no recorder. `defchange name(state, args…) do body end` SHALL be the one way to change state in the `use` form, in a module with or without a `state` block: it SHALL generate the clause `apply_change(state, {:name, args…})` evaluating `body`, and the recorder `name(state, ctx, args…)`, which records the change through `Flezha.SDK.Ctx.apply/3` and answers the new state, so a mutation spanning several fields is one journal record. A module with at least one `defchange` SHALL export `apply_change/2` and be changeful; a module with none SHALL export no `apply_change/2`, whether or not it has a `state` block. A module that `use`s `Flezha.SDK.Tasklet`, or a short-form operator module, that defines `apply_change/2` itself SHALL fail to compile with a `CompileError` naming `apply_change/2` and directing the author to `defchange`; the delegation the short form generates in its nested tasklet module is not the author's definition and SHALL NOT be refused. A module that has a `state` block and also defines `state_from_json/1` itself SHALL fail to compile, naming the function and the block.

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

### Requirement: The Elixir runner reports an operator instance that exits abnormally

The runner SHALL treat the exit of the process registered for an operator id, a flow
instance or a steps driver, as the end of that instance: it SHALL remove the id from its operator
map and its registry at once, so that no input is routed to the dead process and a later
`AddOperator` for the id starts a fresh one. The exit of a process that is not the one registered
for any id SHALL change nothing.

An exit is clean when its reason is `:normal`, `:shutdown` or `{:shutdown, _}`, and abnormal
otherwise. On an abnormal exit the runner SHALL send `OperatorExited { operator_id, reason }` on its
control stream, `reason` being a bounded rendering of the exit reason, written with the `:control`
class. It SHALL keep the report until an `AddOperator` or a `RemOperator` names that id, and SHALL
send every report it keeps again after each `ConnectSocket` it answers successfully, because a
control frame is never held and a report written while no control stream is bound is written
nowhere. A clean exit SHALL send nothing, and a runner that is disconnecting or stopped SHALL send
nothing.

#### Scenario: A killed Operator is dropped and reported

- **GIVEN** a flow instance `X` running on an Elixir runner
- **WHEN** its Operator process is killed
- **THEN** the runner SHALL remove `X` from its operator map and its registry, an input a primary
  stream then reads for `X` SHALL reach no process, and the controller SHALL receive
  `OperatorExited` naming `X` with a non-empty reason

#### Scenario: A clean exit is not reported

- **WHEN** the controller sends `RemOperator` for `X` and its Operator exits `:normal`
- **THEN** the runner SHALL send no `OperatorExited`

#### Scenario: A steps driver that dies is reported

- **GIVEN** a steps instance `W` running on an Elixir runner
- **WHEN** its driver process exits abnormally before it ships `OperatorCompleted`
- **THEN** the runner SHALL remove `W` and send `OperatorExited` naming `W`

#### Scenario: A report outlives a lost control stream

- **GIVEN** an instance `X` whose Operator died while the runner had no control stream bound
- **WHEN** the runner reconnects and answers the controller's `ConnectSocket`
- **THEN** it SHALL send `OperatorExited` naming `X`, and SHALL NOT send it again after the
  controller's `AddOperator` for `X` has arrived

#### Scenario: The exit of a replaced process changes nothing

- **GIVEN** an `AddOperator` for `X` that registered a new Operator while the previous one was
  still exiting
- **WHEN** the previous Operator's exit reaches the runner
- **THEN** `X` SHALL stay registered to the new Operator and no `OperatorExited` SHALL be sent

#### Scenario: Inputs a dead Cell held run again after the rebind

- **GIVEN** an in-node Elixir runner and a flow instance on the effectively-once tier whose
  changeful Cell for key `k` has finished the input at offset 9, is running the input at offset
  10 and has 11 and 12 pushed behind it, while the Cell for key `j` has finished and recorded
  offset 13
- **WHEN** the Cell for `k` is killed
- **THEN** the controller SHALL rebind the instance, the inputs at offsets 10, 11 and 12 SHALL each
  run exactly once on the new Cell for `k` in that order, starting from the state its offset-9
  record left, the input at offset 13 SHALL NOT run again, and the confirmed floor SHALL reach 13

