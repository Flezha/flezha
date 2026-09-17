# elixir-sdk Specification

## Purpose
TBD - created by archiving change elixir-sdk-native-transport. Update Purpose after archive.
## Requirements
### Requirement: The Elixir runner entry point mirrors the cross-SDK runner-as-handle surface
The SDK SHALL expose `Flezha.SDK.create_runner/2` — `create_runner(namespace, opts)` with `transport: :native | :uds | :quic` defaulting to `:native` — returning a runner handle carrying its own declared factory set (empty at creation; no connection, no singleton claim, no process spawn), with `register_factory/2` (an operative module of either kind, the kind read from the callbacks it exports: `channels/1` makes a stream, `procedures/1` or `run/2` a workflow, both answer `{:error, {:AmbiguousOperativeKind, module}}` and neither `{:error, {:UnknownOperativeKind, module}}`) and its inverse `unregister_factory/2`, and the lifecycle verbs `start/1`, `await_termination/1`, `run/1` (exactly `start/1` then `await_termination/1`, never an independent implementation), `close/1`, and `namespace/1`. The process-wide active-runner slot SHALL be claimed atomically inside `start/1` — any number of unstarted handles may exist; the loser receives `{:error, {:already_running, ns}}`; double-start on one handle errors; closing a never-started handle is a no-op. Zero-operative start SHALL be legal. Authoring and connection failures SHALL be named error tuples returned at the causing call site (mirroring the Rust `FlezhaError` variants) — never a raise. `Flezha.SDK.child_spec/1` SHALL accept `namespace:`, `operatives:` (a list of operative modules of either kind), and `transport:`, and wrap exactly the create → register → start → await sequence for the host application's supervision tree — it MUST NOT introduce a second lifecycle path. On start the SDK SHALL connect over the selected transport with a handshake carrying the namespace, the declared factories (id + description), and its stable identity, then handle `AssignRunnerId` (storing the session token) and `ConnectSocket` (establishing N primary + N secondary streams through the mode's transport).

#### Scenario: SDK boots and binds
- **WHEN** a host app starts `{Flezha.SDK, namespace: "default", operatives: [MyOperative]}` on a node running the Controller
- **THEN** FleetManager lists one active runner advertising `MyOperative`'s factory id, and the runner's primary/secondary stream channels are bound

#### Scenario: Handle form is equivalent to the child spec form
- **WHEN** a caller runs `Flezha.SDK.create_runner("default") |> Flezha.SDK.register_factory(MyOperative) |> Flezha.SDK.start()`
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
The flezha application SHALL support a runner-only boot: with `:controller_enabled` set `false` (env `FLEZHA_CONTROLLER_ENABLED`, default `true`), `Flezha.Application.start/2` SHALL NOT start the controller supervision tree — no transport servers, no HubManager/ProcessHub, no convergence, no state-path migration, no boot-ready gate — leaving the host application to run only SDK processes via `Flezha.SDK.child_spec/1`. A runner of its own needs none of this: it depends on the `flezha_sdk` package alone, which starts no controller. Because a dependency's node never evaluates flezha's `config/*.exs`, the host application SHALL be able to set `config :flezha, controller_enabled: false` itself, and the SDK SHALL NOT depend on any other `:flezha` application-env key being present. The default (`true`) SHALL leave every existing deployment byte-identical.

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
Each runner handle SHALL own its declared factory set, starting empty. `register_factory/2` and `unregister_factory/2` SHALL be legal both before and after `start/1`; a post-start change SHALL ship `UpdateFactories` (tag 33, full current set, fire-and-forget, no ack) over the control channel, and a reconnect handshake SHALL re-serialize the same per-runner set as the resync of record. Removing a type with live instances SHALL be refused with `{:error, {:operative_type_in_use, factory_id, live_count}}`; removing an undeclared type SHALL be refused naming the id; defining a duplicate factory id SHALL be refused.

#### Scenario: Post-start definition reaches the controller
- **WHEN** `register_factory/2` is called on a started runner
- **THEN** an `UpdateFactories` frame carrying the full declared set is sent and the controller's stored factory set for that runner converges to it (full-set replacement)

#### Scenario: Removing a type with live instances is refused
- **WHEN** `unregister_factory/2` targets a type with at least one live instance on this runner
- **THEN** the call returns the in-use error and the declared set is unchanged

### Requirement: Operative and Procedure behaviours mirror the current cross-SDK model
The SDK SHALL define `Flezha.SDK.StreamOperative` (callbacks: `factory_id/0`, `channels/1` receiving the graph builder; optional `description/0`, `persistence_enabled/0`) and `Flezha.SDK.Procedure` (mandatory callbacks: `name/0`, `init_state/1` receiving the entity key or nil, `run/3` receiving input, state, and a ctx, returning `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`; optional callbacks with defaults: `entity_keys/0` → `[]`, `entities_for/1`, `mailbox_capacity/0` → 1024 with env override `FLEZHA_MAILBOX_CAPACITY` and range 1..=1_048_576, `max_entities/0` → 100_000 and ENFORCED on a dynamic node — at the cap a new key's input is dropped without an Entity and counted, an existing key's input is unaffected — `entity_idle_ttl_ms/0` → `nil` (no eviction), `schema_version/0` → 1, `min_readable_schema_version/0` → the schema version, `migrate_state/2` and `migrate_change/2` → refuse, `apply_change/2`, `compact_after_bytes/0`, `state_from_json/1`, `clock/0` → `:wall` — the declared clock `now/1` serves: `:wall` (epoch milliseconds at dispatch entry), `:logical` (the Entity's durable change seq, `terminal + 1` after a restore) or `:event_time` (what `event_time_of/1` extracts from the input) — `event_time_of/1`, `on_timer/4` receiving the scheduled timestamp, the kind, the state and a ctx and returning what `run/3` returns, and `max_timers_per_entity/0` → 10_000). The ctx SHALL provide `emit/2`, `emit_with_meta/3` (refusing reserved `flz.`-prefixed metadata keys), `emit_keyed/3` and `emit_keyed_with_meta/4` (the output's natural key on a keyed sink), `apply/3`, `now/1` (the declared clock's value, evaluated once per invocation; inside `on_timer/4` it equals the scheduled timestamp under every clock), `entity_key/1`, the timer surface `timer_after/2`, `timer_at_event_time/2`, `cancel_processing_time_timer/2` and `cancel_event_time_timer/2` (the `entity_timers` requirement below), `checkpoint/1` (closes the open checkpoint interval of a class declaring checkpoint acknowledgement — every input folded in since the last close, this one included, is acknowledged by the record that closes it; a no-op on any other class, as the reference's is) and `stable_id/1` (thirty-two lowercase hex characters of the frozen FNV-1a-128 over the Entity's wire identity, the big-endian change seq the invocation started at and a big-endian per-invocation ordinal — the cross-language fixture every SDK pins, so the n-th call of a replayed invocation equals the n-th call of the original). On a node whose sink is declared keyed, a keyless emit from a Single Entity SHALL be refused at call time with `{:error, {:key_required_on_keyed_channel, channel}}` and nothing emitted; `emit_keyed` on a node with no keyed sink SHALL be refused with `{:error, {:key_not_allowed_on_non_keyed_channel, channel}}`. Emits SHALL accumulate per invocation, with their metadata and key, and flush only on `{:ok, _}` — a failing invocation discards its partial emits. `AddOperative` SHALL instantiate the operative's topology with one process per Entity; `RemOperative` SHALL stop it. Inputs to the same Entity SHALL be processed in arrival order; distinct Entities MAY process concurrently.

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

#### Scenario: The cap on dynamic Entities binds
- **GIVEN** a dynamic node whose Procedure declares `max_entities/0` as 2
- **WHEN** inputs for three distinct keys arrive
- **THEN** two Entities exist, the third key's input is dropped without an Entity and counted, and a later input for either live key is dispatched normally

#### Scenario: A keyless emit on a keyed sink is refused in a Single Entity
- **WHEN** a Single Entity whose node's sink is declared `keyed/1` calls `Ctx.emit/2`
- **THEN** the call answers `{:error, {:key_required_on_keyed_channel, channel}}` and nothing is emitted, while the same call in a dynamic Entity emits under that Entity's own key

#### Scenario: The declared clock is what now/1 serves
- **WHEN** a Procedure declares `clock/0` as `:logical` and its Entity is at change seq 4 when an input is dispatched
- **THEN** `Ctx.now/1` inside `run/3` is 4; under `:event_time` it is `event_time_of/1` of that input; under `:wall` (the default) it is the epoch millisecond read once at dispatch entry

#### Scenario: A firing reaches on_timer/4 with the scheduled timestamp
- **WHEN** a processing-time timer scheduled at 31000 fires on an Entity whose Procedure exports `on_timer/4`
- **THEN** `on_timer(31000, :processing_time, state, ctx)` runs with `Ctx.now(ctx) == 31000`, a change it applies through `Ctx.apply/3` is journaled, and a value it emits leaves exactly as one emitted from `run/3`

#### Scenario: stable_id is the cross-language fixture and checkpoint closes the interval
- **WHEN** a ctx built over the identity bytes `instance/op/key` at change seq 42 calls `Ctx.stable_id/1` twice, and a ctx on a checkpoint-acknowledging class calls `Ctx.checkpoint/1`
- **THEN** the two ids are `bbf81fd4955afd1724203700a240e822` then `bbf81fd4965afd1724203700a240e95d` — the values the Rust and Python SDKs pin — and the Entity ships the interval's closing record at that invocation's end

### Requirement: The graph builder carries the v1 subset of the producer-first surface
`channels/1` SHALL author topologies with: `input/2` and `input_with/3` (bytes passthrough, String UTF-8, custom parser; a parse failure rides the entry channel's retry→park ladder), `into/2` and `into_named/3` (unnamed mentions of one Procedure module unify into one node — fan-in; node-identity collisions refuse), edge transforms `map/2`, `filter/2`, `flat_map/2` and edge keying `key_by/2` (fused structurally into one composed closure per consumer edge — the runtime applies at most one transform + one key per edge and never observes a chain), `named/2`, `keyed/1` (declares output identity on the sink the handle reaches — the reference's `.keyed()`; on an entry handle, or on a handle that reaches `into/2` before any sink, registration refuses with `KeyedOnEntry` / `KeyedInterProcedureChannel`), `sink/2` and `sink_with/3` (returning the stream handle — hybrid by default; default serialization bytes/UTF-8/JSON), `on_full/2`, `retry_max/2`, `retry_backoff_ms/2`, and fan-out by handle reuse. Every plugin frame SHALL carry `channel` = the producing node's identity — the procedure's `name/0`, or the name `named/2` gave the node — never the plugin's name, so a `named/2` survives the plugin terminator and a forward target resolves its entry channel by the name the author declared; an entry routed straight to a sink carries the entry's name. User metadata from `emit_with_meta/3` SHALL ride the frame. Entity mode SHALL be derived from the wiring exactly as in the Rust SDK: non-empty `entity_keys/0` → Static; a keyed edge into a keyless procedure → Dynamic; otherwise Single. Registration SHALL accumulate refusals into `{:error, {:invalid_topology, issues}}` (never a raise) using the shared issue names, including `KeyByOnUnkeyableTarget`, `UnkeyedEdgeIntoDynamicProcedure`, `TransformAfterKeyBy`, `DuplicateChannel`, `Cycle`, `UnreachableProcedure`, `NodeIdentityCollision`, `MailboxCapacityZero`/`TooLarge`, `RetryMaxZero`, `DuplicatePluginSink`, `KeyedOnEntry`, `KeyedInterProcedureChannel`, `UnservableClock` (a `:logical` clock on a Procedure without `apply_change/2` — the reference's build refusal), `EventTimeClockWithoutExtractor` (an `:event_time` clock with no `event_time_of/1`), `UnknownClock` (a `clock/0` outside the three), and the tier's — `CheckpointAckWithoutTier`, `CheckpointEveryZero`, `CheckpointMaxDelayZero`, `UnkeyedChannelUnderEffectivelyOnce` (naming the sink's channel) and `DroppingPolicyUnderEffectivelyOnce` (naming the node's inbound channel and the policy) — the reference's names for the same contradictions. Edge type mismatch is the documented per-language asymmetry: with no type tokens, a mis-typed payload surfaces at dispatch and rides the retry→park ladder. The tier declarations SHALL exist on the builder token: `effectively_once/1`, `checkpoint_ack/1`, `checkpoint_every/2` and `outputs_at_checkpoint/2`, each answering the token so it pipes, the three checkpoint options routing through one private policy amendment that sets the checkpoint granularity itself (idempotent, composing in any order, none clearing another's policy) and refused without `effectively_once/1` — the reference's `checkpoint_policy`. The correction declarations SHALL exist on the same token: `correction_window/2` and `beyond_window_policy/2` (`:process_as_arrival` — the default — or `:drop`), routing through one private policy amendment so they compose in either order and neither clears the other, which makes `beyond_window_policy/2` alone a zero-window declaration and therefore a refusal, exactly as `builder/mod.rs`'s pair does. Registration SHALL refuse with the reference's names: `CorrectionWithoutTier` (a window without `effectively_once/1`), `CorrectionWithoutEventTime` (no Procedure in the class declares an `:event_time` clock, naming the first Procedure and the clock it declares) and `CorrectionWindowZero` (`correction_window(0)`, or a `beyond_window_policy/2` with no window). A shedding overflow policy under a declared window SHALL be refused by the tier's existing `DroppingPolicyUnderEffectivelyOnce`, because a window requires the tier — no second check is added for a program the first already refuses. `merge/1` SHALL return a `%Flezha.SDK.Merge{}` handle that the same `Flezha.SDK.Channels` functions carry forward: `from/3` (a source stream and its mapping), `key_by/2` (declared once, on the merge) and `into/2` / `into_named/3`, each dispatching on the handle it receives, so one import of `Flezha.SDK.Channels` writes a pipe with a merge and `Flezha.SDK.Merge` exports none of the four, with the `multi-input-procedures` semantics: each mapping composes after that source's own transforms, the key is applied to the mapped value on every source's edge, and the merged node's output stream chains like any other; `Topology.build/1` judges the merge groups. `route_latest/4` and `route_latest_named/5` SHALL expand to a merge into an SDK-packaged single-entity router procedure with journaled `{document, version, delivered}` state, as the reference's do. `branch` SHALL NOT exist in v1 (fan-out is handle reuse); calling it fails registration with an error naming the module. `sealed/1` SHALL NOT exist: `sdk-graph-authoring` gives `sealed` only to the SDKs whose type system can enforce it, and an identity that seals nothing is a misleading surface.

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
- **WHEN** `channels/1` calls `branch`, the one builder surface v1 does not author
- **THEN** registration fails with an error naming the operative module — never a silent no-op, never a crash of the runner

#### Scenario: Two channels merge into one procedure
- **WHEN** an operative declares `c |> merge() |> from(ticks, &{:tick, &1}) |> from(quotes, &{:quote, &1}) |> key_by(&symbol_of/1) |> into(Detector)`
- **THEN** registration succeeds with one Detector node fed by two consumer edges, each edge's fused closure applying that source's mapping and then the merge's key, and a value from either entry reaches `run/3` as the mapped term keyed by its symbol

#### Scenario: A merge is judged by the reference names
- **WHEN** a merge declares one source, or the same stream twice, or a source whose channel is already wired into the same node by `into/2`
- **THEN** registration returns `{:error, {:invalid_topology, issues}}` naming `MergeTooFewSources`, `DuplicateMergeSource` and `EdgeAlreadyWired` respectively, with the reference sentences

#### Scenario: The router delivers on a key's own traffic
- **WHEN** `route_latest(c, config, trades, &(&1.symbol))` is declared and a unit test feeds a document, trades for `"A"` and `"B"`, the same document, then a trade for `"A"`
- **THEN** exactly one `{"A", document}` and one `{"B", document}` are emitted, the repeated document is inert, and the node's identity is `route_latest`

#### Scenario: The frame carries the node's channel name, and `named/2` survives the sink
- **WHEN** a node named `"e"` is wired `|> named("e") |> sink("out")` and its Entity emits
- **THEN** the plugin frame's metadata carries `channel: "e"`, and an unnamed node's frame carries its procedure's `name/0`

#### Scenario: A keyed handle with no plugin sink is refused
- **WHEN** `channels/1` declares `keyed/1` on a handle and then routes it with `into/2` before any sink
- **THEN** registration returns `{:error, {:invalid_topology, [{:KeyedInterProcedureChannel, _}]}}`

#### Scenario: A clock the runtime cannot serve is refused at registration
- **WHEN** a Procedure without `apply_change/2` declares `clock/0` as `:logical`, or a Procedure declares `:event_time` without exporting `event_time_of/1`
- **THEN** registration returns `{:error, {:invalid_topology, [{:UnservableClock, name}]}}` for the first and `[{:EventTimeClockWithoutExtractor, name}]` for the second, naming the procedure, and no Entity is started

#### Scenario: The tier declarations register, compose and refuse by the reference's names
- **WHEN** a class declares `effectively_once/1` with a keyed sink; another declares `checkpoint_ack/1`, `checkpoint_every/2` and `outputs_at_checkpoint/2` in any order behind it; a third declares `checkpoint_ack/1` alone; a fourth declares the tier over an unkeyed sink; a fifth declares the tier over an entry with `on_full(:drop_newest)`
- **THEN** the first two register with the tier and the checkpoint policy carrying all three options, and the last three are refused with `CheckpointAckWithoutTier`, `UnkeyedChannelUnderEffectivelyOnce` naming the sink's channel, and `DroppingPolicyUnderEffectivelyOnce` naming the channel and `drop_newest`

#### Scenario: The correction declarations register, compose and refuse by the reference's names
- **WHEN** a class declares `effectively_once/1`, `correction_window/2` and a Procedure with an `:event_time` clock; a second declares them in the opposite order with `beyond_window_policy/2` between them; a third declares `correction_window/2` without the tier; a fourth declares it over a `:wall`-clocked Procedure; a fifth declares `correction_window(0)` and a sixth only `beyond_window_policy(:drop)`
- **THEN** the first two register with the window and the beyond-window policy carried whole whatever the order, and the last four are refused with `CorrectionWithoutTier`, `CorrectionWithoutEventTime` naming the Procedure and its clock, and `CorrectionWindowZero` twice

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
A failing `run/3` — error tuple, raise, or exit — SHALL be retried up to the input channel's `retry_max` (default 3 total attempts) with exponential backoff plus jitter from `retry_backoff_ms` (default 50 ms base, capped at 5 s); `{:error, {:non_retryable, reason}}` SHALL park immediately; a raise/exit is retryable by default. Outside the effectively-once tier, on exhaustion the SDK SHALL ship a `park_payload` frame whose body is the postcard `ParkBody` byte-compatible with the pinned cross-language fixture (`park_id`, `origin_offset` absent for the tier-off runner, and a `DlqEntryBody` of channel, error, attempts, payload — payload bytes present for binary/String inputs), SHALL hold the Entity, and SHALL resend every 2 s until the controller's `ParkPayloadAck` (sent only after the DLQ store fsync) matches the `park_id` — the existing controller park path SHALL require zero changes, in both deployment modes. The `ParkBody` encoder SHALL live in `Flezha.SDK.Wire` beside its decoder, sharing the module's postcard helpers. Under the tier the park SHALL ride the invocation's effect record as its `dlq` entry (channel, error, attempts, payload) beside the poison input's consumed offset, closing any open checkpoint interval, and SHALL hold nothing — no `park_payload` frame, no ack wait: the next queued input dispatches at once, the reference's tier-on park. A parked firing's removal SHALL ride the same record's timer ops.

#### Scenario: Transient failure recovers within the budget
- **WHEN** an invocation fails twice and succeeds on the third attempt
- **THEN** processing continues, nothing is parked, and retry telemetry records the attempts

#### Scenario: Poison payload parks durably and the Entity resumes
- **WHEN** an invocation fails on every attempt of the budget
- **THEN** a `park_payload` ships, the DLQ store persists the entry under the operative class and procedure, the ack releases the Entity, and the next input dispatches normally

#### Scenario: Park frame is byte-compatible
- **WHEN** the SDK's encoded `ParkBody` is compared against the pinned cross-language fixture bytes
- **THEN** they are identical

#### Scenario: A tier-on park rides the effect record and holds nothing
- **WHEN** an invocation on a class declaring `effectively_once/1` fails on every attempt of the budget at consumed offset 7, with an input queued behind it
- **THEN** one `entity_effect_record` ships carrying no change, the `dlq` entry with the error and the payload bytes, and consumed offset 7; no `park_payload` frame ships; and the queued input dispatches without any ack

### Requirement: Partitioned placement is guarded with the controller's own hash
The SDK SHALL decode `AddOperative`'s `partition_index`/`partition_count`/`key_group_from`/`key_group_to` (zero values = unpartitioned; the shipped range is authoritative, never re-derived). When `partition_count >= 2`: a dynamically computed entity key SHALL be admitted against the owned range **before** any Entity state is created — a miss parks the input with the named contract error; statically declared Entities outside the range SHALL be silently idle (`owns` filter); and a topology with no keyed entry-fed procedure SHALL refuse the bind with `PartitionsWithoutKeyedProcedure` naming `key_by`. When `partition_count < 2` the guard SHALL cost no more than one comparison. The key-group function SHALL be `Flezha.SDK.KeyGroup.group_of/1`, the pinned FNV-1a-128 the controller's range assignment calls too, verified against the shared fixture vectors.

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
The SDK SHALL echo `control_ping` on the control channel, answer `Ping` with a `PingResponse` carrying the nonce, and emit `data_heartbeat` frames (empty body) every `FLEZHA_DATA_HEARTBEAT_MS` (default 10 s), jittered into `[base/2, base]`, on each bound secondary stream, with metadata carrying **exactly the key set the shared core's heartbeat builds** — not a subset of it. The keys are `dispatches` (incremented only on completed dispatches — the key the `runner_pool_wedged` verdict requires), `blocked_parks`, `credit_parked` — the count of credit-park wait iterations, so a runner parked on an exhausted window reads as parked rather than wedged beside a frozen `dispatches` — `drops_newest` and `drops_oldest`, `inflight` and `inflight_oldest_ms`, `pending_pushes` and `pending_push_oldest_ms`, `restore_discarded_inputs` (inputs a rebind threw away, the operator's only handle on the size of that loss), and `timers_armed` (the processing-time timers the runner's Entities hold armed on the BEAM's timer service) and `timers_fired` (firings completed, monotonic): the two timer keys every other SDK's heartbeat carries. The contract SHALL hold identically in both deployment modes.

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
- **WHEN** an Entity is parked on an exhausted credit window across two heartbeats
- **THEN** the second heartbeat's `credit_parked` is greater than the first's while `dispatches` stands still

#### Scenario: Timer occupancy and firings ride the heartbeat
- **WHEN** an Entity registers two processing-time timers and later one of them fires
- **THEN** a heartbeat after the registrations carries `timers_armed` = 2, and one after the firing carries `timers_armed` = 1 and `timers_fired` = 1

#### Scenario: Discarded inputs ride the heartbeat
- **WHEN** a rebind discards inputs the runner had not yet consumed
- **THEN** the next heartbeat carries `restore_discarded_inputs` advanced by the number discarded

#### Scenario: The key set equals the shared core's
- **WHEN** this SDK's heartbeat metadata keys are compared with the key set `flz_core`'s heartbeat builds
- **THEN** the two sets SHALL be equal, and a key present in one and not the other SHALL fail the cross-SDK gate

### Requirement: Declarations and deferred commands follow the scope matrix
`AddOperativeResponse.metadata` SHALL be empty for a class declaring nothing (the legal "declared nothing" encoding), except that a keyless topology SHALL declare `keyed: "false"` so the controller's partitioned-start preflight works, a class whose smallest `compact_after_bytes/0` differs from the 8 MiB default SHALL declare `compact_after_bytes` with that value, a class declaring `effectively_once/1` SHALL declare `effectively_once: "true"`, and one declaring checkpoint acknowledgement SHALL declare `ack_granularity: "checkpoint"` — and a class declaring `correction_window/2` SHALL declare `correction_window_ms` with the declared milliseconds and `beyond_window_policy` with `process_as_arrival` or `drop` — both keys whenever a window is declared, as the reference ships them, so an undeclared policy reaches the controller as the default rather than as an absence — the four keys the reference ships and the goldens `tier_store.json`, `checkpoint_batched_store.json` and `candle.json` pin; the cadence, the held outputs and the delay stay SDK-side and reach no metadata. The persistence-family commands — `RestoreInitState`, `CompactEntity`, `EntityState`, `EntityApplyState`, `MigrationSerialization` / `MigrationDeserialization`, `CreditWindowInit`, `CreditGrant` — SHALL be answered for real, by the SDK's own re-expression of the kernel's rules (the `state_persistence` requirement below); `EntityMailboxStatus` SHALL be tolerated with a debug log, as the reference tolerates a pre-credit controller. `WorkflowStop` SHALL be tolerated with a warn (no workflow factory is ever advertised). `RunnerDisconnect` SHALL terminate the connection cleanly: a terminal one runs the graceful shutdown and ends the VM with the terminal status; a non-terminal one SHALL stop the runner in place — the reference's `Stopped` — leaving the controller to take the session down, reconnecting into nothing and keeping the process, so a `:permanent` supervisor has nothing to restart into a second registration.

#### Scenario: Declaration-free response is empty
- **WHEN** an operative with a keyed topology and no declarations binds
- **THEN** its `AddOperativeResponse` carries zero metadata bytes

#### Scenario: Keyless topology declares its keyedness
- **WHEN** an operative whose topology has no keyed procedure binds
- **THEN** its response metadata carries `keyed: "false"`, and a later partitioned start of that class is refused at the controller before any member starts

#### Scenario: A non-default compaction threshold is declared
- **WHEN** a class's only changeful Procedure declares `compact_after_bytes/0` as `1_048_576`
- **THEN** its response metadata carries `compact_after_bytes: "1048576"`, and a class at the default carries no such key

#### Scenario: A non-terminal RunnerDisconnect stops the runner in place
- **GIVEN** an Elixir runner under a `:permanent` supervisor, registered with the controller
- **WHEN** the controller sends a non-terminal `RunnerDisconnect`
- **THEN** the runner acks, the controller's session for it ends, the same process stays alive with no reconnect attempt, and the namespace lists no runner afterwards

#### Scenario: The tier keys match the goldens
- **WHEN** `tier_store` and `checkpoint_batched_store` bind on an Elixir runner
- **THEN** the first's response metadata is exactly `effectively_once: "true", keyed: "false"` and the second's exactly `ack_granularity: "checkpoint", effectively_once: "true", keyed: "false"` — the committed goldens, key set and values

#### Scenario: The correction keys match the golden
- **WHEN** `candle` — a class declaring `effectively_once/1`, `correction_window(600_000)` and no beyond-window policy — binds on an Elixir runner
- **THEN** its response metadata is exactly `beyond_window_policy: "process_as_arrival", correction_window_ms: "600000", effectively_once: "true", keyed: "false"` — the committed golden `test/fixtures/declarations/candle.json`, key set and values

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

Every call the Elixir SDK makes into one of its own processes SHALL wait a bound named by a row of the one configuration table and SHALL answer a target that does not reply with a refusal the caller can act on, never by exiting that caller. A runner runs inside a host application, so a wedged Entity, Operative, RunnerServer or local run must not take a process the host supervises with it.

Three calls SHALL wait with no bound at all, because there the wait is the protocol and the caller is one of the runner's own processes: the `:drop_nothing` enqueue, whose guarantee is that an input is never dropped and never refused; the credit window's acquire, which parks a sender until a grant covers its frame; and the Entity rebind, whose caller replays that Entity's outputs after it returns and so must not proceed before its records reach the wire. None SHALL be given a bound, because a caller that abandoned one would lose an input, ship past a window the Controller is metering, or release outputs ahead of their own records.

A refusal SHALL be phrased in the words the surface it reaches already uses: the live-read and restore commands SHALL answer the `entity_busy` token the Controller matches as a prefix.

A refusal is not a cancellation — the request stays in the target's mailbox and the work still runs — so a call whose duration grows with the state or the payload SHALL take a bound derived from the Controller surface that awaits its response, never the flat control bound. The Entity fold, the snapshot and the state apply SHALL each sit strictly inside the restore await, the compaction bound and the control reply bound respectively, so that the SDK never reports a failure while the Controller is still waiting for the answer.

The bounded call SHALL be the package's own `Flezha.SDK.SafeCall`, which the controller imports rather than holding a second copy. Exactly one other SDK file, `Flezha.SDK.Call`, SHALL name it, and the rest of the SDK asks that one.

#### Scenario: A suspended target refuses its caller

- **GIVEN** one of the runner's own processes suspended so that it holds a call and never replies
- **WHEN** the SDK calls into it
- **THEN** the caller SHALL read a refusal and stay alive, rather than exiting or waiting for ever

#### Scenario: The protocol waits still wait

- **GIVEN** a suspended Entity and a suspended credit window
- **WHEN** an input is enqueued under `:drop_nothing`, a record acquires credit, and the Entity is rebound
- **THEN** all three callers SHALL still be waiting, and no other call the SDK makes SHALL be written without a bound

#### Scenario: The bounded call is the package's own

- **WHEN** the SDK package compiles with no controller code on the path
- **THEN** `Flezha.SDK.SafeCall` SHALL resolve inside it, and no SDK file but `Flezha.SDK.Call` SHALL call it

### Requirement: Bench harness covers the elixir runtime on all three transports
`mix bench --runtime elixir` SHALL run the `regression`, `entity`, and `dynamic` shapes over the `native` transport (in-process SDK start; one runner, because the runner slot is process-wide) and over `uds`/`quic` (spawning the `test/apps/elixir_runner` app via the same fleet mechanism as the rust/java runners), registering factories `do_nothing`, `entity_stress`, and `dynamic_entity` with the rust_runner topologies, producing the same `Throughput: <n> messages/second` output measured by the unchanged controller-side code. The transport SHALL be selected exactly as today — `scripts/bench.sh -t native|uds|quic` exporting `FLEZHA_TRANSPORT`, read by `mix bench` from the application env — with no new `mix bench` flag. The harness SHALL reject `--runtime rust|java` with the native transport; `scripts/bench.sh -t` SHALL accept `native`. The recorded results SHALL include the same-runtime uds-vs-native comparison (the transport measurement) and the elixir-vs-rust comparison on uds (the runtime measurement). `mix bench.roundtrip --runtime elixir` SHALL run the `identity` and `keyed` workloads at durability `off` through the same fleet mechanism, registering `bench_echo` and `bench_keyed_nopersist` as twins of the rust_runner fixtures (same factory ids, sink names, output bytes and key function), over `uds`, `quic` and `native`, at durability `state` registering `bench_keyed`, the persistent twin, at durability `on` registering `tier_store` (the same upsert under `effectively_once/1` with the reference's 300 ms retry backoff) and at durability `checkpoint` registering `bench_checkpoint` (the upsert acknowledging every `FLEZHA_BENCH_CHECKPOINT_EVERY` inputs, holding its outputs when `FLEZHA_BENCH_CHECKPOINT_OUTPUTS` is `at_checkpoint` for `FLEZHA_BENCH_CHECKPOINT_DELAY_MS`, read in `channels/1` as the reference reads them); a round-trip run combining a `--runtime` with a durability posture whose capability that runtime's manifest defers SHALL be refused at the task with an error naming that capability, before any runner starts.

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
- **THEN** the elixir runner serves `bench_keyed`, the controller writes a WAL for its Entity, and every record is received exactly once

#### Scenario: Round-trip tiered runs on the elixir runtime
- **WHEN** `mix bench.roundtrip --runtime elixir --workload keyed --durability on` and then `--durability checkpoint` run with the uds transport configured
- **THEN** the elixir runner serves `tier_store` and then `bench_checkpoint`, every record is received exactly once with zero duplicates, and the report carries the same columns the rust runtime produces for the same cells

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

### Requirement: The Elixir SDK persists per-Entity state in its own core

The Elixir SDK SHALL carry the `state_persistence` capability by re-expressing the kernel's rules in Elixir, inheriting nothing from `flz_core` and changing nothing in the controller, which SHALL keep being unable to tell an Elixir runner from a Rust one. A Procedure that exports `apply_change/2` is changeful and persisted; `Flezha.SDK.Ctx.apply/3` SHALL be the only recorded mutation path, returning `apply_change(state, change)` and recording the change for the record stream, so that one fold is one push; a Procedure without `apply_change/2` is the reference's `NoChange` and ships nothing. `StreamOperative.persistence_enabled/0` (default `true`) SHALL, when `false`, advance the seq and ship nothing. After a successful dispatch the Entity SHALL ship one `entity_state_change` frame per recorded change — the 16-byte header stamping `sdk_lang = 3`, the SDK's version, the Procedure's schema version and the Erlang External Term Format id `7` allocated in `flz_core::header`, over `Flezha.SDK.Wire.encode_frame/4` and the instance-keyed identity — at `seq = change_seq` advancing from `0`, in dispatch order on one secondary stream, before the dispatch's output frame, each under the credit window's `acquire`; a dispatch that committed timer ops SHALL ship them after its change records as one further `entity_state_change` frame at the next seq, stamped `format_id = 3` over the postcard `Vec<TimerOp>` body every SDK writes, so the controller stores it as any change record and never decodes it; a class declaring the tier SHALL ship one `entity_effect_record` per effectful invocation in place of those records (the effectively-once requirement below). The SDK SHALL answer `RestoreInitState` by folding the snapshot then the change log in the controller's on-disk framing under the Entity's own serialisation — CRC over header and body, language before format before schema, the core's refusal wordings — folding a `format_id = 3` record into the pending-timer set rather than the state, and a `format_id = 2` effect record's enclosed change bodies (each under the enclosing record's version decision), timer ops, consumed offset and event time into the state, the set, the consumed floor and the event-time high-water, re-arming `change_seq` to `max(folded terminal, terminal_seq) + 1`, re-arming the folded processing-time timers, and replying `terminal_seq`; `CompactEntity` by shipping `entity_state_snapshot` at `change_seq − 1` after the Entity's records — followed, when the Entity holds pending timers, by one `format_id = 3` record re-stating the whole pending set at the next seq, so the truncation the snapshot authorises cannot take the timers with it — and replying `snapshot_seq` and the body size, with `{0, 0}` and an error for an unknown Entity; `EntityState` with the Decision-10 JSON shape narrowed by its selector, `flz.change_seq` on the reply frame for one Entity, and the refusals `entity_key_without_procedure`, `procedure_not_found`, `entity_not_found` and `entity_busy` by name; `EntityApplyState`, `MigrationDeserialization` and a non-empty `AddOperative.init_values` by handing each Procedure's JSON to its `state_from_json/1` (default: the object itself); `MigrationSerialization` with the whole-operative JSON; `CreditWindowInit` and `CreditGrant` by arming and replenishing one runner-wide window that parks the shipping Entity, never drops, counts `credit_parked`, and is disarmed on reconnect. On graceful termination — SIGTERM through the BEAM's `init:stop`, `Flezha.SDK.close/1`, `RunnerDisconnect` — the RunnerServer SHALL serialise every live operative, ship one `RunnerShutdownNotification` each and wait at most 2 s in parallel for the acks before closing. `test/apps/elixir_runner` SHALL carry the six reference twins and the manifest SHALL list `state_persistence` under `fixtures`.

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

#### Scenario: Timer ops are records in the Entity's own log, and a compaction re-states them
- **GIVEN** an Elixir Entity whose log holds two change records and one `format_id = 3` record registering a processing-time timer, at `change_seq = 3`
- **WHEN** the controller's sweep sends `CompactEntity`
- **THEN** the snapshot ships at seq 2, a `format_id = 3` record registering the timer ships at seq 3 behind it, the reply carries `snapshot_seq = 2`, and a restore over the truncated log rebuilds the pending set from that record

#### Scenario: An effect record folds on restore
- **GIVEN** an Elixir Entity's log holding a change record, then a `format_id = 2` effect record enclosing two change bodies, one timer registration and consumed offset 9 at event time 400
- **WHEN** the Entity is restored over it
- **THEN** the state holds all three changes in seq order, the pending set holds the timer, the consumed floor is 9 and the event-time high-water 400, and the terminal is the effect record's seq

### Requirement: The Elixir SDK mints the identity stamps on keyed plugin output

The Elixir SDK SHALL carry `keyed_channels` in its own core: every plugin frame from a sink declared `keyed/1` SHALL carry `flz.entity` (`<operative id>/<procedure name>/<entity key or empty>`, the label the shared core derives for the same Entity), `flz.seq` (`<change_seq at handout>.<emit ordinal>`, the seq the invocation started at, before its own changes advanced it) and `flz.key` (the emit's key, else the sink route's `key_by` over the value, else the emitting Entity's own key), appended after the user's metadata so a stamp cannot be clobbered. The ordinal SHALL start at zero for each work item, SHALL advance on every plugin-targeted output the invocation produces — stamped or not, each output of a `flat_map` counted — and SHALL NOT advance for an inter-procedure output. A sink not declared keyed SHALL carry no `flz.` key. Under the effectively-once tier the Entity SHALL capture each sink output into its invocation's effect record unstamped — the producing node's channel name, the resolved key, the user's metadata key-sorted with any `channel` key dropped, and the serialised payload — through the same sink leaf the router uses, and the router SHALL deliver no sink frame for that class; the controller derives `flz.entity`, `flz.seq` and `flz.epoch` from the durable record at flush, as it does for every other SDK. `test/apps/elixir_runner`'s `partition_relay` SHALL declare its sink keyed and name its channel `input`, and the manifest SHALL list `keyed_channels` under `fixtures` with the reference's id.

#### Scenario: A keyed sink's frame carries the three stamps after user metadata
- **WHEN** a dynamic Entity keyed `"k1"` at change_seq 4 calls `Ctx.emit_with_meta/3` with `%{"tag" => "a"}` on a node whose sink is keyed
- **THEN** the frame carries `tag: "a"`, `flz.entity: "op-1/counter/k1"`, `flz.seq: "4.0"` and `flz.key: "k1"`, and a second emit in the same invocation carries `flz.seq: "4.1"`

#### Scenario: A fresh work item restarts the ordinal, and an inter-procedure output does not advance it
- **WHEN** the next input for the same Entity emits once on the keyed sink after emitting once towards a consumer node
- **THEN** its plugin frame carries ordinal `0`

#### Scenario: A keyed output forwards to the partition of the target group that owns its key
- **WHEN** `partition_relay` on an Elixir runner, partitioned three ways, feeds a partitioned `dynamic_entity` through `operative_forward`
- **THEN** every key lands on the downstream member owning its key group and nowhere else, because the frame carries `flz.key` and `channel: "input"`

#### Scenario: The smoke golden pins the stamp keys
- **WHEN** `partition_relay` on an Elixir runner emits under a sink with `include_metadata`
- **THEN** the sorted `flz.*` keys of the frame equal `keyed_output_stamps.json`

#### Scenario: No stamps under the tier
- **WHEN** a class declaring `effectively_once/1` emits with `%{"tag" => "a"}` on its keyed sink
- **THEN** no output frame leaves on the primary stream, and the effect record's output carries channel `sink`, the key, metadata `[{"tag", "a"}]` and the payload with no `flz.` key

### Requirement: The Elixir SDK evicts idle dynamic Entities and caps them

The Elixir SDK SHALL carry `dynamic_entity` in its own core. A dynamic node whose Procedure declares `entity_idle_ttl_ms/0` SHALL be swept by one `:reap` tick in its Operative process at `FLEZHA_REAP_INTERVAL_MS`, else `min(ttl / 4, 30 s)` and at least 1 ms; a node declaring none SHALL never be swept and no tick SHALL be scheduled. The sweep SHALL run under the Operative's own routing serialisation — the drain gate: no push can be routed while it runs — and SHALL evict an Entity only when it has been idle past the TTL with an empty queue, no dispatch in flight, no held park and no deferred push, skipping a busy one until the next sweep. Eviction SHALL be destructive: the Entity SHALL ship `delete_entity_state` on its own record stream when the class persists (behind its records by construction), SHALL clear its slot so a successor is a cold start from `init_state/1`, and SHALL stop; the Operative SHALL drop it from the live map and remember the key in a trace of at most 1 024 keys. `Operative.lookup/4` SHALL answer `:entity_evicted` for a traced key that is not live, and the live read SHALL spell the shared core's `entity_evicted:` wording for it, distinguishably from `entity_not_found`. `max_entities/0` SHALL bind at `ensure_entity/3` as the behaviours requirement states. Creation, eviction and capped drops SHALL be reported as one `[:flezha, :sdk, :dynamic]` telemetry event carrying the kind. `test/apps/elixir_runner`'s `dynamic_entity` SHALL journal its push and read its TTL from `DYN_TEST_TTL_MS`, and the manifest SHALL list `dynamic_entity` under `fixtures` with the reference's id.

#### Scenario: An idle Entity is evicted destructively on the sweep
- **GIVEN** a persistent dynamic node with a TTL and an Entity that shipped a record at seq 0 and has been idle past the TTL
- **WHEN** the sweep runs
- **THEN** a `delete_entity_state` frame for that identity follows the record on the same stream, the Entity's slot is gone, `lookup/3` answers `{:error, :entity_evicted}`, and the next input for the key starts a fresh Entity at seq 0 from `init_state/1`

#### Scenario: A busy or recently pushed Entity is kept
- **WHEN** the sweep meets an Entity mid-dispatch, or one pushed within the TTL
- **THEN** neither is evicted on that sweep and both are swept again on the next

#### Scenario: A live dynamic Entity is readable by name, and an evicted key is refused by name
- **WHEN** `EntityState` names a live key of a `dynamic_entity` operative on an Elixir runner, then names a key the reaper evicted, then names a key never seen
- **THEN** the first answers that key's state alone, the second refuses with `entity_evicted`, the third with `entity_not_found`

#### Scenario: No TTL, no tick
- **WHEN** an Operative's dynamic nodes declare no `entity_idle_ttl_ms/0`
- **THEN** no `:reap` message is ever scheduled and its Entities live for the operative's lifetime

### Requirement: The Elixir SDK declares schema versions per Procedure and migrates across them

The Elixir SDK SHALL carry `schema_evolution` in its own core. A Procedure's `schema_version/0` (default 1) SHALL be stamped on every record it ships and resolved per record on restore against its `min_readable_schema_version/0` (default: the version; a floor above the version reads the version only): equal decodes, between the floor and the version migrates, above the version or below the floor refuses with the shared core's wordings. A migrating record SHALL be handed to the Procedure's `migrate_state/2` (a snapshot) or `migrate_change/2` (a change) with the stamped `from_version` and the DECODED term — this SDK's body codec is the language's own term format, so the author receives the term, not bytes — and the fold SHALL continue with the running version's term the migration returns; a Procedure that declares no migration SHALL refuse the record naming the absent migration and the version, and a migration that returns `{:error, reason}` or raises SHALL refuse naming the reason and the version, reseating nothing. A restore that migrated any record SHALL answer `migrated` true, so the controller's compaction rewrites the baseline at the running version. `test/apps/elixir_runner` SHALL carry `schema_evolution_v1` and `schema_evolution_v2` as twins of the reference's `evolving_cart` pair, and the manifest SHALL list `schema_evolution` under `fixtures` with the reference's two ids.

#### Scenario: State written at v1 comes back through the migration a bumped v2 declares
- **GIVEN** an `evolving_cart` Entity on an Elixir runner that accumulated 7 under `schema_evolution_v1` and was compacted
- **WHEN** the operative is stopped with its state preserved and started under `schema_evolution_v2`
- **THEN** the restore migrates the snapshot and the tail, the state reads `total: 7` with `currency: "EUR"`, the controller observes the migration and compacts, and a second restart under v2 migrates nothing

#### Scenario: v1 code meeting v2 state is refused, not decoded
- **WHEN** an Entity written under `schema_evolution_v2` is restarted under `schema_evolution_v1` on an Elixir runner
- **THEN** the restore is refused with `schema_version 2 is above the running procedure's 1` and the operative is left unbound

#### Scenario: A floor without a migration fails at the first old record
- **WHEN** a Procedure at version 2 declaring a floor of 1 and no `migrate_change/2` folds a record stamped 1
- **THEN** the fold refuses naming the absent migration and version 1, and the Entity keeps the state it had

#### Scenario: A failing migration reseats nothing
- **WHEN** `migrate_state/2` answers `{:error, "bad shape"}` for a snapshot stamped 1
- **THEN** the restore refuses with a reason carrying `bad shape` and `1`, and the Entity's state and seq are unchanged

### Requirement: The Elixir SDK carries per-Entity timers and the three clock kinds in its own core

The Elixir SDK SHALL carry the `entity_timers` capability by re-expressing the kernel's rules in Elixir, inheriting nothing from `flz_core` and changing nothing in the controller, which SHALL keep being unable to tell an Elixir runner from a Rust one. `Flezha.SDK.Ctx.timer_after/2` (milliseconds, anchored on `now/1`) and `timer_at_event_time/2` SHALL answer `{:ok, at}` with the absolute scheduled timestamp — the handle `cancel_processing_time_timer/2` and `cancel_event_time_timer/2` take, each answering whether a timer was pending — and SHALL refuse `{:error, {:event_time_timer_without_event_time_clock, procedure, clock}}` under a clock other than `:event_time` and `{:error, {:timer_cap_exceeded, procedure, max}}` past `max_timers_per_entity/0`, registering nothing on a refusal. Pending timers SHALL be one set per kind: a duplicate registration is a no-op that produces no op. An invocation's registrations and cancellations SHALL accumulate as ops over a copy of the pending set and commit only when the invocation completes — a firing's removal is op 0 and is committed on a park as well — and the committed ops SHALL ship after the dispatch's change records as one `format_id = 3` record when the class persists; a Procedure without `apply_change/2` holds its timers in memory only, as the shared core's host path does. A processing-time timer SHALL be armed on the BEAM's timer service by the Entity process and fire through the Entity's own queue in ascending scheduled order, serialised against `run/3` by the same drain; an event-time timer SHALL fire when an input's `event_time_of/1` advances the Entity's event-time high-water to or past it, every due one in ascending order before that input, and never on an Entity nothing arrives at. `on_timer/4` SHALL receive the scheduled timestamp and the kind, `now/1` SHALL equal that timestamp inside the firing under every clock, a firing whose timer is no longer pending SHALL fire nothing, and a firing that fails SHALL ride the input channel's retry ladder and park exactly as an input does, its removal committed either way. A restore SHALL rebuild the pending set from the change log and re-arm the processing-time timers, overdue ones firing at once in ascending order; a compaction SHALL re-state the pending set above the snapshot's seq; the replacement of an Entity killed untrappably SHALL re-arm from the slot; eviction SHALL discard the Entity's timers with its state. Registrations, firings, cancellations and refusals SHALL be reported as one `[:flezha, :sdk, :timers]` telemetry event carrying the kind and the procedure — this SDK's per-Procedure timer counters — and the heartbeat SHALL carry `timers_armed` and `timers_fired`. `test/apps/elixir_runner` SHALL carry `session_timeout` as the reference's twin, and the manifest SHALL list `entity_timers` under `fixtures` with that id.

#### Scenario: A quiet session is closed by its own idle timer
- **WHEN** a `session_timeout` operative on an Elixir runner receives one input and nothing else for `TIMER_TEST_TIMEOUT_MS`
- **THEN** `on_timer/4` runs with no further input, its change is journaled, `EntityState` reads `closed_at` holding the scheduled timestamp, and `closed@<at>` leaves on `test_kafka`

#### Scenario: A pending timer survives a re-seat from disk
- **WHEN** the Entity is rebuilt through `AddOperative` and `RestoreInitState` while its timer is still pending
- **THEN** the fold rebuilds the pending set from the `format_id = 3` record the first run shipped, the timer is re-armed on the fresh Entity, and it fires with no new input

#### Scenario: Crossing the high-water fires in order, before the input
- **WHEN** an Entity under `clock/0` `:event_time` holds event-time timers at 100, 200 and 300, its high-water is 50, and an input with event time 250 arrives
- **THEN** `on_timer/4` runs for 100, then for 200, then `run/3` runs for the input; 300 stays pending, and a quiet period afterwards fires nothing however long it lasts

#### Scenario: A firing is serialised against run, and a poison firing parks once
- **WHEN** a processing-time timer comes due while the Entity is inside `run/3`, and separately `on_timer/4` fails past the retry ladder
- **THEN** the first `on_timer/4` runs only after that invocation returns and after any input queued ahead of the due message, and the second parks with the fired timer's removal committed, so it does not fire again after the park is released

#### Scenario: The pending set is bounded and a wrong-clock registration is refused
- **WHEN** an Entity holding `max_timers_per_entity/0` pending timers registers one more, and a `:wall` Procedure calls `timer_at_event_time/2`
- **THEN** the first answers `{:error, {:timer_cap_exceeded, name, max}}` with the set unchanged, the second `{:error, {:event_time_timer_without_event_time_clock, name, :wall}}` with nothing registered, and both are reported as `:refused`

#### Scenario: Overdue timers fire ascending after a restore, and a stale due message fires nothing
- **WHEN** a restore rebuilds processing-time timers at 100 and 200 that are both in the past, and separately a timer is cancelled after its due message was armed
- **THEN** `on_timer/4` runs for 100 then 200 with no input, and nothing runs for the cancelled one

#### Scenario: Eviction discards timers, and a replacement re-arms from the slot
- **WHEN** a dynamic Entity holding a pending timer is idle-evicted, and separately an Entity holding one is killed untrappably
- **THEN** nothing fires for the evicted one and its key's next input cold-starts with no pending timers, while the killed one's replacement seeds the set from the slot and its timer still fires

### Requirement: The Elixir SDK carries the effectively-once tier in its own core

The Elixir SDK SHALL carry the `effectively_once` capability by re-expressing the kernel's rules in Elixir, inheriting nothing from `flz_core` and changing nothing in the controller, which SHALL keep being unable to tell an Elixir runner from a Rust one. A class declaring `effectively_once/1` SHALL, for every effectful invocation — one that applied a change, emitted, committed a timer op or parked — ship ONE `entity_effect_record` frame at the next seq in place of its change records: the 16-byte header stamped `format_id = 2` over the postcard `EffectRecordBody` `Flezha.SDK.Wire.encode_effect_body/1` writes, enclosing the changes as External Term Format bodies, the captured sink outputs, the input's `flz.offset` as `consumed_offset` (absent for a firing), the park as `dlq`, the declared clock's value as `event_time` and the committed timer ops, under the credit window's `acquire`; an invocation with no effect SHALL ship nothing unless it parked. The Entity SHALL read the offset the router threads from the frame's `flz.offset` metadata onto its queue item, SHALL skip an input whose offset is at or below its consumed floor as a completed dispatch, and SHALL hand an inter-procedure emit the offset of the input that produced it. Under checkpoint acknowledgement the Entity SHALL keep one staged interval — the changes, the timer ops, the offset of the last input folded, the input count, when it opened, and the outputs when `outputs_at_checkpoint/2` is declared — that ships as one closing record carrying the interval's last offset when `Ctx.checkpoint/1` was called, when `checkpoint_every/2` inputs have folded in, when a park closes it, or when `max_delay_ms` has elapsed since it opened (a close message from the BEAM's timer service that rides the Entity's own queue and runs no author code, a stale one finding nothing open); a mid-interval as-produced emit SHALL ship in a record acknowledging nothing; a restore SHALL discard the open interval and rebuild the consumed floor and the event-time high-water from the log's effect records; the replacement of an Entity killed untrappably SHALL re-hold the open interval and the floor from the slot. A compaction SHALL re-state pending timers as a `format_id = 3` record under the tier as it does outside it. Held effect records of an invocation with inter-procedure outputs (the `effectively-once-output` requirement on edge outputs) SHALL ship in seq order once the router reports the pushes admitted. `test/apps/elixir_runner` SHALL carry `tier_store`, `checkpoint_store`, `checkpoint_batched_store` and `bench_checkpoint` as the reference's twins, and the manifest SHALL list `effectively_once` under `fixtures` with those four ids.

#### Scenario: An effectful invocation ships one effect record and no output frame
- **WHEN** a `tier_store` Entity on an Elixir runner receives `k1:a` at `flz.offset` 3
- **THEN** one `entity_effect_record` frame ships at its seq, enclosing the `{:set, "k1", "a"}` body, one output on channel `sink` keyed `k1` with payload `k1=a`, consumed offset 3, and the wall clock's value as event time; no `entity_state_change` and no output frame leave

#### Scenario: An open interval acknowledges nothing and closes on the checkpoint
- **WHEN** a `checkpoint_store` Entity folds `k1:a` and `k2:a` at offsets 0 and 1, then `commit:1` at offset 2
- **THEN** the first two invocations ship records carrying their outputs and no offset, and the third ships one record carrying both changes and consumed offset 2

#### Scenario: A declared cadence and the delay close the interval without a checkpoint call
- **WHEN** a class declaring `checkpoint_every(3)` folds three inputs, and a class declaring `outputs_at_checkpoint(20)` folds one input and receives nothing more
- **THEN** the first ships one record at the third input carrying all three changes and the third offset, and the second ships its held output within the delay in a record carrying its offset

#### Scenario: Held outputs ride the closing record as one batch
- **WHEN** a `checkpoint_batched_store` Entity folds three emitting inputs then `commit:1`
- **THEN** no output leaves before the commit, and the closing record's `outputs` holds the three in production order under one seq

#### Scenario: A restore discards the open interval and rebuilds the floor
- **GIVEN** a checkpoint Entity holding an open interval of two changes whose log's last effect record consumed offset 5
- **WHEN** it is restored over that log and the controller re-forwards offsets 4 and 6
- **THEN** the interval's changes are gone from the state, offset 4 is skipped as a completed dispatch, offset 6 dispatches, and the next close acknowledges 6

#### Scenario: A replacement re-holds the open interval from the slot
- **WHEN** a checkpoint Entity holding two staged changes is killed untrappably and its replacement receives `commit:1`
- **THEN** the closing record carries the two staged changes and the commit's offset at the seq the slot held

#### Scenario: A firing ships an effect record with no offset
- **WHEN** a processing-time timer fires on a class declaring the tier and `on_timer/4` applies a change and emits
- **THEN** one effect record carries the change, the output, the fired timer's removal and no consumed offset

#### Scenario: The tier scenarios pass under the Elixir runtime
- **WHEN** `FLEZHA_E2E_RUNTIME=elixir mix test --include system` runs the eight modules tagged `capability: :effectively_once` and the smoke tier's two tier goldens
- **THEN** every scenario runs and passes on `uds` and the crash matrix on `quic`, the Postgres-gated ones with the `flezha_postgres_sink` container started by name, and `effectively_once` sits in `@required_capabilities`

### Requirement: The Elixir SDK carries late-input correction in its own core
The Elixir SDK SHALL implement late-input correction in its own runner core, which inherits nothing from `flz_core`, against the rules of `stream-correction` rather than its Rust. A class MAY declare `correction_window/2` and `beyond_window_policy/2` on the graph builder; the declaration SHALL require `effectively_once/1` and at least one Procedure declaring an `:event_time` clock, SHALL refuse a zero window, and SHALL reach the controller as `correction_window_ms` and `beyond_window_policy` on `AddOperativeResponse.metadata`. Each Entity SHALL classify every input against the event-time high-water it already tracks and restores, BEFORE advancing it: at or ahead of the high-water, or with no high-water yet, the input SHALL be dispatched and the high-water advanced; behind it by no more than the declared window the input SHALL NOT be dispatched and one `correction_request` frame SHALL be sent on the Entity's own state stream carrying the target event time alone, with the Entity identity on the frame envelope, a seq of zero and a zero record header; behind it by more than the window the declared policy SHALL decide — `process_as_arrival` (the default) dispatches it and `drop` does not — and in NEITHER case SHALL the high-water advance. An Entity with no state stream SHALL dispatch a correctable input as an arrival rather than withhold it for a correction it could not request. An input the controller marked `flz.replay` SHALL be buffered by its Entity rather than dispatched, and the payload-less `flz.replay_end` broadcast SHALL be intercepted before any channel is resolved and delivered to EVERY Entity of the operative, static and dynamic. The close SHALL be placed on each Entity's own queue behind the members that preceded it rather than executed on arrival, so a close can never overtake a member it was meant to include; on reaching it the Entity SHALL order the buffered members by extracted event time, ties broken by ascending input-log offset, and dispatch them ahead of anything enqueued behind the close. A replayed member SHALL advance the high-water and SHALL NOT be re-classified. A `RestoreInitState` SHALL discard a buffered slice. The SDK SHALL write no correction journal, no crash marker, no new control message and no second replay path: the correction is the existing tier restore-and-replay with a rewound baseline and an event-time-ordered slice.

#### Scenario: A late input inside the window is withheld and asks for a correction
- **WHEN** an Entity of a class declaring `correction_window(600_000)` has dispatched an input at event time 300 000 and then receives one at event time 299 000
- **THEN** the second input is not dispatched, exactly one `correction_request` frame is sent on the Entity's state stream whose decoded body is 299 000, and the Entity's high-water is unchanged

#### Scenario: A beyond-window input follows the policy and never moves the high-water
- **WHEN** an Entity with high-water 1 000 000 and a 600 000 ms window receives an input at event time 1 000
- **THEN** under the default `process_as_arrival` it is dispatched and the high-water stays 1 000 000, and under a declared `:drop` it is neither dispatched nor recorded, with the high-water likewise unchanged

#### Scenario: A replayed slice is held to its broadcast and folded in event-time order
- **WHEN** the controller re-forwards three inputs marked `flz.replay` with event times 300, 100 and 200 at ascending input-log offsets and then broadcasts `flz.replay_end`
- **THEN** none of the three is dispatched on arrival, and on the broadcast they are dispatched in the order 100, 200, 300

#### Scenario: A live input behind the broadcast folds after the whole slice
- **WHEN** an input that carries no replay mark is enqueued after the last slice member and before the end-of-slice broadcast
- **THEN** it is dispatched after every member of the sorted slice, never between two of them

#### Scenario: The broadcast reaches every Entity, including one that buffered nothing
- **WHEN** an operative holds two Entities, only one of which received slice members, and the `flz.replay_end` frame arrives
- **THEN** the Entity with members flushes them in event-time order and the Entity with none continues normally, neither stalling nor erroring

#### Scenario: A restore discards a buffered slice
- **WHEN** an Entity holding buffered slice members is sent `RestoreInitState`
- **THEN** the buffer is empty afterwards, and the members return only if the controller re-forwards them from the rewound watermark

#### Scenario: A replayed member is not re-classified as late
- **WHEN** a slice member whose event time is behind the Entity's high-water is dispatched from the ordered flush
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

### Requirement: The Elixir SDK authors and drives workflow operatives in its own core

The Elixir SDK SHALL carry a `Flezha.SDK.WorkflowOperative` behaviour whose class declares
either a run-sugar `run/2` body or a structured `procedures/1` DAG on the same builder token
`channels/1` uses, plus the optional knobs `max_duration_ms/0`, `self_terminating?/0`,
`loop_mode/0` (`:once` | `:per_input` | `:chained`), `wake_after_ms/0`, `retry_attempts/0`,
`retry_backoff_ms/0`, `barrier_retry_attempts/0`, `rollback_mode/0` (`:all_or_nothing` |
`:keep_progress`), `persistence_enabled/0` and `sync_durability/0`, each reading a default when
the class is silent. `Flezha.SDK.register_factory/2` SHALL register such a class onto the runner handle
exactly as it registers a stream class, because the controller places by factory id and learns
the kind from the `AddOperative` it sends rather than from the advertisement.

Registration SHALL accumulate refusals into `{:error, {:invalid_workflow, issues}}` (never a
raise) using the reference's names: `NoWorkflowBody`, `ExternalStopOnly` (not self-terminating
and no duration cap), `ZeroRetryBackoff`, `LoopWithoutChain` (a loop mode on a run-sugar body),
`NoEntryChannel`, `EntryRoot` (an entry feeding more than one procedure),
`MergeFromEntryChannel`, `DynamicEntities` and `BranchingChainedLoop`. A refusal the shared
topology already raises SHALL be carried through as `{:Topology, issue}` rather than restated,
and the reference's `Cycle`, `Disconnected` and `MultipleOutputChannels` SHALL NOT exist here:
the first two are unreachable because the shared topology refuses a cycle first and a node
exists only where an `into` created it, and the last because a node owns exactly one output
channel. A missing inter-procedure codec SHALL be named by no SDK, this one included: every
inter-procedure value has a serialization everywhere, so there is nothing for an author to
declare and nothing to refuse.

A structured class SHALL drive its DAG frontier by frontier: a procedure becomes runnable only
once every producer feeding it has committed, so a fan-out interleaves independently and a
fan-in is dispatched exactly once with the accumulated batch of all its producers. Each stage
SHALL be committed durably before the next begins, its plugin-bound output SHALL be staged into
that commit rather than shipped, and the class's leaf and compensable positions SHALL be
declared to the controller in the `AddOperative` response.

#### Scenario: A structured class is refused for every fault at once

- **WHEN** a class declares a chained loop on a run-sugar body, a zero retry backoff and no
  terminal condition
- **THEN** registration answers `{:error, {:invalid_workflow, issues}}` naming
  `LoopWithoutChain`, `ZeroRetryBackoff` and `ExternalStopOnly` together, not the first alone

#### Scenario: A stage's Entities are the SDK's own, not a second mechanism

- **WHEN** a workflow stage dispatches a batch
- **THEN** it runs on `Flezha.SDK.Entity` processes with the same state, records, timers and
  retry ladder a stream operative's Entities have, differing only in that the ladder's bounds
  come from the class's `retry_attempts`/`retry_backoff_ms` and that an exhausted ladder fails
  the procedure instead of parking the input to the dead-letter queue

#### Scenario: A stage's plugin output reaches nothing before its commit is durable

- **WHEN** a procedure on a hybrid channel emits while the class journals
- **THEN** the output is carried in that stage's `ProcCommit` outbox and reaches the plugin only
  after the controller has journalled and acknowledged the commit, and a class that journals
  nothing stages none

### Requirement: One plugin sink per channel and plugin

`sink/2` and `sink_with/3` SHALL refuse a second sink declared on the same channel to the same
plugin with `DuplicatePluginSink`, naming both. Neither half alone is the duplicate: one channel
MAY reach several plugins — a keyed wire beside a plain one — and one plugin MAY be fed by
several channels, which is how a chain reports every stage to one place with a different encoder
each time. This is a deliberate departure from the reference, whose `sink` closes its channel
and therefore refuses per channel alone.

#### Scenario: The same channel and plugin twice is refused

- **WHEN** a class declares `sink(s, "out")` twice on one handle
- **THEN** registration fails with `DuplicatePluginSink` naming the channel and the plugin

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
evicted Entity ships, the records an Entity still holds when it stops, and the
workflow driver's outputs and records written through the runner. A hold that
dies with the process that owned it is not a hold, so frames an Entity holds
when it stops in an orderly way SHALL pass to a holder that outlives it and be
replayed ahead of anything its replacement writes.

#### Scenario: A record's write fails while the data stream is down

- **WHEN** an Entity's `entity_state_change` frame cannot be written
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

- **WHEN** an Entity is evicted while no secondary stream is bound
- **THEN** its `delete_entity_state` frame SHALL be replayed on the next bound
  stream rather than lost with the process

#### Scenario: An Entity that stops while holding hands its frames on

- **WHEN** an Entity with held records is evicted before a stream is bound
- **THEN** after the rebind those records SHALL reach the stream, ahead of the
  output of the dispatch that produced them and ahead of anything a replacement
  Entity writes

#### Scenario: A workflow write the runner cannot ship is held

- **WHEN** the workflow driver's output or record is cast to the runner with no
  secondary stream bound, or the write is refused
- **THEN** the frame SHALL be held and replayed on the next bound stream, and
  SHALL NOT be dropped behind a log line

### Requirement: The Elixir SDK re-expresses the reference check from the reference fixture

`Flezha.SDK.Topology.build/1` SHALL judge a topology by the same three stages the core's
check runs — the ordered links of each chain, the derivation, the topology and class
facts — over a record of the same shape, refusing with the reference names and sentences
as `{:error, {:invalid_topology, issues}}`, since the Elixir SDK inherits nothing from
`flz_core`. It SHALL carry every reference name except `BranchTooFewArms`, whose surface (`branch`)
the v1 subset does not author and which the fixture rows name under `needs` and the
conformance run skips by name; `MergeTooFewSources` and `DuplicateMergeSource` SHALL be
judged by the same merge-group walk the core's `chains.rs` runs, over the merge links the
builder records. The single checkpoint implication site SHALL remain
`Flezha.SDK.Channels`, proven by the same fixture rows.

#### Scenario: A keyed channel without a plugin wire is refused

- **WHEN** a channel is declared `keyed` and no `sink` closes it
- **THEN** `build/1` returns `{:error, {:invalid_topology, [KeyedChannelWithoutPluginWire ...]}}`

#### Scenario: A merge row refuses with the reference name

- **WHEN** the Elixir interpreter reaches `merge_one_source` or `merge_same_twice`
- **THEN** it authors the row through `Channels.merge/1` and `Flezha.SDK.Merge`, `build/1` refuses with `MergeTooFewSources` or `DuplicateMergeSource`, no row is skipped for `merge`, and the manifest's `topology_surfaces` lists `merge`

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
plugin or workflow emit — held already framed, so a keyed output's channel and
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

The Elixir SDK SHALL carry the restore-time queued-input outcome on its heartbeat, under the same key every other runtime uses, so that the fate of queued input is readable for an Elixir-hosted Operative as it is for every other host.

The Controller already mirrors that key into its liveness counters and its observability source, and the kernel already emits it; the Elixir SDK is the only runtime that carries neither the counter nor the key. A key that one runtime pays for on the wire and another omits leaves an operator surface that is correct for six SDKs and blank for the seventh.

If the Elixir SDK discards no queued input on restore, it SHALL still declare the key and report zero, and the reason SHALL be stated where the key is declared. A key reported as absent and a key reported as zero are different answers to an operator, and only the second is honest about a runtime that was asked.

#### Scenario: The heartbeat carries the key

- **WHEN** an Elixir-hosted runner sends a data heartbeat
- **THEN** the restore-time discard count SHALL be among its keys, and the Controller's mirror SHALL accept it without a slot mismatch

#### Scenario: A discard is visible to an operator

- **GIVEN** an Elixir-hosted Entity holding queued input when a restore lands
- **WHEN** the restore discards that input
- **THEN** the count SHALL rise on the operator surface that reports it for every other runtime

#### Scenario: A runtime that discards nothing says so

- **WHEN** the Elixir SDK performs a restore that discards no queued input
- **THEN** it SHALL report zero rather than omitting the key, so the surface distinguishes "nothing was discarded" from "nothing was measured"

### Requirement: An Elixir procedure requests spawn-or-deliver of a workflow instance

The Elixir SDK SHALL let a procedure request spawn-or-deliver of a workflow instance in its
own namespace with one call, under the contract the other SDKs implement
(`docs/sdk/procedure_requested_spawn.md`). `Flezha.SDK.Ctx.spawn_workflow/4`, and the same call
on `Flezha.SDK.Workflow.Ctx` for a run-sugar body, SHALL take a workflow factory id, a binary
payload and a keyword list of options defaulting to `[]`, whose keys are `id:` (an instance id)
and `if_running:` (`:deliver` or `:error`, `:deliver` when an `id:` is given without it). The call
SHALL send `SpawnWorkflowRequest` on the runner's control plane, with an empty instance id when no
`id:` was given, and return a handle without waiting for the controller. An `if_running:` given
without an `id:` SHALL raise `ArgumentError` at the call and send nothing.

The handle SHALL resolve, through `Flezha.SDK.Spawn.await/2` or `Flezha.SDK.Spawn.try_result/1`,
to `{:ok, %{id: id, outcome: :spawned | :delivered | :skipped_on_replay}}`, where `id` is the
instance id the request resolved to (the caller's, or the one the controller generated), or to
`{:error, {:already_running, instance_id} | {:factory_unknown, factory_id} | {:refused, reason} | :timeout | :unavailable}`.
A refusal SHALL NOT fault the invocation, its emits or its records. A failure no caller read
SHALL be logged once as a warning. An invocation of a replayed input SHALL resolve
`{:ok, %{id: id, outcome: :skipped_on_replay}}` with the caller's id, or `""` when no `id:` was
given, and send nothing. A context with no runner, or a runner with no control
plane, SHALL resolve `:unavailable` at once and send nothing. A request the controller never
answers SHALL resolve `:timeout` after the runner's reply deadline, and the runner SHALL keep
no waiter for it afterwards. The handle SHALL be read inside the invocation that made it; the
SDK MAY drop a reply that arrives after the invocation returned, and the warning is what
reaches such a caller. The request SHALL be written as a `:control` frame.

#### Scenario: A free id spawns and a taken id delivers

- **WHEN** an Elixir stream procedure calls `spawn_workflow(ctx, factory_id, payload, id: "w1")`
  twice, and a connected runner offers the workflow factory
- **THEN** the first handle resolves `{:ok, %{id: "w1", outcome: :spawned}}`, the second
  `{:ok, %{id: "w1", outcome: :delivered}}`, and the workflow instance receives both payloads as
  inputs

#### Scenario: A call without an id spawns under a generated id

- **WHEN** a procedure calls `spawn_workflow(ctx, "worker", payload)` with no options
- **THEN** the request carries an empty instance id, and the handle resolves
  `{:ok, %{id: id, outcome: :spawned}}` where `id` begins `worker-` and names the running instance

#### Scenario: The error policy refuses a running id

- **WHEN** a procedure calls `spawn_workflow` with `id: instance_id, if_running: :error` for an
  instance id that is running
- **THEN** the handle resolves `{:error, {:already_running, instance_id}}`

#### Scenario: if_running without an id raises at the call

- **WHEN** a procedure calls `spawn_workflow(ctx, "worker", payload, if_running: :error)`
- **THEN** the call raises `ArgumentError` naming `if_running` and `id`, and no
  `SpawnWorkflowRequest` leaves the runner

#### Scenario: The call returns before the controller answers

- **WHEN** a procedure calls `spawn_workflow` and the controller has not answered yet
- **THEN** the call has already returned a handle, and `try_result/1` answers `:pending`

#### Scenario: A refusal leaves the invocation intact

- **WHEN** the controller refuses a spawn, and the procedure emits a value and returns
  `{:ok, state}`
- **THEN** the emit ships, the invocation is not retried, and the handle resolves
  `{:error, {:refused, reason}}`

#### Scenario: An unread failure is logged once

- **WHEN** a spawn fails and no caller reads its handle
- **THEN** exactly one warning names the instance id (empty when none was given), the factory id
  and the reason

#### Scenario: A replayed input issues nothing

- **WHEN** a correction-declaring class re-runs a replayed slice and its procedure calls
  `spawn_workflow` with no `id:`
- **THEN** the handle resolves `{:ok, %{id: "", outcome: :skipped_on_replay}}` and no
  `SpawnWorkflowRequest` leaves the runner

#### Scenario: A context with no runner is unavailable

- **WHEN** a procedure runs under a context that carries no runner, or under a runner whose
  session is down, and calls `spawn_workflow`
- **THEN** the handle resolves `{:error, :unavailable}` at once, and nothing is sent or kept

#### Scenario: An unanswered request times out and is forgotten

- **WHEN** the request is written and no `SpawnWorkflowResponse` arrives within the reply
  deadline
- **THEN** the handle resolves `{:error, :timeout}`, an unread one is logged once, and the
  runner holds no waiter for the request

#### Scenario: The request is a control frame

- **WHEN** the runner writes a `SpawnWorkflowRequest`
- **THEN** `Hold` classes it `:control`, and the runner server does not raise

### Requirement: The Elixir per-input loop waits for its inbox as the shared core does

A `:per_input` workflow loop in the Elixir SDK SHALL seed every iteration, the first included,
from the next input in its inbox and never from the spawn args. With nothing queued it SHALL
wait, for up to 60 seconds while a rehydrating bind still owes it inputs and otherwise for
`FLEZHA_WORKFLOW_PARK_IDLE_MS`, and SHALL park only when that wait ends with nothing to consume.
A class that journals nothing SHALL wait until an input or a stop arrives, because it cannot
park. A stop or the duration cap reached during the wait SHALL complete the run as `cancelled`.
An iteration SHALL commit the durable-inbox offset its input carried in `flz.inbox_offset`.

#### Scenario: The first iteration consumes the first input

- **WHEN** a per-input loop is started with spawn args and one input then arrives
- **THEN** its first committed iteration ran over that input, and no iteration ran over the args

#### Scenario: An empty inbox parks only after the idle window

- **WHEN** a journaling per-input loop finds its inbox empty and no input arrives within the idle
  window
- **THEN** it ships `IterPark` and no `WorkflowCompleted`

#### Scenario: An owed tail is waited for past the idle window

- **WHEN** a rehydrated loop still owes an input and its inbox is empty
- **THEN** it waits for that input rather than parking at the idle window or completing

#### Scenario: A loop that journals nothing waits instead of parking

- **WHEN** a per-input loop whose class journals nothing finds its inbox empty
- **THEN** it ships no `IterPark`, runs the next input that arrives, and a stop completes it as
  `cancelled`

#### Scenario: An iteration commits its input's inbox offset

- **WHEN** an input stamped `flz.inbox_offset` `7` is consumed
- **THEN** that iteration's `IterCommit` carries consumed offset `7`

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
- **WHEN** the controller boots and an operative starts on the in-node runner
- **THEN** the SDK's `:native` connect SHALL reach the controller through the registered behaviour and dispatch SHALL proceed as before

#### Scenario: A standalone runner has no native mode
- **WHEN** a runner outside the controller's VM asks for `:native`
- **THEN** the SDK SHALL refuse naming the behaviour and the modes it does support

### Requirement: The Elixir SDK authors with `use` macros that expand to its behaviours
The SDK SHALL provide `use Flezha.SDK.Procedure`, `use Flezha.SDK.StreamOperative` and `use Flezha.SDK.WorkflowOperative`. Each SHALL set the module's `@behaviour` and, for every option written, define the zero-arity callback of the same name returning the option's value, and SHALL define nothing for an option not written, so every default stays resolved by `function_exported?` as it is for a hand-written module. `use Flezha.SDK.Procedure` SHALL require `name` and accept `entity_keys`, `mailbox_capacity`, `max_entities`, `entity_idle_ttl_ms`, `compact_after_bytes`, `schema_version`, `min_readable_schema_version`, `clock` and `max_timers_per_entity`, plus the short-form options; it SHALL import `Flezha.SDK.Ctx`'s authoring functions except `apply/3`, SHALL remove any `Flezha.SDK.Channels` import the module inherits lexically, and SHALL define `init_state/1` returning `nil` at `@before_compile` when the module defines neither a `state` block nor its own `init_state/1`. `use Flezha.SDK.StreamOperative` SHALL require `factory_id` and accept `description` and `persistence_enabled`. `use Flezha.SDK.WorkflowOperative` SHALL require `factory_id` and accept `description`, `max_duration_ms`, `self_terminating?`, `loop_mode`, `wake_after_ms`, `retry_attempts`, `retry_backoff_ms`, `barrier_retry_attempts`, `rollback_mode`, `persistence_enabled` and `sync_durability`, and SHALL import `Flezha.SDK.Workflow.Ctx`'s `emit/2`, `emit_on/3`, `complete/2` and `spawn_workflow/3,4`. Both operative macros SHALL import `Flezha.SDK.Channels`. An unknown option, or a missing required one, SHALL be a `CompileError` naming it. Callbacks that take arguments stay `def`s the author writes. A module written with `@behaviour` and hand-written callbacks SHALL keep working unchanged.

#### Scenario: A written option is a callback, an unwritten one is not

- **WHEN** a module writes `use Flezha.SDK.Procedure, name: "position", max_entities: 50_000`
- **THEN** it exports `name/0` returning `"position"` and `max_entities/0` returning 50_000, and `function_exported?(module, :mailbox_capacity, 0)` is false, so `FLEZHA_MAILBOX_CAPACITY` still decides its mailbox

#### Scenario: A stateless procedure needs no init_state

- **WHEN** a module writes `use Flezha.SDK.Procedure, name: "echo"` and only `run/3`
- **THEN** it compiles, `init_state(nil)` answers `nil`, and a stream wiring it into a sink echoes its input

#### Scenario: A nested procedure does not see the operative's builder

- **WHEN** a module `use`s `Flezha.SDK.StreamOperative` and nests a module that `use`s `Flezha.SDK.Procedure` and defines its own `input/0`
- **THEN** both compile, and the nested module's `input/0` does not conflict with `Flezha.SDK.Channels.input/2`

#### Scenario: A misspelt option fails to compile

- **WHEN** a module writes `use Flezha.SDK.Procedure, name: "p", max_entites: 10`
- **THEN** compilation fails with a `CompileError` naming `max_entites`

#### Scenario: The behaviour form is unchanged

- **WHEN** a module implements `Flezha.SDK.Procedure` with `@behaviour` and hand-written callbacks
- **THEN** it registers and runs exactly as before this change

### Requirement: A `state` block derives field-granular changes, and `defchange` names a hand-written one
`use Flezha.SDK.Procedure` SHALL make `state do … end` and `defchange` available. Inside `state`, `field name, type, default: value` SHALL declare a field, `type` being one of `:integer`, `:float`, `:string`, `:boolean`, `:list`, `:map` or `:term`. The block SHALL generate `init_state/1` returning the map of field defaults, overridable by the author; `state_from_json/1` reading each field's string key and falling back to its default; `apply_change/2` clauses folding `{:set, field, value}` for every field and `{:push, field, item}` (appending) for every `:list` field; and the recorders `set_<field>(state, ctx, value)` and, for `:list` fields, `push_<field>(state, ctx, item)`, each recording its change through `Flezha.SDK.Ctx.apply/3` and answering the new state. `defchange name(state, args…) do body end` SHALL generate the clause `apply_change(state, {:name, args…})` evaluating `body`, and the recorder `name(state, ctx, args…)`, so a mutation spanning several fields is one journal record. A module that has a `state` block and also defines `apply_change/2` or `state_from_json/1` itself SHALL fail to compile, naming the function and the block.

#### Scenario: A field recorder journals one change

- **WHEN** a procedure declares `state do field :qty, :integer, default: 0 end` and `run/3` answers `{:ok, set_qty(state, ctx, 5)}`
- **THEN** one change `{:set, :qty, 5}` is recorded, and a restore folds it back to `%{qty: 5}`

#### Scenario: A named change is one record for several fields

- **WHEN** a procedure declares `defchange record_fill(state, qty, id) do %{state | qty: state.qty + qty, fill_ids: [id | state.fill_ids]} end` and `run/3` calls `record_fill(state, ctx, 3, "f1")`
- **THEN** exactly one change `{:record_fill, 3, "f1"}` is recorded, and a restore reproduces both fields

#### Scenario: JSON state is read by field name

- **WHEN** the controller hands the procedure the JSON object `{"qty": 7}` through `init_values`
- **THEN** `state_from_json/1` answers the defaults with `qty` set to 7

#### Scenario: The block owns apply_change

- **WHEN** a module has a `state` block and defines `apply_change/2`
- **THEN** compilation fails naming `apply_change/2` and the `state` block

### Requirement: The single-procedure short form is three `use` options
`use Flezha.SDK.Procedure` SHALL accept `kind` (`:stream` or `:workflow`), `sink` and `input`. With `sink`, the module SHALL additionally implement `Flezha.SDK.StreamOperative` (for `:stream`, defining `factory_id/0` and `channels/1`) or `Flezha.SDK.WorkflowOperative` (for `:workflow`, defining `factory_id/0` and `procedures/1`), wiring an entry channel named `"input"` (or the `input` value) into the module and the module to the `sink` plugin channel, with `factory_id/0` equal to `name` and every other operative callback left undefined so its default applies. The module SHALL be registered as any operative is: listed under `operatives:` in the child spec, or passed to `register_factory/2`, which reads its kind from the callbacks the macro defined. Each short-form refusal SHALL be a `CompileError` whose message begins with its name: `SinkWithoutId` for `sink` with no `name`, `SinkWithoutKind` for `sink` with no `kind`, `ShorthandKeyWithoutSink` for `kind` or `input` with no `sink`, naming the options. The Elixir SDK cannot see a procedure's output type, owes no `SinkWithoutOutput`, and SHALL NOT list `shorthand_typed_output` in its manifest. The duplicate-id tuple `{:duplicate_factory_id, id}` SHALL keep its shape, and the runner's printed form of it SHALL contain `DuplicateFactoryId`.

#### Scenario: A procedure module is a whole stream operative

- **WHEN** a module writes `use Flezha.SDK.Procedure, name: "upcase", kind: :stream, sink: "out"` and `run/3`, and the child spec lists it under `operatives:`
- **THEN** the runner declares a stream operative `upcase` with entry channel `input` and sink `out`, and no separate `Flezha.SDK.StreamOperative` module exists for it

#### Scenario: A procedure module is a whole workflow operative

- **WHEN** a module writes `use Flezha.SDK.Procedure, name: "place", kind: :workflow, sink: "placed", input: "order"`, is listed under `operatives:`, and an instance is started with init values
- **THEN** `run/3` receives the start values on `order`, what it emits reaches `placed`, and the instance completes

#### Scenario: register reads a short form's kind

- **WHEN** a `kind: :workflow` short form is passed to `register_factory/2`
- **THEN** it is declared as a workflow operative, because the module exports `procedures/1` and no `channels/1`

#### Scenario: A sink without a kind does not compile

- **WHEN** a module writes `use Flezha.SDK.Procedure, name: "upcase", sink: "out"`
- **THEN** compilation fails with a `CompileError` whose message begins `SinkWithoutKind`
