# Flezha Glossary

The vocabulary every other document assumes, for operative authors, operators and contributors.
In one sentence: **a Controller orchestrates Operatives across Runners; each Operative contains
Procedures; each Procedure contains Entities.** Entries are short and point at the document that
owns the mechanism; for concepts, [`architecture/`](architecture/README.md) wins.

## Roles

- **Controller** — the Elixir control plane (`lib/flezha/`) and the source of truth: it owns the
  durable store, routes input, places every operative and heals the fleet. Nodes cluster over
  Erlang distribution, with one ProcessHub hub per namespace (`Flezha.Service.HubManager`).
- **Runner** — the SDK process (Rust, Java or Elixir) where user code runs. It dials the
  controller, never the reverse, and is disposable because every state change is mirrored to the
  controller. A process holds one **runner slot**, claimed at `start()` by compare-and-set.
  See [`architecture/runner.md`](architecture/runner.md).
- **Stable identity vs session token** — the SDK persists a **stable identity**
  (`FLEZHA_RUNNER_ID_PATH`, default `/var/lib/flezha-runner/id`) that survives restarts; the
  controller mints a **session token** per connection (`AssignRunnerId`) and keys routing and the
  WAL owner on it, so a reconnect is a binding swap. See [`internals/runner_identity.md`](internals/runner_identity.md).
- **Namespace** — the hard tenancy boundary: every operative, procedure, state path, audit record
  and signal carries one (`Flezha.AuthZ.Namespace`); `_system` is the singleton compatibility
  namespace. A runner connects into one namespace; the CLI selects one with `--namespace`.
- **Cluster directory** — the `:pg`-backed map of which nodes hold runners for which factories and
  which node hosts each namespace hub (`Flezha.Cluster.Directory`), read locally on every node.
  A request landing on the wrong node is forwarded once, so every node is a valid entry point.
- **Native transport** — the in-node `:native` mode of the Elixir SDK: pid-backed
  `Flezha.Transport.Local.Stream` handles and `{:flz_data, ...}` messages instead of a socket. A
  standalone Elixir runner speaks the binary protocol over UDS/QUIC. See [`sdk/elixir_sdk.md`](sdk/elixir_sdk.md).
- **Source adapter / sink adapter** — where input comes from and where output goes. Both are
  plugins at the edge (a websocket, an HTTP endpoint, Kafka, Postgres), never core components.

## Workload model

- **Operative** — the unit you start and stop (`flz operative start|stop|list|info`), registered
  under a **factory id** (`factory_id()` on both Rust traits, `factoryId()` on Java's `Computation`),
  with a **kind** frozen at start: `stream` (default) or `workflow` (`Flezha.Operative.Kind`).
- **Stream operative** — the always-on kind: an endless input, no barrier, per-entity recovery.
  Implements `StreamOperative`. See [`architecture/stream_profile.md`](architecture/stream_profile.md).
- **Workflow operative** — the terminating kind: a DAG of procedures run level by level behind a
  commit barrier, with an inbox, saga compensation, a durable loop and suspension. Implements
  `WorkflowOperative`; ends on chain completion, `ctx.complete`, a loop's terminal predicate,
  `max_duration` or `flz operative stop`. `run(ctx, args)` is single-procedure, single-entity
  sugar. See [`architecture/workflow_profile.md`](architecture/workflow_profile.md).
- **Procedure** — a typed stage inside an operative, declaring `Input`, `Output`, `State`,
  `Change` and `Clock` types. A procedure has exactly one output channel.
- **Entity** — the per-key state-bearing slice of a procedure and the unit of durability (one WAL,
  one snapshot lineage, one outbox cursor), identified by `(operative_class, procedure_id,
  entity_key)` and stored under `<state_root>/<namespace>/<class>/<procedure_id>/<entity_key>.*`.
- **Entity mode** — **single** (one unkeyed entity, the default), **static** (a fixed keyset from
  `entity_keys()`, one input fanning out via `entities_for`) or **dynamic** (one entity per key on
  a keyed edge, created lazily, evicted after `entity_idle_ttl()`). Workflows refuse dynamic.
- **Channel** — the typed edge between stages: **entry** (input from outside), **inter-procedure**,
  or **plugin-bound** (output to a sink, declared with `.sink(name)`). Terminality is derived from
  the wiring, never declared.
- **Edge transform** — a `map` / `filter` / `flat_map` on a stream handle between two `.into`
  calls; everything before the closing `.into` fuses into one closure on that consumer edge and
  costs no procedure, entity, mailbox or hop. See [`sdk/stream_transforms.md`](sdk/stream_transforms.md).
- **Edge keying (`key_by`)** — the entity key function, declared on the consumer edge rather than
  on the procedure. A procedure reached by a keyed edge is dynamic-mode; discard inputs with
  `filter`, never with a keyless return.
- **Merge** — two or more channels subscribed to one procedure, each with a pure mapping into its
  sum-type `Input`. Per-source FIFO is kept, nothing is ordered across sources, and mappings run
  identically on live delivery and replay. See [`sdk/multi_input_procedures.md`](sdk/multi_input_procedures.md).
- **`route_latest`** — delivers an operative-wide document to each keyed entity on that key's own
  traffic, never as a remembered-set fan-out.
- **Partition** — one member of a partitioned stream operative: an ordinary instance `<id>#pK`
  with its own Overwatch, registry row, binding, input log, state tree, plugins and DLQ identity.
  `flz operative start <id> <factory> --partitions N` starts N; `flz operative scale <group> M`
  rescales. See [`sdk/partitioned_operatives.md`](sdk/partitioned_operatives.md).
- **Key group** — one of 256 buckets a routing key hashes into, `fnv1a128(key) mod 256`
  (`flz_core/src/util.rs`, identical in every SDK). A partition owns a contiguous range carried on
  `AddOperative`; the SDK **key-group guard** parks the first frame whose key falls outside it.
- **Group** — the logical operative N partitions make up, named by the base id. No group record is
  stored: the single `flz operative list` row and `info`'s coverage check derive from the members.
- **Timer (per-entity)** — a deadline a procedure registers on its own entity. An **event-time**
  timer fires when the entity's event-time high-water crosses it, a **processing-time** timer from
  the deferred-wake list. `on_timer` receives the scheduled timestamp; the pending set is folded
  state, so a crash-resume owes exactly the timers it owed. See [`sdk/entity_timers.md`](sdk/entity_timers.md).
- **Plugin** — a `%Flezha.Plugin{}` binding a **provider** module to one or more **extension
  points** on a host such as the Overwatch (`:on_input`, `:on_output`, `:on_message`, ...); installed
  from a `.fpz` (`flz plugin install`), attached per operative (`flz plugin enable|disable`).
  See [`plugins/lifecycle.md`](plugins/lifecycle.md).
- **Operative-to-operative messaging** — one operative's output delivered into another's input
  stream by the `OperativeForward` plugin on the source (same namespace, no self-targeting),
  retried until the target confirms. The guarantee is derived: a transactional-outbox source gets
  effectively-once across the hop, any other at-least-once. See [`plugins/operative_forward.md`](plugins/operative_forward.md).
- **`:on_message`** — the synchronous request/reply extension point (`OperativeMessage` /
  `OperativeMessageResponse` on the control channel, `flz operative send`), as opposed to data
  flow into the input stream. See [`plugins/message_handlers.md`](plugins/message_handlers.md).
- **First-party source plugins** — `HttpPoll` feeds an entry channel from a URL, injecting the
  body only when its SHA-256 changes plus one seed per (re)start, so consumers must be idempotent
  ([`plugins/http_poll.md`](plugins/http_poll.md)); `Websocket` re-subscribes a live socket
  without reconnecting by diffing a polled item set, committing each delta only on an ack carrying
  its `req_id` ([`plugins/websocket_subscription.md`](plugins/websocket_subscription.md)).

## Durability vocabulary

- **WAL** — the per-entity append-only change log of framed, CRC-protected records, owned by one
  `Flezha.State.WalWriter`; the input log, proc journal and durable inbox share the record
  format. See [`architecture/kernel.md`](architecture/kernel.md).
- **Snapshot / compaction** — on WAL size or age the controller asks the runner for a full state
  snapshot (`CompactEntity`) and truncates the log beneath it; a restore (`RestoreInitState`)
  ships the latest snapshot plus the whole WAL tail.
- **Effect record** — under the effectively-once tier, the one WAL record an invocation produces:
  `{changes, outputs, consumed_offset, dlq, event_time}`, appended and fsynced as a unit so
  state, output, progress and a park can never tear apart.
- **Input log** — the offset-addressable durable log every source is normalised into under the
  tier (`Flezha.State.InputLog`): append before forward, and each entity recovers from its own
  consumed offset.
- **At-least-once / effectively-once** — the default tier adds no latency and permits a duplicate
  output on replay; `effectively_once()` on the channel builder turns on the input log, the
  effect record and the outbox and forces every plugin-bound channel to be `.keyed()`. It is
  at-least-once delivery + a deterministic key + an idempotent sink, never exactly-once effects.
- **Checkpoint ack-granularity** — `checkpoint_ack()` moves the acknowledgement from per input to
  `ctx.checkpoint()`; the **interval** is the retention floor, the recovery work and the credit
  cadence. A park closes the interval, a restore discards the open one, and
  `outputs_at_checkpoint(max_delay_ms)` holds the interval's outputs with its changes.
- **Outbox** — the per-entity record of which outputs are durable and which a sink has confirmed
  (`Flezha.State.Outbox`, the `<key>.outbox.dedup` side-file): `outbox_durable_seq` gates the
  flush, `outbox_flushed_seq` advances on the sink's confirmation of the `flz.ack` token,
  `outbox_epoch` names the lineage. A restart re-drives what was durable but unconfirmed.
- **Output identity** — the replay-stable stamps on a keyed output: `flz.entity`, `flz.seq`,
  `flz.key`, and under the tier `flz.epoch`. `flz.` is reserved; a user emit carrying one is rejected.
- **Epoch** — the lineage counter in the outbox identity. An accident (crash retry, takeover,
  re-drive, `flz dlq replay`) **keeps** it so the sink swallows the repeat; a deliberate correction
  (late-input correction, `flz operative replay`, a rewind) **bumps** it so the new output passes
  dedup and overwrites by natural key.
- **Owner / fence / realign** — exactly one runner session writes an entity's WAL. A takeover is
  newest-wins and unbinds the old session first: its stream channels flip `fenced?`, and the WAL
  writer refuses an append from a superseded session token before any realign decision.
  **Realign** is the writer adopting a current runner's non-contiguous sequence and bumping the epoch.
- **Minimum watermark** — `min(consumed offset)` across an operative's entities, advanced only
  from the post-fsync path. It is the input-log retention floor and what mints source credit, so
  a permanently-failing payload must never freeze it.
- **Credit / credit grant** — byte-credit flow control (`Flezha.State.CreditLedger`): the
  runner→controller window is minted on fsync and returned in batches (`CreditGrant`), the
  source→controller ledger by the watermark, and a sender out of credit **parks**. The
  controller→runner direction is deliberately not credit: past 8 MiB it drops the newest frame,
  which the input log re-drives.
- **Park** — four senses, all "hold, never drop": a **DLQ park** ships a retry-exhausted payload
  durably to the controller (`park_payload` / `ParkPayloadAck`, or inside the effect record)
  before the entity advances; a **credit park** waits for a grant; a **key-group park** refuses a
  mis-routed key; an **`IterPark`** suspends a workflow loop.
- **Dead-letter queue (DLQ)** — the controller's durable store for poison payloads
  (`Flezha.Dlq.Store`, `app_data/dlq/<operative_class>/<procedure_id>/`): retry, park durably,
  advance. Entries are idempotent on their origin, retention is 30 days and 4 GiB, the surface is
  `flz dlq list|show|replay|drop`; workflows abort-and-compensate instead. See [`resilience/data_durability.md`](resilience/data_durability.md).
- **Late-input correction** — a declared `correction_window()` lets an event behind the event-time
  high-water mark rewind the entity to a baseline, replay the retained slice in event-time order
  and republish under a bumped epoch. Requires the tier, an `EventTimeFrom` clock and keyed plugin
  channels; the window raises the input-log hold window and the snapshot age floor to meet itself.
- **Live read vs time-travel read** — `flz state get <operative/procedure[/entity_key]>` asks the
  runner holding the entity for its in-memory value (possibly ahead of what is durable, stamped
  with its `change_seq`) and refuses when nothing holds it; `flz state at <operative/procedure/
  entity_key> <seq|timestamp>` answers from the controller's WAL. See [`internals/state_persistence.md`](internals/state_persistence.md).
- **Remote durability tier** — shipping durable artifacts to Postgres or S3 so a controller that
  dies with its disk is recoverable (`Flezha.State.DurabilityTier`); a production boot must choose
  a backend (`FLEZHA_DURABILITY_BACKEND`; `none` is local-only); local files stay authoritative.
  An **artifact** is one immutable shipped blob; the per-reference **manifest** naming a cut's
  artifacts is written last, so a torn upload is invisible. See [`operations/remote_durability_tier.md`](operations/remote_durability_tier.md).
- **Synchronous durability** — a workflow class declaring `SYNC_DURABILITY` / `syncDurability()`
  has each commit barrier's ack gated on the remote ship (RPO 0). Stream classes are refused it.
- **RPO breach** — local progress unshipped past `rpo_target_ms` (default 120 s): alarm-grade
  `[:flezha, :state, :rpo_exceeded]`, shown by `flz controller status` and `/healthz`. A
  **supersession** (`[:flezha, :state, :claim_superseded]`) is routine on every migration, not a breach.
- **Declared set (durable operator intent)** — what should be running, as ProcessHub's declared
  children per namespace hub (`Flezha.Convergence.Intent`, via `HubManager.declared_operatives/1`).
  A wiped runtime registry does not empty it; a deliberate `flz operative stop` removes its entry.
- **Orphan reconcile / start authority** — ProcessHub's round converging running children toward
  the declared set through the one idempotent start authority (the registry claim in ProcessHub's
  `Distributor`, reached identically by the reconcile and by `OperativeManager.start_operative/3`).
  The loser answers `:duplicate` after checking the winner is live; `[:flezha, :boot,
  :reconcile_duplicate]` outside a partition recovery means a start bypassed the claim. See [`internals/process_hub.md`](internals/process_hub.md).

## Workflow vocabulary

- **Proc journal** — the per-instance append-only journal of workflow progress
  (`<state_root>/<instance_id>/<instance_id>/proc.journal`, `Flezha.State.ProcJournal`) on the WAL
  substrate under a reserved entity key. Its entries (`proc_committed`, `outbox_flushed`,
  `iter_committed`, `parked`, `compensated`, `rewound`) are folded, never truncated.
- **Level barrier / `ProcCommit`** — every entity of every procedure in a level joins before the
  SDK ships one `ProcCommit` per procedure `{entities_ran, handoff, staged outbox}`; the controller
  journals it and acks, and only then are the children runnable. A committed procedure is never re-run.
- **Transactional outbox** — inside a structured procedure `ctx.emit` stages rather than sends; the
  staged outbox rides the `ProcCommit` and is flushed after the journal fsync, stamped
  `flz.seq = <proc_idx>.<seq_in_proc>`. The journal, not memory, decides what is owed.
- **Saga compensation** — on an abnormal exit, committed procedures' `compensate` hooks run in
  reverse-topological order, each journaled with its undo outbox. `RollbackMode::AllOrNothing`
  (default) unwinds every committed iteration, `KeepProgress` only the in-flight one; a hookless
  procedure is skipped, and an undo handle must live in entity state.
- **Durable loop** — `LoopMode::Once` (default), `PerInput` (one iteration per inbox item) or
  `Chained` (iteration N+1 seeded by N's terminal output). The **iteration boundary**
  (`IterCommit`) is where suspension, the keep-progress unit and rewind attach.
- **Durable inbox** — a workflow instance's input, journaled to `inbox.wal` (`Flezha.State.ProcInbox`)
  before it is forwarded and deduplicated by message id or payload hash within the undelivered window.
- **Suspension / rehydrate** — between iterations the instance is evicted from its runner entirely
  (`IterPark`, journaled first) while the Overwatch and registry entry remain; a new message or a
  durable wake timer rehydrates it through the ordinary bind path.
- **Rewind** — `flz operative rewind <instance> --to-iteration N` appends a `rewound` marker at
  epoch + 1, compensates the procedures above N in reverse and resumes waiting. It is
  undo-and-continue: inputs are not replayed, and a procedure that staged output without a
  `compensate` hook refuses it. See [`resilience/workflow_operative.md`](resilience/workflow_operative.md).

## Runner internals

- **Poll core (`flz_core`)** — the one Rust kernel every host SDK drives through
  `poll() → Activation` / `complete(ActivationResult)`: transport, per-entity ordering, effect
  records, the retry→park ladder, timers, credit, routing and the workflow driver. Rust consumes
  it as a crate, Java through the C ABI (`flz_core_ffi`); the Elixir SDK is its own core and
  inherits nothing from it. See [`sdk/poll_core.md`](sdk/poll_core.md).
- **Host class** — an operative class declared through the C ABI as data
  (`flz_core_declare_class`, JSON, unknown fields refused); its user code runs host-side and is
  reached only through activations. Java classes are host classes; typed Rust classes are not.
- **Activation / ActivationResult** — one unit of work the core hands a host, never two at once
  for the same entity (**per-entity exclusivity**, why user code needs no locks): an input batch
  with timer firings, one of five host-participating commands (restore fold, snapshot, live read,
  apply-state, migration serialize), a **prepare crossing** (`KeyRequest`, batched keying) or a
  workflow step. The result carries change bytes, outputs, consumed offset, timer ops and a
  verdict (`ok | retryable_error | non_retryable_error`).
- **Deferred wake** — the one ordered list in the poll core holding retry backoffs and
  processing-time timer deadlines; it owns no thread, and a due entry is promoted by the next
  thread that enters or wakes inside `poll`.
- **Retry→park ladder** — a `retryable_error` re-hands the same item after `base × 2^(n-1)` ± 50 %
  jitter (capped); a `non_retryable_error` or a failing final attempt parks it. A panic stays retryable.
- **Control channel vs data streams** — a runner session holds one **control channel**
  (`Flezha.Runner.ControlChannel`: commands, commits, `control_ping`) and N pairs of **data
  streams** (`Flezha.Runner.StreamChannel`: primary `0x01` carries input to the runner, secondary
  `0x02` carries state changes, outputs and the data heartbeat back). See [`sdk/protocol_spec.md`](sdk/protocol_spec.md).

## Control-plane liveness

- **Overwatch** — `Flezha.Operative.Overwatch`, the controller-side process owning one operative
  instance: it binds a runner, owns the plugins, journals every input and receives every output
  on its way to a sink.
- **Convergence loop** — `Flezha.Convergence.Loop`, the controller's only self-healer. Every 5 s
  and on declared events it runs `Intent → Probe → Diff → Planner → Heal` and publishes a
  `Convergence.Snapshot` that `/healthz` and the CLI read. See [`internals/convergence_loop.md`](internals/convergence_loop.md).
- **Chain of custody** — sensors sense, exactly one actor acts, `/healthz` reads the actor's
  snapshot, the orchestrator restarts on `/healthz`. `State.Watchdog`, `StallClassifier` and
  `LivenessCounters` emit facts and invoke no remedy; `Convergence.Loop` is the actor.
- **Producing-liveness** — health asks "is work flowing" with an *is it being fed?* precondition:
  an idle entity is healthy, a fed-but-flat one is stuck (`wal_frozen`, `watermark_frozen`,
  `outbox_flat`; the deepest wins). An **intent mismatch** (state on disk for an operative absent
  from the declared set) is `200 degraded`, never `503`: a restart cannot heal a wiped registry.
- **Stall localization** — `StallClassifier` compares two counter snapshots and returns the deepest
  wedged layer (`driver_loop_frozen` → `nif_queue_backing_up` → `socket_write_blocked` →
  `runner_not_consuming` → `runner_pool_wedged` → `channel_not_sending` → `operative_not_forwarding`),
  emitting `[:flezha, :data_plane, :stall_localized]` on a change. See [`internals/transport_liveness.md`](internals/transport_liveness.md).
- **Fabric wedge** — a stall inside the controller's own NIF driver thread (`driver_loop_frozen`,
  `nif_queue_backing_up`) that no BEAM-level heal reaches; sustained for `FLEZHA_FABRIC_503_POLLS`
  polls (default 3, `0` disables) it is `/healthz` `503 data_plane_fabric_wedged`. A wedged
  *runner* is evicted and rebound instead.
- **Sensor freshness** — a periodic sensor stamps its last completed pass into the snapshot
  (`Convergence.Snapshot.observe_sensor/3`); older than 3× its cadence reads as
  `200 degraded, sensor_stale`. Never stamped is not stale: boot is not a wedge.
- **Idle episode** — one stretch of silence on an entry channel, reported once by the SDK
  (`on_entry_idle`) when the gap since the last frame first exceeds an application-registered
  threshold, never before the first frame. See [`sdk/entry_input_idle.md`](sdk/entry_input_idle.md).
- **`/healthz`** — opt-in (`FLEZHA_HEALTHZ_ENABLED`, port 9569): `healthy`, `recovering` and
  `degraded` are `200`; `unhealthy` `503` is reserved for a stopped convergence loop or a sustained
  fabric wedge, the two things only a process restart fixes.

## Authentication and authorization

- **Enforcement posture** — `Flezha.AuthZ.enforcement_posture/0`: `:open` (the explicit
  `FLEZHA_AUTHZ_ENABLED=false` opt-out, warned every boot), `:bootstrap_pending` (`admin@_system`
  not yet claimed; operator verbs refuse naming `flz auth bootstrap` while dataflow continues) or
  `:enforced`. Enforcement is the production default.
- **Principal / role / token** — a **principal** is a verified identity string
  (`Flezha.AuthZ.Principal`); a **role** is one of `viewer`, `operator`, `admin`, `runner`
  (`flz role bind|unbind|list`); a **token** is a scoped credential (`flz token issue|revoke|list`)
  presented as `--token` or `FLEZHA_TOKEN`. See [`operations/authn_authz.md`](operations/authn_authz.md).
- **Transport-bound principal** — identity minted from the authenticated connection, never from a
  caller-supplied value: the verified token for CLI callers, `runner@<namespace>` (role `runner`)
  for runner connections. `source: :internal` is reachable only from inside the controller BEAM.
- **Namespace claim** — the `Flezha.State.Registry` record that one operative class's durable
  state belongs to one namespace (`<state_root>/<namespace>/<class>/`): recorded at bind,
  derivable from disk, refused on conflict; a class under `_system` is adopted by the first claimant.
- **Break-glass (emergency rebootstrap)** — with the controller stopped, `flz auth rebootstrap`
  writes `app_data/auth/REBOOTSTRAP`; the next boot unbinds the stale `admin@_system`, revokes
  its tokens, re-enters the bootstrap flow and emits an auditable `:rebootstrap_recovery` event.
- **Audit log** — the durable JSONL record of every authorized control-plane action
  (`Flezha.Service.AuditLog`, `flz audit list|read|tail|stats|rotate`). See [`operations/audit_log.md`](operations/audit_log.md).

## Disambiguating "Operator"

In Flezha an **operator** is a person running the controller — the audience of the `operations/`
documents, the subject of "operator verbs" and "operator replay", and the `operator` AuthZ role.
It never means the SDK process: that is a **Runner**. "Operator intervention" means the human.

## See also

- [`architecture/README.md`](architecture/README.md) — roles, the workload model, the eight kernel invariants.
- [`sdk/rust_operative_architecture.md`](sdk/rust_operative_architecture.md) — Operative/Procedure/Entity authoring.
- [`sdk/protocol_spec.md`](sdk/protocol_spec.md) — the wire protocol between Runners and the Controller.
- [`internals/process_hub.md`](internals/process_hub.md) — ProcessHub picks a node, the node picks a Runner.
- [`resilience/overview.md`](resilience/overview.md) — what survives what.
