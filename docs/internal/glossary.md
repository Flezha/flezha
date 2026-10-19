# Flezha Glossary

The vocabulary every other document assumes, for relay authors, users and contributors.
In one sentence: **a Controller orchestrates Relays across Runners; each Relay contains
Tasklets; each Tasklet contains Cells.** A runner hosts relays; a relay is the unit you
start. Entries are short and point at the document that
owns the mechanism; for concepts, [`architecture/`](https://github.com/flezha/flezha/blob/main/docs/public/architecture/overview.md) wins.

## Roles

- **Controller** — the Elixir control plane (`lib/flezha/`) and the source of truth: it owns the
  durable store, routes input, places every relay and heals the fleet. Nodes cluster over
  Erlang distribution, with one ProcessHub hub per namespace (`Flezha.Service.HubManager`).
- **Runner** — the SDK process (Rust, Java, Node, Ruby, PHP, Python or Elixir) where user code runs. It dials the
  controller, never the reverse, and is disposable because every state change is mirrored to the
  controller. A process holds one **runner slot**, claimed at `start()` by compare-and-set.
  See [the runner architecture](https://github.com/flezha/flezha/blob/main/docs/public/architecture/runner.md).
- **Stable identity vs session token** — the SDK persists a **stable identity**
  (`FLEZHA_RUNNER_ID_PATH`, default `/var/lib/flezha-runner/id`) that survives restarts; the
  controller mints a **session token** per connection (`AssignRunnerId`) and routes on it, while a
  relay's owner is the stable identity, so a reconnect re-points each relay's session
  without moving its ownership.
- **Namespace** — the hard tenancy boundary: every relay, tasklet, state path, audit record
  and signal carries one (`Flezha.AuthZ.Namespace`); `_system` is the singleton compatibility
  namespace. A runner connects into one namespace; the CLI selects one with `--namespace`.
- **Cluster directory** — the `:pg`-backed map of which nodes hold runners for which factories and
  which node hosts each namespace hub (`Flezha.Cluster.Directory`), read locally on every node.
  A request landing on the wrong node is forwarded once, so every node is a valid entry point.
- **Source adapter / output adapter** — where input comes from and where output goes. Both are
  plugins at the edge (a websocket, an HTTP endpoint, Kafka, Postgres), never core components.

## Workload model

- **Relay** — a processing unit that works like a worker: it keeps its own state and carries
  data through its tasklets. Flezha runs thousands of relays in parallel. A relay is never the SDK process,
  which is a **Runner**. It is the unit you start and stop (`flz relay start|stop|list|info`), registered
  under a **factory id** (`factory_id()` on the Rust `Relay` trait, `factoryId()` on Java's
  `Relay`), with a **run**, `flow` or `steps`, that its class declares in its settings tree and
  its runner announces with the factory id; a start names only the factory, and the run is frozen
  for the instance's life. Where work is kept (`durable`) and sleeping are settings of every class.
- **Sleeping**: an instance whose class keeps its work (`durable` `local` or `remote`) and has been
  idle for its class's `sleep_after` (unless the class says otherwise, never for a flow class and
  six hours for a steps class; `false` never sleeps) ships `IterPark`, is evicted from its runner (`suspended`) and wakes on its next message
  with its Cells restored. A class kept in memory (`durable: false`) has nothing to sleep on.
- **Run: flow / steps**: how a relay runs, the `run` setting of its class. `flow`, the
  default: each input is handled as it arrives, its commit point is an input, or a checkpoint,
  with per-Cell recovery and no barrier, and `exactly` chooses `at_least_once` (the default) or
  `effectively_once`. It runs until it is stopped, a tasklet calls `ctx.complete(result)` or its
  `max_duration_ms` elapses. `steps`: a DAG of tasklets, each run once its producers have
  committed and committed behind a barrier as soon as its cells finish; see **Steps**. A class
  that runs in flow is a **flow relay**, one that runs in steps a **steps relay**.
- **Durable: memory / disk / remote**: where a relay keeps its work, the `durable` setting
  of its class. `false`: in memory only, and a crash starts the work over. `local`, the default:
  on the controller's disk, and a crash resumes from it. `remote`: in the remote copy as well,
  each commit point acknowledged only once the copy holds it (see **Synchronous durability**).
  `local` and `remote` carry `sleep_after` (see **Sleeping**).
- **Steps**: what `run: steps` adds to a relay: each tasklet's committed work is a
  **stage** on disk before the next runs, one run of the DAG is a **pass**, the **task journal**
  records them, a stage that cannot succeed **undoes** the stages before it through their
  `compensate` hooks (`on_failure`: `undo_all`, the default, or `keep_progress`), and
  `flz relay rewind` takes passes back. `each` is the loop shape: `once` (the default),
  `per_input` or `chained`. It ends on chain completion, a tasklet's `ctx.complete(result)`, a
  loop that runs out of work, `max_duration_ms` or `flz relay stop`; a `per_input` loop may
  never end. See [the steps profile](https://github.com/flezha/flezha/blob/main/docs/public/architecture/steps_profile.md).
- **Bounded / unbounded (input)**: plain words for a finite and an endless input, as Flink
  uses them: a file against a live feed. Not a property of a relay.
- **Tasklet** — a typed stage inside a relay, declaring `Input`, `Output`, `State`,
  `Change` and `Clock` types. A tasklet has exactly one output channel, is wired to the others by
  channels, and lives as long as its relay: it is fed, never called and returned from.
- **Cell** — one keyed instance of a tasklet and the unit of durability, idle eviction and
  compaction: its own mailbox, state, WAL, snapshot lineage and outbox cursor. Identified by
  `(relay_class, tasklet_id, cell_key)` and stored under
  `<state_root>/<namespace>/<class>/<tasklet_id>/<cell_key>.*`. A cell exists whether or not its
  tasklet declares state and whether or not it carries a key: a single-mode tasklet holds one
  unkeyed cell.
- **Cell mode** — **single** (one unkeyed cell, the default), **static** (a fixed keyset from
  `cell_keys()`, one input fanning out via `cells_for`) or **dynamic** (one cell per key on
  a keyed edge, created lazily, evicted after `cell_idle_ttl()`).
- **Channel** — the typed edge between stages: **entry** (input from outside), **inter-tasklet**,
  or **plugin-bound** (sent to the plugin layer, declared with `.output(name)`). Terminality is derived from
  the wiring, never declared.
- **Answer channel**: the channel whose output answers a call. A caller may name it
  (`--answer-channel`, `answer_channel` in `Relay.call/6`'s options, or the option of a
  `call(...)` end); without one, the first output the call's input causes answers. The
  answer still reaches the output's plugins and listeners. See [`sending_and_calling.md`](../public/topics/core/sending_and_calling.md#calls).
- **Part**: a preview of the output an invocation is producing, sent from inside `run` with
  `ctx.part(value)` (`part(ctx, value)` in Elixir) and delivered at once to the relay's
  listeners, and to nothing else. It carries its channel, a number from `0` within its attempt
  and the attempt; it is never durable, may be dropped, and never reaches an output plugin or a waiting
  caller. The output is the answer. See [the SDK protocol specification](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/protocol_spec.md#data-stream-payload-id-output_part).
- **Listener**: a reader of a relay's frames, `flz relay listen` or a plugin through
  `Flezha.Service.Listener.listen/4`: its outputs, its parts and its dead-letter notices (or,
  with `--input`, its inputs). Bounded by `listener_pending_bytes` and
  `listener_pending_frames`, it survives the relay sleeping, being rebound, restarted,
  moved or rescaled, ends when the relay ends, and leaves no plugin row behind.
- **Call**: one input an outside caller (`flz relay call`, `Flezha.Service.Relay.call/6`
  or a feeding plugin) sends to a relay, waiting up to a deadline for the first output it
  causes (on the answer channel, when the caller names one); or one request a relay's `call(...)` end sends to another
  relay, whose outcome comes back as a Response. See [`sending_and_calling.md`](../public/topics/core/sending_and_calling.md#calls).
- **Call token**: `flz.reply_to`, the input metadata key the controller mints for each call
  and the runner core carries to every output the input causes. Nothing else can set it, and
  no tasklet, plugin or listener sees it. See [`sending_and_calling.md`](../public/topics/core/sending_and_calling.md#calls).
- **Response**: the outcome of one relay call as the caller receives it: an ordinary input
  carrying the request, the answer or the error that ended the call (a timeout being
  `call.timeout`), the instance that answered and whether the call started it. See
  [`sending_and_calling.md`](../public/topics/core/sending_and_calling.md#calls).
- **Response channel**: the entry channel a `call(...)` end answers, named after the calling
  channel with `.response`; every Response of that end arrives on it, and it may feed any
  tasklet, the calling one included.
- **Edge transform** — a `map` / `filter` / `flat_map` on a stream handle between two `.into`
  calls; everything before the closing `.into` fuses into one closure on that consumer edge and
  costs no tasklet, cell, mailbox or hop. See [`../public/topics/core/streams.md`](../public/topics/core/streams.md).
- **Edge keying (`key_by`)** — the cell key function, declared on the consumer edge rather than
  on the tasklet. A tasklet reached by a keyed edge is dynamic-mode; discard inputs with
  `filter`, never with a keyless return.
- **Merge** — two or more channels subscribed to one tasklet, each with a pure mapping into its
  sum-type `Input`. Per-source FIFO is kept, nothing is ordered across sources, and mappings run
  identically on live delivery and replay. See [`streams.md`](../public/topics/core/streams.md#merged-inputs).
- **`route_latest`** — delivers a relay-wide document to each keyed cell on that key's own
  traffic, never as a remembered-set fan-out.
- **Partition** — one member of a partitioned relay, a flow relay or a `per_input` steps relay: an
  ordinary instance `<id>#pK` with its own Overwatch, registry row, binding, input log, state tree,
  plugins and DLQ identity, and for a steps relay its own journal and pass sheet.
  `flz relay start <factory> --id <id> --partitions N` starts N; `flz relay scale <group> M`
  rescales. See [`../public/topics/core/partitioning.md`](../public/topics/core/partitioning.md).
- **Key group** — one of 256 buckets a routing key hashes into, `fnv1a128(key) mod 256`
  (`flz_core/src/util.rs`, identical in every SDK). A partition owns a contiguous range carried on
  `AddRelay`; the SDK **key-group guard** parks the first frame whose key falls outside it.
- **Group** — the logical relay N partitions make up, named by the base id, of either run. No
  group record is stored: the single `flz relay list` row and `info`'s coverage check derive
  from the members. A steps relay group has no end, result or rollback of its own; each member has.
- **Timer (per-cell)** — a deadline a tasklet registers on its own cell. An **event-time**
  timer fires when the cell's event-time high-water crosses it, a **processing-time** timer from
  the deferred-wake list. `on_timer` receives the
  scheduled timestamp; the pending set is folded
  state, so a crash-resume owes exactly the timers it owed. In a steps relay a firing opens a pass
  at the tasklet that set it. See [`../public/topics/core/timers.md`](../public/topics/core/timers.md).
- **Plugin** — a `%Flezha.Plugin{}` binding a **provider** module to one or more **hooks** on a
  host such as the Overwatch (`:on_input`, `:on_output`, `:on_message`, ...); its provider is
  first-party or comes from a **plugin package** (`.fpz`, placed by `flz plugin install` and
  loaded when the controller starts), and it is enabled once by name in a namespace
  (`flz plugin enable|disable`) and **bound** to each relay it serves (`flz plugin
  bind|unbind`). A provider declared `shareable` runs one copy per controller for every bound
  relay, or one in the cluster with `--single`. See
  [`../public/topics/plugins/installing_plugins.md`](../public/topics/plugins/installing_plugins.md).
- **Hook**: a named point at which the platform calls a plugin, named by the provider callback it
  runs: `on_input`, `on_output`, `on_interval`, `on_message`, `on_snapshot`, `on_terminate`, and
  `on_relay_init` at relay start only. Chosen with `--hook=<name>` on `flz plugin enable` or `bind`,
  with its options written `--<name>.<key>=<value>`, or under `"hooks"` in a plugin spec and
  `hooks:` in a package manifest. `on_interval` needs `interval_ms`; there is no default. A
  plugin with no hooks still feeds its relay through its provider's own worker: hooks decide
  only when the platform calls the plugin.
- **Relay API**: a factory's entry channels as its runner announces them: each one's format,
  whether it answers and in which format, and its description and JSON Schema when declared.
  `flz relay info <id>` shows it, `Relay.call_channel` calls a channel by value, and each
  described channel is an MCP tool named `<factory>_<channel>`. See
  [`sending_and_calling.md`](../public/topics/core/sending_and_calling.md#a-relay-s-api).
- **Relay-to-relay messaging** — one relay's output delivered into another's input
  stream by the `RelayForward` plugin on the source (same namespace, no self-targeting),
  retried until the target confirms. The guarantee is derived: a source whose Cells record effects
  (an effectively-once flow relay or a durable steps relay) gets effectively-once across the hop, any
  other at-least-once. See [`../public/plugins/relay_forward.md`](../public/plugins/relay_forward.md).
- **`:on_message`** — the synchronous request/reply hook (`RelayMessage` /
  `RelayMessageResponse` on the control channel, `flz relay send`), as opposed to data
  flow into the input stream.
- **First-party input plugins** — `HttpPoll` feeds an entry channel from a URL, injecting the
  body only when its SHA-256 changes plus one seed per (re)start, so consumers must be idempotent
  ([`../public/plugins/http_poll.md`](../public/plugins/http_poll.md)); `Websocket` re-subscribes a live socket
  without reconnecting by diffing a polled item set, committing each delta only on an ack carrying
  its `req_id` ([`../public/plugins/websocket_subscription.md`](../public/plugins/websocket_subscription.md)).
- **HTTP server plugin** — the `flezha_http` package's provider `http_server`, built outside this
  repository: every HTTP request becomes one call of the relay it is enabled on, carrying a
  **request envelope** (method, path, query, the allowed headers, the body), and the relay's
  first output answers with an **answer envelope**, a status, headers and a body, or a **stream
  answer** naming a relay whose parts and outputs the plugin writes as server-sent events
  through a listener ([`../public/plugins/http_server.md`](../public/plugins/http_server.md)).

## Durability vocabulary

- **WAL** — the per-cell append-only change log of framed, CRC-protected records, owned by one
  `Flezha.State.WalWriter`; the input log and the task journal share the record format. See [the kernel architecture](https://github.com/flezha/flezha/blob/main/docs/public/architecture/kernel.md).
- **Snapshot / compaction** — on WAL size or age the controller asks the runner for a full state
  snapshot (`CompactCell`) and truncates the log beneath it; a restore (`RestoreInitState`)
  ships the latest snapshot plus the whole WAL tail.
- **Effect record** — under the effectively-once tier, the one WAL record an invocation produces:
  `{changes, outputs, consumed_offset, dlq, event_time}`, appended and fsynced as a unit so
  state, output, progress and a park can never tear apart.
- **Input log** — the offset-addressable durable log every input is normalised into for an
  instance with durable input, a flow relay under the tier or a durable steps relay
  (`Flezha.State.InputLog`): one per instance, append before forward, stamped `flz.offset`,
  deduplicated by `flz.msg_id`, and each reporter (a Cell, or a steps relay's journal) recovers
  from its own consumed offset.
- **At-least-once / effectively-once** — the default tier adds no latency and permits a duplicate
  output on replay; `exactly: effectively_once` in a flow class's run turns on the input log, the
  effect record and the outbox and forces every plugin-bound channel to be `.keyed()`. It is
  at-least-once delivery + a deterministic key + an idempotent output plugin, never exactly-once effects.
- **Checkpoint ack-granularity** — `ack: checkpoint` moves the acknowledgement from per input to
  `ctx.checkpoint()`; the **interval** is the retention floor, the recovery work and the credit
  cadence. A park closes the interval, a restore discards the open one, and
  `outputs: at_checkpoint` with its `max_delay_ms` holds the interval's outputs with its changes.
- **Outbox** — the per-cell record of which outputs are durable and which an output has confirmed
  (`Flezha.State.Outbox`, the `<key>.outbox.dedup` side-file): `outbox_durable_seq` gates the
  flush, `outbox_flushed_seq` advances on the output plugin's confirmation of the `flz.ack` token,
  `outbox_epoch` names the lineage. A restart re-drives what was durable but unconfirmed.
- **Output identity** — the replay-stable stamps on a keyed output: `flz.cell`, `flz.seq`,
  `flz.key`, and `flz.epoch` for a Cell that records effects (the tier, a durable steps relay). `flz.` is reserved; a user emit carrying one is rejected.
- **Epoch** — the lineage counter in the outbox identity. An accident (crash retry, takeover,
  re-drive, `flz dlq replay`) **keeps** it so the output plugin swallows the repeat; a deliberate correction
  (late-input correction, `flz relay replay`) **bumps** it so the new output passes
  dedup and overwrites by natural key. A steps relay rewind bumps only the journal epoch; its re-run's
  output is a newer record and passes on its higher `flz.seq`.
- **Owner / fence / realign** — a relay's **owner** is the stable identity its ownership
  record names (the `ClassRegistry` row its Overwatch writes before `AddRelay`); a frame about
  the relay from a runner of any other identity is refused, counted as `foreign_frame_refused`
  and answered with `RemRelay`. Within that identity exactly one session writes a cell's WAL:
  a takeover is newest-wins and unbinds the old session first, so its stream channels flip
  `fenced?` and the WAL writer refuses an append from a superseded session token before any realign
  decision.
  **Realign** is the writer adopting a current runner's non-contiguous sequence and bumping the epoch.
- **Minimum watermark** — `min(consumed offset)` across a relay's cells, advanced only
  from the post-fsync path. It is the input-log retention floor and what mints source credit, so
  a permanently-failing payload must never freeze it.
- **Credit / credit grant** — byte-credit flow control (`Flezha.State.CreditLedger`): the
  runner→controller window is minted on fsync and returned in batches (`CreditGrant`), the
  source→controller ledger by the watermark, and a sender out of credit **parks**. The
  controller→runner direction is deliberately not credit: past 8 MiB it drops the newest frame,
  which the input log re-drives.
- **Park** — four senses, all "hold, never drop": a **DLQ park** ships a retry-exhausted payload
  durably to the controller (`park_payload` / `ParkPayloadAck`, or inside the effect record)
  before the cell advances; a **credit park** waits for a grant; a **key-group park** refuses a
  mis-routed key; an **`IterPark`** asks for an idle instance of either run to sleep.
- **Dead-letter queue (DLQ)** — the controller's durable store for poison payloads
  (`Flezha.Dlq.Store`, `app_data/dlq/<relay_class>/<tasklet_id>/`): retry, park durably,
  advance. Entries are idempotent on their origin, retention is 30 days and 4 GiB, the surface is
  `flz dlq list|show|replay|drop`; a steps relay parks only an unreadable message and a
  failed turn of a `per_input` keep-progress loop, and otherwise aborts and compensates. Once an
  input's entry is durable, the relay's listeners see a notice carrying `input.dead_lettered`.
  See [`../public/topics/core/durability.md`](../public/topics/core/durability.md).
- **Late-input correction**: a declared `correction_ms` under `exactly: effectively_once` lets an event behind the event-time
  high-water mark rewind the cell to a baseline, replay the retained slice in event-time order
  and republish under a bumped epoch. Requires the tier, an event-time clock and keyed plugin
  channels; the window raises the input-log hold window and the snapshot age floor to meet itself.
- **Live read vs time-travel read** — `flz state get <relay/tasklet[/cell_key]>` asks the
  runner holding the cell for its in-memory value (possibly ahead of what is durable, stamped
  with its `change_seq`) and refuses when nothing holds it; `flz state at <relay/tasklet/
  cell_key> <seq|timestamp>` answers from the controller's WAL. See [the state persistence guide](https://github.com/flezha/flezha/blob/main/docs/internal/state_persistence.md).
- **Remote durability tier** — shipping durable artifacts to Postgres or S3 so a controller that
  dies with its disk is recoverable (`Flezha.State.DurabilityTier`); a production boot must choose
  a backend (`FLEZHA_DURABILITY_BACKEND`; `none` is local-only); local files stay authoritative
  unless another controller shipped a newer copy.
  An **artifact** is one immutable shipped blob; the per-reference **manifest** naming a cut's
  artifacts is written last, so a torn upload is invisible. See [`../public/topics/core/durability.md`](../public/topics/core/durability.md).
- **Synchronous durability** — a class declaring `durable: remote` has each commit point's
  acknowledgement gated on the remote ship (RPO 0): a steps stage barrier, a flow input, or a
  flow checkpoint under `ack: checkpoint`. An instance is refused its start without a durability
  backend (`relay.remote_without_backend`).
- **RPO breach** — local progress unshipped past `rpo_target_ms` (default 120 s): alarm-grade
  `[:flezha, :state, :rpo_exceeded]`, shown by `flz controller status` and `/healthz`. A
  **supersession** (`[:flezha, :state, :claim_superseded]`) is routine on every migration, not a breach.
- **Declared set (durable relay intent)** — what should be running, as ProcessHub's declared
  children per namespace hub (`Flezha.Convergence.Intent`, via `HubManager.declared_relays/1`).
  A wiped runtime registry does not empty it; a deliberate `flz relay stop` removes its entry.
- **Orphan reconcile / start authority** — ProcessHub's round converging running children toward
  the declared set through the one idempotent start authority (the registry claim in ProcessHub's
  `Distributor`, reached identically by the reconcile and by `RelayManager.start_relay/3`).
  The loser answers `:duplicate` after checking the winner is live; `[:flezha, :boot,
  :reconcile_duplicate]` outside a partition recovery means a start bypassed the claim.

## Steps relay vocabulary

- **Task journal** — the per-instance append-only journal of steps relay progress
  (`<state_root>/<namespace>/<instance_id>/<instance_id>/task.journal`, `Flezha.State.TaskJournal`)
  on the WAL substrate under a reserved cell key. Its entries (`task_committed`,
  `iter_committed`, `compensated`, `rewound`) are markers, folded, never truncated; a stage entry
  names each Cell's effect records and their plugin channels and holds no state and no output.
  Where an instance sleeps is not the journal's but its park record's, beside the class's
  declarations.
- **Stage barrier / `TaskCommit`** — when a tasklet's last item settles, the runner ships one
  `TaskCommit` for it `{cells_ran, handoff}`, `cells_ran` naming each Cell's effect records in
  the stage; the controller waits until those records are durable, journals the stage and acks,
  and only then do its outputs reach its children and its plugin outputs leave. Siblings commit independently. A committed
  tasklet is never re-run.
- **Pass sheet** — a steps instance's run state in the runner core (iteration, tasklets in
  flight and done, outputs held for their commit, completion), advanced by events on the thread
  that brings each one; the in-memory form of what the task journal describes.
- **Outbox (steps relay)**: a steps relay Cell's plugin output is written in its own effect record
  and leaves through that Cell writer's outbox, the one a tier flow relay Cell uses, once the durable
  stage marker naming the record has been acked. It carries the stream's identity
  (`flz.cell`, `flz.seq = <record seq>.<emit ordinal>`, `flz.epoch`, `flz.key`, metadata). A
  record no marker names is dropped, so a failed stage delivers nothing.
- **Saga compensation** — on an abnormal exit, committed tasklets' `compensate` hooks run in
  reverse-topological order, each journaled as a `compensated` entry naming its undo record. `OnFailure::UndoAll`
  (default) unwinds every committed iteration, `KeepProgress` only the in-flight one; a hookless
  tasklet is skipped, and an undo handle must live in cell state.
- **Durable loop** — `Each::Once` (default, one pass over the first message), `PerInput`
  (one iteration per message) or `Chained` (the first message, then iteration N+1 seeded by
  N's terminal output into that message's chain). The **iteration boundary**
  (`IterCommit`) is where suspension, the keep-progress unit and rewind attach.
- **Durable input (steps relay)** — a durable steps instance's input, journaled to its input
  log before it is forwarded, as a tier flow relay's is, and deduplicated by a `flz.msg_id` the log
  still retains; a message without an id is never a duplicate. Its iteration commits move the
  log's watermark, and `flz state inbox` renders it. The driver's own queue of arrived messages
  is still called its inbox.
- **Suspension / rehydrate** — between iterations the instance is evicted from its runner entirely
  (`IterPark`, recorded first) while the Overwatch and registry entry remain; a new message or its
  wake deadline, the earliest pending processing-time Cell timer, rehydrates it through the
  ordinary bind path.
- **Rewind** — `flz relay rewind <instance> --to-iteration N` appends a `rewound` marker at
  epoch + 1, compensates the tasklets above N in reverse and resumes waiting. It is
  undo-and-continue: inputs are not replayed, and a tasklet that wrote plugin output without a
  `compensate` hook refuses it. See [the steps relay internals](https://github.com/flezha/flezha/blob/main/docs/internal/resilience/steps_relay.md).

## Runner internals

- **Poll core (`flz_core`)** — the one Rust kernel every host SDK drives through
  `poll() → Activation` / `complete(ActivationResult)`: transport, per-cell ordering, effect
  records, the retry→park ladder, timers, credit, routing and steps runs. Rust consumes
  it as a crate; Java, Node, Ruby and Python through compiled veneers over the C ABI
  (`flz_core_ffi`), Elixir through a Rustler library over the same veneer; PHP through that same ABI directly, with `ext-ffi` and no veneer of its own. See [the poll core page](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/poll_core.md).
- **Host class** — a relay class declared through the C ABI as data
  (`flz_core_declare_class`, JSON, unknown fields refused); its user code runs host-side and is
  reached only through activations. Java, Node, Ruby, PHP and Python classes are host classes; typed
  Rust classes are not.
- **Activation / ActivationResult** — one unit of work the core hands a host, never two at once
  for the same cell (**per-cell exclusivity**, why user code needs no locks): an input batch
  with timer firings, one of five host-participating commands (restore fold, snapshot, live read,
  apply-state, migration serialize), a **prepare crossing** (`KeyRequest`, batched keying) or a
  steps relay step. The result carries change bytes, outputs, consumed offset, timer ops and a
  verdict (`ok | retryable_error | non_retryable_error`).
- **Overflow policy** — what a channel discards when the consumer Cell's bounded mailbox is
  full, and the only question the enum answers: `DropNothing` (nothing — the producer waits for
  room, the default for every channel kind on every SDK), `DropNewest` (the arriving item) or
  `DropOldest` (the queued head). **How** `DropNothing`'s waiting is realized depends on the
  channel kind and is not part of the declaration: on an inter-tasklet channel the producing
  Cell is withheld further work until the push lands; on an entry channel the shared transport
  reader stalls, which carries the backpressure to the controller. Neither discards anything,
  which is what the author declared. Declared per channel with `.on_full(...)` / `.onFull(...)`
  and frozen at registration — an authoring declaration, never configuration. Not to be confused
  with the Kafka producer's `:queue_overflow`, the observability bus's per-sink `overflow:`, or
  the transport write buffer: separate mechanisms. `:queue_overflow` is configuration and
  answers the same question about a producer queue, so it spells its three values the same way
  (`:drop_nothing` / `:drop_newest` / `:drop_oldest`); the bus's `overflow:` is `:drop | :block`
  and keeps its own vocabulary.
- **Deferred wake** — the one ordered list in the poll core holding retry backoffs and
  processing-time timer deadlines; it owns no thread, and a due entry is promoted by the next
  thread that enters or wakes inside `poll`.
- **Retry→park ladder** — a `retryable_error` re-hands the same item after `base × 2^(n-1)` ± 50 %
  jitter (capped); a `non_retryable_error` or a failing final attempt parks it. A panic stays retryable.
- **Control channel vs data streams** — a runner session holds one **control channel**
  (`Flezha.Runner.ControlChannel`: commands, commits, `control_ping`) and N pairs of **data
  streams** (`Flezha.Runner.StreamChannel`: primary `0x01` carries input to the runner, secondary
  `0x02` carries state changes, outputs and the data heartbeat back). See [the SDK protocol specification](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/protocol_spec.md).

- **Served group** — a factory a controller declares to a namespace's hub because its own fleet
  can run it. A relay's group is its factory id, and the hub places the relay among the
  controllers serving that group and no others, so a relay is never assigned to a controller
  that cannot run it. A controller serves a factory while `placeable_runners/3` answers for it,
  which excludes a runner advertising the factory at a language other than the established one.
  `Flezha.Cluster.GroupedHashing` is the distribution strategy that reads the table; a relay
  with no factory carries no group and is placed over every controller in the hub.
- **Withdrawal grace** — how long a controller keeps declaring a factory after its last runner for
  it goes (`FLEZHA_GROUP_WITHDRAW_GRACE_MS`, default 5 s). A runner back inside the grace moves no
  relay at all; past it the hub reassigns them to a controller that can still run the factory,
  through the migration-consent gate with handover. A disconnect the controller asked for
  (`flz runner disconnect`, a namespace stop) withdraws at once. While no controller serves a
  factory its relays stay where they are, alive and unbound.

## Control-plane liveness

- **Overwatch** — `Flezha.Relay.Overwatch`, the controller-side process owning one relay
  instance: it binds a runner, owns the plugins, journals every input and receives every output
  on its way to an output plugin.
- **Convergence loop** — `Flezha.Convergence.Loop`, the controller's only self-healer. Every 5 s
  and on declared events it runs `Intent → Probe → Diff → Planner → Heal` and publishes a
  `Convergence.Snapshot` that `/healthz` and the CLI read.
- **Chain of custody** — sensors sense, exactly one actor acts, `/healthz` reads the actor's
  snapshot, the orchestrator restarts on `/healthz`. `State.Watchdog`, `StallClassifier` and
  `LivenessCounters` emit facts and invoke no remedy; `Convergence.Loop` is the actor.
- **Producing-liveness** — health asks "is work flowing" with an *is it being fed?* precondition:
  an idle cell is healthy, a fed-but-flat one is stuck (`wal_frozen`, `watermark_frozen`,
  `outbox_flat`; the deepest wins). An **intent mismatch** (state on disk for a relay absent
  from the declared set) is `200 degraded`, never `503`: a restart cannot heal a wiped registry.
- **Stall localization** — `StallClassifier` compares two counter snapshots and returns the deepest
  wedged layer (`driver_loop_frozen` → `nif_queue_backing_up` → `socket_write_blocked` →
  `runner_not_consuming` → `runner_pool_wedged` → `channel_not_sending` → `relay_not_forwarding`),
  emitting `[:flezha, :data_plane, :stall_localized]` on a change.
- **Fabric wedge** — a stall inside the controller's own NIF driver thread (`driver_loop_frozen`,
  `nif_queue_backing_up`) that no BEAM-level heal reaches; sustained for `FLEZHA_FABRIC_503_POLLS`
  polls (default 3, `0` disables) it is `/healthz` `503 data_plane_fabric_wedged`. A wedged
  *runner* is evicted and rebound instead.
- **Sensor freshness** — a periodic sensor stamps its last completed pass into the snapshot
  (`Convergence.Snapshot.observe_sensor/3`); older than 3× its cadence reads as
  `200 degraded, sensor_stale`. Never stamped is not stale: boot is not a wedge.
- **`/healthz`** — opt-in (`FLEZHA_HEALTHZ_ENABLED`, port 9569): `healthy`, `recovering` and
  `degraded` are `200`; `unhealthy` `503` is reserved for a stopped convergence loop or a sustained
  fabric wedge, the two things only a process restart fixes.
- **`/readyz`** — the same listener, the other question. `/healthz` answers whether the kubelet
  should restart this node; `/readyz` answers whether traffic should be routed to it, and so must
  refuse for exactly the window `/healthz` keeps answering `200`. It is `Flezha.Boot.Readiness`'s
  verdict folded from three preconditions — the boot sequence completed, every hub out of the
  recovery state machine, the durable registry reloaded — `200` only on `:ready`, `503` otherwise
  with the unmet rows named. Neither endpoint is derived from the other.

## Authentication and authorization

- **Enforcement posture** — `Flezha.AuthZ.enforcement_posture/0`: `:open` (the explicit
  `FLEZHA_AUTHZ_ENABLED=false` opt-out, warned every boot), `:bootstrap_pending` (`admin@_system`
  not yet claimed; administration verbs refuse naming `flz auth bootstrap` while dataflow continues) or
  `:enforced`. Enforcement is the production default.
- **Principal / role / token** — a **principal** is a verified identity string
  (`Flezha.AuthZ.Principal`); a **role** is one of `viewer`, `app`, `ops`, `admin`,
  `runner`, `spawner`, `stopper` (`flz role bind|unbind|list`), and an identity holds a
  SET of roles in ONE namespace, an action being allowed when any of them grants it;
  a **token** is a scoped credential (`flz token issue|revoke|list`) presented as
  `--token` or `FLEZHA_TOKEN`, carrying one role and judged by that role alone. See
  [`../public/topics/core/authorization.md`](../public/topics/core/authorization.md).
- **Transport-bound principal** — identity minted from the authenticated connection, never from a
  caller-supplied value: the verified token for CLI callers, `runner@<namespace>` for runner
  connections (role `runner` until a user binds it, to `spawner` and `stopper` for
  example; a connection holds only bound roles that grant `runner.connect`).
  `source: :internal` is reachable only from inside the controller BEAM.
- **Namespace claim** — the `Flezha.State.Paths` record that one relay class's durable
  state belongs to one namespace (`<state_root>/<namespace>/<class>/`): recorded at bind,
  derivable from disk, refused on conflict; a class under `_system` is adopted by the first claimant.
- **Break-glass (emergency rebootstrap)** — with the controller stopped, `flz auth rebootstrap`
  writes `app_data/auth/REBOOTSTRAP`; the next boot unbinds the stale `admin@_system`, revokes
  its tokens, re-enters the bootstrap flow and emits an auditable `:rebootstrap_recovery` event.
- **Audit log** — the durable JSONL record of every authorized control-plane action
  (`Flezha.Service.AuditLog`, `flz audit list|read|tail|stats|rotate`).

## See also

- [The architecture overview](https://github.com/flezha/flezha/blob/main/docs/public/architecture/overview.md) — roles, the workload model, the eight kernel invariants.
- [The Rust relay architecture](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/rust_relay_architecture.md) — Relay/Tasklet/Cell authoring.
- [The SDK protocol specification](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/protocol_spec.md) — the wire protocol between Runners and the Controller.
- [`resilience/overview.md`](../public/architecture/overview.md) — what survives what.
