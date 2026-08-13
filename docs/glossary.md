# Flezha Glossary

End-state mental model: **A Controller orchestrates Operatives across Runners.
Each Runner runs Operatives; each Operative contains Procedures; each Procedure
contains Entities.**

## Roles

- **Controller** — the Elixir control plane (`lib/flezha/`). Owns placement,
  WAL persistence, migrations, the CLI/UI surface, and ProcessHub coordination.
  One Controller node serves one or more namespaces; multiple Controller nodes
  serving the same namespace cluster via Erlang distribution + ProcessHub.

- **Runner** — the SDK process (Rust or Java) that connects to a Controller
  and runs user-defined Operatives. Started by the user; opens a control channel +
  data streams to the Controller; its poll workers drain the per-Entity
  mailboxes through the poll core's activation engine. One physical machine
  may run multiple Runners.

- **Cluster directory** — the `:pg`-backed map of which controller nodes hold
  runners for which factories, and which nodes host which namespace hubs
  (`Flezha.Cluster.Directory`). Each node advertises its own fleet truth;
  every node reads it locally, no cluster round. It is what makes every
  controller node a valid entry point: a start or command submitted on a node
  without the namespace's runner/hub is forwarded once to a capable node,
  which re-validates against live truth (staleness costs at most one failed
  forward). Eventually consistent; membership is cleaned by `:pg`'s monitors
  on process or node death.

## Workload abstractions

- **Operative** — the user-facing unit of work; the thing you start and stop.
  An Operative is registered with a factory id; many instances of the same class
  can run across the cluster. Every Operative has a **kind**:

  - **Stream Operative** (kind `stream`, the default) — a long-running,
    always-on streaming unit. Implements the `StreamOperative` trait/interface
    and declares its Channels. This is the original Flezha workload.

  - **Workflow Operative** (kind `workflow`) — an on-demand, terminating,
    possibly-interactive process. Implements the `WorkflowOperative`
    trait/interface. It authors the same Procedures/Channels/Entities a stream
    operative does but runs them **sequentially** (a linear chain with a per-procedure
    count-based barrier; entities within a procedure still run in parallel), can
    **receive input after it starts** (an inbox), and terminates on the first exit
    trigger — chain quiescence, `ctx.complete`, a loop's terminal predicate, a
    `max_duration`, or an external `flz operative stop`. It may declare **saga
    compensation** hooks (reverse-order, per-entity undo on failure) and is
    **durable by default** (opt-out per class): it crash-resumes from the WAL and
    its finished state is retained for a window. `run(ctx, args)` is sugar for a
    single-Procedure, single-Entity workflow operative. Both kinds share the
    `Computation` base trait/interface (whose only shared member is `factory_id()`).
    See [`resilience/workflow_operative.md`](resilience/workflow_operative.md).

  <!-- kind-naming-record:begin -->
  **Why `stream` and `workflow`, not `unbounded` and `bounded`.** Flink uses
  those two words for a property of the *data* — is the input finite — while
  Flezha's distinction is the *execution model*: does one run terminate. The
  words survive here only as adjectives for the **run**, where they are true: a
  workflow operative runs a *bounded* iteration to a terminal state, and a
  stream operative folds an *unbounded* input forever. The caveat that settles
  the question: a durable `PerInput` workflow operative's *input* is unbounded —
  it consumes an inbox for the life of a chat session — and only its *iteration*
  is bounded, so the old kind name was not merely unfamiliar but false. The
  words also remain correct for unrelated capacity concepts such as a bounded mailbox;
  `scripts/kind-naming-allowlist.txt` enumerates every surviving use
  and `scripts/check-kind-naming.sh` fails a kind-sense reintroduction.
  <!-- kind-naming-record:end -->

- **Procedure** — a typed stage inside an Operative that processes inputs from a
  Channel. Implements the `Procedure` trait/interface and declares its `Input`,
  `Output`, `State`, `Change`, and `Clock` types.

- **Entity** — the per-key state-bearing slice of a Procedure. State (WAL,
  snapshot, outbox dedup) lives at the Entity level on disk:
  `app_data/state/{operative_class}/{procedure_id}/{entity_key}.*`.

- **Live read vs time-travel read** — two different questions about one Entity's
  state. A **live read** (`flz state get <operative/procedure[/key]>`) asks the
  runner holding the Entity for its current in-memory value: the freshest value
  there is, possibly *ahead* of what is durable (it includes changes not yet
  flushed), stamped with the `change_seq` it was serialized at. It refuses —
  naming why — when nothing holds the Entity. A **time-travel read**
  (`flz state at <operative/procedure/key> <seq|timestamp>`) asks the
  controller's own WAL for the value at a past point, and needs no runner. Live
  reads are for "what is it now"; time-travel reads are for "what was it then".
  See [`entity_state_live_query.md`](entity_state_live_query.md).

- **Channel** — a typed input/output edge of an Operative. Channels can be entry
  points (input from outside), internal (Procedure → Procedure), or sink (output
  back to the Controller / pipe to another Operative).

- **Edge transform** — a `map` / `filter` / `flat_map` declared on a stream
  handle between two `.into` calls. It costs no Procedure, Entity, mailbox or
  dispatch hop: every transform before the closing `.into` composes into ONE
  closure stored on that consumer edge and evaluated at the routing seam.
  Fusion is therefore structural — `.into` IS the boundary, so there is no
  optimizer and no way to disable chaining. See `docs/stream_transforms.md`.

- **Edge keying (`key_by` / `keyBy`)** — the Entity key function, declared on
  the consumer edge rather than on the Procedure, so a reader of `channels()`
  can see where work fans out. A Procedure reached by a keyed edge is
  dynamic-mode. Total by signature: discard inputs with `filter`, never with a
  keyless return.

- **Merge** — a subscription of two or more Channels to ONE Procedure, each
  supplying a pure mapping from its payload into the Procedure's declared sum
  type `Input` (`c.merge::<I>().from(a, map_a).from(b, map_b).into::<Proc>()`).
  On a stream Operative, entry and inter-procedure Channels are both valid
  sources; a **workflow** Operative merges its procedures' output streams only —
  its spawn-args seed names no Channel, so an entry Channel there is refused at
  definition time. Per-source FIFO is preserved and nothing is ordered across
  sources; `entities_for` is evaluated on the mapped value; mappings run
  identically on live delivery and WAL replay.
  Purely a subscription-time construct — the `Procedure` itself, the journal
  format, and the Controller are all unchanged. See
  [`multi_input_procedures.md`](multi_input_procedures.md).

- **Operative-to-operative messaging** — one Operative's output delivered into
  another Operative's **input stream**, configured as the first-party
  `OperativeForward` plugin on the source (same namespace only; self-targeting
  refused, longer cycles allowed). Delivery is asynchronous and retried until the
  target confirms, so no Overwatch ever blocks on another. The guarantee is
  *derived, never configured*: a source with a transactional outbox gets
  effectively-once across the hop, one without gets at-least-once. Distinct from
  **`:on_message`**, which is synchronous request/reply into the SDK factory's
  `handle_message` callback rather than data flow into the input stream. See
  [`operative_forward_plugin.md`](operative_forward_plugin.md).

- **HTTP poll source** — feeding an Operative's **entry channel** from an HTTP
  resource, configured as the first-party `HttpPoll` plugin. The response body is
  injected as an ordinary journaled entry frame only when its SHA-256 changes,
  plus one unconditional *seed* on every (re)start — which is what converges a
  restarted or handed-over Operative onto current content within one interval,
  and why consumers of a polled channel must be idempotent. See
  [`http_poll_plugin.md`](http_poll_plugin.md).

- **Websocket subscription source** — changing what a live websocket is
  subscribed to *without reconnecting*, by pointing the `Websocket` plugin at a
  URL publishing the desired item set. The provider polls it, diffs it against
  the set it has applied, and sends operator-supplied subscribe/unsubscribe
  templates for the difference on the connection it already holds. A delta
  commits only on an acknowledgement bearing its `req_id`, so retry is nothing
  but re-deriving the same diff next cycle; the applied set is persisted to the
  plugin cspec, so connect and both watchdog tiers re-assert what the socket
  actually holds rather than the boot-time `post_connect`. See
  [`websocket_subscription_source.md`](websocket_subscription_source.md).

- **Dead-letter queue (DLQ)** — the Controller's durable store for poison
  payloads: inputs a Stream Procedure failed on every retry
  (`app_data/dlq/{operative_class}/{procedure_id}/`). A payload is parked
  durably *before* its Entity advances, keeping the min-watermark moving
  without ever silently dropping data; operators inspect/replay/drop entries
  via `flz dlq`. Workflow operatives don't use it — their escape is AbortAll +
  saga compensation. See
  [`resilience/data_durability.md`](resilience/data_durability.md)
  "Dead-letter queue".

- **Timer (per-Entity)** — a deadline a **Procedure** registers on its own
  **Entity**, so it can act because time passed rather than because an input
  arrived. Two kinds: an **event-time** timer fires when that Entity's
  event-time high-water crosses it (so an Entity nothing arrives at never fires
  one), and a **processing-time** timer fires off the poll core's shared
  deferred-wake list onto the Entity's own mailbox (so it *is* the answer to
  "nothing has arrived"). `on_timer` receives the **scheduled** timestamp, never the wall
  clock at firing, which is what keeps it inside the determinism contract. The
  pending set is folded state — registration, cancellation and firing are each
  durable records in the Entity's change log — so a crash-resume owes exactly
  the timers it owed before. See [`entity_timers.md`](entity_timers.md).

## Poll core vocabulary

- **Poll core (`flz_core`)** — the language-agnostic runner kernel: entity
  slots, mailboxes and drain gates, the flusher, credit, the DLQ ladder,
  timers, routing, commands, the workflow driver and the transport loop, in
  one Rust crate. Host SDKs drive it exclusively through
  `poll() → Activation` / `complete(ActivationResult)`; the Rust SDK's typed
  layer is the first host (as a crate), non-Rust SDKs consume the C ABI
  (`flz_core_ffi`). See `docs/sdk_poll_core.md`.
- **Host class** — an operative class declared through the C ABI as data
  (`flz_core_declare_class` JSON): the core owns its kernel behavior; its
  user code runs host-side, reached only through activations. The Java SDK's
  classes are host classes; typed Rust classes are not.
- **Prepare crossing** — the batched `KeyRequest` activation asking a host
  for routing keys and/or event times the core cannot derive from bytes,
  resolved before any mailbox enqueue.
- **Workflow session calls** — `flz_core_workflow_emit/recv/cancelled`: the
  ABI surface a live run-sugar workflow body uses mid-activation (emit
  output, drain the inbox, observe cancellation).
- **Activation** — one unit of work the core hands a host: a per-entity
  batch of inputs and validated timer firings, or one of the five
  host-participating command kinds (restore fold, snapshot request, live
  state read, apply-state, migration serialize). Exclusive per entity: the
  core never hands out two activations for the same entity concurrently.
- **ActivationResult** — the host's completion: per-item effect records
  (host-encoded change bytes, outputs, consumed offset, timer ops,
  checkpoint flag) plus a verdict (`ok | retryable_error |
  non_retryable_error`) that drives the core's retry→park ladder.

## Disambiguating "Operator"

The word "Operator" used to mean "the SDK process" in older Flezha code and
docs. After the rename, that meaning is dropped: **the SDK process is now an
Runner**. The word "Operator" survives only as **ProcessHub-internal
ring-node jargon** — a node in ProcessHub's distribution ring. ProcessHub is a
third-party library; its terminology is not user-facing in Flezha. When the
word appears in `docs/process_hub_architecture.md` or in a doc comment whose
context is ProcessHub, it refers to that internal abstraction, not to the
Runner.

## See also

- `docs/architecture/` — The architectural map: the durable-compute kernel, the
  two profiles, and the diagrams.
- `docs/operative_architecture.md` — Operative/Procedure/Entity deep dive.
- `docs/stream_transforms.md` — edge transforms + edge keying (`key_by`).
- `docs/sdk_protocol_spec.md` — Wire protocol between Runners and the
  Controller.
- `docs/process_hub_architecture.md` — Two-level distribution (ProcessHub picks
  a Controller node; the Controller picks a Runner).
- `docs/entity_state_live_query.md` — reading one Entity's current value
  (`flz state get`), its freshness contract and its refusals.
- `docs/resilience/state_persistence.md` — Per-Entity WAL/snapshot/retention.
