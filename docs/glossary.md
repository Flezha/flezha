# Flezha Glossary

End-state mental model: **A Controller orchestrates Operatives across Executors.
Each Executor runs Operatives; each Operative contains Operations; each Operation
contains Segments.**

## Roles

- **Controller** — the Elixir control plane (`lib/flezha/`). Owns placement,
  WAL persistence, migrations, the CLI/UI surface, and ProcessHub coordination.
  One Controller node serves one or more namespaces; multiple Controller nodes
  serving the same namespace cluster via Erlang distribution + ProcessHub.

- **Executor** — the SDK process (Rust or Java) that connects to a Controller
  and runs user-defined Operatives. Started by the user; opens a control channel +
  data streams to the Controller; runs the work-stealing pool that dispatches
  per-Segment mailboxes. One physical machine may run multiple Executors.

## Workload abstractions

- **Operative** — the user-facing unit of work; the thing you start and stop.
  An Operative is registered with a factory id; many instances of the same class
  can run across the cluster. Every Operative has a **kind**:

  - **Continuous Operative** (kind `continuous`, the default) — a long-running,
    always-on streaming unit. Implements the `ContinuousOperative` trait/interface
    and declares its Channels. This is the original Flezha workload.

  - **Bounded Operative** (kind `bounded`) — an on-demand, terminating,
    possibly-interactive process. Implements the `BoundedOperative`
    trait/interface. It authors the same Operations/Channels/Segments a continuous
    operative does but runs them **sequentially** (a linear chain with a per-operation
    count-based barrier; segments within an operation still run in parallel), can
    **receive input after it starts** (an inbox), and terminates on the first exit
    trigger — chain quiescence, `ctx.complete`, a loop's terminal predicate, a
    `max_duration`, or an external `flz operative stop`. It may declare **saga
    compensation** hooks (reverse-order, per-segment undo on failure) and is
    **durable by default** (opt-out per class): it crash-resumes from the WAL and
    its finished state is retained for a window. `run(ctx, args)` is sugar for a
    single-Operation, single-Segment bounded operative. Both kinds share the
    `Computation` base trait/interface (whose only shared member is `factory_id()`).
    See [`resilience/bounded_operative.md`](resilience/bounded_operative.md).

- **Operation** — a typed stage inside an Operative that processes inputs from a
  Channel. Implements the `Operation` trait/interface and declares its `Input`,
  `Output`, `State`, `Change`, and `Clock` types.

- **Segment** — the per-key state-bearing slice of an Operation. State (WAL,
  snapshot, outbox dedup) lives at the Segment level on disk:
  `app_data/state/{operative_class}/{operation_id}/{segment_key}.*`.

- **Channel** — a typed input/output edge of an Operative. Channels can be entry
  points (input from outside), internal (Operation → Operation), or sink (output
  back to the Controller / pipe to another Operative).

## Disambiguating "Operator"

The word "Operator" used to mean "the SDK process" in older Flezha code and
docs. After the rename, that meaning is dropped: **the SDK process is now an
Executor**. The word "Operator" survives only as **ProcessHub-internal
ring-node jargon** — a node in ProcessHub's distribution ring. ProcessHub is a
third-party library; its terminology is not user-facing in Flezha. When the
word appears in `docs/process_hub_architecture.md` or in a doc comment whose
context is ProcessHub, it refers to that internal abstraction, not to the
Executor.

Note also the **Operation** vs **Operative** distinction: an *Operative* is the
unit you run; an *Operation* is a stage inside it. The unit id is `operative_id`;
the stage id is `operation_id`.

## See also

- `docs/operative_architecture.md` — Operative/Operation/Segment deep dive.
- `docs/sdk_protocol_spec.md` — Wire protocol between Executors and the
  Controller.
- `docs/process_hub_architecture.md` — Two-level distribution (ProcessHub picks
  a Controller node; the Controller picks an Executor).
- `docs/resilience/state_persistence.md` — Per-Segment WAL/snapshot/retention.
</content>
</invoke>
