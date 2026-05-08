# Flezha Glossary

End-state mental model: **A Controller orchestrates Relays across Executors.
Each Executor runs Relays; each Relay contains Shards; each Shard contains
Partitions.**

## Roles

- **Controller** — the Elixir control plane (`lib/flezha/`). Owns placement,
  WAL persistence, migrations, the CLI/UI surface, and ProcessHub coordination.
  One Controller node serves one or more namespaces; multiple Controller nodes
  serving the same namespace cluster via Erlang distribution + ProcessHub.

- **Executor** — the SDK process (Rust or Java) that connects to a Controller
  and runs user-defined Relays. Started by the user; opens a control channel +
  data streams to the Controller; runs the work-stealing pool that dispatches
  per-Partition mailboxes. One physical machine may run multiple Executors.

## Workload abstractions

- **Relay** — the user-facing unit of work. Implements the `Relay` trait/
  interface and declares its Channels. One Relay class is registered with a
  factory id; many Relay instances of the same class can run across the cluster.
  Replaces the older "processor" terminology.

- **Shard** — a layer inside a Relay that processes inputs from a Channel.
  Implements the `Shard` trait/interface and declares its `Input`, `Output`,
  `State`, `Change`, and `Clock` types.

- **Partition** — the per-key state-bearing slice of a Shard. State (WAL,
  snapshot, outbox dedup) lives at the Partition level on disk:
  `app_data/state/{executor_id}/{relay_class}/{shard_id}/{partition_key}.*`.

- **Channel** — a typed input/output edge of a Relay. Channels can be entry
  points (input from outside), internal (Shard → Shard), or sink (output back
  to the Controller / pipe to another Relay).

## Disambiguating "Operator"

The word "Operator" used to mean "the SDK process" in older Flezha code and
docs. After the rename, that meaning is dropped: **the SDK process is now an
Executor**. The word "Operator" survives only as **ProcessHub-internal
ring-node jargon** — a node in ProcessHub's distribution ring. ProcessHub is a
third-party library; its terminology is not user-facing in Flezha. When the
word appears in `docs/process_hub_architecture.md` or in a doc comment whose
context is ProcessHub, it refers to that internal abstraction, not to the
Executor.

## See also

- `docs/relay_architecture.md` — Relay/Shard/Partition deep dive.
- `docs/sdk_protocol_spec.md` — Wire protocol between Executors and the
  Controller.
- `docs/process_hub_architecture.md` — Two-level distribution (ProcessHub picks
  a Controller node; the Controller picks an Executor).
- `docs/controller_state_persistence.md` — Per-Partition WAL/snapshot/retention.
