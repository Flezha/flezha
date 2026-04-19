## Context

The pipeline-stage work (`add-pipeline-stage-composition`) gives Flezha composable, stateful DAGs inside a single node. The next requirement — driven by the flezha_market consumer's per-user indicator subscriptions — is running those stages across a cluster of nodes while preserving state affinity per partition key (typically `symbol`). The Elixir agent already supervises Rust operators over Unix sockets, and BEAM has first-class distribution primitives. The job is to give Flezha a partition model that uses BEAM's strengths without rebuilding a Kafka-Streams-grade rebalance protocol.

## Goals / Non-Goals

**Goals:**
- Each partition is assigned to exactly one node at any moment.
- Rebalance is cooperative: revoke-then-assign, never overlap.
- Rebalance only fires on node join, node leave, or explicit admin action — not on every message.
- Rust SDK stages can opt into partition ownership via a typed trait.
- Cross-node forwarding exists as a fallback for messages whose partition key lives elsewhere, but is not the hot path.
- Single-node deployments behave exactly as before.

**Non-Goals:**
- Durable state replication across nodes — consumer's responsibility (covered by `add-partitioned-state-bootstrap` in the market consumer, which uses Postgres as durable truth).
- State migration during rebalance — revoke drains then discards; new owner rebuilds from the consumer's durable source. Keeps the agent stateless beyond assignment.
- Automatic partition split/merge for load balancing — partitions are externally defined (by symbol).
- Exactly-once delivery semantics — the agent provides at-least-once forwarding with idempotent processing expected from stages.

## Decisions

### Coordinator in Elixir, not Rust

BEAM has battle-tested distribution: `:net_kernel` node monitoring, `:global` locks, Horde CRDT registries. Rewriting any of this in Rust adds quarters of work for no benefit. The agent is already the control plane; the coordinator lives inside it.

### Horde for the partition registry

Horde provides a CRDT-backed `Horde.Registry` that survives node churn without consensus overhead (no Raft). Two properties matter: (1) eventual consistency across nodes is acceptable because we only rebalance on membership changes, not per message; (2) a split-brain resolves automatically when partitions heal, with the last-writer-wins rule fine here since the coordinator is the only writer. Alternatives considered: Raft (overkill for membership-triggered state), ZooKeeper (external dep, ops burden). Rejected.

### Cooperative rebalance state machine

On membership change:
1. Coordinator computes the new assignment map.
2. For each partition whose owner changed: send `PartitionRevoke` to current owner; await `PartitionRevokeAck` (with configurable timeout, default 3s).
3. Upon ack: send `PartitionAssign` to new owner; await `PartitionAssignAck`.
4. Update the registry.

Failure to ack revoke within timeout = coordinator forcibly reassigns and logs a warning. Consumer is responsible for detecting stale state (consumer's durable bootstrap protocol handles this).

### Routing on the miss path, not the hit path

Most messages arrive at the node that owns the destination partition (produced by Kafka consumer groups keyed on the same partition key). For the rare mismatch (during rebalance, or when a consumer-group partition assignment differs from Flezha's partition assignment), the agent forwards over BEAM distribution to the right node, which hands it to its local operator via the existing Unix socket. Hot path is unchanged.

### Consistent hashing with explicit overrides

Default assignment = consistent hash ring with 128 virtual nodes per physical node (mitigates hash imbalance). Rebalance on membership change moves minimal partitions (classic consistent-hash property). Operators can pin `partition_id → node_id` via the admin CLI for workload shaping (e.g., parking high-volume symbols on hardware with more cores).

### Partition key is opaque to the coordinator

Coordinator treats `partition_id` as an opaque string. Consumers choose what to put in it — `BTC/USD`, `BTC/USD|1h|EMA|7`, or anything else. The `PartitionAware` trait in the Rust SDK takes a `PartitionId(String)`; extractors are consumer-defined.

### Liveness grace period

Default 5s between detecting a node-down event and triggering rebalance. Prevents flapping on transient network blips. Admin can tune via `FLEZHA_REBALANCE_GRACE_MS`.

## Risks / Trade-offs

- **Rebalance latency during node failure.** From failure to fully reassigned: grace period (5s) + revoke-ack timeouts for unresponsive node (3s) + assign propagation (<100ms). Target recovery: ~8–10s worst case. Acceptable for market data where the consumer has a durable source of truth to bootstrap from; not acceptable for order matching, which is why that stays in Java and is not a partition-aware Flezha stage.
- **Horde CRDT eventual consistency.** A partition can briefly be considered "owned by no one" during a network partition heal. Mitigated by: (a) coordinator being the only writer, (b) node-level "refuse to serve if assignment age > threshold" safeguard, (c) consumer-side idempotent operations.
- **Forwarding overhead.** BEAM distribution is ms-scale, far more than the local µs channel path. Mitigation: drive Kafka consumer-group partitioning with the same key so misses are rare (< 1% of messages except during rebalance). Instrumented via `message_forwarded_count` metric.
- **Upgrade coupling.** The new agent protocol is not backward-compatible. Mitigation: documented in release notes; single-node deployments don't need multi-node coordination so they can upgrade independently.
- **Horde as a new dependency.** It is MIT-licensed, actively maintained, widely used in production Elixir deployments. Low risk, but a tracked dep.
