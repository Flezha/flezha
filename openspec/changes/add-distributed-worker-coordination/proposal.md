## Why

With pipeline stage composition landing (`add-pipeline-stage-composition`), a single Flezha operator instance can host a DAG of stages. The next constraint shows up immediately: **stateful stages cannot be naively load-balanced across nodes.** A 7-day EMA stage keeping in-memory state for BTC/USD must see every trade for BTC/USD — if two nodes both host that stage and requests round-robin between them, the state is wrong.

The consumer use case driving this is user-subscribed indicators and alerts (tracked in the flezha_market repo's `add-dynamic-indicator-workers` and `add-partitioned-state-bootstrap` changes). Thousands of (symbol, indicator, period) subscriptions need to be computed with strict state affinity: **one node owns one symbol's state for the lifetime of the assignment.** On node failure, the assignment must move — not with stop-the-world mechanics, but with cooperative rebalance so other partitions keep flowing.

Today, the Flezha agent (Elixir/OTP) supervises processor instances per node but has no concept of partition assignment, cross-node routing, or cooperative rebalance. The Rust SDK has no way to discover which partitions it owns. Both sides need first-class partition semantics.

## What Changes

- **Introduce a partition model in the Elixir agent (`lib/flezha/coordinator/`).** A partition is a stable string key (typically `symbol`) that is assigned to exactly one worker node at any time. The coordinator maintains the authoritative `partition → node_id` map, backed by Horde (CRDT-backed distributed registry) so the map survives node churn without a single point of failure.
- **Cooperative rebalance.** Assignments change only on node join, node leave, or explicit admin rebalance. During rebalance, each affected node gets `PartitionRevoke { partition_id }` first and must acknowledge (draining any in-flight work) before `PartitionAssign { partition_id }` is sent to the new owner. No partition is assigned to two nodes simultaneously.
- **Rust SDK gains a `PartitionAware` contract (`sdk/rust/src/pipeline/partition.rs`).** Stages that need partitioned state implement `on_partition_assigned(&mut self, partition_id) -> ReadySignal` and `on_partition_revoked(&mut self, partition_id) -> DrainedSignal`. The SDK routes inbound messages by partition key and only delivers to stages that own the matching partition.
- **Routing happens in the agent, not on the hot path.** When a message's partition key does not match a locally-owned partition, the agent forwards it over the BEAM cluster to the correct node's operator socket. Hot-path local messages still travel through crossbeam channels as in Proposal 1 — no extra latency for the common case.
- **Assignment policy = consistent hashing with explicit overrides.** Default policy is consistent hashing on `partition_id`, producing balanced assignments as nodes are added/removed. Operators can override with explicit `partition_id → node_id` placement (useful for pinning high-traffic symbols to specific hardware).
- **Liveness via BEAM distribution.** The coordinator uses `:net_kernel` monitors; node-down triggers a rebalance within a configurable grace period (default 5s) to avoid flapping on transient network blips.
- **Admin + observability.** A small CLI (`flezha-ctl partitions list | assign | rebalance`) exposes the assignment map. Metrics: `partitions_owned`, `partition_rebalance_count`, `message_forwarded_count`, `message_forward_latency_ns`.
- **BREAKING**: deployments with multiple Flezha operator nodes sharing a cluster must be restarted together after upgrade — the new protocol messages are not understood by old agents. Single-node deployments are unaffected.

## Capabilities

### New Capabilities

- `partition-model`: partition as a first-class primitive in the agent — creation, assignment, revocation, lifecycle semantics.
- `partition-assignment-policy`: consistent-hash default assignment, explicit override, join/leave/admin rebalance triggers, configurable grace periods.
- `partition-routing`: in-cluster message forwarding when the inbound partition key does not match a locally-owned partition. Local dispatch stays on the existing hot path.
- `partition-aware-stage`: Rust SDK trait and lifecycle callbacks (`on_partition_assigned`, `on_partition_revoked`) for stages that hold partitioned state.
- `partition-admin`: CLI commands and metrics surface for inspecting and overriding partition assignment.

### Modified Capabilities

- `pipeline-topology` (from `add-pipeline-stage-composition`): topology can now declare stages as partition-scoped. Unscoped stages keep single-node semantics.

## Impact

- **Flezha agent (Elixir, `lib/flezha/coordinator/`)**: new coordinator GenServer, Horde registry setup, cluster-join/leave hooks, cooperative rebalance state machine, BEAM-cluster forwarding for cross-node messages.
- **Flezha Rust SDK**: new `pipeline/partition.rs` module (`PartitionAware` trait, `PartitionId` type, assignment notification channel), `Topology` extended with `add_partitioned_stage(fn, partition_key_extractor)`, routing hook in `Fanout` to skip non-owned partitions.
- **Protocol**: new control-channel messages `PartitionAssign`, `PartitionRevoke`, `PartitionListQuery`, `PartitionForward`. Protobuf definitions in `protobufs/`.
- **Dependencies**: add `:horde` Elixir dep (MIT-licensed, BEAM-native CRDT registry). No new Rust runtime dependencies — forwarding reuses the existing Unix socket to the local agent.
- **Configuration**: per-node ID (`FLEZHA_NODE_ID`), cluster discovery seed nodes (`FLEZHA_CLUSTER_SEEDS`), rebalance grace period (`FLEZHA_REBALANCE_GRACE_MS`, default 5000).
- **Docs**: new `docs/distributed-workers.md` covering the partition model, rebalance lifecycle, routing; update `docs/architecture.md` with a cluster diagram.
- **Tests**: Elixir ExUnit tests for coordinator (join, leave, rebalance, forward); Rust integration tests for `PartitionAware` lifecycle; a multi-node integration scenario (two Flezha nodes, one partition, failover + rebalance) using Docker Compose.
- **Deployment**: requires coordinated restart of all Flezha operator nodes in a cluster; consumer (market-processor) needs no code change but must declare any partitioned stages it wants (driven by the consumer's own change in `add-partitioned-state-bootstrap`).
- **Compatibility**: single-node deployments continue to work as before — the coordinator runs in single-node mode with all partitions local and no forwarding.
