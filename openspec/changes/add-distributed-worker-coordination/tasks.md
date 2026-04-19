## 1. Agent coordinator (Elixir)

- [ ] 1.1 Add `:horde` dependency to `mix.exs`.
- [ ] 1.2 Create `lib/flezha/coordinator/` with `Coordinator` GenServer, `PartitionRegistry` (Horde.Registry wrapper), `AssignmentPolicy` (consistent-hash impl).
- [ ] 1.3 Hook `:net_kernel.monitor_nodes/1` for node join/leave events.
- [ ] 1.4 Implement cooperative rebalance state machine: compute → revoke → ack → assign → ack → commit.
- [ ] 1.5 Apply configurable grace period between node-down detection and rebalance trigger.

## 2. Protocol extensions

- [ ] 2.1 Add `PartitionAssign`, `PartitionRevoke`, `PartitionAssignAck`, `PartitionRevokeAck`, `PartitionForward`, `PartitionListQuery` messages to `protobufs/`.
- [ ] 2.2 Regenerate Rust and Elixir bindings.
- [ ] 2.3 Document message semantics in `docs/distributed-workers.md`.

## 3. Cross-node forwarding

- [ ] 3.1 Route inbound messages whose partition key is not locally-owned to the correct node via BEAM distribution.
- [ ] 3.2 Remote node's agent hands the message to its local operator via the existing Unix socket.
- [ ] 3.3 Track `message_forwarded_count` and `message_forward_latency_ns`.

## 4. Rust SDK PartitionAware

- [ ] 4.1 Create `sdk/rust/src/pipeline/partition.rs` with `PartitionId(String)`, `PartitionAware` trait: `on_partition_assigned`, `on_partition_revoked`, `partition_keys(&self) -> Vec<PartitionId>`.
- [ ] 4.2 Extend `Topology` with `add_partitioned_stage(fn, partition_key_extractor: Fn(&Input) -> PartitionId)`.
- [ ] 4.3 Routing hook in the inbound dispatcher: for each message, extract the key, look up the locally-owned set, drop/forward as needed.
- [ ] 4.4 Propagate assignment notifications to stages via a per-stage channel.

## 5. Explicit assignment overrides

- [ ] 5.1 Admin API: set/clear explicit `partition_id → node_id` pins in the registry.
- [ ] 5.2 Policy: explicit pins win over consistent-hash default.
- [ ] 5.3 Rebalance respects pins; if pinned node is down, partition is left unassigned until pinned node returns OR pin is cleared.

## 6. Admin CLI

- [ ] 6.1 Add `cli/` commands: `flezha-ctl partitions list`, `flezha-ctl partitions assign <partition> <node>`, `flezha-ctl partitions rebalance`, `flezha-ctl nodes list`.
- [ ] 6.2 CLI talks to coordinator over a local admin socket (separate from the compute channel).

## 7. Configuration

- [ ] 7.1 `FLEZHA_NODE_ID` — stable identifier for the node; default derived from hostname.
- [ ] 7.2 `FLEZHA_CLUSTER_SEEDS` — comma-separated list of seed node names for BEAM cluster formation.
- [ ] 7.3 `FLEZHA_REBALANCE_GRACE_MS` — grace period before node-down triggers rebalance (default 5000).
- [ ] 7.4 `FLEZHA_REVOKE_ACK_TIMEOUT_MS` — max wait for revoke-ack before forced reassignment (default 3000).

## 8. Tests

- [ ] 8.1 ExUnit: coordinator state machine (compute, revoke, ack, assign, commit, timeout path).
- [ ] 8.2 ExUnit: Horde registry under split-brain (inject network partition, verify recovery).
- [ ] 8.3 ExUnit: explicit pins override consistent-hash default.
- [ ] 8.4 Rust integration: `PartitionAware` stage receives assigned/revoked notifications in order.
- [ ] 8.5 Rust integration: messages for non-owned partitions are dropped (when forwarding disabled) or forwarded (when enabled).
- [ ] 8.6 End-to-end multi-node scenario via Docker Compose: start 2 Flezha nodes with a shared Kafka, assign 10 partitions, kill one node, assert within 10s all partitions reassigned and processing resumes.

## 9. Docs

- [ ] 9.1 New `docs/distributed-workers.md` — partition model, assignment policy, rebalance lifecycle, forwarding, admin CLI, configuration.
- [ ] 9.2 Update `docs/architecture.md` with a cluster diagram (multiple Flezha nodes, BEAM distribution, Unix sockets to local operators).
- [ ] 9.3 Update `README.md` with a "Running distributed" section.
- [ ] 9.4 Release note in `CHANGELOG.md` flagging the coordinated-restart requirement for multi-node deployments.
