# ProcessHub in Flezha

How the agent uses the [`process_hub`](https://hex.pm/packages/process_hub) library to
distribute processors across nodes, plus how this layer interacts with the
operator (Rust/Java SDK) connection model.

This document is a reference for anyone designing features that touch processor
placement, restart, recovery, migration, or the operator/agent connection
lifecycle. Read it before proposing changes in those areas — most of the
behaviour described here is already automatic, and the most common mistake is
to assume Flezha owns logic that ProcessHub already provides.

## Two-level distribution

Flezha distributes processors at two independent levels:

1. **Cluster level — ProcessHub.** ProcessHub picks which agent **node** owns
   each processor, using a per-namespace consistent hash ring. Agent nodes
   running the same namespace cluster together via Erlang distribution +
   ProcessHub's coordinator.
2. **Within-agent level — operator selection.** Once an agent owns a processor,
   it picks one of its locally-connected operators (Rust/Java SDK clients) to
   spawn the processor on. Today this pick is `Enum.random` filtered by factory
   support (`lib/flezha/service/processor_manager.ex`).

The two levels are decoupled. ProcessHub knows nothing about operators; the
agent's `alter_child_data` hook handles operator selection at processor start
time.

## Per-namespace ProcessHub instances

Each agent runs **one ProcessHub instance per namespace** it serves. The hub
id is derived from the namespace via `Flezha.Identificator.hub/1`. Agents
serving namespace A and agents serving namespace B are in completely
independent ProcessHub clusters — work is never migrated between unrelated
namespaces.

This means:

- An agent may serve multiple namespaces; it has multiple ProcessHub instances.
- "Peer agents" in any reasoning about migration / failover always means
  **other agents serving the same namespace**, not just other agents in the
  Erlang cluster.
- A namespace dies entirely when the last agent serving it leaves — there is
  nowhere to migrate to.

The hub spec lives in `lib/flezha/service/hub_manager.ex`:

```elixir
ProcessHub.child_spec(%ProcessHub{
  hub_id: Identificator.hub(namespace),
  synchronization_strategy: %ProcessHub.Strategy.Synchronization.PubSub{},
  redundancy_strategy: %ProcessHub.Strategy.Redundancy.Singularity{},
  partition_tolerance_strategy: %ProcessHub.Strategy.PartitionTolerance.Divergence{},
  distribution_strategy: %ProcessHub.Strategy.Distribution.ConsistentHashing{},
  migration_strategy: %ProcessHub.Strategy.Migration.HotSwap{handover: true},
  cross_node_request_timeout: 10_000,
  hooks: %{...}
})
```

## Strategies in use

| Concern | Strategy | Effect |
|---|---|---|
| Distribution | `ConsistentHashing` | `processor_id` hashes to a node on the ring. Adding/removing a node reshuffles only the affected slice. |
| Synchronization | `PubSub` | Registry changes (process starts/stops) are broadcast to all nodes. Each node has the same view of "which processor lives where". |
| Migration | `HotSwap{handover: true}` | On graceful node leave, processor state is captured on the source, shipped to the target, and only then is the source process terminated. See [HotSwap with handover](#hotswap-with-handover) below. |
| Redundancy | `Singularity` | Each processor runs on exactly one node. No replicas. |
| Partition tolerance | `Divergence` | Each network partition independently redistributes. On heal, the larger partition's state wins. |

## Hooks Flezha registers

Flezha plugs into ProcessHub via two hooks (`lib/flezha/service/hub_manager.ex`):

- `child_data_alter` — fires when ProcessHub is about to start a processor on
  this node. Flezha rewrites the child_spec to bake in the assigned
  `compute_engine_id`, picked by `ProcessorManager.assign_compute_engine(namespace,
  factory_id)`. Raises if no operator declaring that factory is currently
  connected. **Net effect:** every processor restart re-runs operator selection.
- `child_registered` → `hook_insert_pid` — fires after a processor has started.
  Flezha pushes the new local PID to all stream channels of the assigned
  `compute_engine_id` so subsequent I/O routes to the right process.

## Lifecycle scenarios

### Graceful node leave (rolling DaemonSet update, normal shutdown)

1. ProcessHub's coordinator on the leaving node observes its own departure
   intent.
2. For each processor on this node, `HotSwap` queries `prepare_handover_state/1`
   (Flezha overrides this in `Overwatch`) — the live process returns its
   current state, including any captured rust/java state binary.
3. The state is stashed in ETS with a TTL on the source node.
4. ProcessHub picks a destination node (consistent-hash recompute on the new
   ring without this node) and fires the start request there.
5. The destination node starts the processor; before it accepts I/O, ETS
   handover delivers the saved state via `alter_handover_state/2`.
6. Only after handover completes does the source process terminate.
7. The destination's `alter_child_data` hook picks a local operator — possibly
   different from the one that hosted the processor on the source node.

Net effect: graceful failover preserves processor + plugin state. Operators on
the leaving node lose their work (it moved). Operators on the destination node
gain the work.

### Ungraceful node loss (crash, OOM, kill -9, network partition that holds)

1. Other coordinators detect `:nodedown` via Erlang's `:net_kernel.monitor_nodes(true)`.
2. ProcessHub batches the departure (small window to absorb flaps), then
   redistributes affected processors across surviving nodes.
3. No state handover occurs — the source died before any capture.
4. Destination nodes start processors fresh; `alter_child_data` picks operators.
5. Per-processor periodic state snapshots (see [State snapshots](#state-snapshots-for-handover-and-restart))
   provide a "last-known good" state if the destination's processor restart
   path consults them.

Net effect: state is lost (or rolled back to the last periodic snapshot,
depending on the processor's logic), but the processors continue to run.

### Node returns to the cluster

When a previously-down node rejoins, ProcessHub's cluster-join handler
recalculates the consistent hash. Depending on the resulting ring shape,
processors **may or may not** move back to the rejoining node. There is no
explicit "snap back" — placement is purely a function of the hash ring.

When ProcessHub does decide to move a processor to the rejoining node, it goes
through the standard `child_data_alter` path on that node, which picks a
locally-connected operator.

### Operator restart (SDK process dies and comes back)

ProcessHub doesn't see this — it's invisible at the cluster layer. Handling
lives in `FleetManager` (`lib/flezha/compute_engine/fleet_manager.ex`):

- Branch A — other operators in this namespace on this agent: reroute the dead
  operator's processors to a peer operator (via `compute_engine_reroute` →
  `Overwatch.compute_engine_migrate` → `start_processor_sdk` → `AddProcessor`
  protobuf to the surviving operator).
- Branch B — no operators here, but other agents serve this namespace:
  `hub_migrate` tears down the local namespace hub. ProcessHub redistributes
  its slice to peer agents. Returning operator gets new work next time
  ProcessHub places something on it.
- Branch C — last operator in the namespace cluster: mark `:inactive` with a
  timestamp. If the operator reconnects within `compute_engine_keepalive`
  (default 30 s, env var `FLEZHA_OPERATOR_KEEPALIVE`),
  `try_migrate_compute_engine` reroutes the preserved work to the new
  `compute_engine_id`.

## HotSwap with handover

The migration strategy is the most subtle piece. `HotSwap{handover: true}`
implements stateful migration in a strict order:

1. Source node receives a "process X moving to node Y" instruction.
2. Source sends `{:process_hub, :query_hot_handover_state, ...}` to the live process.
3. Process replies `{:process_hub, :hotswap_state, child_id, prepared_state}`.
4. Source stashes the state in an ETS table with a short TTL.
5. Source sends a normal start request to the target node.
6. Target node starts the process. Before the new process is added to the
   registry, a post-action callback fires that asks the source for the saved
   state.
7. Source delivers `{:process_hub, :hotswap_handover, child_id, state}` to the
   new process.
8. New process consumes the state via `alter_handover_state/2`.
9. Only now does the source terminate the old process.

Flezha processors `use ProcessHub.Strategy.Migration.HotSwap` in
`lib/flezha/processor/overwatch.ex` and override the handover callbacks to
ship rust/java state binaries alongside the Elixir state.

If handover fails (target unreachable, timeout, callback error), the source
process is NOT terminated — the existing process keeps running. The migration
may be retried later.

## State snapshots for handover and restart

Independent of HotSwap migration, each processor (`Overwatch` GenServer)
periodically captures its state via the `:on_snapshot` extension hook. The
interval is configured by `overwatch_state_snapshot_interval_ms` (default
60 s); a value of `0` disables it. The captured snapshot is held in memory in
the Overwatch struct's `snapshot` field — it is **not persisted to disk**.

The snapshot is consumed in two places:

1. HotSwap's `prepare_handover_state` returns it as part of the migration
   payload, so a graceful migration ships the most recent snapshot in addition
   to the live in-memory state.
2. The manual `flz cluster snapshot` CLI captures every running processor's
   snapshot into the snapshot file, so a later `flz cluster restore` can
   re-issue the processor and replay its state.

For `flezha_market` and similar deployments where Postgres (populated by
Kafka) is the authoritative store, the env var is set to `0` to disable the
secondary snapshot and avoid drift.

## Operator → agent discovery and connection

(Documented here because it's adjacent to ProcessHub-driven assignment.)

- Each operator (Rust/Java SDK) connects to **exactly one agent**. There is no
  multi-agent connection.
- Discovery is filesystem co-location, not service discovery. In Kubernetes
  the `flezha-sockets` hostPath volume is mounted into BOTH the agent pod and
  the operator pod, so the operator opens the local agent's UDS socket via
  `FLEZHA_SOCKET_FILES_PATH` (default `/flezha/sockets`).
- For QUIC transport: the operator dials `FLEZHA_QUIC_ADDRESS`.
- Reconnect loop: exponential backoff `100ms * 2^n` capped at 30 s; retries
  forever; never tries a different agent.
- Stream architecture: one bidirectional control channel for handshake +
  protobuf commands, plus N data streams opened on demand for processor I/O.
  Data streams are routed by `StreamAcceptor` round-robin to `StreamChannel`
  processes.

## What does NOT exist (anti-patterns to avoid in design)

These are common assumptions to double-check against the actual code:

- **No automatic snapshot-on-shutdown.** `Application.prep_stop/1` only stops
  `HubManager`. There is no SIGTERM handler that snapshots state. Snapshots
  are operator-initiated via `flz cluster snapshot`.
- **No automatic restore-on-boot.** `Application.start/2` does not read any
  snapshot file. Restoration is operator-initiated via `flz cluster restore`.
- **No persistent FleetManager state.** `compute_engine_namespaces` and
  `namespace_factories` live only in the FleetManager GenServer. Restarting
  the agent process loses them entirely.
- **No persistent operator → processor assignment.** Assignment is recomputed
  every time `child_data_alter` fires; nothing is stored.
- **No DETS recovery subsystem.** It was designed and archived under
  `openspec/changes/archive/2026-04-21-add-failure-recovery-mode/` but never
  built. The `FLEZHA_RECOVERY_*` env vars and the `flezha-recovery` k8s
  hostPath volume are vestigial — config is parsed, no module reads it.
- **No multi-agent discovery in the SDK.** An operator does not failover to
  another agent if its local one is gone — it waits forever for the local one
  to return.

## Where the code lives

| Concern | File |
|---|---|
| Hub spec, hooks, alter_child_data | `lib/flezha/service/hub_manager.ex` |
| Operator selection (random within namespace+factory) | `lib/flezha/service/processor_manager.ex` |
| FleetManager (tracks operators, factories, disconnect branches) | `lib/flezha/compute_engine/fleet_manager.ex` |
| Processor wrapper, HotSwap callbacks, periodic snapshot | `lib/flezha/processor/overwatch.ex` |
| Control channel (operator handshake, EOF detection) | `lib/flezha/compute_engine/control_channel.ex` |
| Stream channels, attach/detach, frame routing | `lib/flezha/compute_engine/stream_channel.ex` |
| UDS stream acceptance and round-robin | `lib/flezha/socket/stream_acceptor.ex` |
| Manual cluster snapshot / restore | `lib/flezha/cluster/snapshot.ex` and `lib/flezha/cluster/snapshot/coordinator.ex` |
| Rust SDK reconnect loop | `sdk/rust/src/compute_engine/control_unit.rs` |
| Rust SDK reconnect handler | `sdk/rust/src/compute_engine/{fast,legacy}.rs` `handle_reconnect` |

## Related docs

- `docs/sdk_protocol_spec.md` — wire-level binary framing and protobuf commands.
- `docs/cluster-snapshot.md` — manual snapshot/restore CLI semantics.
- `docs/nif_architecture.md` — transport NIF design (UDS + QUIC drivers).
- `docs/deployment.md` — k8s DaemonSet layout, env vars, hostPath volumes.
