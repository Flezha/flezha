# ProcessHub: source of truth & recovery model (READ FIRST)

> This is the model the rest of the codebase must respect. Several controller
> components (healthz, the convergence Snapshot's `durable_registry_pending?`)
> were written as if **DETS** were the authority for "what relays should be
> running." It is not. Getting this wrong causes split-brain, stale-state
> crashes, and dishonest health reporting. Read this before touching anything
> that reads ProcessHub registry/recovery state.

## The one invariant

**The live, cluster-synced ProcessHub registry is ALWAYS the source of truth
for which relays are running and on which node.** Not DETS, not the controller's
own caches — the in-memory registry, kept consistent across the cluster by the
synchronizer, is authoritative at all times.

DETS is a **disk backup**, not the source of truth. It exists for exactly one
situation (see "Recovery" below).

## Clustering & peer sync (the normal path)

- ProcessHub instances sharing the same `hub_id` **auto-form a cluster**
  (`ProcessHub.Service.Cluster` — "instances with the same hub_id will
  automatically form a cluster").
- Nodes continuously exchange their live registry via
  `ProcessHub.Service.Synchronizer` (interval sync + on node join).
  `local_sync_data/1` ships each node's live children to peers.
- **When a node (re)joins a cluster that has at least one live peer, it gets
  the full registry FROM the peers.** It must NOT read its own DETS — that copy
  is stale (it reflects the moment the node went down, not current reality).
  Trusting it re-introduces dead pids / duplicate bindings = split-brain.

## Recovery from DETS — the ONE exception

DETS replay (re-starting children from the persisted registry) is valid in
**exactly one** case: **the entire cluster was down** and is being cold-started,
so no peer survives and the on-disk record is the only thing left.

It is **opt-in**, gated by the recovery marker
(`ProcessHub.Service.Recovery`, file `cluster.healthy`):

- **Marker present** (the cluster was healthy when last seen) → recovery mode
  `:normal` → **skip DETS replay**. The node expects to sync from a peer. On a
  persistent backend, `Storage.Dets.open/2` runs `:dets.delete_all_objects` to
  **discard the stale local copy on purpose** so the peer sync is the single
  authority. This is correct, not a bug.
- **Marker absent / `FLEZHA_PROCESS_HUB_RECOVERY_MARKER` force** → `:recovering`
  → **replay from DETS**. Use only when you KNOW this is the sole surviving
  instance of a fully-downed cluster (operator drives it via
  `flz cluster prepare-recovery`).

So `recovery_replay` controls **only** whether to re-spawn children from disk.
It is deliberately coupled to wiping the local DETS because both answer the same
question: "am I rejoining a live cluster (discard local, sync from peers) or
cold-starting a dead one (trust local disk)?"

## Restart playbook (operational)

- **Deployment / rolling update:** ALWAYS bring the new instance up and confirm
  it is running *before* killing the old one. The new instance syncs live state
  from the surviving peer → zero relay-list loss, no recovery needed.
- **Manual restart, sole instance, cluster was fully down:** start in recovery
  mode (`flz cluster prepare-recovery`) so it replays the relay list from DETS.
- **Manual restart, accept losing the live relay list:** plain restart. The
  registry is rebuilt from peers (if any) and from executors re-announcing their
  relays on reconnect. Do this only when temporary loss is acceptable.

## Implications for flezha controller code (the lessons)

- **Never infer "what should be running" from DETS on a running or rejoining
  node.** DETS is not the authority; the live cluster registry is. A consumer
  that compares DETS row-count to live bindings (e.g. an early
  `durable_registry_pending?`) will be wrong during normal churn and during a
  peer-synced rejoin.
- **Health/recovering signals must derive from cluster-sync state**, not from
  DETS contents. "Still recovering" means "this node has not yet finished
  syncing its registry (from peers, or from executors re-announcing)", not
  "DETS has rows the live view does not."
- **Do not preserve/replay DETS on a rejoin to 'fix' a gap.** That forces stale
  local state onto a node that should sync from peers — split-brain, and it
  crashes boot when stale plugin cspecs are replayed against dead relay pids.

## Where the code lives (ProcessHub library)

- `lib/process_hub/service/cluster.ex` — cluster membership (auto-form by hub_id).
- `lib/process_hub/service/synchronizer.ex` — peer registry sync (interval + join).
- `lib/process_hub/service/process_registry.ex` — the live registry (source of truth).
- `lib/process_hub/service/storage.ex` + `storage/dets.ex` — durable backend; the
  skip-replay `delete_all_objects` lives in `Dets.open/2`.
- `lib/process_hub/service/recovery.ex` — marker logic, recovery-mode resolution,
  `recovery_replay` flag injection.

See also `docs/process_hub_architecture.md` (ProcessHub-in-flezha: two-level
distribution, hooks, HotSwap, lifecycle scenarios).
