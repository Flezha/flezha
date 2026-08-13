# ProcessHub: source of truth & recovery model (READ FIRST)

> This is the model the rest of the codebase must respect. Several controller
> components (healthz, the convergence Snapshot's `durable_registry_pending?`)
> were written as if **DETS** were the authority for "what operatives should be
> running." It is not. Getting this wrong causes split-brain, stale-state
> crashes, and dishonest health reporting. Read this before touching anything
> that reads ProcessHub registry/recovery state.

## The one invariant

**The live, cluster-synced ProcessHub registry is ALWAYS the source of truth
for which operatives are running and on which node.** Not DETS, not the controller's
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

### Registry writes must be cluster-wide for durable manifests

Interval sync propagates **which children run where**, not edits to a row's
child_spec/metadata: `append_data/2` only rewrites a peer row when its pid
changed, and then with the *sender's* cspec. A plain
`ProcessRegistry.update/3` is therefore node-local — an edit issued on one
node never reaches the others and is clobbered on the next pid change.

Durable manifests embedded in the cspec (the operative plugin list written by
`flz plugin enable`, the runner tag from `operative_set_runner/3`) MUST be
written with `ProcessRegistry.update/4` `propagate: true`, which re-applies
the same pure update function on every hub node (2 s per-peer budget,
best-effort; a node missed while down converges on rejoin via peer sync).
All hub nodes are assumed to run the same release — the propagated function
must resolve on every node. This closed the 2026-06-12 outage gap where an
operative restarted from a peer's pluginless cspec and nothing could heal it.

## Recovery is a reconcile, not a mode

Recovery is no longer a boot mode an operator arms. Every node runs an
**orphan reconcile**: `reconcile_grace_ms` after the coordinator starts, and
then at most once per `reconcile_interval_ms`, it starts the difference between
its own durable registry and what the cluster is observed to hold.

That single rule covers both cases the marker used to separate:

- **Cold-starting a dead cluster** — no peer holds anything, so the difference
  is the whole registry and the node restores itself. No arming step.
- **Rejoining a live cluster** — peers already hold every child, so the
  difference is empty and the node starts nothing.

The difference is computed against the durable registry, which is never wiped
to force that outcome. Two upstream fixes are what make this safe, and both
correspond to logged flezha incidents: `Storage.Dets.open/2` no longer deletes
every row on a skip-replay boot (2026-07-25 — a peerless controller came up
with **0 operatives** and every later recovery boot replayed an empty file),
and `Synchronizer.detach_data/2` no longer deletes a row because a peer's
payload did not mention it (2026-06-19 — a restarted controller synced empty
intent from a churn-emptied peer and ran **0 relays** while `/healthz` said
healthy).

A deliberate stop is not an orphan: `stop_children/3` marks the row `:stopped`
rather than deleting it, so the stop survives the stopping node's absence
instead of a peer resurrecting it. The row expires after `stopped_row_ttl_ms`
(flezha: 30 days). Every controller-side reader that means "what should be
running" — `flz operative list`, convergence's intent check — goes through
`HubManager.live_registry_dump/1`, which excludes stopped rows; `dump/1`'s own
default includes them.

## Restart playbook (operational)

- **Deployment / rolling update:** ALWAYS bring the new instance up and confirm
  it is running *before* killing the old one. The new instance syncs live state
  from the surviving peer → zero operative-list loss.
- **Manual restart, sole instance, cluster was fully down:** plain restart. The
  first reconcile round replays the operative list from the durable registry
  within `reconcile_grace_ms`. No operator step.
- **Manual restart, accept losing the live operative list:** there is no longer
  a variant that does this on purpose. A restart recovers.

## Implications for flezha controller code (the lessons)

- **Never infer "what should be running" from DETS on a running or rejoining
  node.** DETS is not the authority; the live cluster registry is. A consumer
  that compares DETS row-count to live bindings (e.g. an early
  `durable_registry_pending?`) will be wrong during normal churn and during a
  peer-synced rejoin.
- **Health/recovering signals must derive from cluster-sync state**, not from
  DETS contents. "Still recovering" means "this node has not yet finished
  syncing its registry (from peers, or from runners re-announcing)", not
  "DETS has rows the live view does not."
- **Do not preserve/replay DETS on a rejoin to 'fix' a gap.** That forces stale
  local state onto a node that should sync from peers — split-brain, and it
  crashes boot when stale plugin cspecs are replayed against dead operative pids.

## Where the code lives (ProcessHub library)

- `lib/process_hub/service/cluster.ex` — cluster membership (auto-form by hub_id).
- `lib/process_hub/service/synchronizer.ex` — peer registry sync (interval + join).
- `lib/process_hub/service/process_registry.ex` — the live registry (source of truth).
- `lib/process_hub/service/storage.ex` + `storage/dets.ex` — durable backend; the
  skip-replay `delete_all_objects` lives in `Dets.open/2`.
- `lib/process_hub/service/recovery.ex` — marker logic, recovery-mode resolution,
  `recovery_replay` flag injection.

See also `docs/process_hub_architecture.md` (ProcessHub-in-flezha: two-level
distribution, hooks, HotSwap, lifecycle scenarios), and
`docs/resilience/graceful_shutdown.md` § "Migration consent" — every
redistribution asks the Overwatch for consent first, so a mid-critical-work
operative (correction in flight, durable workflow off an iteration boundary,
unflushed effectively-once outbox) defers its migration to a safe barrier.
