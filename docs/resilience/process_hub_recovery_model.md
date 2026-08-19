# ProcessHub: source of truth & recovery model (READ FIRST)

> This is the model the rest of the codebase must respect. Several controller
> components (healthz, the convergence Snapshot's `durable_registry_pending?`)
> were written as if **DETS** were the authority for "what operatives should be
> running." It is not. Getting this wrong causes split-brain, stale-state
> crashes, and dishonest health reporting. Read this before touching anything
> that reads ProcessHub registry/recovery state.

## The two invariants

1. **Declared intent lives in the hub's declared set.** Every durable
   operative start (`durable: true` at the single start authority) adds an
   entry; every deliberate stop or workflow completion removes it through the
   hub stop path. The set is versioned, leader-written, persisted beside the
   registry on every node, and optionally shipped off-cluster (see "Remote
   manifest"). It answers exactly one question: *what SHOULD exist* — and it
   answers it even when nothing is running. `Convergence.Intent.declared/0`
   and `HubManager.declared_operatives/1` read it; nothing in flezha outside
   the start/stop authority writes it.

2. **The live, cluster-synced ProcessHub registry is the source of truth for
   what IS running and on which node.** Not DETS, not the controller's own
   caches — the in-memory registry, kept consistent across the cluster by the
   synchronizer, is authoritative for runtime state at all times. DETS is a
   disk backup of runtime rows, not the source of either question.

Losing runtime rows loses no intent: the reconcile re-derives runtime state
from the declared set. Losing the declared set parks the reconcile (loudly)
rather than opening empty — a lost list must never be mistaken for "nothing
declared".

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
- The declared set syncs by version: adoption replaces the whole list, higher
  version wins. A rejoining node adopts the cluster's newer list — including
  stops it was absent for, however long the absence.

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

Note the declared set stores each child's spec as it was at start time. A
restore that has only the declared set (rows lost) replays that start-time
manifest; the registry row remains the fresher record wherever it survives.

## Recovery is a reconcile toward the declared set

Recovery is not a boot mode an operator arms. Every node runs an **orphan
reconcile**: `reconcile_grace_ms` after the coordinator starts, and then at
most once per `reconcile_interval_ms`, it converges the cluster toward the
declared set — starting declared children observed running nowhere, and
stopping running durable children whose declared entry was removed.

That single rule covers every case:

- **Cold-starting a dead cluster** — nothing runs, so the whole declared set
  is started. No arming step. Spawned workflow instances were declared by
  their spawn and restore the same way.
- **Rejoining a live cluster** — peers already run every declared child, so
  the difference is empty and the node starts nothing.
- **Wiped runtime registry** — rows are gone but the declared set is not; the
  reconcile restores the declared children through the same idempotent start
  authority (the registry's duplicate-id claim makes a rebuild unable to
  double-bind).

A deliberate stop is recorded as **absence from the declared set** and never
expires: the removal commits before the child terminates
(remove-before-terminate), so a crashed stop converges to stopped rather than
resurrected, and a node returning after ANY absence adopts a list version the
entry is simply not in. The old stopped-row tombstones and their
`stopped_row_ttl_ms` window are gone — there is no expiry to outwait.
First boot after upgrading seeds list version 1 from the existing durable
registry rows, one-way and logged.

Every controller-side reader that means "what should be running" goes through
the declared set (`HubManager.declared_operatives/1`); readers that mean
"what is running with a live binding" go through
`HubManager.live_registry_dump/1`.

## Remote manifest: the off-cluster copy

With the durability tier enabled, every declared-list version ships
asynchronously to `Flezha.State.DurabilityTier.DeclaredManifest` — an adapter
implementing `ProcessHub.Storage.RemoteManifest` over the tier's configured
backend (`:postgres`, `:s3`, `:local_path`, or any custom `{module, opts}`).
Boot adopts the highest version among the local copy and the remote one, so
the declared set survives the loss of every cluster disk. The ship is
leader-only, retried with backoff, and never a synchronous dependency of a
start/stop command: a tier outage delays the off-cluster copy, not the
command.

## Restart playbook (operational)

- **Deployment / rolling update:** ALWAYS bring the new instance up and confirm
  it is running *before* killing the old one. The new instance syncs live state
  from the surviving peer → zero operative-list loss.
- **Manual restart, sole instance, cluster was fully down:** plain restart. The
  first reconcile round restores the declared set within
  `reconcile_grace_ms`. No operator step.
- **Total disk loss (registry AND declared list gone):** configure the
  durability tier; boot fetches the remote manifest and adopts it. Without a
  remote copy, a lost list parks the reconcile and
  `ProcessHub.Service.DeclaredChildren.clear/1` is the operator override.

## Implications for flezha controller code (the lessons)

- **Never infer "what should be running" from DETS or from live pids.** The
  declared set is the intent authority; the live cluster registry is the
  runtime authority. `Convergence.Intent` reads the former, `Probe` observes
  the latter, and the Diff heals only what it OBSERVED — starting
  declared-but-absent children is the hub reconcile's job, not a heal op's.
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
- `lib/process_hub/service/process_registry.ex` — the live registry (runtime truth).
- `lib/process_hub/service/declared_children.ex` (+ `declared_children/{boot,store}.ex`)
  — the declared set: leader-written mutations, boot resolution, seeding, parking.
- `lib/process_hub/storage/remote_manifest.ex` — the off-cluster copy behaviour;
  flezha's adapter is `Flezha.State.DurabilityTier.DeclaredManifest`.
- `lib/process_hub/service/recovery.ex` + `recovery/round.ex` — the reconcile
  rounds converging the cluster toward the declared set.

See also `docs/process_hub_architecture.md` (ProcessHub-in-flezha: two-level
distribution, hooks, HotSwap, lifecycle scenarios), and
`docs/resilience/graceful_shutdown.md` § "Migration consent" — every
redistribution asks the Overwatch for consent first, so a mid-critical-work
operative (correction in flight, durable workflow off an iteration boundary,
unflushed effectively-once outbox) defers its migration to a safe barrier.
