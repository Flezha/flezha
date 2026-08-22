# Operative startup latency

Measured before the change (bench `-p 4`, two runners): ~55 ms per operative
start and ~30 ms per plugin enable, all serialised, so a four-pipeline bench
spent ~365 ms bringing the engine up and ~400 ms to its first record out.
After: ~30 ms for the whole bring-up (15–18 create + 12–14 plugin) and ~37 ms
to first out; `mix bench -n 100` creates 100 operatives in ~0.2 s (was
~1.5 s) and enables their 200 plugins in ~55 ms (was ~0.9 s);
`scripts/bench.sh -n 1000` creates 1000 in 1.2 s (was 9.1 s) and enables
their 2000 plugins in 0.24 s (was 57 s). Every number here is a laptop SSD
(fsync ≈ 1.6 ms); on a slower disk the registry share is larger.

The A/B that locates the cost: the same code with
`FLEZHA_AUTO_RESTORE_ENABLED=false` (the hub on plain `:ets`, no durable
writes) brings `-p 4` up in 8 + 8 ms and creates 100 operatives in 188 ms.
process_hub's in-memory start path is fast; the whole gap is fsync-per-write
on the durable backend, and the fixes below are about how many syncs a start
pays and how many starts share one.

## Where the time went (wall-clock call trees, not CPU profiles)

* **Three durable ProcessHub registry writes per start, one per plugin enable,
  each a DETS insert + `:dets.sync` ≈ 3.5 ms.** `:dets.sync` writes DETS's
  own free lists and header on top of the fsync, so it costs twice a plain
  append+fsync (1.6 ms) — that floor is a separate, format-level follow-up.
  The three: the declared-intent precommit (Coordinator), the registry row
  (WorkerQueue `bulk_insert`), the runner binding (`operative_set_runner`,
  which must stay durable: `DurabilityTier.ship_cspec/1` rides it).
* **The first operative of a `mix` run paid ~65 ms of code loading** — the VM
  loads lazily under `mix`; a release has everything loaded at boot. Not a
  controller cost, a measurement artefact: `Bench.Fleet.preload_code/0` loads
  the controller + process_hub + kafka_ex modules in `prepare`, before `t0`.
* **Two GenServers serialised I/O instead of bookkeeping:** `PluginManager`
  ran the cspec write + subscribe + worker start inside one `handle_call`;
  `WorkerRegistry` started each `KafkaEx.Client` (TCP + ApiVersions +
  Metadata ≈ 5 ms) inside its `handle_call`. A partition group started its
  members one after another.

## Load-bearing decisions

* **ProcessHub registry group commit (process_hub, `ProcessRegistry` +
  `Storage`/`DurableEts`/`Dets`).** A write is applied at once with
  `sync: false`; the caller's `from` is parked in `state.pending` and ONE
  `:flush` message is queued. The flush lands behind every request already in
  the mailbox, so one `:dets.sync` covers all of them and each caller is
  replied only after the sync that covers its write — the "durable on disk
  before `update/3` returns" contract in `auto-restore-on-boot` holds, and a
  lone serial caller pays exactly what it paid before. A result that wrote
  nothing (an error, a no-op expiry) is answered immediately. Sync failures are
  logged, not turned into caller errors (the inline path never surfaced them
  either).
* **One `ProcessHub.Service.Batch` carries both group commits.** The "queue
  an item, send the flush message once when the batch opens, answer in
  arrival order" mechanism is one module; the registry and the declared list
  keep only what differs (sync dirty tables and reply results / write the
  manifest and run continuations). `Flezha.Parallel.map/2` is likewise the
  one "every element in flight, results in order" fan-out for partition
  members, group plugin enables and the bench.
* **The Coordinator's declared-list precommit takes the same group commit
  (process_hub, `DeclaredChildren.defer/4` + `flush/1`).** A mutation that
  writes answers `{:pending, manifest}`; the start/stop command (or a
  follower's `{:declared_mutate}`) is parked behind one `:flush_declared`
  and dispatched from it, after the sync. The manifest is broadcast/shipped
  once per batch (peers adopt the newest version, so intermediate versions
  need no broadcast). Nothing runs before the sync that covers its entry.
* **The WorkerQueue merges consecutive start operations into one batch**
  (`merge_start_batches/2`): start requests already queued behind the current
  one run in the same `Task.async_stream`, so their registrations share one
  registry sync. The first queued message that is not a start batch ends the
  merge and runs right after it, so queue order between a start and anything
  else is kept. NB a `stop_child` issued right behind an un-awaited
  `start_child` of the same id reports `:not_found` — the Coordinator composes
  the stop before the start has registered; that is the API's pre-existing
  semantics, not the merge. Each request runs against the hub snapshot it was
  dispatched with. Kept on evidence: with the pre-merge WorkerQueue swapped
  back in, 1000 operatives take 4.0–4.2 s instead of 1.0–1.1 s and a `-p 4`
  create 100 ms instead of 20 — once the other serialisation points are
  gone, this is the only thing stopping registrations from paying one sync
  each.
* **The declared manifest is written once per batch (process_hub).** The
  batch's working manifest lives in `hub.declared_unsynced`; `apply_mutation`
  builds on it and answers `{:pending, manifest}` without writing, and
  `flush/1` writes + syncs it once before running the parked commands — a
  failed write answers every parked caller and runs none. The coordinator
  flushes before it considers a peer's copy (adopt / version pull / refetch /
  clear), so an adoption never overwrites a batch in flight. At 1000 children
  one manifest rewrite cost 6.6 ms, and it used to happen on every start.
* **Partition membership is an index lookup.** A member's identity is stamped
  into its registry row's metadata at start (`start_child(..., metadata:)`)
  and re-derived from the row's own cspec on every bind write
  (`RegistryUpdate.set_runner_tag/1` → `HubManager.cspec_partition/1`), and
  `PartitionGroup.members/2` is `HubManager.partition_members/2`: an ETS
  metadata match (`ProcessHub.metadata_query/2`, the generalised `tag_query`)
  over rows carrying `:partition`, never a decode of every row. Every start
  and every plugin enable asks this question; at 1000 operatives it was a
  full registry decode each time.
* **An enable in flight is not a degradation.** With concurrent enables the
  cspec-first window (cspec written, subscribe pending) spans the whole phase,
  and the convergence loop healed those plugins — a second subscribe racing
  the real one. `Loop.without_pending_enables/1` drops `missing_plugins` whose
  id `PluginManager.pending_ids/0` reports. `scripts/bench.sh -n 1000`: plugin
  setup 57 s → 0.25 s.
* **The coordinator's state was copied into every request task.** The
  quadratic term behind `-n 1000` creation (9 s): `delegate_work` handed the
  WorkerQueue the whole `%Hub{}` — `pending_operations` holds every in-flight
  operation, so the copy grew with the load — and `Task.async_stream` copied
  it again into each request task: ~180 KB (max 860 KB) per message, 4 per
  start at N=300, 1.4 MB of copying per start. `Hub.for_workers/1` blanks the
  transient bookkeeping (workers read only `hub_id`/`storage`/`procs`/config)
  and is what the coordinator sends. Found by tracing `:erlang.send/2` with
  `{:message, {:caller}}` and `:erts_debug.flat_size/1` of the payload — a
  CPU profile only showed `send/2` at 18%, not who or what.
  `scripts/bench.sh -n 1000`: creation 9.1 s → 1.2 s (1.2 ms/operative, the
  in-memory floor); `-n 100`: 235 → ~190 ms.
* **`PluginManager` claims, the caller enables.** `{:claim, id, plugin}` puts
  a `{ns, rid, :pending}` entry; the cspec write and subscribe run in the
  caller (the cspec-first order is unchanged); `{:confirm, id}` turns it into
  `{ns, rid}`, a failed enable `unregister`s it. A pending entry refuses a
  concurrent disable and a second enable of the same id, and is hidden from
  `get_plugins/0` — every existing `{ns, rid}` pattern stays valid.
* **Null result: starting the Kafka client off the `WorkerRegistry` loop.**
  Tried (a `Task` per new identity, waiters answered on install) and
  reverted: an interleaved A/B on the bench's plugin phase showed no
  difference at p=4 (~10.5 ms both) or p=8 (~18 ms both) — with a warm
  broker the inline handshake is not the serialisation point. The mechanism
  depended on `KafkaEx.Client` not trapping exits and needed a replace-race
  branch; not worth carrying for zero gain.
* **`PartitionGroup.run_member_starts/6` starts every member at once.** The
  group-level quota preflight already existed; allocation stays per member.
  On any failure every member that did start is rolled back and the lowest
  failing index names the error — so the message counts ALL the members
  that came up beside the failure (`partition_group_test` asserts 2, not 1),
  and the fake registry's start log is sorted before it is compared.
* **Bench:** both bring-up phases run all pipelines concurrently (that is how
  a partition group starts), runner spawns overlap, the topic-readiness poll
  is 25 ms, and `Bench.Fleet.preload_code/0` loads the controller's code in
  `prepare`. `engine_startup` keys are unchanged, so old reports compare.
* **What is left:** a lone serial start pays 2 DETS syncs (~7 ms); halving
  that means a cheaper durable format than DETS (its sync rewrites free lists
  — 2× a plain append+fsync), proposal-sized. `DistributedSupervisor` keeps a
  `cid → pid` map in its pdict and re-puts it per start (O(N) map, small at
  1000). NB measuring a start storm: never trace whole modules at N=1000 with
  an in-heap collector — it OOM-killed the desktop once; trace a dozen seams
  under `systemd-run --user --scope -p MemoryMax=4G`, and attribute sends by
  caller + size rather than reading a CPU profile.

## Cross-repo

The process_hub changes live on process-hub's `dev` branch; flezha's
`mix.lock` pins the commit (`make refresh-process-hub` moves it). While
iterating, `FLEZHA_PROCESS_HUB_PATH=~/Code/process-hub` builds against the
working copy.

## Measuring

`scratchpad`-style probe: trace every exported function of `Flezha.*`,
`ProcessHub.*`, `KafkaEx.*` plus `GenServer.call/3`, `:gen.call/4`,
`:file.sync/1` with `[:call, :timestamp]` + `return_trace`, build per-pid
call trees from the call/return pairs, print inclusive wall time. CPU
profilers (`tprof`, `eprof`) hide the syncs and round trips that dominate.
