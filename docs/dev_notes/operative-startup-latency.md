# Operative startup latency

Measured before the change (bench `-p 4`, two runners): ~55 ms per operative
start and ~30 ms per plugin enable, all serialised, so a four-pipeline bench
spent ~365 ms bringing the engine up and ~400 ms to its first record out.
After: ~30 ms for the whole bring-up (15–18 create + 12–14 plugin) and ~37 ms
to first out; `mix bench -n 100` creates 100 operatives in ~235 ms (was
~1.0 s) and enables their 200 plugins in ~55 ms (was ~0.9 s). Every number
here is a laptop SSD (fsync ≈ 1.6 ms); on a slower disk the registry share
is larger.

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
  dispatched with.
* **What is left:** one registration sync per *batch* and one precommit sync
  per batch; a lone serial start still pays 2 DETS syncs (~7 ms). Halving that
  means a cheaper durable format than DETS (its sync rewrites free lists —
  2× a plain append+fsync), which is a storage-format change with a migration
  story, proposal-sized.
* **`PluginManager` claims, the caller enables.** `{:claim, id, plugin}` puts
  a `{ns, rid, :pending}` entry; the cspec write and subscribe run in the
  caller (the cspec-first order is unchanged); `{:confirm, id}` turns it into
  `{ns, rid}`, a failed enable `unregister`s it. A pending entry refuses a
  concurrent disable and a second enable of the same id, and is hidden from
  `get_plugins/0` — every existing `{ns, rid}` pattern stays valid.
* **`WorkerRegistry` starts a new identity's client in a `Task`.** Acquires
  for the same key queue as waiters on `state.starting` and are answered
  together on `{ref, result}`; the registry `Process.link/1`s the client on
  install. This works because `KafkaEx.Client` does NOT trap exits: the
  task's `:normal` exit is ignored by the linked client, while a registry
  crash before install kills the linked task abnormally and the client with
  it. A key with `pending_refs` (a swap in flight) keeps the inline start —
  its refcount hand-over expects one. If a `force_replace` installs the key
  meanwhile, the task's client is stopped and the waiters attach to the
  replacement.
* **`PartitionGroup.run_member_starts/6` starts every member at once.** The
  group-level quota preflight already existed; allocation stays per member.
  On any failure every member that did start is rolled back and the lowest
  failing index names the error — so the message now counts ALL the members
  that came up beside the failure (`partition_group_test` asserts 2, not 1).
* **Bench:** both bring-up phases run all pipelines concurrently (that is how
  a partition group starts), runner spawns overlap, the topic-readiness poll
  is 25 ms. `engine_startup` keys are unchanged, so old reports compare.

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
