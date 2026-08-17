# Last-runner migration keeps its state (`fix-last-runner-migration-state-loss`)

- **The three defects were one silent failure.** A controller losing every
  runner could neither keep its operatives nor move them, and the healing
  machinery destroyed its own evidence.
- **GOTCHA — runner ownership is NODE-LOCAL.** `ControlUnit` and the
  `StreamChannel` registry live in the per-node `Identificator` Registry and
  `FleetManager` is a per-node GenServer, so an Overwatch can only bind to a
  runner connected to ITS OWN node. `Convergence.Heal.rebind_operative/2`
  (private, behind the `:rebind_operative` primitive)
  handing a local target to a remote Overwatch fails by construction
  (`{:ping_failed, :no_control_unit}`).
- **GOTCHA — `runner_migrate` used to commit the binding BEFORE the ping.** A
  failed migrate then left the operative naming a LIVE runner it could not
  reach; `Probe` read `bound_runner ∈ active_runners`, `Diff` dropped it from
  `misbound_operatives`, and the delta went permanently empty. Symptom: an
  operative that `flz operative list` shows as placed and healthy while every
  frame drops and convergence reports nothing to do. The binding is now
  committed only after the runner answers.
- **GOTCHA — the handover's state source is the RUNNER.**
  `prepare_handover_state/1` calls `serialize_operative_sdk(state.runner_id, ..)`,
  so a migration caused by that runner's death ships `<<>>`. Setting
  `arrived_via_hotswap? = true` on that empty handover suppressed the WAL
  restore at bind time AND the `kick_restore` repair — fresh start, replay
  locked out, state readable on disk the whole time. The flag is now set only
  when the payload was non-empty.
- **`Flezha.State.WalTransfer`** — destination-pulled chunked `:erpc` copy of a
  Entity's durable files. Source answers reads only (`entities/1`,
  `manifest/1`, `read_chunk/3`); destination writes `.transferring` → fsync →
  atomic rename. `read_chunk/3` resolves the requested name against
  `Registry.entity_files/1` (extracted, also backs `delete_files/1`) rather
  than joining onto the state dir — a peer must not be able to read arbitrary
  paths. `prepare_handover_state/1` stamps `source_node`; an empty handover
  triggers `adopt_source_state/2` → pull → the existing `{:kick_restore, _}`.
- **`hub_migrate` drains instead of no-opping.** It had been reduced to
  `stop_hub_if_vacant/1`, which no-ops exactly when the hub holds operatives —
  i.e. whenever there is something to migrate.
  `HubManager.drain_hub/2` is the ONE drain seam (extracted from
  `Shutdown.Coordinator`, including the load-bearing `await_local_children_gone`
  — `Migration.drain/2` returns before the async handovers land). Runs off the
  FleetManager loop: it blocks to its deadline and a runner's disconnect call is
  waiting on that process.
- **NO `ProcessHub.leave/2` was written.** The spec asked for it "or whichever
  ProcessHub-provided leave function honors `HotSwap{handover: true}`" —
  `Migration.drain/2` already is exactly that, and flezha already consumed it.
- **GOTCHA — the migrate drain must be SHORT** (5 s vs graceful shutdown's 60 s).
  A drain holds the node out of the namespace's distribution for its whole
  duration, and `hub_migrate`'s is fire-and-forget: a 60 s one outlived the
  namespace it drained and broke whatever used that namespace next. Same hazard
  in production when a runner reconnects seconds later.
- **GOTCHA — restoring a stopped hub also needs its runner hooks.** A fresh hub
  has no per-runner `child_registered` hook, so `hook_insert_pid` never fires,
  the StreamChannels never learn the operative pids, and every later data-plane
  read (entity state, migration, drain handover) silently gets nothing back
  while everything reads healthy. Restore is `ensure_hub(ns)` PLUS
  `HubManager.register_handler(ns, runner)` per `FleetManager.active_runners(ns)`.
  This bit `hub_manager_registry_test.exs`, whose `durable_registry_pending?/0`
  setup stops every OTHER running hub — including the suite-shared `default`,
  whose absence surfaces as the misleading "no runners in namespace default"
  (`Validator.valid_namespace/1` reports a dead HUB with that string).
- **Pinned by** `test/flezha_test.exs:713` ("runner disconnect migrate hub") on
  its ORIGINAL assertion, `test/flezha/state/wal_transfer_test.exs` (incl. a
  real two-BEAM-node pull with distinct state roots, `:multi_node`), and the
  two new cases in `test/operative/overwatch_async_migrate_test.exs`.
- **Limit**: one-shot at migration time. A controller that dies WITH its disk
  loses that state; replication is still a non-goal.

