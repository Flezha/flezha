# close-silent-plugin-wedges

Controller-side plugin lifecycle + convergence. No wire, SDK or cross-SDK
surface, so the "one change, every SDK" rule does not apply.

## The headline lesson: D1 was inert, and only the e2e drill found it

The design's core claim was that a plugin stuck `:prepared` becomes
`missing_plugins` and "the EXISTING `start_plugin` primitive retries it". It
could not have. The chain is

```
start_plugin → Heal.plugin_call → {:plugin_subscribe, id, plugin}
             → Operative.Plugins.subscribe/3 → Table.subscribe/3
             → validate_plugin_unique/2        ← REFUSES a duplicate id
```

and a plugin whose `activate/1` failed is still SUBSCRIBED. So every heal would
have failed at the subscribe step, on every tick, forever. The change would have
shipped looking correct — divergence visible, op planned, heal attempted, error
reported honestly — and healed nothing.

**Every unit test passed while the mechanism was inert.** They exercised the
detection (Probe/Diff/Planner) and the reporting (`subscribe/3`'s return) as
separate seams; nothing drove a real heal against a real table. Task 7.3's
end-to-end drill is what caught it on its first run.

The fix is at the existing seam, and adds no primitive: `Plugins.subscribe/3` is
idempotent — a plugin already on the table is re-ACTIVATED, not re-subscribed.
`bulk_activate/2` already skips `:running`, so the same call is a no-op for a
plugin that needs nothing. This is now a normative requirement in
`operative-extensions` ("Bringing up a plugin SHALL be idempotent on one already
subscribed") precisely because an unspecified load-bearing behaviour is one a
future cleanup will silently re-break.

`test/flezha/convergence/plugin_activation_recovery_drill_test.exs` is the
regression: a real `%Overwatch{}` served through the production
`Overwatch.handle_call/3`, a real `Extension.Table`, a real
`Diff → Planner → Heal.execute/1` round, and a real socket upstream (down = a
port with nothing listening, up = a listener on that same port). Only
`HubManager.get_pid/2` is stubbed.

## The three composing fail-open decisions

Each is individually correct; the wedge lived in the seam between them.

1. `:prepared` is healthy — it has acquired no upstream resources, so it has
   nothing to be unhealthy about.
2. An uncertain probe is healthy — the reconciler must never escalate on a probe
   it could not complete.
3. An idle operative is not stalled — a genuinely quiet stream must not read as
   a wedge.

`Probe.running_plugins/1` reported `Map.keys(table.plugins)`, phase-blind. So a
dead plugin was counted as running (1), reported healthy (1), never probed (2
never fired), produced nothing, and its whole downstream read `:idle` (3). Empty
Diff, empty plan, forever, with one `Logger.error` at bind time as the only
trace.

The fix keeps all three rules and repairs the seam: **the phase, not the health
verdict, is the authority on whether a plugin is running.**

## The `missing` / `extra` split

`plugin_divergence/2` now reads TWO sets and they are not interchangeable:

- `missing` ← the `:running`-phase set. A declared plugin outside it is missing,
  whether never subscribed or subscribed-and-failed-to-activate.
- `extra` ← the full SUBSCRIBED set. An undeclared plugin must be torn down
  while it is still `:prepared` too.

Reading one set for both checks is the original defect. Reading them the wrong
way round either stops tearing down undeclared plugins or starts a
stop/re-subscribe storm; both directions have a test.

`Loop.without_pending_enables/1` ALREADY covers the new `:prepared` entries — an
enable in flight has written its cspec and not yet subscribed, and its plugin id
is filtered out of `missing_plugins`. **Do not add a second guard for it.**

`plugins_diffable?/1` still gates both checks on a live, non-parked,
non-suspended operative, so a legitimately-`:prepared` plugin on a parked
operative cannot churn.

## Fail-open is reported upward, emitted in one place

There were four sites that swallowed an uncertain probe: `Table.safe_health`,
`ProviderBehaviour.safe_verdict`, `ProviderBehaviour.workflow_verdict`, and each
worker-backed provider's own `:timeout -> :healthy` branch.

They now all report `{:fail_open, reason}` as a VALUE, and
`Flezha.Plugin.Extension.Table` is the single place it becomes
`[:flezha, :plugin, :health, :fail_open]`. The reason Table owns the emit: it is
the only layer that reliably knows which plugin a provider belongs to — a
provider's own opts may carry no `:plugin_id`, so an event emitted from inside
one could not name what had gone quiet.

`health_verdict()` is unchanged; the new `probe_result()` type is
`health_verdict() | {:fail_open, reason}` and never escapes `Table.health/1`.

## The `http_poll` idle window

- Measured from the last SETTLED poll (injected, unchanged, OR failed), stamped
  in `settle/3` — the one funnel every outcome passes through. NOT from the last
  injection: `http_poll` is content-gated, so a healthy poller against unchanged
  content correctly injects nothing for as long as the content holds.
- Default is **3 × `interval_ms`**. A poll that outlives its interval stretches
  the cadence rather than stacking, so one slow cycle plus the poll after it must
  both land inside the window, or a merely slow endpoint reads as a dead one.
- `idle_after_ms: 0` means "derive from the interval". There is **deliberately no
  disable value** — a source that has stopped settling polls is the wedge this
  window exists to catch. This differs on purpose from `websocket`'s
  `resubscribe_after_ms: 0`, which means "disabled": that window is an opt-in
  extra on top of a connection-liveness watchdog that is always on, whereas this
  one IS the liveness check.
- A failure run outranks the idle age when both apply — it names the actual
  error; the age only says nothing has happened.
- `worker_health/3` is called with `budget_ms: 2 * probe_budget_ms()` for this
  provider ONLY. The inner `PollWorker.probe/1` timeout and the outer
  `workflow_verdict` budget are both 500 ms, so at equal budgets they race and
  the provider's precise `:probe_timeout` is replaced non-deterministically by
  the generic `:probe_budget_exceeded`. Doubling the outer one lets the inner
  probe report first. The cost is only paid by an actually-wedged worker.

## Refuted by reproduction — do NOT re-investigate

- **A slow-drip endpoint holding a fetch open.** `:httpc` enforces a TOTAL
  request deadline, not an inter-byte one — measured 325 ms elapsed against
  50 ms drips. The failure run does climb and the poller does go unhealthy. There
  is nothing to fix.
- **A bound-but-parked operative.** It refuses `registry_runner_set`, so the
  registry binding stays unset, it surfaces as unbound, and the EXISTING rebind
  un-parks it. Already handled.

Finding C (`classify_producing` returning `:idle` for an entity dead an hour) is
real but deliberately NOT fixed in the classifier: it cannot distinguish a dead
source from a quiet one, and guessing would make every quiet stream a false
wedge. The source is the only layer that knows whether IT is working, which is
what the idle window above answers.

## Arity drift

`tasks.md` names `Extension.Table.safe_health/3`. It is `safe_health/4` — the
plugin id is threaded in so the fail-open event can name the plugin. The task
text predates that decision; the arity is not an error.

## Probe's observed-operative shape

`Diff` reads every key of an observed operative through `Map.get(_, key,
default)`, so a field added to one Probe constructor and forgotten in another
does not fail — it silently reads as "nothing diverged". The three constructors
are therefore one `observed/1` with a default map, and the keys are listed
exactly once. (`unreadable/0` had already been omitting `suspended?`; the
extraction closed it with no behaviour change, since the default already
matched.)
