# guard-cross-sdk-placement — implementation notes

Durable `State`/`Change` bodies are per-SDK below the shared 16-byte envelope,
and a restore ships them verbatim to whichever runner bound the Entity. This
change makes the SDK language an allocated identifier, puts it on the factory
advertisement, and filters placement on it — plus stops the controller
discarding a refused restore.

## The proposal was stale in one area — verify before implementing

`state-evolution` (5b6022ab) landed between this proposal and its
implementation and had **already** done tasks 1.3 / 2.1 / 2.2. Both restore
arms now share `flz_core::restore::fold_change_log`, which resolves every
record through `header::resolve_record` → `resolve_version`, and that compares
byte 0 against `expected.sdk_lang` — the **reading class's** declared value
(`HostFacts::header/0`, built from `decl.sdk_lang`), never a hardcoded
`SDK_LANG_RUST`. The snapshot half goes through `resolve_state`, same check.

So the proposal's premise ("the host path reads byte 9 and never byte 0") no
longer held. What *was* still open in that area, and is the actual hole this
change closed: `HostClassDecl.sdk_lang` carried
`#[serde(default = "default_sdk_lang")]` returning **Rust's byte**, so a veneer
that declared no language silently became Rust — and then compared its own
records against Rust's value. The fix is that the field is mandatory and an
unallocated value is refused at declare time.

## The FleetManager does no I/O on the connect path

The first implementation resolved the durable half of the established language
inside `store_factories/4` by walking `HubManager.operative_rows/1` →
`Registry.entities_of/1` → `Restore.sdk_lang/1`. That runs **inside the
FleetManager GenServer, on every advertisement**. The suite answered
immediately: `GenServer.call(FleetManager, {:runners_for_factory, ...})`
timing out into `{:error, :unavailable}`, and placements failing across the
board — every concurrent placement lookup queues behind that walk.

`affected_operatives/4` calling `operative_rows/1` is not a precedent for it:
that path runs only when a factory is *dropped*, not on every connect.

The durable read moved to the **bind** (`Lifecycle.complete_init/3`, beside the
existing `ClassRegistry.put_keyless/2`), which is the one seam holding both the
factory id and the operative's on-disk class. `Restore.class_sdk_lang/1` reads
it there, once per bind. Until it does, the SDK-side refusal is the backstop:
the runner refuses the other language's records, and the bind now parks instead
of cold-starting.

## The established language can be LOST, so the read must self-heal

`ClassRegistry`'s ETS table is recreated empty if its owner restarts, and the
durable sidecar lives under the factory's state dir — which
`Flezha.State.TestHelpers.reset_all_entities/0` (and any state wipe) removes.
Treating "no record" as "unknown → refuse everyone" made every operative in the
default namespace unplaceable mid-suite, with 23 failures that looked nothing
like their cause.

**A missing record is not evidence of a conflict.** `placeable_runners/3` falls
back to the factory's own live advertisements: unanimous, it is the established
language for that read; disagreeing, nothing places, because the controller
genuinely cannot tell which language owns the factory. The query derives and
returns — the next advertisement re-records.

## The correction runs BEFORE the restore, or a tier operative parks forever

The bind both corrects the factory's language from the durable header and, for
an effectively-once class, awaits the restore that can now refuse. Order
matters: if the correction ran after the restore, a factory established at the
wrong language would fail its restore, park, and never reach the line that
would have corrected it — every subsequent attempt parking the same way. The
correction is therefore a clause in `complete_init/3`'s `with` chain, ahead of
`tier_restore/2`.

## A query does not write a file

`placeable_runners/3` falls back to deriving the established language from the
live advertisements. The first version *re-recorded* what it derived, which put
`ClassRegistry.put_sdk_lang/2` — `mkdir_p` + `write` + `rename` — inside a
`handle_call` on the placement read path. That is the same defect as the
connect-path walk above, one layer in.

Recording belongs on the write path: `store_factories/4` establishes the
language when nothing is recorded, because that is where the fact arrives and
where the controller is already mutating state. The query derives and returns.
A record lost between advertisements is re-recorded by the next one.

## A control-plane GenServer must not crash on a shape question

Adding `langs` to the stored factory entry made `%{info | langs: ...}` raise
`KeyError` on any entry built before it — including ones test helpers seeded by
hand. The FleetManager died, and **every** test after it failed on a dead
control plane. One constructor (`factory_entry/1`) now states the shape and is
total over an older entry; the two test seeding helpers were collapsed onto
`FleetStateHelper.seed/2` so the shape has one definition on the test side too.

## The restore verdict was already on the wire

`RestoreInitStateResponse` carries no error field, and it does not need one:
errors ride the outer `CommandResponse.status`/`errors`, and
`ControlUnit.handle_payload/3` already turns `status == "error"` into
`{:error, reason}`. So `await_restore_init_state/2` was *already* returning the
refusal — all three call sites simply discarded it (`_ = ...`). The work was
reading it, not plumbing it.

- The tier bind rides the **existing** parked-error contract: `tier_restore/2`
  returns `{:error, reason, state}`, which `complete_init/3`'s `else` already
  maps to `{:error, reason, parked}` and `bind/3` already parks. No new failure
  arm.
- The off-process replay (`replay_persisted_entities/4`) has no bind to fail, so
  it sends `{:restore_refused, reason}` to the Overwatch, which parks through
  the same `Lifecycle.park/2`.
- The correction rewind reports the refusal instead of replaying the slice: the
  rewind already truncated the Entity to its baseline, so there is nothing to
  replay *onto*.
- An Entity with no snapshot and a zero-length WAL never reaches any of this —
  `Restore.has_persisted_state?/1` filters it out, so "empty is a success" is
  structural rather than a special case.

## Smaller traps

- **`Mimic.set_mimic_global/0` is required** for anything the FleetManager reads:
  the filter runs in that process, and a per-process stub never reaches it.
- **`ClassRegistry.put_sdk_lang/2` writes a sidecar under
  `Registry.class_dir(factory_id)`**, creating a directory for a factory that
  may hold no other state. Harmless (`entities_of/1` finds no entity keys under
  a bare file) but it is why a seeding helper must clear what it established.
- **The conformance gate already anticipated this scenario.** Its "no runtime
  literal outside an interop scenario" rule matches `@moduletag interop: true`
  exactly — the interop test opts in with that tag, not `@moduletag :interop`.
- **The committed `test/apps/java_runner/dist/java_runner.jar` must be rebuilt**
  (`scripts/build-dist.sh`). A jar built before this change advertises no
  language and therefore never places — that is the intended BREAKING behaviour,
  and it applies to every committed runner artifact.
- **`Parallel.map/2` gained an opts arg** rather than a second mechanism: the
  bind restore needs the results *and* the bounded concurrency `each/3` had.
  Both now share one private `stream/3`.

## The refusal reason nobody saw

`validate_factory/2` is not the message an operator gets. `start_operative`
checks `locally_startable?/2` first; with no local candidate it routes, and the
cluster Directory is keyed by factory id with no notion of language — so the
error that actually surfaced was `cluster_wide_refusal/2`'s "no runners for
factory X on any cluster node". The interop e2e is what caught it. Both sites
now read one `factory_refusal/3`.

## The interop scenario must not share a run

`:interop` is excluded from `mix test` **and** from `--include system`. Two
reasons, and the second is the one that bites: the tiers are one-runtime-per-run
by contract; and the scenario's namespace, torn down while another tier is
still running, left the FleetManager holding a namespace whose hub ETS table was
already gone. The next `runner_cleanup` tick walked it and died in
`stop_hub_if_vacant`, taking the FleetManager down and failing everything after
it — three of the five "e2e failures" in that run were that one crash.

Its teardown therefore stops the namespace **before** closing the runner ports,
so the namespace leaves the fleet while its hub is still alive.

**Pre-existing defect this surfaced, and FIXED here because it made
verification impossible:** `HubManager.durable_operative_keys/1` does
`:ets.foldl` on a table that may no longer exist. It already had
`catch :exit, _ -> []`, but `:ets` **raises** for a missing table rather than
exiting — so `runner_cleanup/1` killed the FleetManager, and the control plane
with it, on any namespace whose hub died before the tick reached it. That crash
is a strong candidate for a good share of this repo's "load-sensitive flake
population": a FleetManager that dies mid-run fails whatever comes next, in
whatever subsystem, which is exactly the rotating failure sets the e2e tiers
show. The catch now covers `:error` too.

## The record is keyed by NAMESPACE and factory, and holds no disk

The first implementation keyed the established language by factory id alone,
with a durable sidecar — following `put_keyless/2`'s shape. Both halves were
wrong, and the Java e2e tier proved it with 28 failures reading

> factory `session_timeout` in namespace `timers_crash` is established at
> sdk_lang 1

The boot-time runners in the `default` namespace are Rust, and the conformance
fleet gives the SAME reference factory ids to every SDK on purpose. A
cluster-global record therefore let `default`'s Rust runners establish every
shared id at `1`, and every Java-tier namespace was refused. A factory id names
a class **within a deployment**, not across the cluster.

The sidecar went too. It bought nothing — the durable header re-establishes the
language at the next bind, and the SDK's own refusal is the backstop — while
costing `mkdir_p` + `write` + `rename` inside the FleetManager, and it wrote to
a namespace-ambiguous path (`Registry.class_dir/1` resolves its own namespace).
In-memory, exactly like `keyless`.

Note this makes `keyless?/1` the remaining factory-global declaration. It has
the same latent collision; nothing exercises it today.

## Deliberately not done## Deliberately not done

- No escape hatch for a forced cross-language placement. Every use we can name
  is better served by drain-and-cutover under a new factory id, and an override
  is the switch that gets set once during an incident and never unset.
- No portable cross-language State/Change codec, and no portable snapshot export
  for a deliberate Rust→Java migration.
