# The effectively-once tier in the Elixir SDK — one record, a held record, and two mechanisms that were off at the same time

`carry-effectively-once-in-elixir`: the effect record, the staged interval, the
consumed-offset floor, the tier-on park and `Ctx.stable_id/1` in
`lib/flezha/sdk`, which inherits nothing from `flz_core`; `effectively_once`
joins the required set, the seventh capability the programme made universal and
the last one no manifest defers apart from `correction_window` and `workflow`.
What `proposal.md` and `design.md` say is not repeated. This is what the next
person touching `Flezha.SDK.Entity.Tier`, `Entity.Interval`, the routing leaf or
the fixture twins needs.

## The design said nothing splits, and `entity.ex` reached 854

With the tier arms written inline `entity.ex` came out at **854** raw lines
against a **blocking** `files_over_800`, so D14's "nothing splits" was simply
wrong and the change grew an unplanned third module. The split is by mutation
(charter §7): everything that writes `tier.staged`, `tier.deadline` or
`tier.owed` moved to `Flezha.SDK.Entity.Tier`, while the process keeps its
mailbox, its ladder and its item kinds. `Entity.Tier` aliases only `Interval`,
`Records` and `State` and **never names `Entity`**, so the split forms no cycle;
`entity.ex` landed at **782**, under the ceiling. The lesson is the sizing one:
a design can price the lines and still be wrong about where they land, and the
ceiling is the instrument that says so.

## The record is held, and the Entity's queue never is

The reference gates a producing Entity until its inter-procedure push lands.
Re-expressed literally here that is a two-party deadlock: this SDK's router is
one process, and a gated Entity plus a router blocked in `enqueue_block/2` on
that very Entity's full queue wait on each other for ever. So the Entity holds
the *record* in a FIFO (`tier.owed`) and keeps draining its queue; the router
sends one `{:edges_delivered, ref}` after its deliveries and the FIFO ships in
order. The requirement — an offset is never durable ahead of the output it
describes — is satisfied by the hold, not by the gate. Records still owed when
an Entity is killed untrappably die with it, and the seq gap that leaves is what
makes the controller resync the Entity and re-forward those offsets: the tier's
own replay, not silent loss.

## KI-139 — both halves of the output path were off at once

A class under the tier that holds **no** state (no `apply_change/2`) shipped
nothing at all: the router **skips sink delivery for a tier class** — correct,
the record carries the output — while `settle/4` short-circuited on
`changeful?`, so the emit reached neither the wire nor a record. Not a
durability gap: **silent output loss**. The reference settles it and reframes
it — `flz_core/src/effect.rs:159-172` ships on
`parked || !(changes.is_empty() && outputs.is_empty() && timers.is_empty())`, so
**empty changes with an output SHIP**. Two lines fixed it inside this change:
`settle/4`'s `changeful?` clause is deleted (what ships is already decided by
`Interval.fold/4`, which carries the reference's condition verbatim), and
`persistent?` becomes `persistence_enabled and (changeful? or tier.on?)` —
**one flag was conflating two facts**, whether the procedure holds state and
whether the class opted out of durability. Off the tier they coincide, because
a record stream exists there only to carry changes; under it the record carries
the outputs and the offset too. Unreachable before the manifest flip, which is
why it closed before the capability flipped.

The neighbouring clause is **not** the same hole. `ship_effect/4`'s
`persistent?: false` arm discards a tier class's outputs, and
`flz_core/src/flusher.rs:236-262` does exactly that too — it drains to frames,
advances the seq, then discards when `persistence_enabled` is false. An
explicit persistence opt-out loses a tier class's outputs in the reference as
well, so matching it is parity, and diverging quietly would have been the
defect.

## Six tests were written after their code, and inversion is what proved them

Half the tier tests could not be shown red first because their code already
existed. They were proved by **inversion** instead — the tier arm disabled gives
1/12 passing, the hold ignoring its ref gives 11/12 — and the logs kept. For a
test written late that is the honest instrument; a claimed red it never had is
not.

## The gate row is a direction row, and the instrument says so

The tier-off dispatch path pays one boolean in the Operative's input handler,
one in `complete/3`, two more ctx opts and a guard clause that does not match —
priced at ≤ ×1.03 on the harness micro-bench (`CounterOp`, 50 000 pushes,
µs/push). The **before** arm's own three series read 23.26 / 20.44 / 21.10
µs/push at spreads of **15.5 %, 9.4 % and 2.1 %**, so the instrument cannot
resolve the 3 % it was meant to police. The row was declared a direction row in
batch 0 on that evidence and both arms are reported as numbers: after 22.38 /
21.75 / 22.12 at 10.8 / 11.0 / 7.3 %. Guard row (`mix bench --shape regression
-n 10 -m 2000`, three runs): 127 295 / 117 785 / 122 985 msg/s before, 118 646 /
117 459 / 124 327 after — two overlapping populations. **Manufacture no verdict
against a threshold the instrument cannot resolve.**

The feature-cost micro-bench has a property worth knowing before it is read
again: `Flezha.Bench.BenchKeyed` and `Flezha.Bench.TierStore` push `1..n` as
**distinct keys**, and per-push cost grows with the state that accumulates —
46 µs/push at n = 500, 64.7 at 2 000, 131.6 at 5 000, 245 at 10 000. Both arms
pay it identically (245.07 tier-off against 244.63 tier-on at n = 10 000), so
the row says the tier costs nothing measurable on this instrument and says
nothing at all about the absolute number.

## The fixture twins: one shared body, because a copied one moves two ratchets

`ElixirRunner.CheckpointStoreProc` began as a faithful copy of
`Flezha.Bench.TierStoreProc`'s `run/3`, and the eight duplicated lines raised
`duplicate_pairs` on **two blocking areas at once** — `lib/mix` 2 → 3 and
`test/apps` 145 → 146 — because the detector counts one cross-file pair against
both areas it spans. The cure is the reference's own shape read properly: the
tier posture and the checkpoint posture are two *procedures* over one store, so
`TierStoreProc` exports `split/1` and `upsert/4` and the checkpoint twin adds
only its `commit` arm. Both rows went back to baseline.

`lib/mix` is also a **blocking `credo_warnings` ratchet**, and a
fully-qualified `Flezha.SDK.Channels.outputs_at_checkpoint/2` inside a private
helper is one finding ("nested modules could be aliased"). The conditional
belongs inside `channels/1`, under the `import` the file already has.

## The arms drive the test app's own `_build`

`test/apps/elixir_runner` is a separate mix project with its own `_build`. An
e2e or bench arm that is not preceded by the manifest's build command
(`MIX_ENV=dev mix compile` for the e2e arms, `MIX_ENV=prod` for the bench)
measures the **previous** SDK and reports a green that means nothing. Every arm
in this change was preceded by one, and the two fixture files were recompiled in
both envs after every edit to them.

## The two declaration keys are the whole golden

`test/fixtures/declarations/tier_store.json` is
`{"effectively_once": "true", "keyed": "false"}` and
`checkpoint_batched_store.json` adds `"ack_granularity": "checkpoint"`. That is
all the smoke goldens pin, so the tier's cross-SDK surface is two metadata keys
plus the record on the wire — everything else is observed through the
controller.

## What change 5 inherits

`Interval.fold/4`'s `event_time` and the restored `high_water` are where the
correction window's replay slice reads its baseline; `%Entity.Input{}` is where
`flz.replay` rides; `Records.settle/3` is where a corrected record's epoch
lands.
