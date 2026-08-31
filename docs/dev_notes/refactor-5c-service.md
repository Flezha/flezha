# `service` — the seam is what cannot move, and five designed splits said otherwise

`lib/flezha/service` went from 15 files to 33 and from three files over 800 to
none, for **+25 production lines**. Everything below was measured on this tree;
a number that is an estimate says so.

## The ruler had to be fixed first, and it was vindicated by the change it was fixed for

`refactor_metrics.py` kept `mix xref`'s ` (compile)` / ` (export)` suffix on a
cycle member while `summarise()` tested membership against bare `git ls-files`
paths, so **every annotated member was invisible to `cycles` and
`max_cycle_files`** — ten of the 56-cycle's members, `lib/flezha/service`
reading 5 against a true 6. Fixed in its own commit (`3e9a1d28`) with a
red-first self-test: the fixture's annotated cycle vanishes from the area
entirely on the unfixed parser (`cycles: 1, max_cycle_files: 1` against a true
`2, 2`). Three area rows moved, none blocking.

The vindication arrived one commit later. The area's headline change is
`operative_manager.ex:23`'s `@id_in_use Validator.id_in_use()` — a module
attribute, hence the single `(compile)` edge in the repository's 56-file cycle.
Cutting it takes `--label compile` from `1 cycles found` to **`No cycles
found`**, and the cycle header from `(1 compile, 9 export)` to **`(10 export)`**
— because the file did not leave the annotated set, it **downgraded** onto its
surviving `→ operative/builder.ex` struct edge. On the unfixed ruler the area's
best change would have reported `max_cycle_files` **5 → 6, a regression for
progress**. With the ruler fixed it reads 6 before and 6 after.

**The four numbers in the brief were three.** `lib/flezha` is an area of the ten
*root* files (`application.ex`, `constant.ex`, …) at `cycles` 0 /
`max_cycle_files` 0 and cannot move; `46 → 56` was the repo cycle's
visible-member count, not an area row.

## Every designed split was refuted by measurement, and each for a different reason

Five partitions were drawn by the survey. **Three of the four this segment
built were cyclic against the tree**, and the reasons do not generalise —
which is why re-verifying, rather than trusting the drawing, is the rule.

**The survey's own tool had a blind spot.** `cg3.py` read a function's body from
the line *after* its head, so a one-line `defp f(x), do: g(x)` had **no
out-edges at all**. Corrected, `cli_manager` gains three edges — and the
compiler confirmed one of them for me as an undefined function
(`render_authz_verdict → render_forbidden`) after the split. The `cli_manager`
verdict survived the correction unchanged; `operative_manager`'s did not.

**`operative_manager` — D6 is cyclic, and the mechanism is an erpc.**
D6's own text says *"Verified: no edge `Core → Start` appears"*; there are four.
But the deciding fact is not the count. `route_call/4` is
`:erpc.call(target, __MODULE__, fun, args)`, and every `fun` it dispatches
(`:get_operative_pid`, `:request_stop`, `:stop_operative`, `:start_operative`,
`:spawn_or_deliver`) is a **public name a peer node resolves**. Any piece owning
`route_call` must therefore name the parent — a §3 breach *and* a new cycle
edge. So the routing seam stays in the parent and four smaller clusters move out
(`Placement`, `Teardown`, `Admission`, `Init`). The function graph is a DAG
apart from `do_await_runner ↔ poll_again`, so an acyclic partition was
**computed**, not drawn.

**`hub_manager` — the same family, plus a supervisor identity.** It is
`use Supervisor`: nine functions name `__MODULE__` as the supervisor
(`start_child`, `terminate_child`, `which_children`, `stop`), `init/1` is its
`@impl` callback, and `process_hub/1` builds two `%HookManager{m: __MODULE__}`
MFAs a peer resolves. Pin those, take the fixpoint, and D5's own decision rule
answers the shape: privates alone leave the parent at **783 lines**, over its
750 line, so 33 public bodies move and the facade delegates them — **exactly the
33 D7 predicted**.

## The sharpest detail: a behaviour change hiding in a capture

`Init.claim_lost` was the single upward edge left in `operative_manager`, so
`claim_lost/3`, `init_operative/4` and `finalize_init_operative/4` take a
leading `exist?` and the parent passes **`&exist?/2`**. It must be the *local*
capture. `&__MODULE__.exist?/2` is a remote capture, and `OperativeManager` is
`Mimic.copy`'d, so the remote form would start being **intercepted by a stub
where the local call never was** — a behaviour change visible only in the tests
that stub it. The same rule is why no sub-module calls its parent anywhere in
this area: an internal call that becomes a call to a Mimic-copied module
silently changes which calls a stub sees.

## The RPC surface is invisible to the compiler, so it is proved twice

`flz` builds `Flezha.Service.CliManager.<fun>(…)` as a **string**, at 55 sites
across 14 `cli/src` files, five of them generic helpers taking the name as a
parameter. A missed delegation compiles clean on both sides and fails on an
operator's machine.

1. **A golden capture, taken before the first edit** and now at
   `docs/refactor/rpc_surface_head.txt` (it lived only in gitignored
   `openspec/changes/` and cannot be retaken). After the split:
   **178 → 139 name/arity pairs, zero gained**, 39 lost, every one belonging to
   the nine deleted wrappers or the eleven demotions. A changed default count
   would appear as both a loss and a gain; none did.
2. **A test that reads `cli/src` by grep**
   (`cli_manager_rpc_surface_test.exs`, `async: true`). It resolves **67 call
   sites** — 45 direct literals with the arity parsed out of the Rust format
   string, 22 through seven modelled generic helpers — refuses to pass on fewer
   than 60, and **fails when a helper it does not model appears**. Both halves
   were driven red first: dropping `defdelegate state_get` names
   `cli/src/state.rs:11 … state_get/3`; removing `handle_read` from the table
   names the file and the unmodelled helper.

`OperativeManager` (35 pairs) and `HubManager` (52 pairs) are **zero lost, zero
gained** as well. Two `HubManager` names needed **two** delegations each —
`await_recovery_complete/1,2` and `operative_plugins/1,2` are independent clause
groups, not defaults, and one max-arity delegation silently drops the smaller.

## What the split cost, and what the ceiling forced

| row | baseline | now |
|---|---|---|
| `prod_loc` | 5 792 | **5 817** (+25) |
| `files` | 15 | 33 |
| `files_over_800` | **3** | **0** |
| `functions_over_50` | **4** | **0** |
| `narrative_markers` | 4 | **0** |
| `duplicate_pairs` (area) | 8 | **0** |
| credo | 53 | 50 |

+25 against the proposal's ≈ +160, because 130 lines went **down a layer**
(`Identificator` 93, `Config` 27, `Payloader` 10) and deletion funded the rest:
two dead modules (97 lines), nine uncalled CLI wrappers, `maybe_put_param/3`,
and `Payloader.construct_operative!/2`, which has **zero callers anywhere** —
refuting the design's claim that `cli_manager` and `lifecycle` call it, and
letting `service/payloader.ex` disappear entirely.

Two changes beyond relocation, both to keep a gate green and both worth
knowing: cross-module calls read `Context.f(…)` rather than `Cli.Context.f(…)`
because the longer form made the formatter break `SpecDecoder.decode(…)` across
four lines and pushed `operative_start` from 47 to **51**, the split's own
name-lengthening breaching §7; and four aliases were added because splitting
concentrated repeated fully-qualified calls into small files and credo's
`AliasUsage` fired fifteen times.

## `max_cycle_files` rises, and it is membership, not edges

`lib/flezha/service` reads **6 → 12** and `cycles` stays **1**. The arithmetic
is exact and none of it is an edge:

| | files |
|---|---|
| in the cycle at HEAD (ruler fixed) | **6** |
| `payloader.ex` **leaves** | −1 |
| `hub_manager.ex` becomes four | +3 |
| `operative_manager.ex` becomes five | +4 |
| **now** | **12** |

The repo cycle moves 56 → 62 by the same +6. Each piece reaches exactly what its
parent reached; **no edge changed to put any of them there.** This is §11f's
third instance verbatim — 5a's `wal_writer` 17 → 21 — and it is reported as
membership rather than explained away as a regression.

**The edges are the actual result.** Inbound `lib/flezha/** → service`,
D2's own ledger method (`--format dot`, aggregated by subtree):
**60 → 37, twenty-three cut.**

| source | D2 at `79eab9ef` | now |
|---|---|---|
| `runner` | 13 | **5** |
| `operative` | 12 | **9** |
| `convergence` | 10 | **8** |
| `plugin` | 8 | **5** |
| `lib/flezha` root | 6 | **5** |
| `state` | 2 | **1** |
| `shutdown` | 2 | **1** |
| `observability` | 2 | **1** |
| `transport` | 2 | **0** |
| `boot` | 1 | **0** |
| `schedule` | 2 | 2 |

Named individually: the compile edge
(`operative_manager.ex:23 → validator.ex`, the only one in the repository's
56-file cycle, now `No cycles found` under `--label compile`); **seventeen**
`→ Identificator`; **two** `→ Payloader`; **four** `→ ConfigManager`
(17 + 2 + 4 = 23, which is 60 − 37 exactly); and
`payloader → operative/builder`, which is what let `payloader.ex` leave the
cycle outright — the one membership change in this area that *is* an edge.

**`plugin → service` is 8 → 5, not D2's 8 → 4**: the design lists five survivors
and then subtracts to four. Three are cut; `operative_forward.ex` contributes
two of the five on its own.

## `duplicate_pairs`, and both scans say something different

| scan | baseline | now |
|---|---|---|
| area (`--area lib/flezha/service`) | 8 | **0** |
| full (what `baseline.json` compares) | 11 | **3** |

**All eight area-internal pairs are gone** — they were `audit_log.ex ↔
observability.ex`, and the `CallerContext` fold removed the duplication rather
than moving it. The three that remain are the **same three cross-area pairs the
survey measured at HEAD**, re-anchored to the files the split created:
`operative/plugins.ex ↔ hub_manager/rows.ex`, `cli/controller.ex ↔
shutdown/coordinator_test.exs`, and `cli/mem_top.ex ↔
state/durability_tier/backend/local_path.ex`. Not one of them is internal to
this area, and not one of them moved in count. A number read without its scan
would report 0 against a baseline of 11 and be wrong twice over.

## Small things that cost time

- **A generator must not read its own output.** `split.py` re-read
  `cli_manager.ex` after the facade had replaced it and emitted fifteen broken
  modules. Pin the generator to a pristine `git show HEAD:` copy.
- **A multi-line `@doc """…"""` is not part of a leader** under a naive
  backward scan — its closing `"""` does not match the attribute pattern — so
  every such block travelled with the *previous* function and orphaned itself on
  a deleted one. The same holds for a wrapped `@spec`, which put
  `request_stop/3`'s spec in the wrong module.
- **`prod_loc` read ~2 000 low all afternoon** because the new files were
  untracked (§11e). Every number here was taken with the metric's own classifier
  over the real file set; `--write-baseline` must not run until the supervisor
  has `git add -N`'d them, or it arms every ratchet below reality.

## What no row could resolve

**S1 was reclassified `direction` and not run, and the evidence that replaced it
is stronger.** Every function on the plugin routing seam was diffed against
HEAD: `send_io/4`, `send_io_confirming/5`, `send_io_confirming_many/5`,
`pipeline/6`, `get_operative_pid/3` and `route_call/4` are **byte-identical**,
and the only change in `on_hub_member/5` is `pick_runner(…)` →
`Placement.pick_runner(…)` **inside the cross-node branch**, which the
per-record path never reaches. `bench.roundtrip` has no measured floor anywhere
in this programme; running one against a path proven byte-identical spends the
machine for a number nobody may act on.

**The registry read path is unmeasured.** Nothing on this box drives
`HubManager.operative_rows/1` or `partition_members/2` under the
1 000-operative load `operative-startup-latency` optimised, and this area split
that module into four.
