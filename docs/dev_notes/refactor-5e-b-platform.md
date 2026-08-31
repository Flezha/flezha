# `refactor-5e-b-platform` — fifteen trees, and a struct is an edge

## The finding that outlived the area

**A struct pattern is a dependency edge, and no call-graph tool used this week could
see it.** Design §7.1 proved `convergence/loop.ex`'s four groups a star by resolving
*local function calls* between them, and called it the first split the programme
produced acyclic on the first drawing. It was not. All four groups pattern-match
`%__MODULE__{}` and three return a mutated one, so each new sibling would name `Loop`
for its struct while `loop.ex` named the sibling — three 2-cycles.

Measured, not inferred, with one throwaway line in a module `loop.ex` already points
at:

```elixir
# lib/flezha/convergence/snapshot.ex
def probe_struct_edge(%Flezha.Convergence.Loop{}), do: :ok
```

`mix xref graph --format cycles`: SCC **63 → 66**, and **`loop.ex` itself entered the
cycle**. Reverted: back to 63. A bare `@spec` reference is enough too — `diff.ex →
intent.ex` exists in the graph although `diff.ex` names `Intent` only in a `@spec` and
a comment.

The remedy is charter §7's own rule (*split by mutation × SCC*) and a shape the tree
already had twice (`state/wal_writer/state.ex`, `service/cli/state.ex`): a **fourth**
file, `convergence/loop/state.ex`, holds the `defstruct`. Every sibling aliases
`Loop.State` and **never `Loop`**. Graph: `loop → {state, liveness, progress, summary}`,
`sibling → state`, `state → backoff` (a leaf). `convergence max_cycle_files` stayed 5.

## Edge ledger — what was cut, by name and direction

| edge | direction | cut by | effect |
|---|---|---|---|
| `sdk/operative → sdk/runner_server` | up | A1 | `RunnerServer.park/4` gone |
| `sdk/entity → sdk/operative` (park) | up | A1 | `Operative.park/4` gone |
| `sdk/entity → sdk/operative` (emit) | up | A2 | `Operative.emitted/4` → `Inbox` |
| `sdk/data_stream → sdk/operative` | up | A2 | `Operative.input/4` → `Inbox` |
| `runner/fleet_manager → convergence/safe_call` | cross | A4 | §3 breach closed |
| `service/hub_manager/placement → convergence/safe_call` | cross | A4 | §3 breach closed |
| `service/operative_manager → convergence/safe_call` | cross | A4 | §3 breach closed |
| `schedule/hub → convergence/safe_call` | cross | A4 | §3 breach closed |
| `schedule/store → authz/dets_store` | cross | C4 | §3 breach closed |

**No edge was added.** A1 is a two-into-one fold whose three call sites already named
`Park`; `Inbox` and `Loop.State` have zero out-edges. **Repo `cycles` 2 → 1**, `sdk`
`cycles` 1 → 0 and `max_cycle_files` 4 → 0.

**Not cut, priced**: `runner/fleet_manager/session.ex → convergence/heal.ex`
(`reroute_one/3`; the board's name for it is stale since 5e-a's split). Simulated on
the live graph: cutting it alone takes the SCC **62 → 59** and `convergence`'s members
**5 → 2**; cutting it with `hub_manager → recovery_gate` takes them to **57** and **0**
— charter §3's stated target. It cannot be taken: `reroute_one/3` consumes
`Heal.rebind/2`'s verdict **synchronously**, and the per-operative `rescue`/`catch`
exists so one raising rebind cannot strand its siblings. Making it asynchronous is a
behaviour change. Both lanes concur.

## What the design got wrong, and how it was caught

Twelve task boxes assert something measurement refuted. The pattern is worth more than
the list: **every one was found by running the instrument rather than re-reading the
plan.**

- **`use GenServer, <opt>` emits no `type: :worker`** (and no `restart: :permanent`),
  and `Supervisor.child_spec/2` does not add one. So task 2.1's "green on unmodified
  code" and 2.3's "green without editing the expectation" are **not simultaneously
  satisfiable**. KI-67's real requirement was met the other way and more strongly: the
  five assertions are **red 5/9 on unmodified code**, and four more prove the dropped
  keys inert by putting both shapes through a **live supervisor** and comparing
  `:supervisor.get_childspec/2` — OTP normalises them byte-identically.
- **Checks 1 and 3 of the A2 proof are stated over A1+A2**, but A1 is a two-into-one
  fold, so the `-1` they report is A1's by design. Separated: cast count **5 → 4 → 4**,
  `{:park, …}` literal **4 → 3 → 3**; **A2 alone is neutral on both**.
- **`compute_health/0`'s per-arm comments count toward its extent** and left it at 56.
  Each moved onto the builder that returns its status; not one word was cut.
- **Four arms omit the operative counts, not three** (`tick_resuming` is the fourth),
  so §7.4's refusal to merge them into `respond_healthz/1` is stronger than stated.
- **E4 was half already done**: `stall_classifier.ex:59-64` carried the QUIC caveat at
  the baseline tag, naming KI-72. Only `healthz/server.ex` lacked it.
- Arities: `start_controller/**0**`, `plugin_divergence/**2**`, `resolve_collision/**4**`,
  and the `DetsStore` caller list is **six** (`quota_counters.ex` was omitted).
- **`prod_loc` down ~112** was a raw-line estimate: measured, `prod_loc` **−68**,
  `doc_lines` **−46**, `comment_lines` **+2** — the total lands exactly, split across
  rows the estimate never named.

## The rulers, twice moving against the work

- **`authz duplicate_pairs` 5 → 6 after batch 0, and back to 5 after batch 2.**
  Deleting two `list_for_*` and moving `hash_token/1` private brought two files'
  `@doc`/`@spec`/`def path()`/`## GenServer`/`@impl` runs into one jscpd 40-token
  window (`role_store.ex:77-88 ↔ token_store.ex:165-176`). C2's deletion of the
  68-token `on_boot ↔ source_registration` pair cancelled it. **A false positive whose
  source was read**, not debt.
- **`lib/flezha duplicate_pairs` 0 → 1** is `application.ex`'s `plugin_children/0`
  against `application_children_test.exs`'s `@order` literal. The test duplicates the
  list **on purpose** — a characterization test that reads its expectation from the
  source proves nothing. It appears only under the 36-area scan, which includes
  `test/`; the 15-area scan does not see it. **Always name the scan.**

## Gotchas worth the next reader's time

- **`git ls-tree` and `git grep <rev>` answer "does this commit build" without
  checking anything out** — the only safe way to ask on a shared working tree. A
  directory-wide `git add` swept two of this lane's in-flight `alias` lines into the
  peer's commit, leaving `85f722cd` aliasing `Flezha.SafeCall` with four live call
  sites and no such module. Found from the object store; healed by the next commit.
- **`mix credo` takes one path argument**, and treats extra ones as relative to the
  first. Use `--format json` over the whole repo and filter.
- **Converting a `cond` whose first arm is a negation creates
  `NegatedConditionsWithElse`** — invert the condition and swap the branches. Two of
  eleven conversions needed it.
- **A `@moduletag :capture_log` does not survive a fold.** Folding
  `server_close_on_reject_test.exs` in as-is would have silenced the correlation tests
  too, which changes what the suite's uncaptured-log gate can see. It became two
  per-test `@tag :capture_log`.
- **The fold threshold moves.** `1 − async_false_share` read **0.203** at this area's
  batch 0 and **0.21** at its batch 7, because the peer lane folded ~25 test files in
  between. Read it from the instrument, every time.
- **`scripts/format-check.sh` is RED at HEAD**, on `service/cli_manager.ex` —
  unmodified here, unchanged since the baseline tag, last touched by 5c's `716061cf`.
  Its `reuse lint` step also fails on unstaged deletions, because
  `git ls-files -co` still lists a deleted-but-unstaged path.
- **`test/flezha_test.exs:419`'s recorded exposure was one instance of the real one.**
  Two sightings here with **no maven build at all**, both overlapping the peer lane's
  concurrent suite. A reproducer should force concurrent load.

## Refused, with the price

- **C1** (`healthz ↔ observability/export`, 3 pairs / 379 tokens) — refused whole by
  ruling. The cluster straddles a lane, `observability` is blocking, and moving only
  the `healthz` half **re-anchors** the clones rather than removing them. `healthz
  duplicate_pairs` stays 3.
- **A3** `dlq/store → state/durability_tier` — telemetry inversion hides a failure
  inside an emit; an injected ship function is a start-time contract that can be
  forgotten on a durability path. `dlq max_cycle_files` stays 1.
- **A5** `coordinator → {signal_handler, transport/server}` — the first is
  spec-mandated (`controller-graceful-shutdown` §101), the second is the drain's job.
- **E3** `Flezha.Payloader` — no ruling given, so the default held. Options re-priced
  in tasks.md 5.6.
- **F2/F3/F4** — the convergence fold would add 670 lines to a blocking
  `files_over_800` member (owed to area 7 as a split); two fold candidates carry
  `@moduletag :multi_node`; the three `sdk` e2e modules use `setup`, and
  `setup` → `setup_all` is a per-test namespace question not done blind.
- **`authz` 18 files / `sdk` 17** — fragmentation refused both ways: each small module
  owns a noun with its own validation, and folding three saves ~20 lines while building
  the `util` bucket §2 forbids.
