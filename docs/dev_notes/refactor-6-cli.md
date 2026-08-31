# `cli` refactor (`refactor-6-cli`)

Area 6 of the refactor programme, run as the second concurrent track beside area
4. Three commits: a defect fix (`b0b27a30`), the refactor (`b73c940c`), and this
close-out. Zero behaviour change in the refactor commit — every command, flag,
exit code and byte of help text is what it was. What follows is what a later
session would otherwise rediscover; the plan is in the archived change.

## The area was three parallel mechanisms, not one big file

The survey measured 34 % of the area as three copied mechanisms, and each is now
one:

**One spawn site.** `cli/src/rpc.rs` holds the only `Command::new` in all of
`cli/src` (`rpc.rs:189`) and `rpc::run(expr, Failure)` is called from **47** sites
across thirteen modules. `Failure` carries three independent fields — what is said
on a refusal, what the process exits with, and whether a failed *spawn* is
diagnosed or panics — because those three are independent across this tree: a
verb that panics on an unspawnable `flzvm` may still be silent on a refusal.
Collapsing them would have moved an operator-visible code. There are **six**
named policies (`REFUSED`, `PROPAGATE`, `UNREACHABLE`, `DIAGNOSED`,
`UNKNOWN_STATUS`, `QUIET`), and `Exit::Propagate(n)` carries the fallback the
site used to spell inline, which is what keeps `audit`'s `unwrap_or(3)` distinct
from `boot`'s `unwrap_or(1)`.

**One help renderer.** `cli/src/help.rs` is 31 `const Help = &'static str` screens
and a three-line `render`. No renderer computes a column — see below.

**One dispatcher.** `cli/src/dispatch.rs`'s `Command` names the five decisions
sixteen verbs each hand-wrote: the screen, the verb table, the spellings that
print the screen, what a bare invocation says, and how an unknown subcommand is
refused. `flz` itself is a seventeenth table in `main.rs`, so the top level is
not a second copy of the rule. Two details are load-bearing: **the verb table
wins over the help spellings**, so `flz plugin`'s own `help` verb still runs
rather than printing a screen, and `unknown_prints_help` is a field because
exactly two commands print the screen after refusing.

## The golden-file method — the only way 506 lines of hand-aligned text move

`cli/tests/golden/` holds **31 screens, 423 lines**, captured from the HEAD
binary **before any edit**, and `cli/tests/help.rs` re-drives every one of them
through `CARGO_BIN_EXE_flz` and compares byte for byte.

**"Regenerate it correctly" was never available.** Two rows in this tree are
already misaligned — `flz help`'s `runner` row puts its description at column 18
where its sixteen siblings use 20, and `flz audit help`'s `stats` row at 14
where its four siblings use 15. Any renderer that computes a column silently
corrects both, which is an operator-visible change inside a refactor commit. So
the capture preserves them, and the encoding chosen is one where a screen *is*
its own bytes.

Three things make the method work rather than merely look like proof:

- **The model was proved before any Rust was written**, in Python, by
  decomposing all 31 screens into fields and re-rendering them — which found two
  screens the obvious `{name, summary, usage, subcommands, options, examples}`
  model breaks on. Discovering that in a scratch script costs minutes;
  discovering it after 27 printers are gone costs the batch.
- **The negative control was run twice** — one `println!` altered by one space,
  confirmed red, reverted.
- **A second test guards the set.** `the_golden_files_and_the_screens_are_the_same_set`
  fails if a screen has no golden file or a golden file names no screen. Without
  it a deleted screen stops being checked *silently*, which is the same class of
  failure as everything else this area found.

The planned correction commit (tasks 2.4, "batch 2b") was **not taken**: both
misalignments still stand, uncorrected and captured as they are. Correcting them
is two lines plus two golden files, and it is a behaviour change that belongs in
its own commit — as does [[KI-88]], the `flz state help` screen that omits `help`
from its own subcommand list.

## The help-table estimate was refuted by measurement, not by argument

The survey projected **≈ −200 lines** for the help table and called it the area's
single largest deletion. Three encodings were priced against the 506 lines of
`println!` they replace:

| encoding | lines | note |
|---|---|---|
| structured `Section` tables + a renderer | **623** (670 after rustfmt) | *more* than today |
| a flat `&[&str]` array per screen | ≈ 516 | break-even |
| raw string per screen | **468** | **−38** |
| `include_str!` from a `.txt` | *would read* −466 | **refused** |

The `include_str!` encoding was refused although it is by far the best-looking
number, because a `.txt` file is invisible to every metric in
`refactor_metrics.py` while the text still exists on disk and still has to be
maintained. That is the same instinct as [[KI-87]] from the other direction: a
number the instrument cannot see is not a saving.

**The win is structural, not LOC**: 27 printers became one renderer plus a table
the dispatcher can key by command, which is what removes the second hand-written
copy of the sixteen-command list that `main.rs::print_help` used to be — the
copy that produced the `runner` misalignment in the first place.

## `duplicate_pairs` after the split alone: 22, unchanged

`design.md` named the risk explicitly, because area 3 had lived it: splitting
`workflow/mod.rs` took `benches/workflow_scaling.rs` from **5 pairs to 65** for
identical source, since every new file repeated the same prologue. So
`reset.rs` (953 lines) was split into `reset/{mod,plan,clear,report}.rs` and
`--area cli` was re-measured **after the split alone, before any deletion**:
**22 pairs, unchanged**.

The shape did not repeat because the cut is at function boundaries with no
repeated prologue. A risk named, priced and then found *absent* is worth
recording as precisely as one found present — the next session can now say which
kind of split multiplies pairs and which does not.

The three survivors at the close are `use` blocks of 40–51 tokens
(`boot.rs`↔`runner.rs`, `namespace.rs`↔`role.rs`, `operative.rs`↔`schedule.rs`),
from 22 pairs that included the 111-token `audit`↔`observe` clone and all five
`boot`↔`plugin` pairs.

## Batch 0 — a tool reporting a success it had not earned, and a refused helper

[[KI-79]]: `operative stop`, `rewind`, `replay`, `state` and `controller start`
each matched on the spawn result with **no `exit()` in either failure arm**, so a
refused RPC exited **0** — while `operative send`, six lines away, carries the
identical match with `exit(1)` and a comment. Drift, not a decision.

Proved red first against a `flzvm` shim that exits 7. The fixture detail that
matters: `flz` resolves `flzvm` **beside its own executable**, so the shim has to
sit next to a *copy* of the binary in a `TempDir` — putting it on `PATH` proves
nothing. All five exited 0 at HEAD with one failure naming all five; all five
exit 1 after; and the four other dialects (list → 1, `boot status` → 7,
`controller status` → 3, `audit list` → 7) are pinned by the same test, which is
what let the next batch move 47 spawn sites without an operator-visible code
moving.

**The reuse decision is the part to keep.** The reference arm was *extended* to
five sites rather than extracted into a helper, deliberately: the six bodies are
not one function with a parameter (different error texts, one prints a success
line, `controller start` spawns `flzvm start` rather than `flzvm rpc`), and the
helper that *is* right — taking the expression and a failure policy — is
`rpc::run` from the next batch. Extracting a different six-site helper first
would have been a parallel mechanism that the next batch deletes. Charter §2's
order stops at *extend*.

**And batch 0's reasoning was right in shape, wrong in detail** — which only
doing the work exposed. It predicted three failure policies and one message; the
tree has **six** live policies, and the message is not one string: 17 sites say
one thing, four say another, and one embeds the child's `ExitStatus`.

## The numbers, and which one to quote

`prod_loc` **3 344 → 2 831**, a raw **−513** — but **the honest number is −438**.
75 of those lines are a **classifier artefact**: a blank line inside a raw string
counts as blank, where the `println!()` that used to print it counted as code.
`help.rs` is the whole of it — 468 file lines, 352 counted. Both numbers are
true; **−438** is the one to quote, and the area's total still falls with room.

The recorded baseline row moves **3 314 → 2 831**, a different subtraction:
3 314 was measured before batch 0, whose defect fix added 30 production lines
before the refactor started.

`test_loc` **991 → 1 355**, and every line of the +364 is a proof this area
rests on: `exit_codes.rs` (105 file lines) and `help.rs` (130) did not exist;
`dispatch.rs` (216) **replaced** `cluster.rs` (47) and `schedule.rs` (92) while
taking dispatch coverage from **2 commands to all 16** — a fold that raises what
is covered rather than lowering it — with
`schedule_add_requires_cron_tz_and_factory` moved to its own 48-line file
because it tests `handle_add`, not dispatch; and `reset.rs` grew 514 → 537 on
[[KI-84]]'s fixture fix. The golden capture itself is free — 423 lines of `.txt`
that no metric counts — and that is the distinction the `include_str!` refusal
turns on: a capture is regenerable evidence a counted test asserts byte for
byte, where help text is the product.

Every ceiling is zero: `files_over_800` **1 → 0** (`reset.rs` 953),
`functions_over_50` **8 → 0**, `long_names` **1 → 0**
(`extract_opts_with_default_namespace`), `narrative_markers` 0, `sleeps` 0,
credo 0, clippy 0, cycles 0.

## What the flip means

`cli` is under `blocking` in `docs/refactor/baseline.json` from this commit,
re-recorded from the landed tree over the full 36-area scan set: `prod_loc`
**2 831**, `test_loc` **1 355**, `files_over_800` **0**, `functions_over_50`
**0**, `long_names` **0**, `narrative_markers` **0**, `duplicate_pairs` **3**,
`cycles` 0, credo 0, clippy 0.

**The flip was proved armed, not assumed.** With `cli`'s baseline `prod_loc`
lowered by one, `--check` prints `BLOCKING cli prod_loc: 2830 -> 2831` and exits
**1**; with the same lowered number and `cli` removed from `blocking` it prints
`measuring cli prod_loc: 2830 -> 2831` and exits **0**. The baseline was restored
from a byte copy and `sha256sum -c` re-verified afterwards.

## The A/B rows exist, are declared, and were not run

`cli` is on **none** of charter §8's hot paths, and no benchmark in this
repository drives the `flz` binary at all. Both rows were declared `direction`
in `design.md` **with their floor measured before anything was decided**:

```
flz help          n=30  median 3.83 ms  min 1.78  p90 5.08
flz reset --help  n=30  median 1.97 ms  min 1.51  p90 2.05
flz state help    n=30  median 1.71 ms  min 1.41  p90 1.84
```

A min→p90 spread of 0.3–3.3 ms on a 1.7–3.8 ms median is a floor between 18 %
and 86 % of the signal, against an effect — walking a 16-row table instead of a
`match`, one `render` instead of 39 `println!` calls — that is microseconds.
A row whose floor cannot resolve the effect it would claim is reported and not
run (§8's last bullet, §11b's "cut the rows that cannot gate").

The evidence recorded in their place is the `sha256sum` of each arm's
`target/release/flz`:

| arm | commit | sha256 |
|---|---|---|
| A (pre-refactor) | `dd04cd33` | `f62782f8796dd3493ee7c1a58c7dbe87678d41a4ed43f0f9e9a5d5f74a68eedd` |
| B (final) | `ebce266e` | `eeafc5fbdc6b1a88bca9e13bf3dc7d583f7d4ad0b4d4ad7174433d1cfa8ca942` |

Arm A is `dd04cd33` and **not** the commit before batch 0: batch 0 is a
deliberate behaviour change, so the pre-refactor arm is the one *after* it, which
is what makes the pair isolate the refactor. The two were built in separate
worktrees with separate target directories, so the pair documents provenance —
which binaries the rows would have driven — and is not a bit-for-bit claim.

## Traps the next session should not re-pay

- **`--write-baseline` now refuses without `--lint`** (exit 2, before measuring,
  file untouched) and **`--timing` now merges**, both fixed in this area's batch
  0 as [[KI-83]]. Verified in use here: recording with no `--timing` at all kept
  all three keys — `mix_test_exunit`, `mix_test_wall`, `mvn_test_sdk_java` — on
  the stamps they were measured under. Still verify after writing: 36 areas, 0
  credo nulls, 0 clippy nulls, repo credo 378.
- **[[KI-87]] is still open and still flatters.** The metrics walk reads
  `git ls-files`, so an untracked file is invisible to every metric, and because
  refactors *add* files the error is one-directional. Batch 1 read a phantom
  `prod_loc` of **2 745** on `cli` — −599 — with the whole `reset/` directory
  absent from every ceiling. Charter §11e is the operational answer: a worker
  **names its new files** and the supervisor `git add -N`s them before any number
  is read.
- **The Elixir suite does not cover this area.** `mix test` is green (1 872
  passed, 121 excluded) and proves only that nothing outside `cli/` moved. The
  one Elixir test that drives the binary,
  `test/plugin/installer/release_cli_system_test.exs`, is `@moduletag :system`
  and needs `_build/dev/rel/flezha`, so a plain `mix test` never runs it. The
  area's own suite is `cargo test -p flz` — **77 tests**, seconds.
- **The suite timing was measured and deliberately not recorded.** 60.5 s ExUnit
  / 62.2 s wall here against a baseline of 56.2 / 59.0 at `a147111c`. This area
  changes no Elixir, so the delta is the box (a release build had just run), and
  re-recording it would relax a §9 ratchet on a load artefact. Recording a
  timing is for a run that measured it *and* means it.
- **Two gates are red at HEAD and neither is this area's.**
  `scripts/format-apply.sh --check` exits 1 because `rubocop` is absent on this
  box and `php-cs-fixer` dies with a parse error inside `sdk/php/vendor` under
  the system PHP, which the gate reports as *unformatted* ([[KI-76]]);
  `scripts/check-kind-naming.sh` exits 1 on 11 prose hits, every one in `docs/`
  ([[KI-65]]). `cargo fmt`, `mix format`, `mvn spotless`, prettier, ruff and the
  other five checkers are clean. **A dev note is prose in `docs/` and can add a
  twelfth hit** — grep your own file before handing it over, and remember the
  gate cannot see it until it is tracked.
- **Left standing on purpose**: [[KI-80]] (two output-format dialects),
  [[KI-81]] (`docker/prod` calling two `flz cluster` verbs that do not exist),
  [[KI-85]] (`reset --include-sockets` resolving past the rung it was given),
  [[KI-86]] (a short `-o=value` flag is silently dropped — its branch is
  unreachable, preserved byte-for-byte by the zero-behaviour-change contract),
  [[KI-88]], and the two help misalignments above. Every one is a behaviour
  choice, and this area's contract is that it makes none.
