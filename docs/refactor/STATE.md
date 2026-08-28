# Refactor board

**Last updated**: 2026-08-28 (supervisor session 1, setup)
**Resume**: open a session in this repo and run `/loop /refactor`. The skill
reads this file, `git status`, and the open change's `tasks.md`, then continues.
**Note**: `openspec/changes/` is gitignored — open and archived changes live on
this machine's disk only; their `tasks.md` is still the fine-grained progress.
**Rules**: `charter.md`. **Programme**: zero behaviour change, net code down,
one host core, tests fast and real.

## Status

Phase 0 set up; no area started. Next action: run change `refactor-0-baseline`.

## Board

Order is dependency order; an area starts only when its dependencies are `done`.

| # | Area | Change id | Depends on | Status | Commit | Gates before → after |
|---|------|-----------|------------|--------|--------|----------------------|
| 0a | Baseline, gates, formatters | `refactor-0-baseline` | — | proposed | | |
| 0b | Test suite mechanics (flakes, sleeps, fleets) | `refactor-0-tests` | 0a | proposed | | |
| 1s | Dispatcher-unification A/B spike (numbers only) | `refactor-1s-dispatcher-spike` | 0a | pending | | |
| 1 | `flz_core` + `flz_transport` + `flz_nif` | `refactor-1-kernel` | 0b, 1s | pending | | |
| 2 | `flz_core_ffi` glue + Node/Ruby/PHP/Python veneers | `refactor-2-ffi-veneers` | 1 | pending | | |
| 3 | `sdk/rust` + `flezha-macros` | `refactor-3-rust-sdk` | 1 | pending | | |
| 4 | `sdk/java` | `refactor-4-java-sdk` | 1 | pending | | |
| 5a | Controller `state` | `refactor-5a-state` | 0b | pending | | |
| 5b | Controller `plugin` | `refactor-5b-plugin` | 5a | pending | | |
| 5c | Controller `service` (+ `cli_manager` split) | `refactor-5c-service` | 5b | pending | | |
| 5d | Controller `observability` + `mix` tasks | `refactor-5d-observability` | 5c | pending | | |
| 5e | Controller `operative`, `runner`, `convergence`, rest | `refactor-5e-runtime` | 5d | pending | | |
| 6 | `cli` | `refactor-6-cli` | 1 | pending | | |
| 7 | Tests, helpers, test apps | `refactor-7-tests` | 2,3,4,5e,6 | pending | | |
| 8 | Docs reconcile (`CLAUDE.md` index, dev_notes names) | `refactor-8-docs` | 7 | pending | | |

Status values: `pending` → `surveying` → `proposed` → `implementing` →
`checkpoint` → `done`; `blocked` names the blocker below.

## Current worker

None.

Fields when one runs: change id · agent role (survey/implement) · started ·
last seen (time, tasks ticked/total, `git diff --stat` tail) · resume note.

## Awaiting the user's decision

Nothing.

## Found defects (not fixed inline — each becomes its own change)

None yet.

## Decisions

- 2026-08-28 Workers run sequentially in the shared tree; worktrees only for a
  proven-disjoint pair, and only after Phase 0.
- 2026-08-28 The supervisor commits per area without asking; nothing is pushed
  by any session.
- 2026-08-28 Flakes are fixed, not quarantined.
- 2026-08-28 Public SDK authoring names change only where the charter's
  vocabulary demands it, and then in every SDK + docs in one change.
- 2026-08-28 The typed Rust/Java dispatchers and the erased host converge on
  one generic core only if the 1s spike shows no loss beyond noise.
- 2026-08-28 File ceiling 800 lines; function ceiling 50.
- 2026-08-28 Dialyzer is not a Phase 0 gate (PLT cost); revisit in area 8.

## Baseline (rough, pre-Phase-0 — replaced by `baseline.json` in 0a)

Measured 2026-08-28 with grep/awk on this box; production = non-test lines.

| Area | Prod | Test | Notes |
|------|------|------|-------|
| `lib/` (controller) | 66.2K | 49.6K | 8 % `#` lines + 6.9K doc-block lines; 159/200 test files `async: false` |
| `flz_core` | 18.2K | 3.7K | 19 % doc comments; `host.rs` 3389 lines |
| `flz_transport` / `flz_nif` | 3.7K / 2.6K | 0.4K / 0.2K | |
| `flz_core_ffi` | 4.7K | 2.3K | four glue files ≈ one adapter ×4 |
| `sdk/rust` + macros | 10.6K + 1.2K | 20.1K | reference impl; `workflow/mod.rs` 426 prod / 3.8K test |
| `sdk/java` | 14.3K | 6.6K | incl. 1.5K examples |
| Node / Ruby / PHP / Python | 4.0K / 2.8K / 6.1K / 4.7K | 2.0K / 1.5K / 2.5K / 2.9K | |
| `cli` | 4.1K | 1.2K | |

Signals: 33 files > 800 lines; `convert_value` ×9, `now_ms` ×4,
`monotonic_nanos` ×4, `schedule_sweep` ×4; 213 sleeps + 138 `refute_receive`
in ExUnit, 60 `thread::sleep` in Rust tests; 11/22 e2e modules spawn a fleet
per test; both naming gates RED at HEAD; `mix format` differs local (1.20) vs
CI (1.19); no formatter for the four veneers.

## Next steps

1. Implement `refactor-0-baseline` (worker), verify, commit.
2. Implement `refactor-0-tests`, verify, commit.
3. Survey 1s, then 1.
