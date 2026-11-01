#!/usr/bin/env bash
# The full check: every row of the `@clean` group in scripts/gate_rows.py (the
# list CI selects from), run by scripts/run_gates.py four at a time. It covers
# compiles with warnings as errors, the core suite, the `:system` and
# `:multi_node` tiers, the Rust crates, every SDK, the plugins and the repo's
# gates. A row is PASS, FAIL, NOISY (green but with stray warnings or log
# lines) or NO ANSWER. No limit applies to the whole run; each row still has
# its own time limit in scripts/run_gates.py, so a hung row cannot stall it.
#
#   scripts/test-full.sh          run it; logs land in storage/test-runs/<time>/
#   TEST_JOBS=2 scripts/test-full.sh  fewer rows at once, on a machine busy with other work
#   scripts/test-full.sh --list   print every row and its command, run nothing
#
# Each row's output is <logs>/<row>.log and the verdicts are <logs>/summary.txt.
# One row again: python3 scripts/run_gates.py --run <row>
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${1:-}" = "--list" ]; then
  exec python3 scripts/run_gates.py --list @clean
fi

logs="storage/test-runs/$(date +%Y%m%d-%H%M%S)"
echo "logs: $logs"
exec nice -n 15 python3 scripts/run_gates.py --logs "$logs" --jobs "${TEST_JOBS:-4}" --run @clean
