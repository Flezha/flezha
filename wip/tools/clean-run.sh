#!/usr/bin/env bash
# The campaign's clean-run bar (wip/RULES.md): every row of `@clean` in
# scripts/gate_rows.py, the one gate list CI selects from too.
#
#   wip/tools/clean-run.sh
#
# Each row runs under its time limit and is reported PASS, FAIL, NOISY (green,
# but a warning, an uncaptured log line or a stray stderr line in its log) or NO
# ANSWER with its duration. Logs land in $LOGS/<row>.log (stderr also in
# <row>.err) and the verdicts in $LOGS/summary.txt, under the directory printed
# first. Exits non-zero unless every row passed clean.
set -uo pipefail

ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
cd "$ROOT" || exit 1
LOGS=$ROOT/wip/tools/.cache/clean-run/$(date +%Y%m%d-%H%M%S)
echo "logs: $LOGS"
exec python3 scripts/naming_gate.py --logs "$LOGS" --run @clean
