#!/usr/bin/env bash
# Every gate an item needs: the rows of `@item` in scripts/gate_rows.py (the
# OpenSpec validation, the naming gates, a warnings-as-errors compile and the AI
# kit's freshness), then `mix test` over the named test files when any are given.
#
#   wip/tools/verify.sh [test_file ...]
#
# Each row prints its verdict; its full output lands in a log under the
# directory printed first. Exits non-zero when any gate failed.
set -uo pipefail

ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
cd "$ROOT" || exit 1
LOGS=$ROOT/wip/tools/.cache/verify/$(date +%Y%m%d-%H%M%S)
echo "logs: $LOGS"
python3 scripts/naming_gate.py --logs "$LOGS" --run @item
failed=$?
if [ $# -gt 0 ]; then
  timeout 600 mix test "$@" >"$LOGS/tests.log" 2>&1 && echo "PASS tests" || { echo "FAIL tests"; failed=1; }
fi
exit $failed
