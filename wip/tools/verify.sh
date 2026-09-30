#!/usr/bin/env bash
# Every gate an item needs, one line each.
#
#   wip/tools/verify.sh [test_file ...]
#
# Runs the OpenSpec validation, the naming gate group and a warnings-as-errors
# compile, then `mix test` over the named test files when any are given. Each
# gate prints PASS or FAIL and its name; the full output of each lands in a
# log under the directory printed first. Exits non-zero when any gate failed.
set -uo pipefail

ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
LOGS=$ROOT/wip/tools/.cache/verify/$(date +%Y%m%d-%H%M%S)
mkdir -p "$LOGS"
echo "logs: $LOGS"
failed=0

gate() { # name, command...
  local name=$1
  shift
  if (cd "$ROOT" && timeout 600 "$@") >"$LOGS/$name.log" 2>&1; then
    echo "PASS $name"
  else
    echo "FAIL $name"
    failed=1
  fi
}

gate openspec openspec validate --specs --strict
gate naming python3 scripts/naming_gate.py --run @naming
gate compile mix compile --warnings-as-errors
[ $# -gt 0 ] && gate tests mix test "$@"

exit $failed
