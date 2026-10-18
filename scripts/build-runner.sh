#!/usr/bin/env bash
# Build one runtime's runner with its conformance manifest's `build.<profile>`
# command, through Mix.Tasks.Bench.Fleet.ensure_built/2, the one build entry.
#
#   scripts/build-runner.sh <runtime> <test|bench>
set -euo pipefail

[[ $# -eq 2 && $2 =~ ^(test|bench)$ ]] || { echo "usage: $0 <runtime> <test|bench>" >&2; exit 2; }
cd "$(dirname "${BASH_SOURCE[0]}")/.."
exec mix run --no-start -e '
  [runtime, profile] = System.argv()
  Mix.Tasks.Bench.Fleet.ensure_built(String.to_atom(runtime), String.to_existing_atom(profile))
' -- "$1" "$2"
