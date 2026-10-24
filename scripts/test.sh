#!/usr/bin/env bash
# The basic test run: the core Elixir suite, as `mix test` runs it. The slow
# tiers (`:system`, `:multi_node`, interop and the like) are excluded by
# test/test_helper.exs; scripts/test-full.sh runs them. Extra arguments go to
# `mix test`, so `scripts/test.sh test/flezha/convergence` runs one directory.
set -euo pipefail
cd "$(dirname "$0")/.."

set -x
mix test "$@"
