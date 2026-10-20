#!/usr/bin/env bash
# Compile test files without running them: bash wip/tools/compile-test.sh <file.exs>...
# Prints COMPILED <file> per file; any warning or error is printed above it.
set -euo pipefail
cd "$(dirname "$0")/../.."
files=$(printf '"%s",' "$@")
MIX_ENV=test mix run --no-start -e "
ExUnit.start(autorun: false)
for f <- [${files%,}] do
  Code.compile_file(f)
  IO.puts(\"COMPILED \" <> f)
end"
