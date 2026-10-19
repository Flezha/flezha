#!/usr/bin/env bash
# Retake every flz help capture in cli/tests/golden/ from a fresh debug build,
# reading the screen list from cli/tests/help.rs so the two never drift. Read
# `git diff cli/tests/golden` afterwards: the capture is only proof when the
# diff is what you meant.
set -euo pipefail
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
cargo build -q -p flz --manifest-path "$ROOT/Cargo.toml"
FLZ="$ROOT/target/debug/flz"
grep -oE '^ *\("[a-z-]+", &\[[^]]*\]\),' "$ROOT/cli/tests/help.rs" |
  while IFS= read -r row; do
    name=$(sed -E 's/^ *\("([a-z-]+)".*/\1/' <<<"$row")
    mapfile -t argv < <(sed -E 's/^[^&]*&\[(.*)\]\),$/\1/' <<<"$row" | grep -oE '"[^"]*"' | tr -d '"')
    "$FLZ" "${argv[@]}" >"$ROOT/cli/tests/golden/$name.txt"
  done
