#!/usr/bin/env bash
# Production lines per repository (tests, fixtures, examples and docs excluded),
# the measure a campaign's line budget is held to. Prints one line per repository
# and a total.
set -euo pipefail
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

EXCLUDE='(^|/)(test|tests|spec|fixtures|examples?|benches)/|_test\.|Test\.java$|\.md$|^flezha-sdk-examples/'

count() {
  local repo=$1; shift
  (cd "$repo" && git ls-files -- "$@" | grep -Ev "$EXCLUDE" | xargs -r cat | wc -l)
}

total=0
row() { printf '%-28s %7d\n' "$1" "$2"; total=$((total + $2)); }

row core-lib            "$(count . lib)"
row core-flz_core       "$(count . flz_core/src)"
row core-flz_core_ffi   "$(count . flz_core_ffi/src)"
row core-flz_nif        "$(count . flz_nif/src)"
row core-flz_transport  "$(count . flz_transport/src)"
row core-cli            "$(count . cli/src)"
row sdk-elixir          "$(count sdk/elixir lib)"
row sdk-java            "$(count sdk/java ':(glob)*/src/main/**')"
row sdk-node            "$(count sdk/node src)"
row sdk-php             "$(count sdk/php src)"
row sdk-python          "$(count sdk/python src)"
row sdk-ruby            "$(count sdk/ruby lib)"
row sdk-rust            "$(count sdk/rust src)"
for p in plugins/*/; do
  row "$(basename "$p")" "$(count "$p" lib)"
done
printf '%-28s %7d\n' total "$total"
