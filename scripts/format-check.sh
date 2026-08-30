#!/usr/bin/env bash
# The source-shape gate: every formatter in check mode, then the licensing
# checks. .github/workflows/format.yml runs the same three, invoking
# scripts/format-apply.sh --check for the formatter half so there is one list
# of formatters and not two. Auto-fix with scripts/format-apply.sh.
#
# Every check is attempted and every verdict is reported: an absent `reuse`
# does not hide a stale THIRD_PARTY_NOTICES.md, and neither hides the
# formatters. scripts/gate_lib.sh holds that rule.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
source scripts/gate_lib.sh

# lint-file over git-listed paths: a full `reuse lint` walks the tree and chokes on
# the stale test sockets under app_data/, which git never lists. CI lints the
# whole clean checkout.
reuse_lint() { git ls-files -co --exclude-standard -z | xargs -0 reuse lint-file; }

notices_fresh() {
  local generated="${TMPDIR:-/tmp}/third_party_notices.md"
  cargo about generate about.hbs -o "$generated"
  diff -u THIRD_PARTY_NOTICES.md "$generated"
}

# format-apply.sh is itself a gate_lib gate: it prints its own per-language
# summary and its exit status keeps a missing formatter apart from a dirty
# tree, so `--nested` carries that distinction up instead of flattening it.
gate_run --nested "formatters (scripts/format-apply.sh --check)" -- scripts/format-apply.sh --check
gate_run "reuse lint" reuse:"pipx install reuse" -- reuse_lint
gate_run "THIRD_PARTY_NOTICES.md is fresh" cargo-about:"cargo install cargo-about" -- notices_fresh

gate_report "FAILED — the output above says why:"
