#!/usr/bin/env bash
# The source-shape gate: every formatter in check mode, then the licensing
# checks. .github/workflows/format.yml runs the same three, invoking
# scripts/format-apply.sh --check for the formatter half so there is one list
# of formatters and not two. Auto-fix with scripts/format-apply.sh.
#
# Every check is attempted and every verdict is reported: an absent `reuse`
# does not hide a stale THIRD_PARTY_NOTICES.md, and neither hides the
# formatters. A tool that ran and did not complete is reported apart from both.
# scripts/gate_lib.sh holds those rules; each check below declares how its own
# tool says "I found something", since a status alone does not.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
source scripts/gate_lib.sh

# lint-file over git-listed paths: a full `reuse lint` walks the tree and chokes on
# the stale test sockets under app_data/, which git never lists. CI lints the
# whole clean checkout.
#
# xargs collapses every status from 1 to 125 into 123, which alone would make a
# reuse crash and a file with no licence header the same number. The wrapper
# keeps reuse's own 1 and turns every other failure into 255, which xargs
# reports as 124: 123 is the finding, anything else is a tool with no answer.
reuse_lint() {
  git ls-files -co --exclude-standard -z |
    xargs -0 sh -c 'reuse lint-file "$@" || exit $(($? == 1 ? 1 : 255))' reuse-lint
}

notices_fresh() {
  local generated="${TMPDIR:-/tmp}/third_party_notices.md"
  # A generator that cannot run is not a stale notices file: keep its status out
  # of diff's, whose 1 is the only finding here.
  cargo about generate about.hbs -o "$generated" || return 255
  diff -u THIRD_PARTY_NOTICES.md "$generated"
}

# format-apply.sh is itself a gate_lib gate: it prints its own per-language
# summary and its exit status keeps a missing formatter, a dirty tree and a
# formatter that did not complete apart, so `--nested` reads that status as one
# of ours and carries all three up instead of flattening them.
gate_run --nested "formatters (scripts/format-apply.sh --check)" -- \
  scripts/format-apply.sh --check
gate_run --finding status:123 "reuse lint" reuse:"pipx install reuse" -- reuse_lint
gate_run --finding status:1 "THIRD_PARTY_NOTICES.md is fresh" \
  cargo-about:"cargo install cargo-about" -- notices_fresh

gate_report "FAILED — the output above says why:"
