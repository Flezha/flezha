#!/usr/bin/env bash
# Fails when `bounded` / `unbounded` is used to name an operative kind.
#
# The kinds are `stream` and `workflow`. Both words survive in two other senses
# — mailbox capacity and plain English — which scripts/kind-naming-allowlist.txt
# masks out of each line before the checker fails on whatever is left.
#
# Two judgements run, and a line fails either one:
#
#   mask-then-match — the allowlist erases the legitimate senses, one broad
#     pattern flags the residue. It carries the open-ended half of the problem:
#     the plain-English sense takes a new noun every time someone describes a
#     new limit.
#   match-directly  — KIND_SENSE below, which no allowlist entry can mask. It
#     carries the closed half: the word bound to a noun that can only name the
#     operative construct. This is what makes a broad allowlist rule safe, and
#     it is the half to widen when the gate should catch more.
#
# Scope is `git ls-files`, so .git/, target/, _build/ and openspec/changes/
# (which holds the archive) are excluded by being untracked or ignored; dist/
# build artifacts and binaries are excluded explicitly. Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

ALLOWLIST = "scripts/kind-naming-allowlist.txt"
# Any identifier containing the token, so `BoundedOperative` and `spawn_bounded`
# are caught as well as the bare word. `bound` / `unbound` (the binding family)
# share no substring with `bounded` and are never matched.
WORD = re.compile(r"[A-Za-z0-9_]*bounded[A-Za-z0-9_]*", re.IGNORECASE)
# The kind sense, matched with no allowlist in front of it: the word bound to a
# noun that names the operative construct and nothing else, and a kind or
# operative predicated to BE the retired name. `stream`, `workflow`, `run`,
# `input` and `iteration` are deliberately absent — the glossary keeps the
# adjectives for the RUN ("a workflow operative runs a bounded iteration",
# "folds an unbounded input"), so binding them here would fail correct prose.
KIND_SENSE = [
    re.compile(r"(?:un)?bounded[\s`'\"*_/-]{0,4}"
               r"(?:operatives?\b|kinds?\b|class(?:es)?\b|factor(?:y|ies)\b|one(?![-\w]))",
               re.IGNORECASE),
    re.compile(r"\bkinds?[\s`'\"*]{0,6}(?:[:=]{1,2}|is|are|was|were|named|called)"
               r"[\s`'\":]{0,6}(?:un)?bounded\b", re.IGNORECASE),
    re.compile(r"\boperatives?[\s`'\"*]{0,6}(?:is|are|was|were)"
               r"[\s`'\":]{0,6}(?:un)?bounded\b", re.IGNORECASE),
]

allowed = naming_gate.pattern_check([WORD], naming_gate.load_allowlist(ALLOWLIST))
kind_sense = naming_gate.pattern_check(KIND_SENSE)

sys.exit(naming_gate.walk(
    check=lambda rel, line: allowed(rel, line) or kind_sense(rel, line),
    prefilter=lambda text: "ounded" in text or "OUNDED" in text,
    # The gate's own files carry the retired names as data.
    self_files={ALLOWLIST, "scripts/check-kind-naming.sh", ".github/workflows/naming.yml"},
    record_tag="kind-naming-record",
    headline="kind-sense use of 'bounded'/'unbounded' found "
             "(the kinds are 'stream' and 'workflow'):",
    hint=f"If a use is genuinely the capacity or plain-English sense, add it to {ALLOWLIST}.",
    ok_message="kind naming OK",
))
PY
