#!/usr/bin/env bash
# Fails when `bounded` / `unbounded` is used to name an operative kind.
#
# The kinds are `stream` and `workflow`. Both words survive in two other senses
# — mailbox capacity and plain English — and every preserved occurrence is
# enumerated in scripts/kind-naming-allowlist.txt. The checker masks those
# phrases out of each line and fails on whatever is left.
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

sys.exit(naming_gate.walk(
    check=naming_gate.pattern_check([WORD], naming_gate.load_allowlist(ALLOWLIST)),
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
