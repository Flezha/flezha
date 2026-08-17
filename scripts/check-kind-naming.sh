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
import os
import re
import subprocess
import sys

ALLOWLIST = "scripts/kind-naming-allowlist.txt"
# The gate's own files carry the retired names as data.
SELF = {ALLOWLIST, "scripts/check-kind-naming.sh", ".github/workflows/naming.yml"}
BINARY_EXT = {
    ".jar", ".so", ".dll", ".dylib", ".png", ".jpg", ".gif", ".ico", ".pdf",
    ".class", ".zip", ".gz", ".fpz", ".dets", ".wal", ".bin",
}
# Any identifier containing the token, so `BoundedOperative` and `spawn_bounded`
# are caught as well as the bare word. `bound` / `unbound` (the binding family)
# share no substring with `bounded` and are never matched.
WORD = re.compile(r"[A-Za-z0-9_]*bounded[A-Za-z0-9_]*", re.IGNORECASE)
RECORD_BEGIN = "kind-naming-record:begin"
RECORD_END = "kind-naming-record:end"

rules = []
with open(ALLOWLIST, encoding="utf-8") as fh:
    for raw in fh:
        line = raw.rstrip("\n")
        if not line or line.lstrip().startswith("#"):
            continue
        scope, _, pattern = line.partition(":")
        rules.append((scope, re.compile(pattern)))

listing = subprocess.run(
    ["git", "ls-files", "-z"], capture_output=True, check=True
).stdout.decode()

failures = []
for rel in listing.split("\0"):
    if not rel or rel in SELF or "/dist/" in rel:
        continue
    if os.path.splitext(rel)[1] in BINARY_EXT:
        continue
    try:
        with open(rel, encoding="utf-8") as fh:
            text = fh.read()
    except (UnicodeDecodeError, OSError):
        continue
    if "ounded" not in text and "OUNDED" not in text:
        continue
    active = [r for scope, r in rules if scope == "*" or rel.startswith(scope)]
    in_record = False
    for lineno, line in enumerate(text.split("\n"), 1):
        # A migration record MENTIONS the retired names rather than using them.
        # Delimited explicitly so the exemption is greppable and scoped to the
        # block, instead of blinding the whole file.
        if RECORD_BEGIN in line:
            in_record = True
            continue
        if RECORD_END in line:
            in_record = False
            continue
        if in_record:
            continue
        stripped = line
        for rule in active:
            stripped = rule.sub("", stripped)
        if WORD.search(stripped):
            failures.append((rel, lineno, line.strip()))

if failures:
    print("kind-sense use of 'bounded'/'unbounded' found "
          "(the kinds are 'stream' and 'workflow'):\n", file=sys.stderr)
    for rel, lineno, line in failures:
        print(f"  {rel}:{lineno}: {line[:160]}", file=sys.stderr)
    print(f"\n{len(failures)} occurrence(s). If a use is genuinely the capacity "
          f"or plain-English sense, add it to {ALLOWLIST}.", file=sys.stderr)
    sys.exit(1)

print("kind naming OK")
PY
