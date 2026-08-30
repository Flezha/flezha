#!/usr/bin/env bash
# Repository-weight gate. Two checks, both about bytes git can never give back:
#
#   tracked  no compiled artefact is tracked. Native libraries and fat jars are
#            build output: a compiler regenerates them, and every refresh adds a
#            fresh multi-megabyte blob that stays in the history forever.
#   size     the packed repository stays under the ceiling, so a regression is
#            caught at the commit that causes it rather than at the next clone.
#
# Usage:
#   scripts/check-build-artifacts.sh              # both checks
#   scripts/check-build-artifacts.sh tracked      # only the tracked-artefact check
#   scripts/check-build-artifacts.sh size         # only the packed-size check
#
# The `tracked` check answers a question about PATHS, not about what a file
# says, so it does not ride `scripts/naming_gate.py` — that walk reads every
# tracked file line by line and skips binaries and `dist/` by construction,
# which is exactly the population this gate exists to look at.
#
# A binary fixture whose bytes are the subject of a test is exempt BY PATH in
# EXEMPT below, never by extension: the extension is what the gate is for.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - "$@" <<'PY'
import os
import subprocess
import sys

# Whole directories that hold nothing but build output.
ARTIFACT_DIRS = (
    ("sdk/java/flezha-sdk-native/src/main/resources/native/",
     "sdk/java/flezha-sdk-native/src/main/resources/native/"),
    ("sdk/java/flezha-sdk/dist/", "sdk/java/flezha-sdk/dist/"),
    ("test/apps/java_runner/dist/", "test/apps/java_runner/dist/"),
)

# Compiled output wherever it lands. `.pyc` is here because a runner launch
# rewrites it, so a tracked one is a diff on every run.
ARTIFACT_EXT = {
    ".jar", ".so", ".dylib", ".dll", ".class", ".a", ".o", ".obj",
    ".node", ".pyd", ".pyc", ".exe", ".wasm", ".beam", ".war",
}

# Binary fixtures whose bytes an existing test reads and asserts against. A scan
# of what is tracked today found none: the only tracked binaries in the
# repository were the seven artefacts this gate refuses. Declared by path, so
# adding one is a visible decision rather than a widened extension list.
EXEMPT = frozenset()

CEILING_MB = 150


def tracked_check():
    listing = subprocess.run(
        ["git", "ls-files", "-z"], capture_output=True, check=True
    ).stdout.decode()

    failures = []
    for rel in listing.split("\0"):
        if not rel or rel in EXEMPT:
            continue
        for prefix, ignore_entry in ARTIFACT_DIRS:
            if rel.startswith(prefix):
                failures.append((rel, ignore_entry))
                break
        else:
            if os.path.splitext(rel)[1] in ARTIFACT_EXT:
                failures.append((rel, rel))

    if not failures:
        print("no build artefacts tracked")
        return 0

    print("Build artefacts are tracked in git:\n", file=sys.stderr)
    for rel, ignore_entry in failures:
        print(f"  {rel}\n      .gitignore should carry: {ignore_entry}", file=sys.stderr)
    print(
        f"\n{len(failures)} tracked artefact(s). Compiled output is produced by "
        "`scripts/build-native.sh` and `scripts/build-dist.sh`; a release "
        "publishes it as an asset. Untrack it with `git rm --cached <path>` and "
        "add the entry above to .gitignore. A binary FIXTURE whose bytes a test "
        "asserts against belongs in EXEMPT in this script, by path.",
        file=sys.stderr,
    )
    return 1


def heaviest_paths(limit=10):
    """The path prefixes carrying the most blob bytes across all refs. Only
    computed when the size check fails — it walks every object in the repo."""
    names = subprocess.run(
        ["git", "rev-list", "--objects", "--all"], capture_output=True, check=True
    ).stdout
    described = subprocess.run(
        ["git", "cat-file", "--batch-check=%(objecttype) %(objectsize) %(rest)"],
        input=names, capture_output=True, check=True,
    ).stdout.decode(errors="replace")

    weight = {}
    for line in described.split("\n"):
        parts = line.split(" ", 2)
        if len(parts) < 3 or parts[0] != "blob":
            continue
        group = "/".join(parts[2].split("/")[:3])
        weight[group] = weight.get(group, 0) + int(parts[1])
    return sorted(weight.items(), key=lambda kv: -kv[1])[:limit]


def size_check():
    counted = subprocess.run(
        ["git", "count-objects", "-v"], capture_output=True, check=True
    ).stdout.decode()
    fields = dict(
        (k.strip(), v.strip()) for k, _, v in (l.partition(":") for l in counted.split("\n")) if k
    )
    kib = int(fields.get("size-pack", 0)) + int(fields.get("size", 0))
    mb = kib / 1024

    if mb <= CEILING_MB:
        print(f"packed repository {mb:.0f} MB, under the {CEILING_MB} MB ceiling")
        return 0

    print(f"Packed repository is {mb:.0f} MB, over the {CEILING_MB} MB ceiling.\n", file=sys.stderr)
    print("Heaviest paths by blob bytes across all refs:", file=sys.stderr)
    for group, size in heaviest_paths():
        print(f"  {size / 1048576:8.0f} MB  {group}", file=sys.stderr)
    print(
        "\nBytes already in the history are reclaimed only by a rewrite "
        "(docs/runbooks/purge-build-artifacts.md). Stop the flow first: an "
        "artefact still being committed grows back.",
        file=sys.stderr,
    )
    return 1


CHECKS = {"tracked": tracked_check, "size": size_check}

requested = sys.argv[1:] or list(CHECKS)
unknown = [name for name in requested if name not in CHECKS]
if unknown:
    print(f"unknown check(s): {', '.join(unknown)}; known: {', '.join(CHECKS)}", file=sys.stderr)
    sys.exit(2)

sys.exit(max(CHECKS[name]() for name in requested))
PY
