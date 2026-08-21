#!/usr/bin/env bash
# Packaging-boundary gate: modules under lib/flezha/sdk/ may reference the
# controller application (`Flezha.*` outside the SDK namespace) ONLY through the
# named seams the later hex extraction swaps. This keeps `package-elixir-sdk`
# a swap of seam internals, not a rewrite.
#
# Allowed seams:
#   Flezha.SDK.*                 (the SDK's own namespace)
#   Flezha.Transport.Stream      (per-stream dispatch seam)
#   Flezha.Transport.Native      (client NIF entry points)
#   Flezha.Transport.Local.Stream / Flezha.Transport.Local  (native handles)
#   Flezha.Transport.Server      (native connect / bind_data_stream / ensure_started)
#   Flezha.State.Wire            (encode_park_body/1 postcard seam)
#   Flezha.Partition             (key-group hash seam)
#   Flezha.Runner.CommandsProto / RunnerCommands  (protobuf — only inside Codec)
#
# Scope is `git ls-files` under lib/flezha/sdk/. Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import re
import subprocess
import sys

SDK_DIR = "lib/flezha/sdk"
# The codec module is the ONLY place Protox / RunnerCommands may appear.
CODEC = "lib/flezha/sdk/codec.ex"

ALLOWED = {
    "Flezha.SDK",
    "Flezha.Transport.Stream",
    "Flezha.Transport.Native",
    "Flezha.Transport.Local",
    "Flezha.Transport.Server",
    "Flezha.State.Wire",
    "Flezha.Partition",
}

# Any `Flezha.<Something>` reference (module path), captured with its full dotted head.
REF = re.compile(r"\bFlezha(?:\.[A-Z][A-Za-z0-9_]*)+")
PROTO = re.compile(r"\b(?:RunnerCommands|Protox)\b")

listing = subprocess.run(
    ["git", "ls-files", "-z", SDK_DIR], capture_output=True, check=True
).stdout.decode()

failures = []
for rel in listing.split("\0"):
    if not rel.endswith(".ex"):
        continue
    with open(rel, encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue

            for m in REF.finditer(line):
                ref = m.group(0)
                # allowed if it IS or is nested under an allowed seam
                if any(ref == a or ref.startswith(a + ".") for a in ALLOWED):
                    continue
                failures.append(f"{rel}:{lineno}: stray controller reference `{ref}`")

            if rel != CODEC and PROTO.search(line):
                failures.append(f"{rel}:{lineno}: Protox/RunnerCommands outside the codec seam")

if failures:
    print("SDK packaging-boundary violations (SDK must touch the controller only")
    print("through the named seams — see scripts/check-sdk-boundary.sh):\n")
    for f in failures:
        print("  " + f)
    print(f"\n{len(failures)} violation(s).")
    sys.exit(1)

print("SDK packaging boundary OK")
PY
