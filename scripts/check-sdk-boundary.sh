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
#   Flezha.State.RecordFrame     (the durable record frame: the SDK reads the
#                                 same framing from bytes the controller ships.
#                                 The pure half only — decode_bytes/1,
#                                 fold_bytes/4, decode_header/1, encode_header/5.
#                                 Its device-oriented half opens, syncs, renames
#                                 and truncates files and is not the SDK's to
#                                 call.)
#   Flezha.Partition             (key-group hash seam)
#   Flezha.Process.Counters      (the counter-table macro. COMPILE-TIME ONLY:
#                                 `use` generates the slot accessors into the
#                                 SDK's own module, which afterwards calls
#                                 `:atomics` and nothing else, so this adds no
#                                 runtime edge to the controller at all. The
#                                 extraction vendors one self-contained module
#                                 rather than restoring two hand-numbered
#                                 slot tables.)
#   Flezha.Runner.CommandsProto / RunnerCommands  (protobuf — only inside Codec)
#   Flezha.Transport.LivenessCounters  (the declared heartbeat key set — only
#                                 inside Knob, for the reason below)
#   Flezha.Config.*              (the declared tables — only inside Knob, which
#                                 is the SDK's own reader of them. A runner is a
#                                 different process and never reads the
#                                 controller's application env, so the rest of
#                                 the SDK asks Knob and the extraction swaps one
#                                 file.)
#
# Scope is `git ls-files` under lib/flezha/sdk/. Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

SDK_DIR = "lib/flezha/sdk"
# The codec module is the ONLY place Protox / RunnerCommands may appear.
CODEC = "lib/flezha/sdk/codec.ex"
# The knob module is the ONLY place the controller's declared tables may appear.
KNOB = "lib/flezha/sdk/knob.ex"

ALLOWED = {
    "Flezha.SDK",
    "Flezha.Transport.Stream",
    "Flezha.Transport.Native",
    "Flezha.Transport.Local",
    "Flezha.Transport.Server",
    "Flezha.State.Wire",
    "Flezha.State.RecordFrame",
    "Flezha.Partition",
    "Flezha.Process.Counters",
}

# Seams allowed in ONE file only, each in its code and in the moduledoc that
# says why it is the only one. The codec is the single place the protobuf may be
# named; the knob module is the single place the configuration tables may be —
# it IS the SDK's reader of them, so the rest of the SDK asks it rather than the
# controller, and the extraction swaps one file instead of six.
PER_FILE_ALLOWED = {
    CODEC: {"Flezha.Runner.CommandsProto"},
    # The heartbeat key set is a declared table like the configuration rows, and
    # it is read in the same one file for the same reason.
    KNOB: {"Flezha.Config", "Flezha.Transport.LivenessCounters"},
}

# Any `Flezha.<Something>` reference (module path), captured with its full dotted head.
REF = re.compile(r"\bFlezha(?:\.[A-Z][A-Za-z0-9_]*)+")
PROTO = re.compile(r"\b(?:RunnerCommands|Protox)\b")


def check(rel, line):
    if line.strip().startswith("#"):
        return []
    allowed = ALLOWED | PER_FILE_ALLOWED.get(rel, set())
    out = []
    for match in REF.finditer(line):
        ref = match.group(0)
        # allowed if it IS or is nested under an allowed seam
        if any(ref == seam or ref.startswith(seam + ".") for seam in allowed):
            continue
        out.append(f"stray controller reference `{ref}`")
    if rel != CODEC and PROTO.search(line):
        out.append("Protox/RunnerCommands outside the codec seam")
    return out


sys.exit(naming_gate.walk(
    check=check,
    pathspec=[SDK_DIR],
    path_filter=lambda rel: rel.endswith(".ex"),
    headline="SDK packaging-boundary violations (SDK must touch the controller only\n"
             "through the named seams — see scripts/check-sdk-boundary.sh):",
    hint="",
    noun="violation(s)",
    ok_message="SDK packaging boundary OK",
))
PY
