#!/usr/bin/env bash
# Fails when a workflow sets up a toolchain itself instead of using the
# composite action that reads the one manifest.
#
# `.github/toolchains.json` carries one version set, `scripts/manifest_pin.sh`
# is its reader, and the actions under `.github/actions/` own the setup step,
# the cache key and the deps fetch. A workflow names a version only to OVERRIDE
# the manifest from a matrix, which is `${{ matrix.x }}` and never a literal:
# Node reached 24 in one workflow against 22 in four others, and Python 3.12
# against 3.11, because a new workflow re-declared the whole world instead of
# naming an action.
#
# `.github/actions/` is out of scope BY CONSTRUCTION: it is the seam, and the
# setup action and the version literal live there on purpose.
#
# SOURCES ARE READ FROM DISK, not from `git ls-files`: a workflow being written
# is exactly the work this should judge, and an index walk would call an
# unstaged one clean. Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

# A third-party action that installs a language toolchain. `actions/checkout`,
# `upload-artifact`, `taiki-e/install-action` and `fsfe/reuse-action` install no
# toolchain and are not this gate's business.
SETUP_ACTIONS = re.compile(
    r"uses:\s*(erlef/setup-beam"
    r"|dtolnay/rust-toolchain"
    r"|actions/setup-(?:node|java|python|go|dotnet)"
    r"|ruby/setup-ruby"
    r"|shivammathur/setup-php"
    r"|arduino/setup-protoc)"
)

# A pinned version of one of the manifest's toolchains, written as a literal. A
# `${{ ... }}` value is a matrix fan-out or an action input and is the supported
# way to override the pin.
VERSION_LITERAL = re.compile(
    r"^\s*(?:otp|elixir|java|node|python|ruby|php)-version:\s*\"?[0-9]"
)

# The one place a Rust channel is named. `nightly-2026-05-07` is a date and
# `stable` a word, so neither is caught by the numeric shape above.
RUST_CHANNEL = re.compile(r"^\s*toolchain:\s*(stable|beta|nightly)")


def check(_rel, line):
    for pattern, why in (
        (SETUP_ACTIONS, "sets a toolchain up itself"),
        (VERSION_LITERAL, "pins a toolchain version as a literal"),
        (RUST_CHANNEL, "names a Rust channel"),
    ):
        if pattern.search(line):
            return [f"{why}: {line.strip()}"]
    return []


sys.exit(naming_gate.walk(
    check=check,
    source="disk",
    roots=[".github/workflows"],
    path_filter=lambda rel: rel.endswith((".yml", ".yaml")),
    headline="a workflow sets up a toolchain instead of using the composite action:",
    hint="Use `uses: ./.github/actions/setup-<toolchain>`. It reads "
         ".github/toolchains.json through scripts/manifest_pin.sh and owns the "
         "cache key. A matrix arm passes `version: ${{ matrix.x }}`.",
    ok_message="CI toolchain declarations OK",
    noun="line(s)",
))
PY
