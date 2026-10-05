#!/usr/bin/env bash
# Fails when a local-mode symbol or documentation page is named as testing
# material.
#
# Local mode is a way to run an application, not a way to test one. A user
# reaching for it must not be asked to import something that reads as test
# scaffolding, and the page that introduces it must not be filed under testing.
# `TaskletTestHarness` and its per-SDK equivalents keep their names: they are
# for writing tests, which is the one place the word is correct.
#
# Two judgements, both narrow-pattern (`pattern_check` with no allowlist), the
# style `check-overflow-naming.sh` uses:
#
#   SYMBOL, repo-wide: a testing word fused into a local-mode symbol, in any
#   SDK's spelling. The words are ordinary English, so only the fused shapes
#   are matched — prose about testing is nobody's business here.
#
#   SURFACE, the public files only: a DECLARATION whose own name carries a
#   testing word, and a Markdown heading that files the page as testing
#   material. Prose inside those files is untouched for the same reason.
#
# Tests are out of scope BY CONSTRUCTION, not by exemption: the surface list
# names the public files and nothing else, and a path under a test directory is
# refused entry to it. A test of local mode can therefore be called whatever a
# test should be called.
#
# The surface list may only shrink or be corrected: an entry the tree no longer
# holds fails, because a gate that silently stops reading a file is
# indistinguishable from one reading a clean tree. Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import os
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

# The public local-mode surface: the files a user's import or a reader's link
# reaches. Not a scope for prose — see SURFACE below for what is judged here.
SURFACE = (
    "flz_core/src/local.rs",
    "sdk/rust/src/local.rs",
    "sdk/rust/examples/local_word_count.rs",
    "sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/LocalRelay.java",
    "sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/LocalOutput.java",
    "sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/internal/LocalRunHandle.java",
    "sdk/java/flezha-sdk-runtime/src/main/java/com/flezha/sdk/runtime/LocalImpl.java",
    "sdk/java/flezha-sdk-examples/src/main/java/com/flezha/examples/local/Main.java",
    "sdk/elixir/lib/flezha/sdk/local.ex",
    "flz_core_ffi/src/local.rs",
    "sdk/node/src/local.ts",
    "examples/node/local_word_count.mjs",
    "sdk/python/src/flezha/local.py",
    "sdk/python/examples/local_word_count.py",
    "sdk/ruby/lib/flezha/local.rb",
    "sdk/ruby/examples/local_word_count.rb",
    "sdk/php/src/LocalRelay.php",
    "sdk/php/src/LocalOutput.php",
    "sdk/php/examples/E07_LocalMode.php",
    "docs/topics/basics/local_mode.md",
)

TESTING = r"[Tt]est|TEST|[Tt]esting|TESTING|[Hh]arness|HARNESS|[Mm]ock|MOCK|[Ff]ake|FAKE|[Ss]tub|STUB|[Dd]ummy|DUMMY|[Ss]ample|SAMPLE"

# A testing word FUSED into a local-mode symbol — the only shape that could
# reintroduce the name. Every SDK's spelling of the symbol is listed, because
# the point is the identifier and not the sentence around it.
LOCAL_NAME = r"Local(?:Run|Relay|Mode|Impl|Session|Frame|Output)|flz_core_local_[a-z_]+|local_(?:run|mode|relay)|Flezha\.SDK\.Local|flz_core::local|flezha_runner::local"
SYMBOL = [re.compile(p) for p in (
    rf"(?:{LOCAL_NAME})[A-Za-z0-9_.:]*(?:{TESTING})",
    rf"(?:{TESTING})[A-Za-z0-9_.:]*(?:{LOCAL_NAME})",
)]

# Inside the public surface: a declaration whose declared name carries a
# testing word, and a heading that files the page as testing material.
SURFACE_NAMES = [re.compile(p) for p in (
    r"\b(?:fn|struct|enum|trait|mod|type|const|static|class|interface|record|"
    r"defmodule|defstruct|def|defp)\s+[A-Za-z0-9_.]*(?:" + TESTING + r")",
    r"^\s*#{1,6}\s.*(?:" + TESTING + r")",
)]

missing = [rel for rel in SURFACE if not os.path.exists(rel)]
if missing:
    print("the local-mode surface list names files the tree no longer holds:\n",
          file=sys.stderr)
    for rel in missing:
        print(f"  {rel}", file=sys.stderr)
    print("\nCorrect scripts/check-local-mode-naming.sh: a gate that stopped "
          "reading a file is indistinguishable from a clean tree.\n",
          file=sys.stderr)
    sys.exit(naming_gate.FINDING)

symbol = naming_gate.pattern_check(SYMBOL)
surface = naming_gate.pattern_check(SURFACE_NAMES)

# A path under a test directory is not part of the public surface, so it is
# never read: `tests/`, `test/`, `src/test/` and a `*_test`/`*_tests` file.
TEST_PATH = re.compile(r"(^|/)tests?/|(^|/)src/test/|_tests?\.[a-z]+$|_test\.exs$|Test\.java$")

def check(rel, line):
    if TEST_PATH.search(rel):
        return []
    found = symbol(rel, line)
    if rel in SURFACE and not found:
        found = surface(rel, line)
    return found

sys.exit(naming_gate.walk(
    check=check,
    prefilter=lambda text: "ocal" in text or "OCAL" in text,
    # The gate's own files carry the retired shapes as data.
    self_files={"scripts/check-local-mode-naming.sh", ".github/workflows/naming.yml"},
    record_tag="local-mode-naming-record",
    headline="a local-mode symbol or page named as testing material:",
    hint="Local mode runs an application; it does not test one. Name the "
         "symbol for what it does, and file its page as user-facing material.",
    ok_message="local-mode naming OK",
))
PY
