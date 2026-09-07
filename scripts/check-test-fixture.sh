#!/usr/bin/env bash
# Fails when a test module builds a fixture in its own `setup` instead of
# declaring one.
#
# `test/helpers/case.ex` holds the fixture table and says what it owns: the
# state a test has to put back. Written by hand, each of those is a `setup` and
# a matching `on_exit` the next module spells slightly differently.
#
# `start_supervised` is deliberately NOT one of the shapes below. ExUnit already
# owns a supervised child and unwinds it with the test, so it IS the seam;
# putting it in the table would replace one seam with another rather than remove
# a bypass.
#
# SOURCES ARE READ FROM DISK, not from `git ls-files`. A test being written is
# exactly the work this gate should judge, and an index walk would call an
# unstaged module clean.
#
# THE BASELINE, scripts/test-fixture-baseline.txt, lists the modules that build
# a fixture the table could own and are staying that way, each with its reason.
# It may only shrink: an entry the tree no longer holds fails, so the list
# cannot rot into an exemption. Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

BASELINE = "scripts/test-fixture-baseline.txt"

# The opener of a module's own callback, and how far its body reaches. Forty
# lines is what the census that measured this scatter used; a `setup` whose
# fixture is further down than that is not the shape the table replaces.
SETUP = re.compile(r"^\s*setup(_all)?\b.*\bdo\s*$")
WINDOW = 40

# The state a fixture row puts back. Each is a `setup` plus an `on_exit` when it
# is written by hand.
FIXTURE = re.compile(
    r"System\.tmp_dir|claim_tmp_dir\(|tmp_app_data_dir\(|File\.mkdir_p"
    r"|Application\.put_env|setup_auth_stores|isolated_dir\("
)
DECLARED = re.compile(r"use\s+FlezhaTest\.[A-Za-z.]*Case\b")

baseline = naming_gate.load_baseline(BASELINE)
hits = set()
state = {"rel": None, "window": 0}


def check(rel, line):
    if state["rel"] != rel:
        state["rel"] = rel
        state["window"] = 0
    if SETUP.match(line):
        state["window"] = WINDOW
        return []
    if state["window"] <= 0:
        return []
    state["window"] -= 1
    if not FIXTURE.search(line):
        return []
    # One finding per module: the unit the table replaces is the module's own
    # setup, not each line inside it.
    state["window"] = 0
    hits.add(rel)
    if rel in baseline:
        return []
    return [line.strip()]


code = naming_gate.walk(
    check=check,
    source="disk",
    roots=["test"],
    path_filter=lambda rel: rel.endswith("_test.exs"),
    prefilter=lambda text: not DECLARED.search(text),
    headline="a test module builds a fixture in its own `setup` and declares none:",
    hint="Declare it: `use FlezhaTest.Case, fixtures: [:tmp_app_data, :env, ...]`, "
         "or add the module to " + BASELINE + " with the reason it stays. "
         "See test/helpers/case.ex for the rows.",
    ok_message="test fixture declarations OK",
    noun="module(s)",
)

# Both halves report: a run with a new module AND a stale entry says so once.
stale = naming_gate.stale_baseline(BASELINE, baseline, hits, "build a fixture by hand")
sys.exit(code or stale)
PY
