#!/usr/bin/env bash
# Fails when a `java-sdk` scenario has no Java test that names it.
#
# `java-sdk-testing` requires at least one harness test per `java-sdk` scenario,
# and requires the rule to be enforced by a gate `mvn -f sdk/java verify` runs.
# This is that gate. Without it the requirement is unfalsifiable: it can never
# be the reason a test exists, and a reader looking for the check finds nothing.
#
# It is run by the `java-spec-coverage` execution of `exec-maven-plugin` in
# sdk/java/pom.xml, bound to `verify` on the aggregator alone, which
# .github/workflows/java-sdk.yml reaches on every pull request. Unlike the
# naming gates, it is not invoked from .github/workflows/naming.yml — the Java
# build is where the requirement puts it, and running it twice would only mean
# two places to keep in step.
#
# SCOPE is every `#### Scenario:` under every `### Requirement:` in
# openspec/specs/java-sdk/spec.md. It is computed from the live spec tree on
# every run, so it cannot rot: an earlier wording scoped the rule to the
# requirements "added by operative-state-persistence-java", and most of those
# titles no longer exist — they were renamed when Shard/Relay became
# Procedure/Operative, and the scope stopped resolving against anything.
#
# MATCHING reuses the annotation the suite already carries. A scenario is
# covered when its title, reduced to lower-case letters and digits, appears
# inside the same reduction of a test's `@DisplayName`, its method name, or its
# class name — the three spellings `java-sdk-testing` itself names. Reduction
# means punctuation, case and word breaks do not matter, so `checkpointAck
# without the tier is refused` covers "checkpointAck() without the tier is
# refused". The scenario must appear in full: a test that names half of one has
# not named it.
#
# THE BASELINE, scripts/java-spec-coverage-baseline.txt, lists the scenarios
# that had no such test when the gate landed. They are the debt, written down
# rather than absorbed. The gate fails on:
#
#   * a scenario with no test that is not in the baseline — the new gap the
#     requirement is about;
#   * a baseline entry that is now covered — the list may only shrink, so a
#     covered scenario comes off it;
#   * a baseline entry the spec no longer carries — a rotting list is the
#     disease this gate treats, so it is not allowed to rot itself.
#
# TEST SOURCES ARE READ FROM DISK, not from `git ls-files` as the naming gates
# are. This gate is bound to `mvn -f sdk/java verify`, so it runs in the inner
# loop, and it must judge the same files the build compiles. Reading the index
# would fail a developer who has written the test and not yet staged it, and a
# gate that goes red on correct work is a gate somebody disables.
#
# The gate has no answer when there is no spec, no scenario or no Java test
# source. That is exit 3, not exit 2: `scripts/naming_gate.py` documents the
# contract at the top, where 2 means a tool this gate needs is absent and 3
# means it ran and reached no verdict. Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import pathlib
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

SPEC = "openspec/specs/java-sdk/spec.md"
BASELINE = "scripts/java-spec-coverage-baseline.txt"
TESTS = "sdk/java"
TEST_SOURCES = "*/src/test/java/**/*.java"

REQUIREMENT = "### Requirement:"
SCENARIO = "#### Scenario:"
# A `@DisplayName` may wrap, and may be written as adjacent literals.
DISPLAY_NAME = re.compile(r'@DisplayName\s*\(\s*((?:"(?:[^"\\]|\\.)*"\s*\+?\s*)+)\)', re.S)
STRING = re.compile(r'"((?:[^"\\]|\\.)*)"')
METHOD = re.compile(r"\bvoid\s+(\w+)\s*\(")


def fingerprint(text):
    """Lower-case letters and digits only, so case, punctuation and word breaks
    cannot decide whether a test names a scenario."""
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def fail(message):
    print(message, file=sys.stderr)
    sys.exit(naming_gate.NO_ANSWER)


try:
    with open(SPEC, encoding="utf-8") as fh:
        spec = fh.read()
except OSError as exc:
    fail(f"cannot read {SPEC}, so no scenario can be judged: {exc}")

# `Requirement :: Scenario` -> the two halves, in spec order. That string is the
# baseline's line, so the baseline is compared by set algebra and never parsed
# back into halves.
scenarios = {}
duplicates = []
malformed = []
under = None
for line in spec.splitlines():
    if line.startswith(REQUIREMENT):
        under = line[len(REQUIREMENT):].strip()
    elif line.startswith(SCENARIO):
        title = line[len(SCENARIO):].strip()
        # An untitled scenario reduces to the empty string, which is inside
        # every test name; one with no requirement above it has no entry a
        # baseline line could spell. Either would be judged covered by nothing
        # at all, so neither is judged.
        if not title or not under:
            malformed.append(line.strip() or SCENARIO)
            continue
        entry = f"{under} :: {title}"
        if entry in scenarios:
            duplicates.append(entry)
        scenarios[entry] = (under, title)
if not scenarios:
    fail(f"{SPEC} declares no `{SCENARIO}` blocks, so no scenario can be judged")
if malformed:
    fail(f"{SPEC} carries a scenario with no title, or none under a requirement:\n  "
         + "\n  ".join(malformed))
if duplicates:
    # One baseline line cannot address one of two identical scenarios, so the
    # second would be judged by the first's entry and silently disappear.
    fail(f"{SPEC} carries the same scenario under one requirement twice, which "
         f"the baseline cannot tell apart:\n  " + "\n  ".join(sorted(duplicates)))

sources = sorted(pathlib.Path(TESTS).glob(TEST_SOURCES))
if not sources:
    fail(f"{TESTS} carries no test sources, so no scenario can be judged")

named = set()
for source in sources:
    text = source.read_text(encoding="utf-8")
    named.add(fingerprint(source.stem))
    named.update(fingerprint(m) for m in METHOD.findall(text))
    for match in DISPLAY_NAME.finditer(text):
        named.add(fingerprint("".join(STRING.findall(match.group(1)))))
named.discard("")

uncovered = {entry for entry, (_, scenario) in scenarios.items()
             if not any(fingerprint(scenario) in name for name in named)}

try:
    baseline = naming_gate.load_baseline(BASELINE)
except OSError as exc:
    fail(f"cannot read {BASELINE}, so no gap can be told from the recorded debt: {exc}")

problems = []
for entry in scenarios:                              # spec order
    if entry in uncovered and entry not in baseline:
        requirement, scenario = scenarios[entry]
        problems.append("  no test names this scenario, and it is not recorded debt:\n"
                        f"    requirement: {requirement}\n"
                        f"    scenario:    {scenario}")
for entry in sorted(baseline - scenarios.keys()):
    problems.append(f"  recorded as uncovered, but {SPEC} no longer carries it:\n    {entry}")
for entry in sorted((baseline & scenarios.keys()) - uncovered):
    problems.append(f"  now covered by a test — take it off the baseline:\n    {entry}")

if problems:
    print("java-sdk scenarios and the Java test suite disagree:\n", file=sys.stderr)
    print("\n\n".join(problems), file=sys.stderr)
    print(f"\nName the scenario in a test's @DisplayName, method name or class name, "
          f"or — for a gap you are recording rather than closing — add its "
          f"`Requirement :: Scenario` line to {BASELINE}. That list may only shrink.",
          file=sys.stderr)
    sys.exit(naming_gate.FINDING)

print(f"java-sdk scenario coverage OK: {len(scenarios)} scenarios, "
      f"{len(scenarios) - len(uncovered)} named by a test, "
      f"{len(uncovered)} recorded as debt.")
PY
