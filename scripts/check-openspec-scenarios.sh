#!/usr/bin/env bash
# Fails when an open OpenSpec change would delete a published scenario on
# archive without saying so.
#
# A `## MODIFIED Requirements` block REPLACES the whole requirement when its
# change is archived, so a published `#### Scenario:` the block does not repeat
# is removed from `openspec/specs/` silently. `openspec validate --strict` judges
# the delta's own shape and never reads the requirement it replaces, so it
# passes such a change. This gate reads the pair: for every
# `specs/<capability>/spec.md` of every open change, each MODIFIED requirement
# is matched against `openspec/specs/<capability>/spec.md`, and every published
# scenario the block does not carry is refused, naming the change, the
# requirement and the scenario.
#
# Leaving a scenario out of a MODIFIED block is also the only way OpenSpec has to
# remove one on purpose, so a drop is accepted when the change's `proposal.md`
# or `design.md` carries the line `Removes scenario: <exact scenario title>`.
# The line lives outside the delta, so nothing extra is archived into the spec.
#
# Titles are read the way the CLI reads them: sections by `## ` heading, case
# aside, and requirement titles trimmed. A `## RENAMED Requirements` pair in the
# same delta is applied first, as the archive applies it, so a MODIFIED block
# names the TO title and its published counterpart is the FROM title. A MODIFIED
# requirement with no published counterpart is `openspec validate`'s question,
# not this gate's, and is skipped.
#
# SCOPE IS DISK: `/openspec/changes` is in .gitignore, so on a CI checkout this
# gate reads an empty queue, says so, and has judged nothing.
#
# `--self-test` runs the judgement over inline specs with known answers. Exit 0
# clean, 1 on a refusal, and 3 when called with an argument it does not take.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - "$@" <<'PY'
import os
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

SPECS = "openspec/specs"

SECTION = re.compile(r"^##\s+(.+?)\s*$")
REQUIREMENT = re.compile(r"^###\s*Requirement:\s*(.+?)\s*$")
SCENARIO = re.compile(r"^####\s+Scenario:\s*(.+?)\s*$")
RENAME = re.compile(r"^\s*-?\s*(FROM|TO):\s*`?###\s*Requirement:\s*(.+?)`?\s*$")
REMOVAL = re.compile(r"^Removes scenario:\s*(.+?)\s*$", re.M)
NOTES = ("proposal.md", "design.md")


def sections(text):
    """Each `## ` section's lines, keyed by its lower-cased title."""
    found, current = {}, None
    for line in text.splitlines():
        match = SECTION.match(line)
        if match:
            current = found.setdefault(match.group(1).lower(), [])
        elif current is not None:
            current.append(line)
    return found


def scenarios_by_requirement(lines):
    """Each requirement title in `lines`, with the scenario titles its block holds."""
    blocks, current = {}, None
    for line in lines:
        header = REQUIREMENT.match(line)
        if header:
            current = blocks.setdefault(header.group(1), [])
        elif current is not None and (scenario := SCENARIO.match(line)):
            current.append(scenario.group(1))
    return blocks


def renamed_from(lines):
    """The RENAMED pairs in `lines`, as {TO title: FROM title}."""
    pairs, source = {}, None
    for line in lines:
        match = RENAME.match(line)
        if match and match.group(1) == "FROM":
            source = match.group(2)
        elif match and source is not None:
            pairs[match.group(2)] = source
            source = None
    return pairs


def dropped(delta, published, notes=""):
    """(requirement, scenario) for every published scenario that a MODIFIED
    block in `delta` replaces and does not carry, unless `notes` (the change's
    proposal and design) records its removal."""
    removed = set(REMOVAL.findall(notes))
    parts = sections(delta)
    modified = scenarios_by_requirement(parts.get("modified requirements", []))
    sources = renamed_from(parts.get("renamed requirements", []))
    before = scenarios_by_requirement(sections(published).get("requirements", []))
    return [(title, scenario)
            for title, carried in modified.items()
            for scenario in before.get(sources.get(title, title), [])
            if scenario not in carried and scenario not in removed]


def read(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return None


def judge():
    opened = naming_gate.open_changes()
    if not opened:
        print(f"openspec scenarios: no open change to judge ({naming_gate.OPENSPEC_CHANGES}/ "
              "is untracked, so a fresh checkout carries none and this run checked nothing)")
        return naming_gate.OK

    refusals = []
    for change_id in opened:
        change = os.path.join(naming_gate.OPENSPEC_CHANGES, change_id)
        notes = "\n".join(read(os.path.join(change, name)) or "" for name in NOTES)
        specs = os.path.join(change, "specs")
        for capability in sorted(os.listdir(specs)) if os.path.isdir(specs) else ():
            delta = read(os.path.join(specs, capability, "spec.md"))
            published = read(os.path.join(SPECS, capability, "spec.md"))
            if delta is None or published is None:
                continue
            refusals.extend(
                f"{change_id}: specs/{capability}/spec.md MODIFIED `{title}` drops "
                f"the published scenario `{scenario}`; if that is meant, add the line "
                f"`Removes scenario: {scenario}` to its proposal.md or design.md"
                for title, scenario in dropped(delta, published, notes))

    return naming_gate.report(
        refusals,
        headline="an open change would delete published scenarios on archive:",
        hint="A MODIFIED block replaces the whole requirement: copy every scenario "
             "that survives into it, and record each deliberate removal as the line "
             "named above.",
        noun="dropped scenario(s)",
        ok_message=f"openspec scenarios OK ({len(opened)} open change(s) read)")


PUBLISHED = """## Requirements
### Requirement: Kept
#### Scenario: First
#### Scenario: Second
### Requirement: Other
#### Scenario: Third
"""


def self_test():
    """Known answers for the judgement, each case one shape it must get right."""
    modified = "## MODIFIED Requirements\n"
    cases = {
        "every scenario carried": (
            modified + "### Requirement: Kept\n#### Scenario: Second\n#### Scenario: First\n", []),
        "one scenario dropped": (
            modified + "### Requirement:  Kept \n#### Scenario: First\n", [("Kept", "Second")]),
        "one scenario dropped and acknowledged": (
            modified + "### Requirement: Kept\n#### Scenario: First\n", [],
            "Depends-On: none\n\nRemoves scenario: Second \n"),
        "every scenario dropped": (
            modified + "### Requirement: Kept\n", [("Kept", "First"), ("Kept", "Second")]),
        # A scenario carried by another requirement's block does not count.
        "carried by the wrong block": (
            modified + "### Requirement: Kept\n#### Scenario: First\n"
            "### Requirement: Other\n#### Scenario: Third\n#### Scenario: Second\n",
            [("Kept", "Second")]),
        "renamed, then dropped": (
            "## RENAMED Requirements\n- FROM: `### Requirement: Kept`\n"
            "- TO: `### Requirement: Renamed`\n\n" + modified
            + "### Requirement: Renamed\n#### Scenario: Second\n", [("Renamed", "First")]),
        "no published counterpart": (modified + "### Requirement: New\n", []),
    }
    answers = {name: dropped(delta, PUBLISHED, *notes)
               for name, (delta, _, *notes) in cases.items()}
    problems = [f"  {name}: got {answers[name]}, expected {want}"
                for name, (_, want, *_) in cases.items() if answers[name] != want]
    if problems:
        print("check-openspec-scenarios self-test FAILED", file=sys.stderr)
        print("\n".join(problems), file=sys.stderr)
        return naming_gate.FINDING
    print(f"check-openspec-scenarios self-test OK ({len(cases)} cases)")
    return naming_gate.OK


argv = sys.argv[1:]
if argv == ["--self-test"]:
    sys.exit(self_test())
if argv:
    print("usage: check-openspec-scenarios.sh [--self-test]", file=sys.stderr)
    sys.exit(naming_gate.NO_ANSWER)
sys.exit(judge())
PY
