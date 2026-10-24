#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Anuar Alfetahe
#
# SPDX-License-Identifier: MPL-2.0
#
# The publication-readiness gate. Two arms, because the record it reads is open
# by construction on the day it lands:
#
#   (default)  shape    every entry parses and carries a class, a door, a rule,
#                       a check and an owner; ids are unique; a settled entry
#                       names its evidence; the record states rules rather than
#                       instances. Runs on every pull request and is GREEN from
#                       the first commit.
#   --release  content   every BLOCKING entry is settled. Runs on a version tag,
#                       at the head of the job the tag triggers, so an open
#                       entry stops the release BEFORE an artefact exists. An
#                       OWED entry never stops a release: if it did, every entry
#                       would drift into `owed` to get a release out, and the
#                       classification would stop meaning anything.
#
# A gate that judged content on every pull request would be red from its first
# commit, and a gate that lands red stops being read - this repository has paid
# for that once already, in three sessions spent rewording correct prose to
# satisfy a gate nobody had read.
#
# Both arms print how many entries they read, so a green that means "the parser
# matched nothing" is distinguishable from a green that means the record is
# sound. Exit codes are the runner's contract, which gate_support.py documents.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - "$@" <<'PY'
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import gate_support
import record_table

RECORD = "docs/internal/publishing/readiness.md"

COLUMNS = ("id", "class", "door", "rule", "check", "owner", "settled-by")

# Every column but `settled-by`, which is empty until the entry is closed.
REQUIRED = COLUMNS[:-1]

CLASSES = ("blocking", "owed")
QUALIFIERS = ("decision", "settled")


def read_entries():
    """The record's lines and every entry row under its entries header.

    Returns (lines, entries, error). An error is a string; it is a FINDING
    rather than a crash, because a record whose table nobody can find is
    exactly the state this gate exists to refuse. Both arms read the file
    through here, so the record is opened once and one error path answers for
    it.
    """
    try:
        with open(RECORD, encoding="utf-8") as fh:
            lines = fh.read().split("\n")
    except OSError as exc:
        return [], [], f"{RECORD}: {exc}"

    entries = record_table.rows(lines, COLUMNS)
    if entries is None:
        return lines, [], (
            f"{RECORD}: no entries table. Expected a header row spelling "
            f"| {' | '.join(COLUMNS)} |"
        )
    return lines, entries, None


def classify(entry):
    """The class word and the qualifiers of an entry's `class` column."""
    words = [w.strip() for w in entry.get("class", "").split("·")]
    base = [w for w in words if w in CLASSES]
    extra = [w for w in words if w in QUALIFIERS]
    unknown = [w for w in words if w and w not in CLASSES and w not in QUALIFIERS]
    return (base[0] if len(base) == 1 else None), extra, unknown


def shape_check(lines, entries):
    failures = []
    seen = {}
    blocking = 0
    for entry in entries:
        ident = entry.get("id") or "?"
        where = f"{RECORD}:{entry['lineno']}"

        if "_width" in entry:
            failures.append(
                f"{where}: entry {ident} has {entry['_width']} columns, expected {len(COLUMNS)}"
            )
            continue

        for column in REQUIRED:
            if not entry.get(column):
                failures.append(f"{where}: entry {ident} has no {column}")

        if ident in seen:
            failures.append(f"{where}: id {ident} repeats {RECORD}:{seen[ident]}")
        seen[ident] = entry["lineno"]

        base, qualifiers, unknown = classify(entry)
        if base == "blocking":
            blocking += 1
        if base is None:
            failures.append(
                f"{where}: entry {ident} class is {entry.get('class')!r}, "
                f"expected one of {', '.join(CLASSES)}"
            )
        if unknown:
            failures.append(
                f"{where}: entry {ident} class carries {', '.join(unknown)}; "
                f"known qualifiers are {', '.join(QUALIFIERS)}"
            )

        # An entry whose author cannot name the letter has not shown it is
        # one-way, which is the whole discipline of the record.
        door = entry.get("door", "")
        if base == "blocking" and not re.search(r"\((?:a|b|c)\)", door):
            failures.append(
                f"{where}: blocking entry {ident} names no door letter (a), (b) or (c)"
            )
        if base == "owed" and door != "—":
            failures.append(f"{where}: owed entry {ident} names a door, so it is blocking")

        if "settled" in qualifiers and not entry.get("settled-by"):
            failures.append(
                f"{where}: entry {ident} is marked settled and names no evidence"
            )

    failures += record_table.forbidden(RECORD, lines)

    code = gate_support.report(
        failures,
        headline="Publication-readiness record is malformed:",
        hint=f"Every entry carries {', '.join(COLUMNS[:-1])}; `settled-by` is filled in "
             f"when the class gains the `settled` qualifier. The record states RULES, so "
             f"it names no file, commit or count that only exists on one box.",
        noun=f"problem(s) across {len(entries)} entries",
    )
    if code:
        return code

    if not entries:
        print(f"{RECORD} carries no entries at all", file=sys.stderr)
        return gate_support.FINDING

    print(
        f"publication-readiness record OK - {len(entries)} entries read, "
        f"{blocking} blocking, {len(entries) - blocking} owed"
    )
    return gate_support.OK


def release_check(_lines, entries):
    open_blocking = []
    blocking = 0
    for entry in entries:
        base, qualifiers, _ = classify(entry)
        if base != "blocking":
            continue
        blocking += 1
        if "settled" not in qualifiers:
            open_blocking.append(entry)

    if open_blocking:
        print(
            "A release may not be produced while a blocking publication decision "
            "is open.\n",
            file=sys.stderr,
        )
        for entry in open_blocking:
            print(
                f"  {entry['id']}  {entry.get('rule', '')}\n"
                f"      door: {entry.get('door', '')}\n"
                f"      owner: {entry.get('owner', '')}",
                file=sys.stderr,
            )
        print(
            f"\n{len(open_blocking)} of {blocking} blocking entries are open in "
            f"{RECORD}. Each one is a decision this release makes permanent. Settle it, "
            f"then add the `settled` qualifier to its class and name the evidence in "
            f"`settled-by`. An OWED entry does not stop a release and none is listed here.",
            file=sys.stderr,
        )
        return gate_support.FINDING

    print(
        f"publication readiness OK - {blocking} blocking entries, all settled "
        f"({len(entries)} entries read)"
    )
    return gate_support.OK


ARMS = {"--shape": shape_check, "--release": release_check}

argv = sys.argv[1:] or ["--shape"]
unknown = [a for a in argv if a not in ARMS]
if unknown or len(argv) != 1:
    print(
        f"usage: check-publish-readiness.sh [--shape|--release]\n"
        f"  unknown or repeated argument(s): {', '.join(unknown) or 'too many'}",
        file=sys.stderr,
    )
    sys.exit(gate_support.NO_ANSWER)

# Read once, here, so an unreadable record is one answer rather than one per
# arm. Each arm below is then a judgement over what was read and nothing else.
record_lines, record_entries, read_error = read_entries()
if read_error:
    print(f"Publication-readiness record is unreadable:\n\n  {read_error}", file=sys.stderr)
    sys.exit(gate_support.FINDING)

sys.exit(ARMS[argv[0]](record_lines, record_entries))
PY
