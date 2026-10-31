#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Anuar Alfetahe
#
# SPDX-License-Identifier: MPL-2.0
#
# The feature-maturity gate. Keeps docs/public/publishing/maturity.md and the tree
# agreeing, in both directions:
#
#   - both tables parse, every row carries its fields, ids are unique, and
#     every entry is experimental or preview;
#   - every surface a row lists is tracked and carries that row's marker,
#     with the entry's label;
#   - every announce site an experimental entry names is tracked, carries the
#     symbol it is named by, and points at the entry's row; a preview entry's
#     announce site is none;
#   - no tier row uses the experimental label, a tier 2 row states its promise,
#     and a capability a runtime's conformance manifest defers is named in its row;
#   - no tracked prose page calls a feature experimental outside a marker for
#     a listed entry. The word preview is not judged: it also names a
#     streamed part.
#
# It reads only what git tracks, and prints how many rows of each label it
# read and each entry's id with its date, so a green that means "the parser
# matched nothing" is distinguishable from a sound record, and a long-lived
# entry shows on every run. Exit codes are the runner's contract, which
# gate_support.py documents.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - "$@" <<'PY'
import json
import os
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import gate_support
import record_table

RECORD = "docs/public/publishing/maturity.md"
ENTRY_COLUMNS = ("id", "feature", "status", "marked", "promise", "surfaces", "announced-by")
TIER_COLUMNS = ("sdk", "tier", "promise", "surfaces")
TIER_2_PROMISES = ("full conformance run", "permanently supported")
STATUSES = ("experimental", "preview")

EXPERIMENTAL = re.compile(r"\bexperimental\b", re.IGNORECASE)
MARKER = re.compile(rf"\[({'|'.join(STATUSES)}): ([a-z0-9-]+)\]"
                    r"\([^)\s]*maturity\.md#([a-z0-9-]+)\)")
TIER_MARKER = re.compile(r"\[tier ([12])\]\([^)\s]*maturity\.md#sdk-([a-z]+)\)")
CODE = re.compile(r"`([^`]+)`")
ANCHOR = re.compile(r'^<a id="([a-z0-9-]+)"></a>([a-z0-9-]+)$')
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def prose(rel):
    """A page whose words the experimental label is judged on."""
    return (rel in ("README.md", "CHANGELOG.md")
            or (rel.startswith("docs/") and rel.endswith(".md"))
            or re.fullmatch(r"sdk/[a-z]+/README\.md", rel) is not None)


def read(rel):
    with open(rel, encoding="utf-8") as fh:
        return fh.read()


def check_rows(rows, columns, failures, what, anchor_prefix=""):
    """Width, anchor, unique id and required fields, for one table.

    Returns `(id, row)` for every row of the right width, each row saying
    whether it is complete: an incomplete row still lists its id, so its
    markers are not reported a second time as unlisted."""
    judged, seen = [], {}
    for row in rows:
        where = f"{RECORD}:{row['lineno']}"
        cell = row[columns[0]] if "_width" not in row else row["id"]
        found = ANCHOR.match(cell)
        key = found.group(2) if found else cell
        if "_width" in row:
            failures.append(f"{where}: {what} {key} has {row['_width']} columns, "
                            f"expected {len(columns)}")
            continue
        if not found or found.group(1) != anchor_prefix + key:
            failures.append(f'{where}: {what} {key!r} does not open with '
                            f'<a id="{anchor_prefix}<id>"></a><id>')
        if key in seen:
            failures.append(f"{where}: {what} {key} repeats {RECORD}:{seen[key]}")
        seen[key] = row["lineno"]
        missing = [c for c in columns if not row.get(c)]
        for column in missing:
            failures.append(f"{where}: {what} {key} has no {column}")
        judged.append((key, {**row, "_complete": not missing}))
    return judged


def check_surfaces(key, cell, carries, tracked, failures, lineno):
    for rel in CODE.findall(cell):
        if rel not in tracked:
            failures.append(f"{RECORD}:{lineno}: {key} lists {rel}, which git does not track")
        elif not carries(read(rel)):
            failures.append(f"{rel}: carries no marker for {key} ({RECORD}:{lineno})")


def check_entries(entries, tracked, failures):
    for key, row in entries:
        where = f"{RECORD}:{row['lineno']}"
        if not row["_complete"]:
            continue
        if row["status"] not in STATUSES:
            failures.append(f"{where}: {key} status is {row['status']!r}, expected "
                            f"{' or '.join(STATUSES)}")
        if not DATE.match(row["marked"]):
            failures.append(f"{where}: {key} marked {row['marked']!r} is not a YYYY-MM-DD date")

        check_surfaces(key, row["surfaces"],
                       lambda text, k=key, s=row["status"]: any(
                           m.groups() == (s, k, k) for m in MARKER.finditer(text)),
                       tracked, failures, row["lineno"])

        # Preview stays, so nothing warns about it at runtime.
        if row["status"] == "preview" and row["announced-by"] != "none":
            failures.append(f"{where}: preview entry {key} names an announce site; "
                            f"expected none")
        if row["status"] != "experimental":
            continue

        # One site per `;`-separated part, opening with its `path` `symbol`.
        sites = [CODE.findall(part)[:2] for part in row["announced-by"].split(";")]
        if any(len(site) != 2 for site in sites):
            failures.append(f"{where}: {key} has an announce site that does not open with "
                            f"`path` `symbol`")
        for rel, symbol in (site for site in sites if len(site) == 2):
            if rel not in tracked:
                failures.append(f"{where}: {key} announce site {rel} is not tracked")
                continue
            text = read(rel)
            if symbol not in text:
                failures.append(f"{where}: {key} announce site {rel} carries no {symbol}")
            if f"maturity.md#{key}" not in text:
                failures.append(f"{rel}: the announcement for {key} does not point at "
                                f"maturity.md#{key}")


def deferred(sdk):
    manifest = f"test/apps/{sdk}_runner/conformance.json"
    if not os.path.isfile(manifest):
        return []
    with open(manifest, encoding="utf-8") as fh:
        return json.load(fh).get("deferred") or []


def check_tiers(tiers, tracked, failures):
    for sdk, row in tiers:
        where = f"{RECORD}:{row['lineno']}"
        if not row["_complete"]:
            continue
        if row["tier"] not in ("1", "2"):
            failures.append(f"{where}: {sdk} tier is {row['tier']!r}, expected 1 or 2")
        if EXPERIMENTAL.search(" ".join(row[c] for c in TIER_COLUMNS)):
            failures.append(f"{where}: tier row {sdk} uses the word experimental")
        if row["tier"] == "2":
            for promise in TIER_2_PROMISES:
                if promise not in row["promise"]:
                    failures.append(f"{where}: tier 2 row {sdk} does not state {promise!r}")
        for capability in deferred(sdk):
            if capability not in row["promise"]:
                failures.append(f"{where}: {sdk} defers {capability} and its row does not "
                                f"name it")
        check_surfaces(sdk, row["surfaces"],
                       lambda text, s=sdk, t=row["tier"]: any(
                           m.groups() == (t, s) for m in TIER_MARKER.finditer(text)),
                       tracked, failures, row["lineno"])


def prose_check(listed):
    """A `walk` check: a marker names a listed entry by its label, and the
    word experimental appears nowhere else on the line."""
    def check(_rel, line):
        found = [f"marker {m.group(0)}, which {RECORD} does not list"
                 for m in MARKER.finditer(line) if listed.get(m.group(2)) != m.group(1)]
        if EXPERIMENTAL.search(MARKER.sub("", line)):
            found.append(f"calls something experimental outside a marker: {line.strip()}")
        return found
    return check


def main():
    try:
        lines = read(RECORD).split("\n")
    except OSError as exc:
        print(f"Feature-maturity record is unreadable:\n\n  {RECORD}: {exc}", file=sys.stderr)
        return gate_support.FINDING

    failures = record_table.forbidden(RECORD, lines)
    tables = {}
    for name, columns in (("entries", ENTRY_COLUMNS), ("tier", TIER_COLUMNS)):
        found = record_table.rows(lines, columns)
        if not found:
            failures.append(f"{RECORD}: no {name} table with rows under a header spelling "
                            f"| {' | '.join(columns)} |")
        tables[name] = found or []

    tracked = set(gate_support.tracked_files(carried=True))
    entries = check_rows(tables["entries"], ENTRY_COLUMNS, failures, "entry")
    tiers = check_rows(tables["tier"], TIER_COLUMNS, failures, "tier row", "sdk-")
    check_entries(entries, tracked, failures)
    check_tiers(tiers, tracked, failures)

    dated = ", ".join(f"{key} marked {row.get('marked')}" for key, row in entries)
    counts = ", ".join(f"{status} entries read: "
                       f"{sum(row.get('status') == status for row in tables['entries'])}"
                       for status in STATUSES)
    read_line = f"{counts} ({dated or 'none'}); tier rows read: {len(tables['tier'])}"
    record_code = gate_support.report(
        failures,
        headline="The feature-maturity record and the tree disagree:",
        hint=f"{RECORD} is the one declaration. Mark a listed part with "
             f"[experimental: <id>](.../maturity.md#<id>) or [preview: <id>](...), "
             f"matching its status, and an SDK's tier with "
             f"[tier N](.../maturity.md#sdk-<sdk>); {read_line}.",
        noun="problem(s)",
        ok_message=f"feature-maturity record OK - {read_line}",
    )
    prose_code = gate_support.walk(
        check=prose_check({key: row.get("status") for key, row in entries}),
        path_filter=prose,
        self_files={RECORD},
        headline="Tracked prose labels something that the record does not list as such:",
        hint=f"Use the marker for an entry {RECORD} lists, or say it without the label.",
        ok_message=None,
    )
    return record_code or prose_code


sys.exit(main())
PY
