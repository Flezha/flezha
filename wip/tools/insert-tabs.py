#!/usr/bin/env python3
"""Insert language tabs into a guide page's tab blocks.

    wip/tools/insert-tabs.py <page.md> <tabs.md>

<tabs.md> holds sections opened by a line `@@ <n>`: the tabs to add to the
page's n-th tabs block (counted from 1), inserted just before its
`<!-- tabs-close -->`, in the order written. A line `@@file <path>` is
replaced by that file of the repository, so a tracked file is shown verbatim.
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
page, tabs = sys.argv[1], sys.argv[2]
sections = {}
current = None
for line in open(tabs).read().split("\n"):
    marker = re.match(r"^@@ (\d+)\s*$", line)
    if marker:
        current = int(marker.group(1))
        sections[current] = []
    elif current is not None and line.startswith("@@file "):
        sections[current].extend((ROOT / line[7:].strip()).read_text().rstrip("\n").split("\n"))
    elif current is not None:
        sections[current].append(line)

out, block = [], 0
for line in open(page).read().split("\n"):
    if line.startswith("<!-- tabs-close"):
        block += 1
        if block in sections:
            body = "\n".join(sections[block]).strip("\n")
            out.extend([body, ""])
    out.append(line)
missing = set(sections) - set(range(1, block + 1))
if missing:
    sys.exit(f"{page} has no tabs block {sorted(missing)}")
open(page, "w").write("\n".join(out))
