#!/usr/bin/env python3
"""Derive the campaign's numbers from the boards. Never retype a count.

    python3 wip/status.py            counts, net lines, what is next
    python3 wip/status.py --check    also exit non-zero if a board is malformed

Reads WORK.md, DONE.md and NEW-ISSUES.md as they are, so the boards stay
human-written: a table row is a row, and a heading is a heading.
"""
import re
import sys
from pathlib import Path

WIP = Path(__file__).resolve().parent
STATUSES = {"todo", "reproducing", "coding", "review", "verifying", "blocked"}


def rows(path, columns):
    """Table rows under any heading, as dicts. Skips header and rule lines."""
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < columns or set("".join(cells)) <= set("-: "):
            continue
        if cells[0].lower() in ("#", "closed"):
            continue
        out.append(cells)
    return out


def outside_fences(path):
    """The file's text with fenced blocks removed, so a template row in a code
    fence is never counted as a real one."""
    if not path.exists():
        return ""
    keep, fence = [], False
    for line in path.read_text().splitlines():
        if line.lstrip().startswith("```"):
            fence = not fence
            continue
        if not fence:
            keep.append(line)
    return "\n".join(keep)


def main():
    check = "--check" in sys.argv
    problems = []

    work = rows(WIP / "WORK.md", 6)
    done = rows(WIP / "DONE.md", 7)

    by_status = {}
    for r in work:
        status = r[3].strip("`").lower()
        if status not in STATUSES:
            problems.append(f"WORK.md row {r[1]}: unknown status {r[3]!r}")
        by_status[status] = by_status.get(status, 0) + 1
        if not r[4] or r[4] in ("", "-"):
            problems.append(f"WORK.md row {r[1]}: no reproduce command")

    net = 0
    for r in done:
        m = re.search(r"-?\d+", r[5].replace("−", "-"))
        if m:
            net += int(m.group(0))
        elif r[5] not in ("", "-", "n/a"):
            problems.append(f"DONE.md row {r[1]}: production lines not a number")

    issues = outside_fences(WIP / "NEW-ISSUES.md")
    open_issues = len(re.findall(r"^- \*\*Status\*\*: open", issues, re.M))
    fixed_issues = len(re.findall(r"^- \*\*Status\*\*: fixed", issues, re.M))

    print(f"work      {len(work):3d} open")
    for s in sorted(by_status):
        print(f"          {by_status[s]:3d} {s}")
    print(f"done      {len(done):3d}")
    print(f"net prod  {net:+d} lines")
    print(f"found     {open_issues} open, {fixed_issues} fixed during the campaign")

    nxt = next((r for r in work if r[3].strip("`").lower() == "todo"), None)
    print(f"next      {nxt[1] + ' ' + nxt[2] if nxt else 'nothing queued'}")

    if problems:
        print("\nmalformed:")
        for p in problems:
            print("  " + p)
    return 1 if (problems and check) else 0


if __name__ == "__main__":
    sys.exit(main())
