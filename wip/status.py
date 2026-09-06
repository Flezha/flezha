#!/usr/bin/env python3
"""Derive the campaign's numbers from the boards. Never retype a count.

    python3 wip/status.py            counts, what is in flight, what closed
    python3 wip/status.py --check    also exit non-zero if a board is malformed
    python3 wip/status.py --report   the same, as a block to paste to a human

    watch -n 60 python3 wip/status.py    live, from another terminal

Reads WORK.md, DONE.md, NEW-ISSUES.md and STATE.md as they are, so the boards
stay human-written: a table row is a row, and a heading is a heading. Every
number a report carries comes from here, so a report is derived rather than
remembered and cannot drift from the files the next session reads.
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


def state_now():
    """The live row, phase and worker, from STATE.md's Now block."""
    path = WIP / "STATE.md"
    if not path.exists():
        return {}
    out = {}
    for line in path.read_text().splitlines():
        m = re.match(r"- \*\*(Current item|Phase|Worker|Critic)\*\*: *(.*)", line)
        if m:
            out[m.group(1).lower().replace(" ", "_")] = m.group(2).strip()
        m2 = re.match(r"\*\*Campaign\*\*: *`?([^`·]+)`? *·.*\*\*Updated\*\*: *(.*)", line)
        if m2:
            out["campaign"] = m2.group(1).strip()
            out["updated"] = m2.group(2).strip()
    return out


def first_sentence(text, limit=72):
    text = re.sub(r"`", "", text)
    cut = text.split(". ")[0]
    return cut if len(cut) <= limit else cut[: limit - 1] + "…"


def render_report(now, work, done, net, open_issues, nxt):
    """The same numbers as a block a human can read, for pasting into a chat."""
    left = sum(1 for r in work if r[3].strip("`").lower() != "done")
    lines = [
        f"**{now.get('campaign', 'campaign')}** · {len(done)} closed · {left} left"
        f" · net production {net:+d} lines · {open_issues} found and not fixed",
        "",
        f"In flight: {first_sentence(now.get('current_item', 'none'))}"
        f" ({first_sentence(now.get('phase', '?'), 40)})",
    ]
    if done:
        lines += ["", "| Closed | Row | Verdict | Commit | Prod lines |", "|---|---|---|---|---|"]
        for r in done[:5]:
            lines.append(f"| {r[0]} | {r[1]} | {r[3]} | {r[4]} | {r[5]} |")
    if nxt:
        lines += ["", f"Next: {first_sentence(nxt[1] + ' ' + nxt[2])}"]
    return "\n".join(lines)


def main():
    check = "--check" in sys.argv
    report = "--report" in sys.argv
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
    print(f"next      {first_sentence(nxt[1] + ' ' + nxt[2]) if nxt else 'nothing queued'}")

    now = state_now()
    if now:
        print()
        print(f"campaign  {now.get('campaign', '?')}  ·  board updated {now.get('updated', '?')}")
        print(f"in flight {first_sentence(now.get('current_item', 'none'))}")
        print(f"          phase {first_sentence(now.get('phase', '?'), 60)}")
        if now.get("worker", "none").lower() != "none":
            print(f"          worker {first_sentence(now['worker'], 60)}")

    if done:
        print()
        print("closed, newest first")
        for r in done[:5]:
            print(f"  {r[0][:10]:<10} {r[1][:18]:<18} {r[3][:16]:<16} "
                  f"{r[4].strip('`')[:10]:<10} {r[5][:14]:>14}")

    if report:
        print()
        print(render_report(now, work, done, net, open_issues, nxt))

    if problems:
        print("\nmalformed:")
        for p in problems:
            print("  " + p)
    return 1 if (problems and check) else 0


if __name__ == "__main__":
    sys.exit(main())
