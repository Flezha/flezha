#!/usr/bin/env bash
# Fails when the order of the OpenSpec queue is recorded dishonestly, or when a
# change was started while a change it names is still open.
#
# Several changes in `openspec/changes/` can only be implemented in one order:
# one settles a decision another needs, or rewrites the lines another is about
# to move. OpenSpec 1.2.0 has no dependency field, so the fact lives on the
# first line of each `proposal.md`:
#
#   Depends-On: <change-id>[, <change-id>...]
#   Depends-On: none
#
# The CLI's parser reads `## ` sections, so a line above the first heading
# belongs to no section: it is neither validated nor lost, and
# `openspec validate --strict` passes with it in place.
#
# Two modes, because two questions are asked.
#
#   check-openspec-order.sh            is the record honest?
#   check-openspec-order.sh <id>       may this change be picked up now?
#
# Whole-queue mode refuses five shapes: no line at all; a named id that resolves
# to neither an open change nor an archived one, which is a typo or a change
# someone deleted; a change naming itself, which is how a copied template fails;
# a cycle among open changes, which is a queue that can never drain; and a
# change that has been STARTED (any `- [x]` in its `tasks.md`) while a change
# it names is still open. A change that merely WAITS behind an open predecessor
# is accepted: a queue is allowed to hold work that is not ready yet, and a gate
# that reddened on that would be red from the day it landed.
#
# Pick-up mode asks the other question, and is the first task of every change
# that names a predecessor. It refuses while any named predecessor is still an
# open directory, naming each, so a change blocked by two learns about both. A
# cycle needs no separate message here: every change on one has an open
# predecessor by construction, so it is refused anyway.
#
# An id resolves as archived through `archive/????-??-??-<id>`, because the
# archive prefixes every directory with the day it was archived and a bare
# `archive/<id>` is never there. The glob is anchored both sides, so an id that
# is the suffix of a longer id cannot match it.
#
# SCOPE IS DISK, not `git ls-files`: `/openspec/changes` is in `.gitignore`, so
# the queue is untracked by policy and an index walk would see none of it. The
# consequence is worth stating rather than discovering: on a CI checkout the
# directory is absent, so the gate reads an empty queue and says so instead of
# reporting a silent pass. Where it bites is a developer's tree and the
# `--run @naming` run made before an archive, which is where the queue exists.
#
# No baseline file: unlike the naming gates this one has no grandfathered
# population, because every open change carries the line.
#
# Takes an optional change id. Exit 0 clean, 1 on a refusal, and 3 when the gate
# reached no verdict at all: more than one argument, or an argument that is not
# an open change, which is what a mistyped id in a pick-up task looks like.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - "$@" <<'PY'
import glob
import os
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

CHANGES = "openspec/changes"
ARCHIVE = os.path.join(CHANGES, "archive")

DECLARATION = re.compile(r"^Depends-On:\s*(\S.*?)\s*$")
# A checked task is the one thing a worker writes before code, and the
# spec-driven schema records progress nowhere else.
CHECKED = re.compile(r"^\s*-\s*\[[xX]\]")

HINT = ("A change names its predecessors as `Depends-On: <id>[, <id>...]`, or "
        "`Depends-On: none`, on the first line of its proposal.md, and is "
        "picked up only once each one is archived.")


def open_changes():
    """Every change directory in the queue, the archive aside."""
    if not os.path.isdir(CHANGES):
        return []
    return sorted(name for name in os.listdir(CHANGES)
                  if name != "archive" and os.path.isdir(os.path.join(CHANGES, name)))


def is_archived(change_id):
    return bool(glob.glob(os.path.join(ARCHIVE, f"????-??-??-{change_id}")))


def declared(change_id):
    """The ids a change names, [] for `none`, None when it carries no line."""
    path = os.path.join(CHANGES, change_id, "proposal.md")
    try:
        with open(path, encoding="utf-8") as fh:
            first = fh.readline().rstrip("\n")
    except OSError:
        return None
    match = DECLARATION.match(first)
    if not match:
        return None
    if match.group(1) == "none":
        return []
    return [part.strip() for part in match.group(1).split(",") if part.strip()]


def is_started(change_id):
    """Whether any task is checked. A change with no `tasks.md` has none: what
    a change must carry is `openspec validate`'s question, not this gate's."""
    path = os.path.join(CHANGES, change_id, "tasks.md")
    try:
        with open(path, encoding="utf-8") as fh:
            return any(CHECKED.match(line) for line in fh)
    except OSError:
        return False


def record_refusals(change_id, names, opened):
    """What is wrong with one change's own record, whatever the queue does."""
    if names is None:
        return [f"{CHANGES}/{change_id}/proposal.md: no `Depends-On:` first line; "
                "write `Depends-On: none` or a comma-separated list of change ids"]
    refusals = []
    for dep in names:
        if dep == change_id:
            refusals.append(f"{change_id} names itself; a change cannot depend on itself")
        elif dep not in opened and not is_archived(dep):
            refusals.append(f"{change_id} names `{dep}`, which is neither an open "
                            "change nor an archived one")
    return refusals


def rings(edges):
    """Every cycle among open changes, each reported once.

    A cycle would otherwise be read as "still open" for ever, with nothing
    saying why. Self-edges are excluded by the caller: naming yourself has its
    own message.
    """
    found = {}

    def descend(node, path):
        for dep in edges.get(node, ()):
            if dep in path:
                ring = path[path.index(dep):]
                found.setdefault(tuple(sorted(ring)), ring)
            else:
                descend(dep, path + [dep])

    for node in sorted(edges):
        descend(node, [node])
    return [found[key] for key in sorted(found)]


def judge_queue():
    """Whole-queue mode: is the record honest, and has anyone jumped it?"""
    opened = open_changes()
    if not opened:
        print(f"openspec order: no open change to judge ({CHANGES}/ is untracked, "
              "so a fresh checkout carries none)")
        return naming_gate.OK

    names_of = {change_id: declared(change_id) for change_id in opened}
    refusals = []
    edges = {}
    for change_id in opened:
        refusals.extend(record_refusals(change_id, names_of[change_id], opened))
        edges[change_id] = [dep for dep in names_of[change_id] or ()
                            if dep != change_id and dep in opened]
    for ring in rings(edges):
        refusals.append("cycle among open changes: " + " -> ".join(ring + [ring[0]]))
    for change_id in opened:
        if not is_started(change_id):
            continue
        for dep in edges[change_id]:
            refusals.append(f"{change_id} has a checked task while `{dep}` is still "
                            f"open; `{dep}` must be archived first")

    return naming_gate.report(
        sorted(refusals),
        headline="the openspec queue's Depends-On record is wrong:",
        hint=HINT,
        noun="refusal(s)",
        ok_message=f"openspec order OK ({len(opened)} open change(s) read)")


def judge_pickup(change_id):
    """Pick-up mode: may this change be started now?"""
    opened = open_changes()
    if change_id not in opened:
        print(f"check-openspec-order.sh: {change_id} is not an open change under "
              f"{CHANGES}/", file=sys.stderr)
        return naming_gate.NO_ANSWER

    names = declared(change_id)
    refusals = record_refusals(change_id, names, opened)
    refusals.extend(f"`{dep}` is still open; it must be archived before "
                    f"{change_id} is picked up"
                    for dep in names or () if dep != change_id and dep in opened)
    return naming_gate.report(
        refusals,
        headline=f"{change_id} cannot be picked up yet:",
        hint=HINT,
        noun="refusal(s)")


argv = sys.argv[1:]
if len(argv) > 1:
    print("usage: check-openspec-order.sh [<change-id>]", file=sys.stderr)
    sys.exit(naming_gate.NO_ANSWER)
sys.exit(judge_pickup(argv[0]) if argv else judge_queue())
PY
