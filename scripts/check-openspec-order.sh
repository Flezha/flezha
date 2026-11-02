#!/usr/bin/env bash
# Fails when the order of the OpenSpec queue is recorded dishonestly, when a
# change was started while a change it names is still open, or when the spec
# tree or a proposal breaks the layout rules declared in openspec/config.yaml.
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
# Whole-queue mode also judges the layout, which keeps the spec tree grouped
# and growing by extension rather than by new folders. It refuses a folder in
# `openspec/specs/` whose name starts with no area declared in the `context`
# block of `openspec/config.yaml` (one `- <area>: ...` line each); a spec whose
# Purpose is still the placeholder the archive writes; and a bullet under
# `### New Capabilities` of an open proposal that names a capability outside
# every area, or carries no `Not in `<spec>`: <reason>` line naming an existing
# spec folder it could not extend. The tree checks read files a checkout
# carries, so they run before the empty-queue return and judge CI too.
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
# reached no verdict at all: more than one argument, an argument that is not an
# open change, which is what a mistyped id in a pick-up task looks like, or a
# config that declares no area.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - "$@" <<'PY'
import glob
import os
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import gate_support

CHANGES = gate_support.OPENSPEC_CHANGES
ARCHIVE = os.path.join(CHANGES, "archive")

DECLARATION = re.compile(r"^Depends-On:\s*(\S.*?)\s*$")
# A checked task is the one thing a worker writes before code, and the
# spec-driven schema records progress nowhere else.
CHECKED = re.compile(r"^\s*-\s*\[[xX]\]")

HINT = ("A change names its predecessors as `Depends-On: <id>[, <id>...]`, or "
        "`Depends-On: none`, on the first line of its proposal.md, and is "
        "picked up only once each one is archived.")

SPECS = gate_support.OPENSPEC_SPECS
CONFIG = "openspec/config.yaml"
AREA_LINE = re.compile(r"^  - ([a-z][a-z0-9-]*): ")
PLACEHOLDER = "TBD - created by archiving change"
NOT_IN = re.compile(r"Not in `([^`]+)`:\s*\S")

LAYOUT_HINT = ("A spec folder is named `<area>-<subject>` after an area declared in "
               f"{CONFIG}, its Purpose says what it covers, and a proposed new "
               "capability says which existing spec it could not extend as "
               "`Not in `<spec>`: <reason>`.")


def declared_areas():
    """The areas of config.yaml's `context` block, read by line so the gate
    needs no YAML library."""
    areas, inside = [], False
    for line in (gate_support.read_text(CONFIG) or "").splitlines():
        if line.startswith("context:"):
            inside = True
        elif inside and line.strip() and not line.startswith(" "):
            break
        elif inside and (match := AREA_LINE.match(line)):
            areas.append(match.group(1))
    return areas


def in_area(name, areas):
    return any(name == area or name.startswith(area + "-") for area in areas)


def purpose(name):
    text = gate_support.read_text(os.path.join(SPECS, name, "spec.md")) or ""
    return "\n".join(gate_support.sections(text).get("purpose", []))


def tree_refusals(specs, areas):
    refusals = []
    for name in specs:
        if not in_area(name, areas):
            refusals.append(f"{SPECS}/{name}/ starts with no declared area "
                            f"(declared: {', '.join(areas)})")
        if PLACEHOLDER in purpose(name):
            refusals.append(f"{SPECS}/{name}/spec.md: its Purpose is the archive "
                            "placeholder; write what the capability covers")
    return refusals


def new_capabilities(change_id):
    """Each bullet under `### New Capabilities`, continuation lines joined and
    comments dropped."""
    text = gate_support.read_text(os.path.join(CHANGES, change_id, "proposal.md")) or ""
    capabilities = "\n".join(gate_support.sections(text).get("capabilities", []))
    section = "\n".join(gate_support.sections(capabilities, 3).get("new capabilities", []))
    section = re.sub(r"<!--.*?-->", "", section, flags=re.S)
    bullets = []
    for line in section.splitlines():
        if line.startswith("- "):
            bullets.append(line[2:].strip())
        elif bullets and line.startswith(" ") and line.strip():
            bullets[-1] += " " + line.strip()
    return bullets


def proposal_refusals(change_id, areas, specs):
    refusals = []
    for bullet in new_capabilities(change_id):
        named = re.match(r"`([^`]+)`", bullet)
        capability = named.group(1) if named else bullet.split(":")[0].strip()
        if not in_area(capability, areas):
            refusals.append(f"{change_id} proposes new capability `{capability}`, "
                            "which starts with no declared area")
        reason = NOT_IN.search(bullet)
        if not reason:
            refusals.append(f"{change_id} proposes new capability `{capability}` "
                            "with no `Not in `<spec>`: <reason>` line")
        elif reason.group(1) not in specs:
            refusals.append(f"{change_id} proposes new capability `{capability}` as "
                            f"not in `{reason.group(1)}`, which is no folder in {SPECS}/")
    return refusals


def is_archived(change_id):
    return bool(glob.glob(os.path.join(ARCHIVE, f"????-??-??-{change_id}")))


def declared(change_id):
    """The ids a change names, [] for `none`, None when it carries no line."""
    text = gate_support.read_text(os.path.join(CHANGES, change_id, "proposal.md"))
    if text is None:
        return None
    match = DECLARATION.match(text.split("\n", 1)[0])
    if not match:
        return None
    if match.group(1) == "none":
        return []
    return [part.strip() for part in match.group(1).split(",") if part.strip()]


def is_started(change_id):
    """Whether any task is checked. A change with no `tasks.md` has none: what
    a change must carry is `openspec validate`'s question, not this gate's."""
    text = gate_support.read_text(os.path.join(CHANGES, change_id, "tasks.md")) or ""
    return any(CHECKED.match(line) for line in text.splitlines())


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
    """Whole-queue mode: is the record honest, has anyone jumped it, and do the
    tree and the proposals keep the layout?"""
    areas = declared_areas()
    if not areas:
        print(f"check-openspec-order.sh: {CONFIG} declares no spec area", file=sys.stderr)
        return gate_support.NO_ANSWER
    opened = gate_support.open_changes()
    specs = gate_support.subdirectories(SPECS)
    layout = tree_refusals(specs, areas)
    for change_id in opened:
        layout.extend(proposal_refusals(change_id, areas, specs))
    layout_code = gate_support.report(
        layout,
        headline="the openspec spec tree or a proposal breaks the layout rules:",
        hint=LAYOUT_HINT,
        noun="refusal(s)")
    if not opened:
        if layout_code == gate_support.OK:
            print(f"openspec order: {len(specs)} spec folder(s) read; no open change "
                  f"to judge ({CHANGES}/ is untracked, so a fresh checkout carries none)")
        return layout_code

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

    queue_code = gate_support.report(
        sorted(refusals),
        headline="the openspec queue's Depends-On record is wrong:",
        hint=HINT,
        noun="refusal(s)",
        ok_message=None if layout_code else
        f"openspec order OK ({len(specs)} spec folder(s), {len(opened)} open change(s) read)")
    return max(layout_code, queue_code)


def judge_pickup(change_id):
    """Pick-up mode: may this change be started now?"""
    opened = gate_support.open_changes()
    if change_id not in opened:
        print(f"check-openspec-order.sh: {change_id} is not an open change under "
              f"{CHANGES}/", file=sys.stderr)
        return gate_support.NO_ANSWER

    names = declared(change_id)
    refusals = record_refusals(change_id, names, opened)
    refusals.extend(f"`{dep}` is still open; it must be archived before "
                    f"{change_id} is picked up"
                    for dep in names or () if dep != change_id and dep in opened)
    return gate_support.report(
        refusals,
        headline=f"{change_id} cannot be picked up yet:",
        hint=HINT,
        noun="refusal(s)")


argv = sys.argv[1:]
if len(argv) > 1:
    print("usage: check-openspec-order.sh [<change-id>]", file=sys.stderr)
    sys.exit(gate_support.NO_ANSWER)
sys.exit(judge_pickup(argv[0]) if argv else judge_queue())
PY
