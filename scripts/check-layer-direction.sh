#!/usr/bin/env bash
# Layer-direction gate: a lower directory under lib/flezha/ never names a
# higher one, and every module lives in a directory.
#
# Charter §3 gives the controller one dependency order — transport -> runner ->
# relay -> state / plugin -> service -> http / healthz, with observability,
# authz and convergence as cross-cutting consumers no layer below service
# names. Nothing enforced it: a cycle count says a cycle exists and never says
# which way an edge points, and no test asserts direction at all.
#
# WHAT AN EDGE IS. One (file, module) pair: a file under lib/flezha/<dir>/ that
# names a module whose own defining file lives under a higher directory. The
# pair is the unit, not the line, so moving a reference inside a file or adding
# a second call to a module the file already names is not a new finding — the
# edge is what the layering rule is about, and a line-keyed ledger would go red
# on edits that change no structure.
#
# WHY THE DIRECTORY AND NOT THE NAME. A module's layer is read from where its
# file lives, never from its name. Elixir module names are durable state here —
# thirteen of them are pinned by on-disk data (`fix-opaque-module-names`), so
# the restructuring ahead moves files and renames nothing. Read by name,
# `Flezha.Service.HubManager` stays "service" wherever its file goes and this
# gate's count could never move; a whole-tree move simulation over the planned
# restructuring returned the identical number. Read by directory, moving a file
# into the directory that owns its feature retires its entries here, which is
# the number the restructuring is measured by.
#
# A reference that resolves to no file in this tree — a registered process name
# like `Flezha.Relay.MessageHandlerTaskSup`, a bare namespace prefix — falls
# back to its first segment read as a directory name, and a name whose segment
# is no directory here (the Elixir SDK's `Flezha.SDK.*`, a dependency) is no
# edge. That is the only meaning such a name has,
# and skipping it would hide real edges. The fallback never applies to a module
# that has a file, so a moved file is judged by its new home.
#
# LOOSE FILES. A module in `lib/flezha/*.ex` is in no directory, so no rule can
# place it and no directory owns its feature. Each loose file is one finding of
# its own, retired when the file moves into a directory.
#
# DOC AND COMMENT LINES ARE NOT EDGES. A moduledoc that explains which manager
# a module talks to is prose, not a dependency; removing the alias and leaving
# the sentence is not progress and the gate should not reward it. Comments and
# `@doc`/`@moduledoc`/`@typedoc` heredocs are stripped before the scan.
#
# THE BASELINE, scripts/layer-direction-baseline.txt, lists what the tree held
# when the gate landed. They are the debt, written down rather than absorbed.
# The gate fails on:
#
#   * an edge or loose file not in the baseline — the new breach the rule is
#     about, which is what makes the gate blocking on a tree that is red today;
#   * a baseline entry the tree no longer holds — the list may only shrink, so
#     a cut edge comes off it and the count is the restructuring's own number;
#   * a directory under lib/flezha/ that the layer table does not rank, which
#     is exit 3, the gate with no answer. A new directory has to declare where
#     it sits before it can be judged, so the table cannot rot behind the tree.
#
# SOURCES ARE READ FROM DISK, not from `git ls-files`. An untracked module has
# existed under lib/flezha/ here before (`trace_tmp.ex`), and a walk that reads
# the index alone would have called that tree clean. A blind walk always
# flatters.
#
# `--write-baseline` records the current tree. It is for the change that lands
# a deliberate structural move, and like every recording in this repository it
# is a judgement, not a reset: what it absorbs is exactly what the gate exists
# to report. Exit 3 means the gate reached no verdict rather than the tree being
# dirty; `scripts/run_gates.py` documents the four codes.
#
# `--self-test` runs the whole judgement over a throwaway tree with known
# answers, including the one claim the gate is built on: that moving a file
# into the directory that owns its feature retires its entry. A gate whose
# count cannot move is one nobody can work down, and asserting that in prose
# is not the same as running it.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - "$@" <<'PY'
import os
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import gate_support

LIB = "lib/flezha"
BASELINE = "scripts/layer-direction-baseline.txt"
BASELINE_HEADER = """\
# Upward references and unplaced modules under lib/flezha/ when
# scripts/check-layer-direction.sh recorded this tree. Recorded debt, not an
# exemption: the gate fails on any entry outside this list, and on any entry
# here the tree no longer holds.
#
# The list may only shrink. Close an entry by moving the file into the
# directory that owns its feature, or by turning the upward call into a
# downward one, then delete the line.
#
# One `<file> -> <module>` per line, sorted; `(no directory owns it)` is a
# module still loose at the top of lib/flezha/.
"""

# Charter §3's order is the spine (1 transport .. 6 the outermost consumers);
# every other directory is placed against it, and a directory absent here is
# refused rather than guessed. One reason per entry, because a rank nobody can
# argue with is a rank nobody wrote down.
LAYERS = {
    # 0 — foundation: named by anyone, naming nothing above. Neither directory
    # holds a single outbound `Flezha.` name, which is the property that keeps
    # them foundations rather than a second junk drawer.
    "config": 0,  # the key table, durations, constants
    "layout": 0,  # the subtree table: where a file lands, and who clears it
    "process": 0,  # the shared process primitives: bounded call, concurrency, names
    "error": 0,  # the error model every surface answers a failure in, from transport up
    # 1 — the machine underneath: framing, sockets, certificates, and the
    # cluster's own membership. Read by everything, reaching for nothing.
    "transport": 1,
    "authn": 1,  # the TLS material transport presents
    "cluster": 1,  # node discovery and the `:pg` directory every layer reads
    # 2 — a connected runner and its channels.
    "runner": 2,
    # 3 — a relay running on a runner.
    "relay": 3,
    # 4 — what a relay durably keeps, and what it forwards to.
    "state": 4,
    "plugin": 4,
    "dlq": 4,  # the plugin path's parking store
    # 5 — node-wide services, and the cross-cutting three the charter names as
    # consumers no layer below service may reach for.
    "service": 5,
    "schedule": 5,  # a service that owns its own hub
    "authz": 5,
    "convergence": 5,
    "observability": 5,
    "logs": 5,  # the durable logs: a relay's history and the audit trail the bus feeds
    # 6 — the outermost: the user and network surfaces, and the node's own
    # bring-up and tear-down. Boot starts the services and shutdown stops them,
    # so both name every layer and none may name them.
    "boot": 6,
    "shutdown": 6,
    "cli": 6,  # the operations surface, carved out of service/
    "http": 6,
    "mcp": 6,  # the MCP surface `flz mcp` and the HTTP API both serve
    "healthz": 6,
}

# One directory can hold two layers, and `http/` does. `Flezha.Http.Client` is
# an outbound request over `:mint` that names nothing and any layer may call;
# `Flezha.Http.Listener` is the inbound surface at the top. Ranking the
# directory alone would make the S3 client's and the OTLP exporter's calls
# permanent debt nobody could ever close, which is how a gate teaches people to
# stop reading it. One entry per module, and only where the directory's rank is
# wrong for that module.
#
# The rank is read on both sides — the module a file names and the module the
# file defines — because a feature directory holds its whole feature. A
# service API filed with its feature takes a caller context, binds an AuthZ
# verb and answers data; it is at service height wherever its file sits, and
# ranking it by the directory would record its every call to AuthZ, to a
# manager or to the state layer as debt nobody could close.
MODULE_LAYERS = {
    "Flezha.Http.Client": 0,
    # signed S3 requests over the client, naming only it and the knob table
    "Flezha.Http.S3": 0,
    "Flezha.Service.Dlq": 5,
    # the runner drop marks: a store replicated with the authz stores
    "Flezha.Runner.DropMarks": 5,
    "Flezha.Service.Steps": 5,
    # the cell-frame codec the runner channels decode with, naming only the
    # record header beside it
    "Flezha.State.Wire": 1,
    # a compile-time slot table that names nothing; transport counts with it
    "Flezha.Observability.CounterTable": 0,
    # the checked words of a signal, naming nothing; a plugin's own telemetry
    # verbs are checked with it exactly as a core source's signals are
    "Flezha.Observability.SignalWords": 0,
}

DEFMODULE = re.compile(r"^\s*defmodule\s+([A-Z][\w.]*)\s+do", re.M)
# `Flezha.A.B` — dotted segments while they are capitalised, so a call's
# lower-case function name is not swallowed.
REF = re.compile(r"\bFlezha(?:\.[A-Z][A-Za-z0-9_]*)+")
# `alias Flezha.State.{Paths, Wire}` names two modules on one line and may wrap
# over several; expanded before the scan so neither is invisible.
MULTI = re.compile(r"\b(Flezha(?:\.[A-Z][A-Za-z0-9_]*)*)\.\{([^{}]*)\}", re.S)
# Any doc attribute, heredoc or one-line string — both are prose.
DOC_LINE = re.compile(r"^\s*@(?:module|type|)doc\b")


def source_files():
    return [rel for rel in gate_support.disk_files([LIB]) if rel.endswith(".ex")]


def directory_of(rel):
    """The directory that owns a file, or None for a loose `lib/flezha/*.ex`."""
    parts = rel.split("/")
    return parts[2] if len(parts) > 3 else None


def strip_prose(text):
    """Drop `@doc` heredocs and `#` comments — prose that names a module does
    not depend on it. A `#` inside a string literal is left alone by counting
    the quotes before it."""
    out = []
    in_doc = False
    for line in text.split("\n"):
        if in_doc:
            if '"""' in line:
                in_doc = False
            continue
        if DOC_LINE.match(line):
            # A heredoc runs on unless it also closes on this line; a one-line
            # `@doc "..."` is over here.
            if line.count('"""') == 1:
                in_doc = True
            continue
        cut = -1
        quotes = 0
        for i, ch in enumerate(line):
            if ch == '"' and (i == 0 or line[i - 1] != "\\"):
                quotes += 1
            elif ch == "#" and quotes % 2 == 0:
                cut = i
                break
        out.append(line if cut < 0 else line[:cut])
    return "\n".join(out)


def expand_multi(text):
    def repl(match):
        head, inner = match.group(1), match.group(2)
        return " ".join(
            f"{head}.{part.strip()}" for part in inner.split(",") if part.strip()
        )

    return MULTI.sub(repl, text)


class Broken(Exception):
    """The gate cannot reach a verdict, which is exit 3 and never a clean tree."""


def census():
    """Every finding in the tree."""
    files = source_files()
    if not files:
        raise Broken(f"no Elixir sources under {LIB}/ — the gate cannot judge")

    unranked = sorted(
        {d for rel in files if (d := directory_of(rel)) and d not in LAYERS}
    )
    if unranked:
        raise Broken(
            "a directory under lib/flezha/ has no layer:\n\n"
            + "".join(f"  {d}\n" for d in unranked)
            + "\nRank it in scripts/check-layer-direction.sh — a directory the "
            "table does not name cannot be judged."
        )

    # Module -> the file that defines it. Built before the scan so a reference
    # is resolved to a directory rather than to its own name.
    home = {}
    bodies = {}
    defines = {}
    for rel in files:
        with open(rel, encoding="utf-8") as fh:
            text = fh.read()
        bodies[rel] = expand_multi(strip_prose(text))
        names = DEFMODULE.findall(text)
        defines[rel] = names[0] if names else ""
        for name in names:
            home.setdefault(name, rel)

    def rank_named(ref, fallback):
        """The rank a reference is judged at: the module's own where the table
        overrides its directory, else the directory's."""
        for name, layer in MODULE_LAYERS.items():
            if ref == name or ref.startswith(name + "."):
                return layer
        return fallback

    def layer_named(ref):
        """The directory a reference points at: its defining file's, else the
        first segment of its name read as a directory."""
        parts = ref.split(".")
        while len(parts) > 1:
            rel = home.get(".".join(parts))
            if rel:
                return directory_of(rel)
            parts.pop()
        segment = ref.split(".")[1].lower() if ref.count(".") else ""
        return segment if segment in LAYERS else None

    found = set()
    for rel in files:
        owner = directory_of(rel)
        if owner is None:
            found.add(f"{rel} -> (no directory owns it)")
            continue
        rank = rank_named(defines[rel], LAYERS[owner])
        for ref in set(REF.findall(bodies[rel])):
            target = layer_named(ref)
            if target is None or target == owner:
                continue
            if rank_named(ref, LAYERS[target]) > rank:
                found.add(f"{rel} -> {ref}")
    return found


def main(args):
    if args == ["--self-test"]:
        return self_test()
    write = args == ["--write-baseline"]
    if args and not write:
        print(
            "usage: check-layer-direction.sh [--write-baseline | --self-test]",
            file=sys.stderr,
        )
        return gate_support.NO_ANSWER

    try:
        found = census()
    except Broken as broken:
        print(broken, file=sys.stderr)
        return gate_support.NO_ANSWER

    if write:
        with open(BASELINE, "w", encoding="utf-8") as fh:
            fh.write(BASELINE_HEADER)
            for entry in sorted(found):
                fh.write(entry + "\n")
        print(f"recorded {len(found)} entrie(s) in {BASELINE}")
        return 0

    try:
        recorded = gate_support.load_baseline(BASELINE)
    except OSError as exc:
        print(f"cannot read {BASELINE}, so nothing can be judged: {exc}", file=sys.stderr)
        return gate_support.NO_ANSWER

    new = sorted(found - recorded)
    closed = sorted(recorded - found)
    if new:
        print(
            "a lower directory names a higher one (charter §3), and this is not "
            "in the recorded debt:\n",
            file=sys.stderr,
        )
        for entry in new:
            print(f"  {entry}", file=sys.stderr)
    if closed:
        print(
            "\nthese entries are recorded debt the tree no longer holds — the "
            "list may only shrink, so delete them:\n",
            file=sys.stderr,
        )
        for entry in closed:
            print(f"  {entry}", file=sys.stderr)
    if new or closed:
        print(
            f"\n{len(new)} new, {len(closed)} stale, {len(recorded)} recorded. "
            f"See scripts/check-layer-direction.sh.",
            file=sys.stderr,
        )
        return 1

    print(f"layer direction OK — {len(recorded)} recorded, 0 new")
    return 0


def self_test():
    """Run the judgement over a throwaway tree whose answers are known.

    The cases the gate stands on: prose is not an edge, a multi-alias names
    every module in its braces, a reference with no defining file is still
    placed by its name, a downward call is not a finding, a loose module is
    one, an unranked directory is the gate broken — and moving a file into the
    directory that owns its feature retires its entry.
    """
    import shutil

    tree = {
        # A file in a low directory reaching up, in four spellings, and one
        # downward call that must stay silent.
        "lib/flezha/relay/mover.ex": '''
defmodule Flezha.Relay.Mover do
  @moduledoc """
  Talks to Flezha.Service.Prose, which is a sentence and not an edge.
  """
  # Flezha.Service.Comment is a comment and not an edge either.
  @doc "So is Flezha.Service.OneLiner, written without a heredoc."
  alias Flezha.Service.Manager
  alias Flezha.State.{Paths, Store}
  alias Flezha.Transport.Server

  def go, do: {Manager, Paths, Store, Server, Flezha.Service.NamedProcess}
end
''',
        # The higher layer naming a lower one is the correct direction.
        "lib/flezha/service/manager.ex": """
defmodule Flezha.Service.Manager do
  def go, do: Flezha.Relay.Mover
end
""",
        "lib/flezha/state/paths.ex": """
defmodule Flezha.State.Paths do
  # One directory, two layers: the outbound client is foundation and the
  # inbound listener is the top, so only the second is a finding.
  def go, do: {Flezha.Http.Client, Flezha.Http.Listener}
end
""",
        # A service API filed with its feature: the directory is layer 4, the
        # module MODULE_LAYERS ranks 5, and the call up to a manager is
        # therefore not a finding. Delete the entry and this line becomes one.
        "lib/flezha/dlq/dlq.ex": """
defmodule Flezha.Service.Dlq do
  def go, do: Flezha.Service.Manager
end
""",
        "lib/flezha/state/store.ex": "defmodule Flezha.State.Store do\nend\n",
        "lib/flezha/http/client.ex": "defmodule Flezha.Http.Client do\nend\n",
        "lib/flezha/http/listener.ex": "defmodule Flezha.Http.Listener do\nend\n",
        "lib/flezha/transport/server.ex": "defmodule Flezha.Transport.Server do\nend\n",
        "lib/flezha/loose.ex": "defmodule Flezha.Loose do\nend\n",
    }
    expected = {
        "lib/flezha/loose.ex -> (no directory owns it)",
        "lib/flezha/relay/mover.ex -> Flezha.Service.Manager",
        "lib/flezha/relay/mover.ex -> Flezha.State.Paths",
        "lib/flezha/relay/mover.ex -> Flezha.State.Store",
        # No file defines it, so its name places it — a registered process name
        # is still a reach into the service layer.
        "lib/flezha/relay/mover.ex -> Flezha.Service.NamedProcess",
        # `Flezha.Http.Client` is not here: MODULE_LAYERS ranks it foundation
        # even though its file sits in the top layer's directory.
        "lib/flezha/state/paths.ex -> Flezha.Http.Listener",
    }

    failures = []
    with gate_support.scratch_tree(tree):
        os.makedirs("scripts", exist_ok=True)
        found = census()
        if found != expected:
            failures.append(f"census: missing {expected - found}, extra {found - expected}")

        # The move claim: the module keeps its name, its file changes directory,
        # and its two state edges retire. Nothing else about the tree changes.
        os.rename("lib/flezha/relay/mover.ex", "lib/flezha/state/mover.ex")
        moved = census()
        want = {
            "lib/flezha/loose.ex -> (no directory owns it)",
            "lib/flezha/state/mover.ex -> Flezha.Service.Manager",
            "lib/flezha/state/mover.ex -> Flezha.Service.NamedProcess",
            "lib/flezha/state/paths.ex -> Flezha.Http.Listener",
        }
        if moved != want:
            failures.append(f"after the move: missing {want - moved}, extra {moved - want}")
        if not len(moved) < len(found):
            failures.append(f"the move did not lower the count: {len(found)} -> {len(moved)}")
        os.rename("lib/flezha/state/mover.ex", "lib/flezha/relay/mover.ex")

        # An unranked directory is the gate broken, not a clean tree.
        os.makedirs("lib/flezha/unranked")
        with open("lib/flezha/unranked/x.ex", "w", encoding="utf-8") as fh:
            fh.write("defmodule Flezha.Unranked.X do\nend\n")
        try:
            census()
            failures.append("an unranked directory was judged instead of refused")
        except Broken:
            pass
        shutil.rmtree("lib/flezha/unranked")

        # The baseline's two failure modes, through main() and its exit code.
        if main(["--write-baseline"]) != 0 or not os.path.exists(BASELINE):
            failures.append("--write-baseline did not record")
        if main([]) != 0:
            failures.append("a tree that matches its baseline was not clean")
        with open("lib/flezha/state/paths.ex", "a", encoding="utf-8") as fh:
            fh.write("defmodule Flezha.State.Extra do\n  def go, do: Flezha.Service.Manager\nend\n")
        with open("lib/flezha/transport/server.ex", "a", encoding="utf-8") as fh:
            fh.write("defmodule Flezha.Transport.New do\n  def go, do: Flezha.Runner.Thing\nend\n")
        if main([]) != 1:
            failures.append("a new upward reference did not fail the gate")
        with open(BASELINE, "a", encoding="utf-8") as fh:
            fh.write("lib/flezha/state/paths.ex -> Flezha.Service.Manager\n")
            fh.write("lib/flezha/transport/server.ex -> Flezha.Runner.Thing\n")
            fh.write("lib/flezha/gone.ex -> (no directory owns it)\n")
        if main([]) != 1:
            failures.append("a baseline entry the tree no longer holds did not fail the gate")

    return gate_support.self_test_verdict("check-layer-direction",
                                         [f"  {line}" for line in failures])
sys.exit(main(sys.argv[1:]))
PY
