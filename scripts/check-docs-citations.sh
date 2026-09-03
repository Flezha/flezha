#!/usr/bin/env bash
# Fails when documentation cites a name this repository no longer carries.
#
# Documentation points at code: a module, a function, a file, a `file:line`
# coordinate, an environment variable, a telemetry event. Code moves and the
# pointer stays, so the citation rots in silence — `mix compile` never reads a
# document, and neither does any suite. This gate is the layer that does.
#
# It checks only what has a decidable answer in the tree. A prose claim about
# how something behaves is out of scope by design: a gate with false positives
# gets turned off, and the repository has already paid for one that went red on
# correct prose.
#
# Five citation kinds, each judged against the tracked tree:
#
#   module     `Flezha.A.B` — live when the tracked Elixir source names it, or
#              names something below it. `#{__MODULE__}` is resolved first, so a
#              process name composed at runtime counts as named.
#   function   `Flezha.A.B.fun/2` — live when `fun` is defined in the file that
#              defines `Flezha.A.B`. Arity is not checked: defaults make one
#              clause answer several arities.
#   path       a code-spanned or linked path, with an optional `:line` — live
#              when the repository tracks it, when it resolves relative to the
#              citing document, or when a tracked path ends with it (an SDK's
#              own root, `.../` elision). `:line` must be a line the file has.
#   env var    `FLEZHA_*` — live when any tracked non-document file names it.
#   telemetry  `[:flezha, …]` — live when the source carries the literal, or
#              carries the prefix and the final atom, which is how an event
#              assembled from a variable is written.
#
# WHAT IS NOT JUDGED, and why each is a shape rather than a list of instances:
#
#   A citation with no directory component (`poll_core.md`, `mod.rs`) is
#   shorthand for a file named elsewhere in the sentence, not a coordinate.
#
#   A path rooted where the repository carries nothing — `deps/`,
#   `flezha_market/`, `META-INF/` — cannot be checked against this tree. So can
#   a path the repository ignores by policy (`openspec/changes/`,
#   `docs/dev_notes/`): those directories are working material kept out of git,
#   and a citation into one is unresolvable here rather than wrong.
#
#   `openspec/specs/` is NORMATIVE. A requirement states what SHALL hold, which
#   may name a helper nobody has written yet — `cli/src/utils.rs (or
#   equivalent)` — or an environment variable the code still owes. Reading a
#   mandate as a citation of present fact fails correct, unimplemented
#   requirements, and conflates spec drift (its own workflow: measure which side
#   is wrong, then amend) with documentation rot.
#
#   A changelog is a dated record. Its removal entries necessarily name what no
#   longer exists, every release adds more, and the entry is anchored to the
#   release that wrote it — the history that is never edited.
#
# Deliberate mentions inside a document in scope take a
# `docs-citation-record:begin` / `:end` marker pair, which exempts the lines it
# delimits and no others. Use it for a name this repository does not carry and
# is naming on purpose: a removal quoted as a record, or a coordinate that
# belongs to another repository. What it is NOT for is quieting a live pointer.
#
# scripts/docs-citation-allowlist.txt masks shapes that are not citations at
# all. Prefer widening this script over adding entries there: entries make the
# gate quieter, the script makes it stronger.
#
# Scope is `git ls-files`, so .git/, target/, _build/, openspec/changes/ and the
# repository's untracked working documents are excluded by being untracked or
# ignored. A file added in the same change is invisible until it is staged, so a
# document citing one reads as dead until `git add` puts it in the index.
# Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import os
import re
import subprocess
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

ALLOWLIST = "scripts/docs-citation-allowlist.txt"
SELF_FILES = {ALLOWLIST, "scripts/check-docs-citations.sh", ".github/workflows/naming.yml"}

TRACKED = set(naming_gate.tracked_files())
# Every tail of every tracked path, so a citation written from an SDK's own root
# (`lib/flezha/entry_idle.rb`) or with a `.../` elision resolves without the
# gate having to know which roots this repository nests.
SUFFIXES = set()
for _path in TRACKED:
    _parts = _path.split("/")
    for _i in range(len(_parts)):
        SUFFIXES.add("/".join(_parts[_i:]))
TOP_LEVEL = {p.split("/", 1)[0] for p in TRACKED}

SOURCE_EXT_SKIP = {".md"}
NAME = re.compile(r"\bFlezha(?:\.[A-Z][A-Za-z0-9_]*)+")
DEFMODULE = re.compile(r"^\s*defmodule\s+([A-Za-z0-9_.]+)")
DEF = re.compile(r"^\s*(?:def|defp|defmacro|defmacrop|defdelegate)\s+([a-z_][A-Za-z0-9_?!]*)")
ATOM = re.compile(r":([a-z_][A-Za-z0-9_]*)")
ENV = re.compile(r"\bFLEZHA_[A-Z0-9_]+\b")
EVENT = re.compile(r"\[\s*:flezha(?:\s*,\s*:?[A-Za-z0-9_]+)*\s*\]")

elixir_names = set()      # every `Flezha.…` the source names
module_file = {}          # module -> the file whose `defmodule` opens it
module_defs = {}          # module -> the names that file defines
source_atoms = set()      # every `:atom` the non-document source writes
source_envs = set()       # every `FLEZHA_*` the non-document source writes
source_events = set()     # every `[:flezha, …]` literal, whitespace removed


def squash(text):
    return re.sub(r"\s+", "", text)


for rel in TRACKED:
    if os.path.splitext(rel)[1] in SOURCE_EXT_SKIP or os.path.splitext(rel)[1] in naming_gate.BINARY_EXT:
        continue
    try:
        with open(rel, encoding="utf-8") as fh:
            text = fh.read()
    except (UnicodeDecodeError, OSError):
        continue
    source_atoms.update(ATOM.findall(text))
    source_envs.update(ENV.findall(text))
    source_events.update(squash(m) for m in EVENT.findall(text))
    if not rel.endswith((".ex", ".exs")):
        continue
    # A stack of (indentation, module), because a nested `defmodule Child` owns
    # the definitions indented under it and its siblings belong to the parent.
    open_modules = []
    for line in text.split("\n"):
        indent = len(line) - len(line.lstrip())
        opened = DEFMODULE.match(line)
        if opened:
            while open_modules and open_modules[-1][0] >= indent:
                open_modules.pop()
            name = opened.group(1)
            if open_modules and not name.startswith("Flezha."):
                name = f"{open_modules[-1][1]}.{name}"
            open_modules.append((indent, name))
            module_file.setdefault(name, rel)
            module_defs.setdefault(name, set())
        elif open_modules:
            defined = DEF.match(line)
            if defined:
                while len(open_modules) > 1 and open_modules[-1][0] >= indent:
                    open_modules.pop()
                module_defs[open_modules[-1][1]].add(defined.group(1))
            # A name spelled from `__MODULE__` is still a name this file declares.
            line = line.replace("#{__MODULE__}", open_modules[-1][1])
        elixir_names.update(NAME.findall(line))

# The prefix of every source event, so `[:flezha, :state, :wal_pulled]` is
# recognised where the source writes `:telemetry.execute([:flezha, :state, event])`.
event_prefixes = {e[: e.rindex(",") + 1] for e in source_events if "," in e}

EXT = ("ex|exs|eex|heex|erl|rs|java|py|pyi|rb|php|js|mjs|cjs|ts|sh|bat|toml|yml|yaml"
       "|json|proto|xml|gradle|lock|txt|md|hbs|sql|args|env")
SPANNED = re.compile(r"`([A-Za-z0-9_.][A-Za-z0-9_./+-]*\.(?:" + EXT + r"))(?::(\d+))?(?:-\d+)?`")
LINKED = re.compile(r"\]\((?!https?:)([A-Za-z0-9_.][A-Za-z0-9_./+-]*\.(?:" + EXT + r"))(?:#[^)]*)?\)")
FUNCTION = re.compile(r"\b(Flezha(?:\.[A-Z][A-Za-z0-9_]*)+)\.([a-z_][A-Za-z0-9_?!]*)/\d+")

line_counts = {}
ignored = {}
# A gate's green is only evidence if it can say how much it read, so the run
# reports what it judged beside its verdict.
tally = {"citations": 0, "documents": set()}


def repo_ignores(path):
    """Whether git would refuse to track this path. A citation into a directory
    the repository keeps out of version control has no answer in this tree."""
    if path not in ignored:
        ignored[path] = subprocess.run(
            ["git", "check-ignore", "-q", "--no-index", path]
        ).returncode == 0
    return ignored[path]


def line_count(path):
    if path not in line_counts:
        with open(path, encoding="utf-8", errors="replace") as fh:
            line_counts[path] = sum(1 for _ in fh)
    return line_counts[path]


def resolve(rel, cited):
    """The tracked path a citation names, or None."""
    if "..." in cited:
        # `flezha-sdk-runtime/.../core/ActivationDispatcher.java` — the head is
        # elided, so only the tail can be resolved.
        tail = cited.rsplit("...", 1)[1].lstrip("/")
        return tail if tail and tail in SUFFIXES else None
    if cited in TRACKED:
        return cited
    relative = os.path.normpath(os.path.join(os.path.dirname(rel), cited))
    if relative in TRACKED:
        return relative
    return cited if cited in SUFFIXES else None


def unjudgeable(cited):
    """A citation the tree cannot answer either way."""
    if "/" not in cited:
        return True                                    # a basename, not a coordinate
    if cited.startswith("..") or cited.startswith("/"):
        return True                                    # outside the repository
    head = cited.split("/", 1)[0]
    if "..." not in cited and head not in TOP_LEVEL:
        return True                                    # rooted where nothing is tracked
    return repo_ignores(cited)


def check_paths(rel, line):
    out = []
    for match in list(SPANNED.finditer(line)) + list(LINKED.finditer(line)):
        cited = match.group(1)
        tally["citations"] += 1
        lineno = match.group(2) if match.re is SPANNED else None
        if "/" not in cited:
            continue
        resolved = resolve(rel, cited)
        if resolved is None:
            if not unjudgeable(cited):
                out.append(f"cites `{cited}`, which the repository does not carry")
            continue
        if lineno and resolved in TRACKED:
            count = line_count(resolved)
            if int(lineno) > count:
                out.append(f"cites `{cited}`, but that file has {count} lines")
    return out


def check_elixir(line):
    out = []
    for match in NAME.finditer(line):
        name = match.group(0)
        tally["citations"] += 1
        if name in elixir_names:
            continue
        if any(known.startswith(name + ".") for known in elixir_names):
            continue                                   # a namespace, not a module
        out.append(f"names `{name}`, which no Elixir source declares")
    for match in FUNCTION.finditer(line):
        module, function = match.group(1), match.group(2)
        tally["citations"] += 1
        if module in module_defs and function not in module_defs[module]:
            out.append(f"names `{module}.{function}/…`, which `{module_file[module]}` does not define")
    return out


def check_env(line):
    # A trailing underscore is a prefix standing for a family of variables
    # (`FLEZHA_DURABILITY_PG_*`), not a variable anything reads.
    named = [name for name in ENV.findall(line) if not name.endswith("_")]
    tally["citations"] += len(named)
    return [f"names `{name}`, which no source reads"
            for name in named if name not in source_envs]


def check_telemetry(line):
    out = []
    for cited in EVENT.findall(line):
        squashed = squash(cited)
        atoms = squashed.strip("[]").split(",")
        if any(not a.startswith(":") or a == ":_" for a in atoms):
            continue                                   # a match pattern, not an event
        tally["citations"] += 1
        if squashed in source_events:
            continue
        prefix = squashed[: squashed.rindex(",") + 1] if "," in squashed else None
        if prefix in event_prefixes and atoms[-1][1:] in source_atoms:
            continue                                   # assembled from a variable
        out.append(f"names telemetry `{cited}`, which nothing emits")
    return out


allowlist = naming_gate.load_allowlist(ALLOWLIST)
scoped = {}


def check(rel, line):
    if rel not in scoped:
        scoped[rel] = [r for scope, r in allowlist if scope == "*" or rel.startswith(scope)]
    tally["documents"].add(rel)
    for rule in scoped[rel]:
        line = rule.sub("", line)
    return check_paths(rel, line) + check_elixir(line) + check_env(line) + check_telemetry(line)


def in_scope(rel):
    return (not rel.startswith("openspec/")
            and os.path.basename(rel) != "CHANGELOG.md")


code = naming_gate.walk(
    check=check,
    pathspec=["*.md"],
    path_filter=in_scope,
    self_files=SELF_FILES,
    record_tag="docs-citation-record",
    headline="documentation cites something the repository does not carry:",
    hint="Correct the citation. A name mentioned on purpose — a removal quoted as a "
         "record, a coordinate in another repository — takes a `docs-citation-record` "
         "block. Prose that is not a citation at all belongs in "
         f"{ALLOWLIST}.",
    noun="dead citation(s)",
    ok_message="documentation citations OK",
)
print(f"{tally['citations']} citations judged across {len(tally['documents'])} documents.")
sys.exit(code)
PY
