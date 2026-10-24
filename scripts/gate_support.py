"""Shared walk for the repo's file-content gates.

Every gate asks one question — does a tracked, human-readable file still say
something the repo has decided against? — and differs only in what counts as a
violation. A gate supplies that judgement as a per-line `check`; this module
supplies the git listing, the file filters, the migration-record escape hatch
and the report.

Two judgements are in use, both built here:

- **mask-then-match**: an allowlist erases the legitimate senses from each
  line, then one broad pattern flags whatever is left. Right when the retired
  word survives in other senses everywhere.
- **match-directly** (`check-overflow-naming.sh`): narrow patterns pinned to
  the surface being policed, no allowlist — `pattern_check` with its allowlist
  left empty. Right when the retired word is ordinary English and only a few
  shapes are the retired name.

A third kind of gate judges a whole tree against a recorded list rather than a
line against a pattern: `check-layer-direction.sh`. It does not share the walk,
because its finding is a set difference and not a line, but it shares the list
format and the rule that goes with it: `load_baseline`.
"""

import contextlib
import io
import os
import re
import shutil
import subprocess
import sys
import tempfile


# ---------------------------------------------------------------------------
# The exit contract every gate in this repository answers on.
#
# A check has three outcomes and none of them is another. A tool that is not
# installed removes its own check from the report and nothing else; a tool that
# ran and says the tree is wrong is a finding; a tool that ran and never reached
# a verdict is neither, and gets its own list. Conflating the first two is what
# made the old gate unreadable: it tested every tool BEFORE running any check,
# so one absent formatter ended the run and said nothing about the six languages
# behind it. Conflating the last two names a defect in a tree that does not have
# one, and sends the reader to fix what is already correct.
#
#   0  everything ran, nothing to report
#   1  a check ran and found something
#   2  nothing failed, but a tool was missing so some check did not run
#   3  a tool ran and did not complete, so its check has no answer
#
# When several apply the status is 3, else 1, else 2: an unanswered check is
# reported before one that answered "no", because until it runs the silence
# behind it means nothing. CI fails on 1, 2 and 3; a developer's box may choose
# to skip on 2.
OK = 0
FINDING = 1
TOOL_MISSING = 2
NO_ANSWER = 3

BINARY_EXT = {
    ".jar", ".so", ".dll", ".dylib", ".png", ".jpg", ".gif", ".ico", ".pdf",
    ".class", ".zip", ".gz", ".fpz", ".dets", ".wal", ".bin",
}


def load_allowlist(path):
    """Parse a `scope:pattern` allowlist into (scope, compiled) rules."""
    rules = []
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            if not line or line.lstrip().startswith("#"):
                continue
            scope, _, pattern = line.partition(":")
            rules.append((scope, re.compile(pattern)))
    return rules


def load_baseline(path):
    """Parse a gate's recorded-debt list into a set of entries.

    A baseline is not an allowlist. An allowlist erases a legitimate sense from
    every line for ever; a baseline records what the tree held when a gate
    landed and MAY ONLY SHRINK — a gate that reads one fails both on an entry
    outside it and on an entry it carries that the tree no longer holds, so a
    list nobody prunes rots into an exemption. One entry per line, `#` comments
    and blank lines ignored.
    """
    with open(path, encoding="utf-8") as fh:
        return {
            line.strip()
            for line in fh
            if line.strip() and not line.lstrip().startswith("#")
        }


def tracked_files(pathspec=(), carried=False):
    """Every path `git ls-files` reports, so a gate and its source index read
    the same tree. Untracked and ignored files — build output, and the working
    documents the repository deliberately keeps out — are absent by
    construction rather than by exclusion.

    `carried` adds what the extracted SDKs' submodules carry and the names
    reached through a tracked symlink (`carried_files`), under the same
    pathspec prefixes."""
    listing = subprocess.run(
        ["git", "ls-files", "-z", *pathspec], capture_output=True, check=True
    ).stdout.decode()
    files = [rel for rel in listing.split("\0") if rel]
    if carried:
        files += [rel for rel in carried_files()
                  if not pathspec or any(rel.startswith(spec) for spec in pathspec)]
    return files


def carried_files():
    """Every path this repository carries that `git ls-files` does not report.

    Two kinds, both of which an extracted SDK produces. A SUBMODULE's contents:
    the index holds the gitlink and nothing inside it, but the files are on
    disk at the path `.gitmodules` names and a reader following a citation into
    one finds them. And a path through a tracked DIRECTORY SYMLINK: the index
    holds the link, so `sdk/node/test_app/conformance.json` is reported and
    `test/apps/node_runner/conformance.json`, the name every manifest reader
    and every document uses, is not.

    A submodule that is not checked out contributes nothing, which is the same
    answer the rest of the tooling gives for one.
    """
    paths = []
    listing = subprocess.run(
        ["git", "submodule", "--quiet", "foreach", "--recursive", "echo $sm_path"],
        capture_output=True,
        check=False,
    )
    for sm_path in listing.stdout.decode().split("\n"):
        sm_path = sm_path.strip()
        if not sm_path:
            continue
        inner = subprocess.run(
            ["git", "-C", sm_path, "ls-files", "-z"], capture_output=True, check=False
        ).stdout.decode()
        paths += [f"{sm_path}/{rel}" for rel in inner.split("\0") if rel]

    # A tracked symlink to a directory, as the paths reached through it.
    for link in tracked_files():
        if os.path.islink(link) and os.path.isdir(link):
            paths += disk_files([link])
    return paths


# Directories that hold build output or a vendored tree rather than source.
# A disk walk has to name them; `git ls-files` never lists them at all.
UNWALKED_DIRS = {
    ".git", "_build", "deps", "target", "node_modules", "vendor", ".venv",
    "storage", "dist", "__pycache__", ".elixir_ls", "cover",
}


def disk_files(roots):
    """Every readable file under `roots` as it is on disk, build output aside.

    The counterpart to `tracked_files`, and an option rather than a second
    walker: a gate that judges work in progress must see a file its author has
    written and not yet staged. `check-layer-direction.sh` states the reason a
    gate reaches for this — an untracked module has existed under lib/flezha/
    here before, and a walk that reads the index alone called that tree clean.
    A blind walk always flatters.
    """
    found = []
    for root in roots:
        if os.path.isfile(root):
            found.append(root)
            continue
        for base, dirs, names in os.walk(root):
            dirs[:] = sorted(d for d in dirs if d not in UNWALKED_DIRS)
            found.extend(os.path.join(base, name) for name in sorted(names))
    return found


OPENSPEC_CHANGES = "openspec/changes"


def open_changes():
    """Every change directory in the OpenSpec queue, the archive aside.

    Read from disk: `/openspec/changes` is in .gitignore, so a CI checkout
    carries none of it and every gate that reads the queue judges nothing there.
    """
    if not os.path.isdir(OPENSPEC_CHANGES):
        return []
    return sorted(name for name in os.listdir(OPENSPEC_CHANGES)
                  if name != "archive" and os.path.isdir(os.path.join(OPENSPEC_CHANGES, name)))


def stale_baseline(path, baseline, hits, what):
    """The other half of `load_baseline`: an entry the tree no longer holds.

    Reported by every gate that reads a baseline: the list may only shrink,
    because one nobody prunes rots into an exemption. `hits` is everything the
    walk flagged, baselined or not, and `what` completes "entries that no
    longer ...". Returns an exit code.
    """
    stale = sorted(baseline - hits)
    if not stale:
        return 0
    print(f"\n{path} names entries that no longer {what}:\n", file=sys.stderr)
    for entry in stale:
        print(f"  {entry}", file=sys.stderr)
    print(f"\n{len(stale)} stale entr(y|ies). The list may only shrink: remove them.",
          file=sys.stderr)
    return FINDING


def walk_baselined(*, check, baseline, what, **walk_args):
    """`walk` for a gate that reads a recorded-debt list (`load_baseline`).

    `check(rel, line)` returns `(entry, message)` pairs. Every entry is a hit; a
    hit the list records is not reported, and an entry the list carries that no
    line produced is stale (`stale_baseline`, whose `what` this is). Both halves
    report, so a run with a new finding AND a stale entry says so once. Returns an
    exit code.
    """
    recorded = load_baseline(baseline)
    hits = set()

    def unrecorded(rel, line):
        found = []
        for entry, message in check(rel, line):
            hits.add(entry)
            if entry not in recorded:
                found.append(message)
        return found

    code = walk(check=unrecorded, **walk_args)
    stale = stale_baseline(baseline, recorded, hits, what)
    return code or stale


def put(rel, body):
    """Write `rel` (creating its directories) and stage it, in a scratch tree."""
    os.makedirs(os.path.dirname(rel) or ".", exist_ok=True)
    with open(rel, "w", encoding="utf-8") as fh:
        fh.write(body)
    subprocess.run(["git", "add", "--", rel], check=True, stdout=subprocess.DEVNULL)


@contextlib.contextmanager
def scratch_tree(files):
    """A throwaway git tree holding `files` (`{path: body}`, every one staged),
    entered as the working directory and removed on exit.

    A gate's self-test runs the real gate against it: `walk` reads `git ls-files`,
    so a fixture has to be in an index, and one written under this repository
    would be judged by every other gate.
    """
    root = tempfile.mkdtemp(prefix="gate-selftest-")
    cwd = os.getcwd()
    try:
        subprocess.run(["git", "init", "-q", root], check=True)
        os.chdir(root)
        for rel, body in files.items():
            put(rel, body)
        yield root
    finally:
        os.chdir(cwd)
        shutil.rmtree(root, ignore_errors=True)


def report(failures, *, headline, hint, noun="finding(s)", ok_message=None):
    """Print a findings list in the house shape and return an exit code.

    Headline, one indented finding per line, then the count and what to do
    about it. `walk` reports what it flagged this way; a gate whose subject is
    a file or a table rather than a line calls this directly with its own
    findings. `ok_message` is printed when there are none, and omitting it
    leaves a clean run silent, which is what a second report in one run wants.
    """
    if not failures:
        if ok_message:
            print(ok_message)
        return OK
    print(f"{headline}\n", file=sys.stderr)
    for failure in failures:
        print(f"  {failure}", file=sys.stderr)
    print(f"\n{len(failures)} {noun}. {hint}".rstrip(), file=sys.stderr)
    return FINDING


def self_test_verdict(name, problems, detail=""):
    """Print a self-test's verdict, its indented `problems` under it when it
    failed, and return its exit code."""
    if problems:
        print(f"{name} self-test FAILED", file=sys.stderr)
        print("\n".join(problems), file=sys.stderr)
        return FINDING
    print(f"{name} self-test OK{detail}")
    return OK


def pattern_check(patterns, allowlist=()):
    """A `check` that flags any line still matching `patterns` once the
    allowlist rules in scope for the file have been erased from it. The
    offending line is its own message."""
    scoped = {}

    def active_for(rel):
        if rel not in scoped:
            scoped[rel] = [r for scope, r in allowlist if scope == "*" or rel.startswith(scope)]
        return scoped[rel]

    def check(rel, line):
        stripped = line
        for rule in active_for(rel):
            stripped = rule.sub("", stripped)
        return [line.strip()] if any(p.search(stripped) for p in patterns) else []

    return check


def walk(*, check, headline, hint, ok_message, pathspec=(), path_filter=None,
         prefilter=None, self_files=frozenset(), record_tag=None,
         noun="occurrence(s)", source="tracked", roots=()):
    """Run `check` over every tracked file and report what it flags.

    `check(rel, line)` returns a list of messages, empty when the line is
    clean. `prefilter(text)` skips whole files before the per-line scan;
    `path_filter(rel)` skips them by path. `self_files` are the gate's own
    files, which carry the retired names as data. `record_tag` names the marker
    pair that exempts a block of prose: the exemption covers the lines the
    markers delimit and no others, both markers on one line exempt that line,
    and a block left open at end of file fails.

    `source` picks the listing: `"tracked"` reads `git ls-files` (the default,
    and what every naming gate wants) together with what the extracted SDKs'
    submodules carry, `"disk"` reads `roots` as they are on disk so an unstaged
    file is still judged. Returns an exit code.
    """
    begin, end = (f"{record_tag}:begin", f"{record_tag}:end") if record_tag else (None, None)
    failures = []
    unclosed = []
    listing = disk_files(roots) if source == "disk" else tracked_files(pathspec, carried=True)
    for rel, text in _texts(listing, self_files, path_filter, prefilter):
        in_record = _scan(rel, text, check, begin, end, failures)
        if in_record:
            unclosed.append((rel, in_record))

    unclosed_code = report(
        [f"{rel}:{lineno}: `{begin}` has no matching `{end}`" for rel, lineno in unclosed],
        headline="a record block is never closed, so its exemption runs to end of file:",
        hint="Close the block, or put both markers on one line to exempt that line alone.",
        noun="unclosed record block(s)")
    # The clean line is earned only when neither block reported.
    found_code = report(
        [f"{rel}:{lineno}: {message[:160]}" for rel, lineno, message in failures],
        headline=headline, hint=hint, noun=noun,
        ok_message=None if unclosed else ok_message)
    return found_code or unclosed_code


def _texts(listing, self_files, path_filter, prefilter):
    """(rel, text) of each listed file a walk judges: its own and binary files
    aside, and only what `path_filter` and `prefilter` accept."""
    # A file reached through a tracked symlink is the file its submodule
    # carries; judged once, under the name met first.
    seen = set()
    for rel in listing:
        real = os.path.realpath(rel)
        if real in seen:
            continue
        seen.add(real)
        if (rel in self_files or "/dist/" in rel or (path_filter and not path_filter(rel))
                or os.path.splitext(rel)[1] in BINARY_EXT):
            continue
        try:
            with open(rel, encoding="utf-8") as fh:
                text = fh.read()
        except (UnicodeDecodeError, OSError):
            continue
        if not prefilter or prefilter(text):
            yield rel, text


def _scan(rel, text, check, begin, end, failures):
    """Append `(rel, lineno, message)` for each line `check` flags outside a
    record block; return the line a block left open was opened on, else 0."""
    in_record = 0
    for lineno, line in enumerate(text.split("\n"), 1):
        # A migration record MENTIONS the retired names rather than using
        # them. Delimited explicitly so the exemption is greppable and
        # scoped to the block, instead of blinding the whole file. A line
        # carrying both markers is a whole record: it exempts itself and
        # leaves the surrounding block state alone.
        opens = begin is not None and begin in line
        closes = end is not None and end in line
        if opens or closes:
            if opens != closes:
                in_record = lineno if opens else 0
            continue
        if in_record:
            continue
        for message in check(rel, line):
            failures.append((rel, lineno, message))
    return in_record



def self_test():
    """Prove the record escape hatch exempts its own block and nothing else.

    `walk` reads `git ls-files`, so the fixtures live in a throwaway git tree.
    Each fixture pairs a body with the lines the walk is expected to flag.
    """
    tag = "selftest-record"
    cases = {
        # Both markers on one line are a whole record: it exempts its own line,
        # and the line after it is still read.
        "inline.md": (f"<!-- {tag}:begin -->PROBE<!-- {tag}:end -->\nPROBE\n", ["inline.md:2"]),
        # A block exempts what it wraps and stops at its closing marker.
        "block.md": (f"<!-- {tag}:begin -->\nPROBE\n<!-- {tag}:end -->\nPROBE\n", ["block.md:4"]),
        # A block left open would exempt every line below it, so it is refused
        # at the line that opened it.
        "unclosed.md": (f"<!-- {tag}:begin -->\nPROBE\n", ["unclosed.md:1"]),
        "clean.md": ("nothing here\n", []),
    }
    expected = sorted(hit for _, hits in cases.values() for hit in hits)

    report = io.StringIO()
    with scratch_tree({name: body for name, (body, _) in cases.items()}), \
            contextlib.redirect_stderr(report), contextlib.redirect_stdout(io.StringIO()):
        code = walk(
            check=pattern_check([re.compile("PROBE")]),
            record_tag=tag,
            headline="self-test probe found:",
            hint="",
            ok_message="self-test clean",
        )

    found = sorted(re.findall(r"^  (\S+:\d+):", report.getvalue(), re.M))
    if code == 0 or found != expected:
        print("gate_support self-test FAILED", file=sys.stderr)
        print(f"  expected {expected}", file=sys.stderr)
        print(f"  found    {found}", file=sys.stderr)
        print(report.getvalue(), file=sys.stderr)
        return 1
    print("gate_support self-test OK")
    return 0
