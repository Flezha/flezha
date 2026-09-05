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

`check-kind-naming.sh` runs both and fails a line that trips either: the
allowlist carries the open-ended plain-English sense, and an unmaskable pattern
set carries the closed kind sense, so a broad allowlist rule cannot hide the
error the gate exists to catch.

`check-sdk-boundary.sh` writes its own `check` because its violation is a
reference outside an allowed set, not a spelling.

A third kind of gate judges a whole tree against a recorded list rather than a
line against a pattern — `check-java-spec-coverage.sh` and
`check-layer-direction.sh`. They do not share the walk, because their finding
is a set difference and not a line, but they share the list format and the rule
that goes with it: `load_baseline`.
"""

import os
import re
import subprocess
import sys

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


def tracked_files(pathspec=()):
    """Every path `git ls-files` reports, so a gate and its source index read
    the same tree. Untracked and ignored files — build output, and the working
    documents the repository deliberately keeps out — are absent by
    construction rather than by exclusion."""
    listing = subprocess.run(
        ["git", "ls-files", "-z", *pathspec], capture_output=True, check=True
    ).stdout.decode()
    return [rel for rel in listing.split("\0") if rel]


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
         noun="occurrence(s)"):
    """Run `check` over every tracked file and report what it flags.

    `check(rel, line)` returns a list of messages, empty when the line is
    clean. `prefilter(text)` skips whole files before the per-line scan;
    `path_filter(rel)` skips them by path. `self_files` are the gate's own
    files, which carry the retired names as data. `record_tag` names the marker
    pair that exempts a block of prose: the exemption covers the lines the
    markers delimit and no others, both markers on one line exempt that line,
    and a block left open at end of file fails. Returns an exit code.
    """
    begin, end = (f"{record_tag}:begin", f"{record_tag}:end") if record_tag else (None, None)
    failures = []
    unclosed = []
    for rel in tracked_files(pathspec):
        if rel in self_files or "/dist/" in rel:
            continue
        if path_filter and not path_filter(rel):
            continue
        if os.path.splitext(rel)[1] in BINARY_EXT:
            continue
        try:
            with open(rel, encoding="utf-8") as fh:
                text = fh.read()
        except (UnicodeDecodeError, OSError):
            continue
        if prefilter and not prefilter(text):
            continue
        # The line a still-open record block was opened on, 0 outside a block.
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
        if in_record:
            unclosed.append((rel, in_record))

    if unclosed:
        print("a record block is never closed, so its exemption runs to end "
              "of file:\n", file=sys.stderr)
        for rel, lineno in unclosed:
            print(f"  {rel}:{lineno}: `{begin}` has no matching `{end}`", file=sys.stderr)
        print(f"\n{len(unclosed)} unclosed record block(s). Close the block, or "
              "put both markers on one line to exempt that line alone.\n", file=sys.stderr)

    if failures:
        print(f"{headline}\n", file=sys.stderr)
        for rel, lineno, message in failures:
            print(f"  {rel}:{lineno}: {message[:160]}", file=sys.stderr)
        summary = f"{len(failures)} {noun}."
        print(f"\n{summary} {hint}".rstrip(), file=sys.stderr)

    if failures or unclosed:
        return 1

    print(ok_message)
    return 0


def _self_test():
    """Prove the record escape hatch exempts its own block and nothing else.

    `walk` reads `git ls-files`, so the fixtures live in a throwaway git tree.
    Each fixture pairs a body with the lines the walk is expected to flag.
    """
    import contextlib
    import io
    import shutil
    import tempfile

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

    root = tempfile.mkdtemp(prefix="naming-gate-selftest-")
    cwd = os.getcwd()
    try:
        for name, (body, _) in cases.items():
            with open(os.path.join(root, name), "w", encoding="utf-8") as fh:
                fh.write(body)
        subprocess.run(["git", "init", "-q", root], check=True)
        os.chdir(root)
        subprocess.run(["git", "add", "-A"], check=True)
        report = io.StringIO()
        with contextlib.redirect_stderr(report), contextlib.redirect_stdout(io.StringIO()):
            code = walk(
                check=pattern_check([re.compile("PROBE")]),
                record_tag=tag,
                headline="self-test probe found:",
                hint="",
                ok_message="self-test clean",
            )
    finally:
        os.chdir(cwd)
        shutil.rmtree(root, ignore_errors=True)

    found = sorted(re.findall(r"^  (\S+:\d+):", report.getvalue(), re.M))
    if code == 0 or found != expected:
        print("naming_gate self-test FAILED", file=sys.stderr)
        print(f"  expected {expected}", file=sys.stderr)
        print(f"  found    {found}", file=sys.stderr)
        print(report.getvalue(), file=sys.stderr)
        return 1
    print("naming_gate self-test OK")
    return 0


if __name__ == "__main__":
    if sys.argv[1:] != ["--self-test"]:
        print("usage: naming_gate.py --self-test", file=sys.stderr)
        sys.exit(2)
    sys.exit(_self_test())
