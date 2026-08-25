"""Shared walk for the repo's file-content gates.

Every gate asks one question — does a tracked, human-readable file still say
something the repo has decided against? — and differs only in what counts as a
violation. A gate supplies that judgement as a per-line `check`; this module
supplies the git listing, the file filters, the migration-record escape hatch
and the report.

Two judgements are in use, both built here:

- **mask-then-match** (`check-kind-naming.sh`): an allowlist erases the
  legitimate senses from each line, then one broad pattern flags whatever is
  left. Right when the retired word survives in other senses everywhere.
- **match-directly** (`check-overflow-naming.sh`): narrow patterns pinned to
  the surface being policed, no allowlist. Right when the retired word is
  ordinary English and only a few shapes are the retired name.

`check-sdk-boundary.sh` writes its own `check` because its violation is a
reference outside an allowed set, not a spelling.
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
    files, which carry the retired names as data. Returns an exit code.
    """
    listing = subprocess.run(
        ["git", "ls-files", "-z", *pathspec], capture_output=True, check=True
    ).stdout.decode()

    begin, end = (f"{record_tag}:begin", f"{record_tag}:end") if record_tag else (None, None)
    failures = []
    for rel in listing.split("\0"):
        if not rel or rel in self_files or "/dist/" in rel:
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
        in_record = False
        for lineno, line in enumerate(text.split("\n"), 1):
            # A migration record MENTIONS the retired names rather than using
            # them. Delimited explicitly so the exemption is greppable and
            # scoped to the block, instead of blinding the whole file.
            if begin and begin in line:
                in_record = True
                continue
            if end and end in line:
                in_record = False
                continue
            if in_record:
                continue
            for message in check(rel, line):
                failures.append((rel, lineno, message))

    if failures:
        print(f"{headline}\n", file=sys.stderr)
        for rel, lineno, message in failures:
            print(f"  {rel}:{lineno}: {message[:160]}", file=sys.stderr)
        summary = f"{len(failures)} {noun}."
        print(f"\n{summary} {hint}".rstrip(), file=sys.stderr)
        return 1

    print(ok_message)
    return 0
