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
    "app_data", "dist", "__pycache__", ".elixir_ls", "cover",
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
    root = tempfile.mkdtemp(prefix="naming-gate-selftest-")
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
    # A file reached through a tracked symlink is the file its submodule
    # carries; judged once, under the name met first.
    seen = set()
    for rel in listing:
        real = os.path.realpath(rel)
        if real in seen:
            continue
        seen.add(real)
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


# ---------------------------------------------------------------------------
# The gate registry. One row per gate; `--run` is the one reader.
#
# A gate is declared here and nowhere else: CI runs `naming_gate.py --run @<group>`
# rather than re-spelling one step per gate, so adding a gate is a row and a
# workflow that forgets to call it is not a thing that can happen. `tools` names
# what the gate needs on PATH together with how to install it; an absent tool
# takes that gate out of the report and nothing else. A gate answers on this contract
# unless it declares `finding`, which is how a tool whose status alone cannot
# tell "found something" from "did not complete" says which it was.
GATES = (
    {"id": "overflow-naming", "group": "naming",
     "argv": ["scripts/check-overflow-naming.sh"]},
    {"id": "local-mode-naming", "group": "naming",
     "argv": ["scripts/check-local-mode-naming.sh"]},
    {"id": "module-naming", "group": "naming",
     "argv": ["scripts/check-module-naming.sh"]},
    {"id": "layer-direction", "group": "naming",
     "argv": ["scripts/check-layer-direction.sh"]},
    {"id": "mimic-global", "group": "naming",
     "argv": ["scripts/check-mimic-global.sh"]},
    {"id": "docs-citations", "group": "naming",
     "argv": ["scripts/check-docs-citations.sh"]},
    {"id": "test-fixture", "group": "naming",
     "argv": ["scripts/check-test-fixture.sh"]},
    {"id": "live-env-in-tests", "group": "naming",
     "argv": ["scripts/check-live-env-in-tests.sh"]},
    {"id": "ci-toolchain", "group": "naming",
     "argv": ["scripts/check-ci-toolchain.sh"]},
    {"id": "vm-args", "group": "naming",
     "argv": ["scripts/check-vm-args.sh"]},
    # The queue's order, and the scenarios a MODIFIED block would delete.
    # `/openspec/changes` is in .gitignore, so a CI checkout carries none of it
    # and both rows read an empty queue and say so; where they bite is a
    # developer's tree and the `--run @naming` made before an archive.
    {"id": "openspec-order", "group": "naming",
     "argv": ["scripts/check-openspec-order.sh"]},
    {"id": "openspec-scenarios", "group": "naming",
     "argv": ["scripts/check-openspec-scenarios.sh"]},
    {"id": "build-artifacts-tracked", "group": "naming",
     "argv": ["scripts/check-build-artifacts.sh", "tracked"]},
    {"id": "naming-gate-self-test", "group": "naming",
     "argv": ["python3", "scripts/naming_gate.py", "--self-test"]},
    {"id": "layer-direction-self-test", "group": "naming",
     "argv": ["scripts/check-layer-direction.sh", "--self-test"]},
    {"id": "openspec-scenarios-self-test", "group": "naming",
     "argv": ["scripts/check-openspec-scenarios.sh", "--self-test"]},
    # The publication-readiness record's SHAPE. Its content arm is the row
    # below, in its own group, because every blocking entry is open until the
    # day of the release and a gate that lands red stops being read.
    {"id": "publish-readiness", "group": "naming",
     "argv": ["scripts/check-publish-readiness.sh"]},
    {"id": "feature-maturity", "group": "naming",
     "argv": ["scripts/check-feature-maturity.sh"]},
    # Content: every blocking entry settled. Selected only by `@release`, which
    # the tag-triggered release job runs before it builds any artefact - after
    # an artefact is on a registry it is too late by definition.
    {"id": "publish-readiness-release", "group": "release",
     "argv": ["scripts/check-publish-readiness.sh", "--release"]},
    # Every SDK repository's CI, run locally against a release built from this
    # tree. Run by hand before a release: it builds, and it needs docker.
    {"id": "sdk-ci-rehearsal", "group": "rehearsal",
     "tools": ["docker:install Docker", "act:brew install act"],
     "argv": ["scripts/rehearse-sdk-ci.sh"]},
    # Its `size` arm reads the packed repository, so it needs the whole history
    # and runs in a job that checks out with fetch-depth 0.
    {"id": "build-artifacts-size", "group": "history",
     "argv": ["scripts/check-build-artifacts.sh", "size"]},
    # The source-shape gate. `format-apply.sh --check` answers on this contract
    # itself, so its status is read as one of ours instead of being flattened.
    {"id": "formatters", "group": "format",
     "argv": ["scripts/format-apply.sh", "--check"]},
    # `reuse lint-file` over git-listed paths: a full `reuse lint` walks the
    # tree and chokes on the stale test sockets under app_data/, which git never
    # lists. xargs collapses every status from 1 to 125 into 123, which alone
    # would make a reuse crash and a file with no licence header the same
    # number, so the wrapper keeps reuse's own 1 and turns every other failure
    # into 255 — which xargs reports as 124.
    {"id": "reuse-lint", "group": "format", "finding": "status:123",
     "tools": ["reuse:pipx install reuse"],
     "argv": ["bash", "-c",
              "git ls-files -co --exclude-standard -z | "
              "xargs -0 sh -c 'reuse lint-file \"$@\" || exit $(($? == 1 ? 1 : 255))' reuse-lint"]},
    {"id": "third-party-notices", "group": "format", "finding": "status:1",
     "tools": ["cargo-about:cargo install cargo-about"],
     "argv": ["bash", "-c",
              'g="${TMPDIR:-/tmp}/third_party_notices.md"; '
              # A generator that cannot run is not a stale notices file: keep
              # its status out of diff's, whose 1 is the only finding here.
              'cargo about generate about.hbs -o "$g" || exit 255; '
              'diff -u THIRD_PARTY_NOTICES.md "$g"']},
)

# `scripts/check-feature-symbols.sh` is deliberately not a row. It is not a gate
# over the tree: it builds an artefact three ways and compares the exported
# symbols, so it takes arguments, needs a compiler, and answers about one build
# rather than about the repository. Two bounded operators call it directly.


def gate(gate_id):
    """The declared row for `gate_id`, or None."""
    return next((row for row in GATES if row["id"] == gate_id), None)


def select(spec):
    """The rows `spec` names: `all`, `@<group>`, or a comma list of ids."""
    if spec == "all":
        return list(GATES)
    rows = []
    for name in spec.split(","):
        if name.startswith("@"):
            group = name[1:]
            matched = [row for row in GATES if row["group"] == group]
            if not matched:
                raise KeyError(f"no gate is declared in group {group!r}")
            rows.extend(matched)
        else:
            row = gate(name)
            if row is None:
                raise KeyError(f"no gate is declared as {name!r}")
            rows.append(row)
    return rows


def run_gate(row, report):
    """Run one declared gate and file its outcome under this contract.

    `report` is the three lists `run_gates` prints. Returns nothing: the verdict
    is the lists, because a gate that could not run must not end the loop. That
    rule is the whole point — testing every tool before running any check is
    what let one absent formatter hide six unformatted languages, and only the
    install hint survived from it.
    """
    label = row["id"]
    missing = [need for need in row.get("tools", ())
               if shutil.which(need.split(":", 1)[0]) is None]
    if missing:
        for need in missing:
            tool, _, hint = need.partition(":")
            report["unavailable"].append(f"{label} — no {tool}: {hint}")
        print(f"==> {label} — NOT RUN, see the summary")
        return

    print(f"==> {label}", flush=True)
    proc = subprocess.Popen(row["argv"], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    captured = []
    for chunk in proc.stdout:
        line = chunk.decode("utf-8", "replace")
        captured.append(line)
        sys.stdout.write(line)
    sys.stdout.flush()
    status = proc.wait()
    out = "".join(captured)

    if status == 0:
        return
    # A gate that fails and prints nothing has told the reader nothing. It is
    # not a finding, because a finding names what it found.
    if not out.strip():
        report["errored"].append(
            f"{label} — exit {status} with no output: a gate that fails says what it found")
        return
    finding = row.get("finding")
    if finding is None:
        # No rule declared, so the gate answers on this contract itself and its
        # status carries straight up. That is the ordinary case: a plain tool's
        # 2 is not a gate's 2, which is why the exception has to be the one that
        # is declared.
        if status == TOOL_MISSING:
            report["unavailable"].append(f"{label} — see its own summary above")
        elif status == NO_ANSWER:
            report["errored"].append(f"{label} — see its own summary above")
        elif status == FINDING:
            report["failed"].append(label)
        else:
            report["errored"].append(f"{label} — exit {status}")
        return
    if finding.startswith("status:"):
        codes = {int(code) for code in finding[len("status:"):].split(",")}
        matched = status in codes
    else:
        matched = re.search(finding[len("says:"):], out) is not None
    if matched:
        report["failed"].append(label)
    else:
        report["errored"].append(f"{label} — exit {status}")


def run_gates(rows):
    """Run `rows` in order and return the status the contract at the top says."""
    report = {"unavailable": [], "failed": [], "errored": []}
    for row in rows:
        run_gate(row, report)

    status = OK
    if report["unavailable"]:
        print("\nNOT RUN — the tool is not installed here:")
        for line in report["unavailable"]:
            print(f"  {line}")
        status = TOOL_MISSING
    if report["failed"]:
        print("\nFAILED — the output above says why:")
        for line in report["failed"]:
            print(f"  {line}")
        status = FINDING
    if report["errored"]:
        print("\nNO ANSWER — the gate ran and did not complete:")
        for line in report["errored"]:
            print(f"  {line}")
        status = NO_ANSWER
    if status == OK:
        print(f"\nevery gate ran and had nothing to report ({len(rows)} gates)")
    return status


def _contract_self_test():
    """Drive the runner over gates with known outcomes and check the verdict.

    The contract is what makes gates substitutable, so it is proved rather than
    asserted: one gate per outcome, plus the two shapes that are easy to get
    wrong. A gate that exits non-zero and prints NOTHING is not a finding — a
    finding names what it found — so it is filed as "no answer"; that is the
    planted case a bare `1` with no headline has to fail on. And a gate whose
    tool is absent must leave the loop running, because ending it is the defect
    this contract removed.
    """
    import io

    def sh(script):
        return ["bash", "-c", script]

    rows = {
        "clean": {"id": "clean", "argv": sh("echo fine")},
        "finding": {"id": "finding", "argv": sh("echo 'found one'; exit 1")},
        "silent": {"id": "silent", "argv": sh("exit 1")},
        "crashed": {"id": "crashed", "argv": sh("echo boom; exit 9")},
        "absent-tool": {"id": "absent-tool", "argv": sh("echo unreachable"),
                        "tools": ["flezha-no-such-tool:there is no install for this"]},
        "says-finding": {"id": "says-finding", "finding": "says:Diff in ",
                         "argv": sh("echo 'Diff in a.rs'; exit 1")},
        "says-crash": {"id": "says-crash", "finding": "says:Diff in ",
                       "argv": sh("echo 'panicked'; exit 1")},
        "child-missing": {"id": "child-missing", "argv": sh("echo child summary; exit 2")},
        "child-no-answer": {"id": "child-no-answer", "argv": sh("echo child summary; exit 3")},
    }
    # id -> the list the runner must file it under, and the status one such gate
    # alone must produce.
    expected = {
        "clean": (None, OK),
        "finding": ("failed", FINDING),
        "silent": ("errored", NO_ANSWER),
        "crashed": ("errored", NO_ANSWER),
        "absent-tool": ("unavailable", TOOL_MISSING),
        "says-finding": ("failed", FINDING),
        "says-crash": ("errored", NO_ANSWER),
        "child-missing": ("unavailable", TOOL_MISSING),
        "child-no-answer": ("errored", NO_ANSWER),
    }

    problems = []
    for name, (bucket, status) in expected.items():
        report = {"unavailable": [], "failed": [], "errored": []}
        with contextlib.redirect_stdout(io.StringIO()):
            run_gate(rows[name], report)
        landed = [key for key, entries in report.items() if entries]
        if landed != ([bucket] if bucket else []):
            problems.append(f"  {name}: filed under {landed}, expected {bucket}")
        with contextlib.redirect_stdout(io.StringIO()):
            got = run_gates([rows[name]])
        if got != status:
            problems.append(f"  {name}: run_gates returned {got}, expected {status}")

    # Several at once: an unanswered check outranks a finding, which outranks a
    # missing tool, because until a check runs the silence behind it means
    # nothing.
    with contextlib.redirect_stdout(io.StringIO()):
        worst = run_gates([rows["absent-tool"], rows["finding"], rows["crashed"]])
    if worst != NO_ANSWER:
        problems.append(f"  precedence: got {worst}, expected {NO_ANSWER}")
    with contextlib.redirect_stdout(io.StringIO()):
        pair = run_gates([rows["absent-tool"], rows["finding"]])
    if pair != FINDING:
        problems.append(f"  precedence: got {pair}, expected {FINDING}")

    if problems:
        print("naming_gate exit-contract self-test FAILED", file=sys.stderr)
        print("\n".join(problems), file=sys.stderr)
        return 1
    print(f"naming_gate exit-contract self-test OK ({len(expected)} outcomes)")
    return 0


def _registry_self_test():
    """Every declared gate is a row a caller can actually run.

    A registry whose rows point at nothing is the silence this table replaced:
    CI used to hand-write one step per gate, so a gate could be added and never
    called. Here the row IS the call, and this refuses a row whose command is
    not on disk, whose id repeats, or whose group nobody selects.
    """
    problems = []
    seen = set()
    for row in GATES:
        gate_id = row.get("id")
        if not gate_id or gate_id in seen:
            problems.append(f"  {gate_id!r}: missing or repeated id")
        seen.add(gate_id)
        if not row.get("group"):
            problems.append(f"  {gate_id}: declares no group, so no `--run @group` reaches it")
        argv = row.get("argv") or []
        if not argv:
            problems.append(f"  {gate_id}: declares no command")
            continue
        # `bash -c` and the interpreter running this file are commands by
        # construction; anything else is a path in the tree and must be there.
        head = argv[0]
        if head == "python3":
            target = argv[1] if len(argv) > 1 else None
        elif head == "bash":
            target = None
        else:
            target = head
        if target and not os.path.isfile(target):
            problems.append(f"  {gate_id}: {target} is not a file")
        elif target and head != "python3" and not os.access(target, os.X_OK):
            problems.append(f"  {gate_id}: {target} is not executable")

    for spec in ("all", "@naming", "@format", "@history", "@release", "@rehearsal"):
        try:
            if not select(spec):
                problems.append(f"  {spec}: selects no gate")
        except KeyError as exc:
            problems.append(f"  {spec}: {exc}")

    if problems:
        print("naming_gate registry self-test FAILED", file=sys.stderr)
        print("\n".join(problems), file=sys.stderr)
        return 1
    print(f"naming_gate registry self-test OK ({len(GATES)} gates declared)")
    return 0


def _self_test():
    """Prove the record escape hatch exempts its own block and nothing else.

    `walk` reads `git ls-files`, so the fixtures live in a throwaway git tree.
    Each fixture pairs a body with the lines the walk is expected to flag.
    """
    import io

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
        print("naming_gate self-test FAILED", file=sys.stderr)
        print(f"  expected {expected}", file=sys.stderr)
        print(f"  found    {found}", file=sys.stderr)
        print(report.getvalue(), file=sys.stderr)
        return 1
    print("naming_gate self-test OK")
    return 0


USAGE = """usage: naming_gate.py --self-test
       naming_gate.py --list
       naming_gate.py --run all|@<group>|<id>[,<id>...]"""


def _main(argv):
    if argv == ["--self-test"]:
        return max(_self_test(), _contract_self_test(), _registry_self_test())
    if argv == ["--list"]:
        for row in GATES:
            print(f"{row['id']:<26} {row['group']:<8} {' '.join(row['argv'])}")
        return OK
    if len(argv) == 2 and argv[0] == "--run":
        try:
            rows = select(argv[1])
        except KeyError as exc:
            print(f"naming_gate.py: {exc}", file=sys.stderr)
            print(USAGE, file=sys.stderr)
            return NO_ANSWER
        return run_gates(rows)
    print(USAGE, file=sys.stderr)
    # Not a gate result: the runner itself was called wrongly, which is the
    # caller's own mistake and not a verdict about the tree.
    return TOOL_MISSING


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
