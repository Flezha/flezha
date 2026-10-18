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
import signal
import subprocess
import sys
import tempfile
import threading
import time

from gate_rows import GATES, noisy

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


def select(spec, table=GATES):
    """The rows `spec` names: `all`, `@<group>`, or a comma list of ids. A row
    declaring `groups` stands for what that spec selects; a row reached twice runs once."""
    names = spec.split(",") if spec != "all" else \
        [row["id"] for row in table if "groups" not in row]
    rows = {}
    for name in names:
        if name.startswith("@"):
            matched = [row for row in table if row["group"] == name[1:]]
            if not matched:
                raise KeyError(f"no gate is declared in group {name[1:]!r}")
        else:
            matched = [row for row in table if row["id"] == name]
            if not matched:
                raise KeyError(f"no gate is declared as {name!r}")
        for row in matched:
            for picked in select(row["groups"], table) if "groups" in row else [row]:
                rows.setdefault(picked["id"], picked)
    return list(rows.values())


# A row's time limit unless it declares `timeout`; a stopped row's grace before SIGKILL.
TIMEOUT = 600
KILL_AFTER = 30


def _execute(row, logs):
    """Run a row's command; return (status, merged output, stderr). The status
    is None when the row outran its limit: its session, all it spawned, is stopped."""
    env = {**os.environ, **{k: os.path.expandvars(v) for k, v in row.get("env", {}).items()}}
    proc = subprocess.Popen(row["argv"], cwd=row.get("cwd"), env=env, start_new_session=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    files = [open(os.path.join(logs, f"{row['id']}.{ext}"), "w", encoding="utf-8")
             for ext in ("log", "err")] if logs else []
    log = files[0] if files else sys.stdout
    merged, errs, lock = [], [], threading.Lock()

    def pump(stream, captures, sinks):
        for chunk in stream:
            line = chunk.decode("utf-8", "replace")
            with lock:
                for capture in captures:
                    capture.append(line)
                for sink in sinks:
                    sink.write(line)

    pumps = [threading.Thread(target=pump, args=args, daemon=True) for args in (
        (proc.stdout, [merged], [log]), (proc.stderr, [merged, errs], [log, *files[1:]]))]
    for pumper in pumps:
        pumper.start()
    try:
        status = proc.wait(timeout=row.get("timeout", TIMEOUT))
    except subprocess.TimeoutExpired:
        status = None
    finally:
        if proc.poll() is None:
            _stop(proc)
    # A process the row left behind may hold a pipe open; its output is not
    # worth waiting for once the row itself has ended.
    for pumper in pumps:
        pumper.join(timeout=10)
    with lock:
        for sink in files:
            sink.close()
    return status, "".join(merged), "".join(errs)


def _stop(proc):
    """SIGTERM the row's session, then SIGKILL it if it is still there."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, sig)
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=KILL_AFTER)
            return


def run_gate(row, report, logs=None):
    """Run one declared gate and file its outcome under this contract.

    `report` is the three lists `run_gates` prints, and the only verdict: a gate
    whose tool is absent or whose command cannot start must not end the loop.
    Testing every tool before running any check is what let one absent formatter
    hide six unformatted languages. With `logs` the output goes to `<id>.log`
    (stderr also to `<id>.err`) there, and each verdict to `summary.txt`.
    """
    label = row["id"]
    started = time.monotonic()
    missing = [need for need in row.get("tools", ())
               if shutil.which(need.split(":", 1)[0]) is None]
    if missing:
        for need in missing:
            tool, _, hint = need.partition(":")
            report["unavailable"].append(f"{label} — no {tool}: {hint}")
        verdict = ("NOT RUN", None, None)
    else:
        print(f"==> {label}", flush=True)
        try:
            verdict = _judge(row, *_execute(row, logs))
        except OSError as exc:
            verdict = ("NO ANSWER", "errored", f"{label} — could not start: {exc}")
    word, bucket, entry = verdict
    if bucket:
        report[bucket].append(entry)
    line = f"{word} {label} ({round(time.monotonic() - started)}s)"
    print(line, flush=True)
    if logs:
        with open(os.path.join(logs, "summary.txt"), "a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def _judge(row, status, out, err):
    """(summary word, report list or None, report entry) for one finished row."""
    label = row["id"]
    if status is None:
        return "NO ANSWER", "errored", f"{label} — timed out after {row.get('timeout', TIMEOUT)}s"
    if status == 0:
        line = noisy(row, out, err)
        if line is None:
            return "PASS", None, None
        return "NOISY", "failed", f"{label} — noisy: {line.strip()[:160]}"
    # A gate that fails and prints nothing has told the reader nothing. It is
    # not a finding, because a finding names what it found.
    if not out.strip():
        return ("NO ANSWER", "errored",
                f"{label} — exit {status} with no output: a gate that fails says what it found")
    finding = row.get("finding")
    if finding is None:
        # No rule declared, so the gate answers on this contract itself and its
        # status carries straight up. That is the ordinary case: a plain tool's
        # 2 is not a gate's 2, which is why the exception has to be the one that
        # is declared.
        if status == TOOL_MISSING:
            return "NOT RUN", "unavailable", f"{label} — see its own summary above"
        if status == NO_ANSWER:
            return "NO ANSWER", "errored", f"{label} — see its own summary above"
        matched = status == FINDING
    elif finding == "status:any":
        matched = True
    elif finding.startswith("status:"):
        matched = status in {int(code) for code in finding[len("status:"):].split(",")}
    else:
        matched = re.search(finding[len("says:"):], out) is not None
    if matched:
        return "FAIL", "failed", label
    return "NO ANSWER", "errored", f"{label} — exit {status}"


def run_gates(rows, logs=None):
    """Run `rows` in order and return the status the contract at the top says."""
    if logs:
        os.makedirs(logs, exist_ok=True)
    report = {"unavailable": [], "failed": [], "errored": []}
    for row in rows:
        run_gate(row, report, logs)

    status = OK
    for key, code, heading in (
            ("unavailable", TOOL_MISSING, "NOT RUN — the tool is not installed here:"),
            ("failed", FINDING, "FAILED — its output says why:"),
            ("errored", NO_ANSWER, "NO ANSWER — the gate ran and did not complete:")):
        if report[key]:
            print(f"\n{heading}")
            for line in report[key]:
                print(f"  {line}")
            status = code
    if status == OK:
        print(f"\nevery gate ran and had nothing to report ({len(rows)} gates)")
    return status


def _sh(script):
    return ["bash", "-c", script]


# One planted gate per outcome: its row, the list the runner must file it under,
# and the status one such gate alone must produce.
_OUTCOMES = {
    "clean": ({"argv": _sh("echo fine")}, None, OK),
    "finding": ({"argv": _sh("echo 'found one'; exit 1")}, "failed", FINDING),
    "silent": ({"argv": _sh("exit 1")}, "errored", NO_ANSWER),
    "crashed": ({"argv": _sh("echo boom; exit 9")}, "errored", NO_ANSWER),
    "absent-tool": ({"argv": _sh("echo unreachable"),
                     "tools": ["flezha-no-such-tool:there is no install for this"]},
                    "unavailable", TOOL_MISSING),
    "says-finding": ({"finding": "says:Diff in ", "argv": _sh("echo 'Diff in a.rs'; exit 1")},
                     "failed", FINDING),
    "says-crash": ({"finding": "says:Diff in ", "argv": _sh("echo 'panicked'; exit 1")},
                   "errored", NO_ANSWER),
    "child-missing": ({"argv": _sh("echo child summary; exit 2")}, "unavailable", TOOL_MISSING),
    "child-no-answer": ({"argv": _sh("echo child summary; exit 3")}, "errored", NO_ANSWER),
    "noisy": ({"quiet": True, "argv": _sh("echo 'warning: unused'")}, "failed", FINDING),
    "stray-stderr": ({"quiet": True, "argv": _sh("echo oops >&2")}, "failed", FINDING),
    "timed-out": ({"timeout": 1, "argv": _sh("echo started; sleep 30")}, "errored", NO_ANSWER),
    "plain-tool": ({"finding": "status:any", "argv": _sh("echo red; exit 2")}, "failed", FINDING),
    "unstartable": ({"argv": ["flezha-no-such-command"]}, "errored", NO_ANSWER),
    "no-cwd": ({"cwd": "flezha-no-such-dir", "argv": _sh("echo x")}, "errored", NO_ANSWER),
}


def _contract_self_test():
    """Drive the runner over gates with known outcomes and check the verdict.

    The contract is what makes gates substitutable, so it is proved rather than
    asserted: one gate per outcome, plus the two shapes that are easy to get
    wrong. A gate that exits non-zero and prints NOTHING is not a finding — a
    finding names what it found — so it is filed as "no answer"; that is the
    planted case a bare `1` with no headline has to fail on. And a gate whose
    tool is absent, or that runs past its time limit, must leave the loop
    running, because ending it is the defect this contract removed.
    """
    rows = {name: {"id": name, **row} for name, (row, _, _) in _OUTCOMES.items()}
    problems = []
    for name, (_, bucket, status) in _OUTCOMES.items():
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
    # missing tool, because until a check runs the silence behind it means nothing.
    for names, want in ((("absent-tool", "finding", "crashed"), NO_ANSWER),
                        (("absent-tool", "finding"), FINDING)):
        with contextlib.redirect_stdout(io.StringIO()):
            got = run_gates([rows[name] for name in names])
        if got != want:
            problems.append(f"  precedence {names}: got {got}, expected {want}")
    problems += _runner_problems(rows)
    return self_test_verdict("naming_gate exit-contract", problems,
                             f" ({len(_OUTCOMES)} outcomes)")


def _runner_problems(rows):
    """The row behind a timed-out one still runs, `--logs` files a noisy row's
    output and verdict, and a `groups` row stands for its rows once each."""
    problems = []
    with contextlib.redirect_stdout(out := io.StringIO()):
        run_gates([rows["timed-out"], rows["clean"]])
    if "PASS clean" not in out.getvalue():
        problems.append("  timed-out: the row behind it did not run")
    logs = tempfile.mkdtemp(prefix="naming-gate-logs-")
    with contextlib.redirect_stdout(io.StringIO()):
        run_gates([rows["noisy"]], logs)
    summary, log = (open(os.path.join(logs, name), encoding="utf-8").read()
                    for name in ("summary.txt", "noisy.log"))
    shutil.rmtree(logs)
    if not summary.startswith("NOISY noisy (") or "warning: unused" not in log:
        problems.append(f"  --logs: summary {summary!r}, log {log!r}")
    table = ({"id": "a", "group": "x"}, {"id": "b", "group": "y"},
             {"id": "u", "group": "union", "groups": "@x,@y,a"})
    if [row["id"] for row in select("@union", table)] != ["a", "b"]:
        problems.append("  groups: @union does not stand for the rows of @x and @y once each")
    return problems


def _registry_self_test():
    """Every declared gate is a row a caller can actually run.

    A registry whose rows point at nothing is the silence this table replaced:
    CI used to hand-write one step per gate, so a gate could be added and never
    called. Here the row IS the call, and this refuses a row whose command is
    not on disk, whose id repeats, whose group nobody selects, or whose
    `groups` names nothing.
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
        if "groups" in row:
            continue
        argv = row.get("argv") or []
        if not argv:
            problems.append(f"  {gate_id}: declares no command")
            continue
        # A bare name is a command on PATH and `bash -c` one by construction; a
        # path run from the root is in the tree and must be there. A row run
        # elsewhere runs what its own build puts there.
        target = argv[1] if argv[0] == "python3" and len(argv) > 1 else argv[0]
        if argv[0] == "bash" or "/" not in target or row.get("cwd"):
            continue
        if not os.path.isfile(target):
            problems.append(f"  {gate_id}: {target} is not a file")
        elif argv[0] != "python3" and not os.access(target, os.X_OK):
            problems.append(f"  {gate_id}: {target} is not executable")

    unions = [row["id"] for row in GATES if "groups" in row]
    for spec in ["all", *sorted({f"@{row['group']}" for row in GATES}), *unions]:
        try:
            if not select(spec):
                problems.append(f"  {spec}: selects no gate")
        except KeyError as exc:
            problems.append(f"  {spec}: {exc}")
    return self_test_verdict("naming_gate registry", problems, f" ({len(GATES)} gates declared)")


def _self_test():
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
        print("naming_gate self-test FAILED", file=sys.stderr)
        print(f"  expected {expected}", file=sys.stderr)
        print(f"  found    {found}", file=sys.stderr)
        print(report.getvalue(), file=sys.stderr)
        return 1
    print("naming_gate self-test OK")
    return 0


USAGE = """usage: naming_gate.py --self-test
       naming_gate.py --list [all|@<group>|<id>[,<id>...]]
       naming_gate.py [--logs DIR] --run all|@<group>|<id>[,<id>...]"""


def _command(row):
    """A row's command as `--list` prints it."""
    if "groups" in row:
        return f"= {row['groups']}"
    words = [f"cd {row['cwd']} &&"] if row.get("cwd") else []
    words += ["env", *(f"{k}={v}" for k, v in row["env"].items())] if row.get("env") else []
    return " ".join(words + row["argv"])


def _main(argv):
    logs = None
    if argv[:1] == ["--logs"] and len(argv) > 2:
        logs, argv = argv[1], argv[2:]
    if argv == ["--self-test"]:
        return max(_self_test(), _contract_self_test(), _registry_self_test())
    if argv[:1] == ["--list"] and len(argv) <= 2 or argv[:1] == ["--run"] and len(argv) == 2:
        try:
            rows = select(argv[1]) if len(argv) == 2 else None
        except KeyError as exc:
            print(f"naming_gate.py: {exc}", file=sys.stderr)
            print(USAGE, file=sys.stderr)
            return NO_ANSWER
        if argv[0] == "--run" and rows is not None:
            return run_gates(rows, logs)
        for row in GATES if rows is None else rows:
            print(f"{row['id']:<26} {row['group']:<8} {_command(row)}")
        return OK
    print(USAGE, file=sys.stderr)
    # Not a gate result: the runner itself was called wrongly, which is the
    # caller's own mistake and not a verdict about the tree.
    return TOOL_MISSING


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
