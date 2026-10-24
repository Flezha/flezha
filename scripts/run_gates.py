"""Run the repository's checks: the rows scripts/gate_rows.py declares.

Each row is one command (a compile, a test suite, a linter, a repo check).
`--run` executes the rows a selection names, several at once, files each row's
log and a summary under `--logs`, and answers on the exit contract in
gate_support.py. `--list` prints the rows and their commands without running
them. scripts/test-full.sh is the full run.
"""

import concurrent.futures
import contextlib
import glob
import io
import math
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
from gate_support import FINDING, NO_ANSWER, OK, TOOL_MISSING, self_test, self_test_verdict


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
    with _REPORTING:
        if bucket:
            report[bucket].append(entry)
        _summarise(f"{word} {label} ({round(time.monotonic() - started)}s)", logs)


# Lanes running side by side file their verdicts one at a time.
_REPORTING = threading.Lock()


def _summarise(line, logs):
    """Print a summary line, and file it in `summary.txt` under `logs`."""
    print(line, flush=True)
    if logs:
        with open(os.path.join(logs, "summary.txt"), "a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def recorded(logs):
    """Each row's duration in the newest summary that records it, among the runs
    filed beside `logs` (`*/summary.txt` under its parent)."""
    durations = {}
    if not logs:
        return durations
    pattern = os.path.join(os.path.dirname(os.path.abspath(logs)), "*", "summary.txt")
    for path in sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True):
        with open(path, encoding="utf-8") as fh:
            for row_id, took in re.findall(r"^\S.* (\S+) \((\d+)s\)$", fh.read(), re.M):
                durations.setdefault(row_id, int(took))
    return durations


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


def lanes(rows):
    """`rows` as lanes, one per group in first-seen order: a lane's rows keep
    their order (an e2e runtime's build before its shards), lanes share nothing
    a run does not derive for itself (state root, sockets, ports, node name)."""
    by_group = {}
    for row in rows:
        by_group.setdefault(row["group"], []).append(row)
    return list(by_group.values())


def alone(lane):
    """How many of `lane`'s rows run alone before the lanes start: all of the
    dependency compile every other lane builds on, and a lane's rows up to its
    last build, whose output other lanes run (a runner jar, an SDK's copy of the
    native library every veneer builds to one path)."""
    if lane[0]["group"] == "deps":
        return len(lane)
    return max((at + 1 for at, row in enumerate(lane) if row.get("builds")), default=0)


def plan(rows, durations):
    """`rows` as `--jobs` runs them: the rows `alone` names, in order, then the
    units the lanes run, longest first by `durations` (a row with none recorded
    counts as the longest, as it may be) so the short ones fill the gaps. A lane
    is one unit unless every row left in it is `apart`: then each row is one."""
    cuts = [(lane, alone(lane)) for lane in sorted(
        lanes(rows), key=lambda lane: lane[0]["group"] != "deps")]
    units = []
    for lane, cut in cuts:
        rest = lane[cut:]
        units += [[row] for row in rest] if all(row.get("apart") for row in rest) else [rest]
    units.sort(key=lambda unit: -sum(durations.get(row["id"], math.inf) for row in unit))
    return [row for lane, cut in cuts for row in lane[:cut]], units


def run_gates(rows, logs=None, jobs=1, wall=None):
    """Run `rows` and return the status the contract at the top says: in order,
    or with `jobs` > 1 as `plan` lays them out, `jobs` units at a time. The run's
    wall time is the summary's last line; over `wall` seconds it is a finding."""
    started = time.monotonic()
    if logs:
        os.makedirs(logs, exist_ok=True)
    report = {"unavailable": [], "failed": [], "errored": []}
    if jobs <= 1:
        for row in rows:
            run_gate(row, report, logs)
    else:
        first, units = plan(rows, recorded(logs))
        for row in first:
            run_gate(row, report, logs)
        with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
            for done in [pool.submit(lambda unit=unit: [run_gate(row, report, logs)
                                                        for row in unit]) for unit in units]:
                done.result()
    took = time.monotonic() - started
    over = wall is not None and took > wall
    line = f"wall time {round(took)}s" + (f", over the {wall}s a run may take" if over else "")
    _summarise(line, logs)
    if over:
        report["failed"].append(line)

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
    problems += _runner_problems(rows) + _schedule_problems(rows)
    return self_test_verdict("run_gates exit-contract", problems,
                             f" ({len(_OUTCOMES)} outcomes)")


def _runner_problems(rows):
    """The row behind a timed-out one still runs, `--logs` files a noisy row's
    output and verdict, a `groups` row stands for its rows once each, and a
    build runs before the lanes."""
    problems = []
    with contextlib.redirect_stdout(out := io.StringIO()):
        run_gates([rows["timed-out"], rows["clean"]])
    if "PASS clean" not in out.getvalue():
        problems.append("  timed-out: the row behind it did not run")
    logs = tempfile.mkdtemp(prefix="gate-logs-")
    with contextlib.redirect_stdout(io.StringIO()):
        run_gates([rows["noisy"]], logs)
    summary, log = (open(os.path.join(logs, name), encoding="utf-8").read()
                    for name in ("summary.txt", "noisy.log"))
    shutil.rmtree(logs)
    if (not summary.startswith("NOISY noisy (") or "warning: unused" not in log
            or not summary.splitlines()[-1].startswith("wall time ")):
        problems.append(f"  --logs: summary {summary!r}, log {log!r}")
    table = ({"id": "a", "group": "x"}, {"id": "b", "group": "y"},
             {"id": "u", "group": "union", "groups": "@x,@y,a"})
    if [row["id"] for row in select("@union", table)] != ["a", "b"]:
        problems.append("  groups: @union does not stand for the rows of @x and @y once each")
    lanes_table = [{"id": row_id, "group": group, "argv": _sh("true"), "builds": row_id == "b"}
                   for row_id, group in (("a", "x"), ("b", "x"), ("c", "x"), ("d", "y"))]
    with contextlib.redirect_stdout(out := io.StringIO()):
        run_gates(lanes_table, jobs=2)
    order = re.findall(r"^PASS (\S+)", out.getvalue(), re.M)
    if order[:2] != ["a", "b"] or sorted(order[2:]) != ["c", "d"]:
        problems.append(f"  builds: ran {order}; a lane's rows up to its build run before any lane")
    return problems


def _schedule_problems(rows):
    """Lanes start longest first by the newest record beside `--logs`, an
    unrecorded row before any, an `apart` lane split into its rows; a run over
    its wall time is a finding."""
    problems = []
    root = tempfile.mkdtemp(prefix="gate-history-")
    for run, body, age in (("old", "PASS short (500s)\n", 60),
                           ("new", "PASS short (2s)\nNO ANSWER long (90s)\nPASS y1 (30s)\n"
                                   "PASS y2 (70s)\nwall time 192s\n", 0)):
        os.makedirs(os.path.join(root, run))
        summary = os.path.join(root, run, "summary.txt")
        with open(summary, "w", encoding="utf-8") as fh:
            fh.write(body)
        os.utime(summary, (time.time() - age,) * 2)
    table = [{"id": row_id, "group": "x", "apart": True} for row_id in ("short", "long", "unseen")]
    table += [{"id": row_id, "group": "y"} for row_id in ("y1", "y2")]
    order = [[row["id"] for row in unit]
             for unit in plan(table, recorded(os.path.join(root, "this-run")))[1]]
    shutil.rmtree(root)
    if order != [["unseen"], ["y1", "y2"], ["long"], ["short"]]:
        problems.append(f"  plan: units {order}; longest first, unrecorded before any")
    for wall, want in ((0, FINDING), (TIMEOUT, OK)):
        with contextlib.redirect_stdout(out := io.StringIO()):
            got = run_gates([rows["clean"]], wall=wall)
        if got != want or (wall == 0) != ("over the 0s a run may take" in out.getvalue()):
            problems.append(f"  --wall {wall}: returned {got}, expected {want}")
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
    return self_test_verdict("run_gates registry", problems, f" ({len(GATES)} gates declared)")


USAGE = """usage: run_gates.py --self-test
       run_gates.py --list [all|@<group>|<id>[,<id>...]]
       run_gates.py [--logs DIR] [--jobs N] [--wall SECONDS] --run all|@<group>|<id>[,<id>...]

--jobs defaults to the CPU count; --logs files each row's log and the summary,
and orders the lanes by the durations the runs beside it recorded."""


def _command(row):
    """A row's command as `--list` prints it."""
    if "groups" in row:
        return f"= {row['groups']}"
    words = [f"cd {row['cwd']} &&"] if row.get("cwd") else []
    words += ["env", *(f"{k}={v}" for k, v in row["env"].items())] if row.get("env") else []
    return " ".join(words + row["argv"])


def _main(argv):
    logs = wall = None
    jobs = os.cpu_count() or 1
    if argv[:1] == ["--logs"] and len(argv) > 2:
        logs, argv = argv[1], argv[2:]
    if argv[:1] == ["--jobs"] and len(argv) > 2 and argv[1].isdigit():
        jobs, argv = int(argv[1]), argv[2:]
    if argv[:1] == ["--wall"] and len(argv) > 2 and argv[1].isdigit():
        wall, argv = int(argv[1]), argv[2:]
    if argv == ["--self-test"]:
        return max(self_test(), _contract_self_test(), _registry_self_test())
    if argv[:1] == ["--list"] and len(argv) <= 2 or argv[:1] == ["--run"] and len(argv) == 2:
        try:
            rows = select(argv[1]) if len(argv) == 2 else None
        except KeyError as exc:
            print(f"run_gates.py: {exc}", file=sys.stderr)
            print(USAGE, file=sys.stderr)
            return NO_ANSWER
        if argv[0] == "--run" and rows is not None:
            return run_gates(rows, logs, jobs, wall)
        for row in GATES if rows is None else rows:
            print(f"{row['id']:<26} {row['group']:<8} {_command(row)}")
        return OK
    print(USAGE, file=sys.stderr)
    # Not a gate result: the runner itself was called wrongly, which is the
    # caller's own mistake and not a verdict about the tree.
    return TOOL_MISSING


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
