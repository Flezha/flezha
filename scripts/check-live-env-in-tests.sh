#!/usr/bin/env bash
# Fails when a test sets an OS variable a runner reads while it is running.
#
# `Flezha.SDK.Knob.get/2` calls `System.get_env/1` on every call, so a row it
# reads is consulted at each construction rather than once at boot. An OS
# variable is process-wide, so a test that writes one is not choosing a value
# for itself: it is choosing it for whatever else in the run builds the same
# thing while the variable is set, and `async: false` does not help: a process
# an earlier module left running reads the value the current module set.
#
# The remedy is to state the quantity where the thing is built.
# `Flezha.SDK.Hold.new/1` takes `:cap` and `Flezha.SDK.Relay.Server` takes
# `:hold_cap`, so a test that needs a small reconnect ring says so and depends
# on no global at all.
#
# THE VARIABLES ARE DERIVED, NOT LISTED. A row that declares `:elixir_sdk`
# among its `runtimes` is a name the runner reads in its own process, and its
# `env:` is the variable that carries it. Both row tables are read, so a row
# moving between them changes nothing here. A run that derives an implausibly
# small set says so and answers NO_ANSWER rather than reporting a clean tree:
# a gate at zero sites cannot tell a closed rule from a drifted row shape.
#
# THE NAME IS RESOLVED, NOT MATCHED. A write whose name is spelled anywhere but
# at the call is the obvious way around a gate like this one, so a literal, a
# module attribute, a local binding and `Keys.env_name(:row)` all resolve to
# the same name, and a write this cannot resolve at all is a finding rather
# than a pass. Naming the row instead of the variable is not a way out: it is
# the same write, spelled through the table.
#
# THE BASELINE, scripts/live-env-in-tests-baseline.txt, lists the modules whose
# result still depends on one, each with its reason. It may only shrink.
# Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

BASELINE = "scripts/live-env-in-tests-baseline.txt"
ROW_TABLES = ("lib/flezha/config/keys.ex", "lib/flezha/config/duration_rows.ex")
# Below this the derivation has stopped seeing the rows rather than the tree
# having got clean. Sixteen rows declared one when this gate landed.
FLOOR = 10

# A row opens at four spaces and runs to the next one; `env:` and `runtimes:`
# may sit on any of its lines.
ROW = re.compile(r"\n    (?=\{:)")
ENV = re.compile(r'env:\s*"([A-Z0-9_]+)"')
NAME = re.compile(r"\{:([a-z0-9_]+),")


def live_variables():
    """`{row name: variable}` for every row a runner reads in its own process."""
    rows = {}
    for path in ROW_TABLES:
        with open(path, encoding="utf-8") as fh:
            chunks = ROW.split(fh.read())
        for chunk in chunks[1:]:
            body = chunk.split("\n    {:")[0]
            env, name = ENV.search(body), NAME.match(chunk.strip())
            if env and name and ":elixir_sdk" in body:
                rows[name.group(1)] = env.group(1)
    return rows


ROWS = live_variables()
VARIABLES = set(ROWS.values())
if len(VARIABLES) < FLOOR:
    print(f"only {len(VARIABLES)} runner-read variable(s) derived from "
          f"{' and '.join(ROW_TABLES)}; the row shape has moved and this gate "
          "is measuring nothing. Fix the derivation before trusting a green.",
          file=sys.stderr)
    sys.exit(naming_gate.NO_ANSWER)

# The write, and the three ways its name reaches it other than as a literal.
WRITE = re.compile(r"System\.put_env\(\s*((?:[^,()]|\([^)]*\))+)")
LITERAL = re.compile(r'^"([^"#]*)')
ENV_NAME = re.compile(r"env_name\(:([a-z0-9_]+)\)")
# `@var "FLEZHA_X"` and `var = "FLEZHA_X"`, the prefix up to any interpolation.
ATTRIBUTE = re.compile(r'^\s*@([a-z_][a-zA-Z0-9_]*)\s+"([^"#]*)')
BINDING = re.compile(r'^\s*([a-z_][a-zA-Z0-9_]*)\s*=\s*"([^"#]*)')
NAMED_ROW = re.compile(r"^\s*([a-z_][a-zA-Z0-9_]*)\s*=\s*.*env_name\(:([a-z0-9_]+)\)")


def names_in(text):
    """What each identifier in this file stands for, when it stands for a name."""
    known = {}
    for line in text.split("\n"):
        for pattern in (ATTRIBUTE, BINDING):
            match = pattern.match(line)
            if match:
                known[match.group(1)] = match.group(2)
        match = NAMED_ROW.match(line)
        if match and match.group(2) in ROWS:
            known[match.group(1)] = ROWS[match.group(2)]
    return known


def resolve(argument, known):
    """The variable `argument` names, or None when nothing here can say."""
    argument = argument.strip()
    literal = LITERAL.match(argument)
    if literal:
        return literal.group(1)
    row = ENV_NAME.search(argument)
    if row:
        return ROWS.get(row.group(1))
    return known.get(argument.lstrip("@"))


def reads_live(name):
    # A resolved PREFIX (an interpolated name) counts when a real variable
    # could still start with it; one nothing starts with is another variable.
    return name is not None and any(v == name or v.startswith(name) for v in VARIABLES)


def check_file(text):
    known = names_in(text)

    def check(rel, line):
        match = WRITE.search(line)
        if not match:
            return []
        name = resolve(match.group(1), known)
        if name is not None and not reads_live(name):
            return []
        said = name or "the name is not spelled here"
        return [(rel, f"{line.strip()}   [{said}]")]

    return check


# `walk` gives `check` one line at a time, so the per-file bindings are read by
# a check the walk rebuilds when the path changes.
state = {"rel": None, "check": None}


def check(rel, line):
    if state["rel"] != rel:
        state["rel"] = rel
        with open(rel, encoding="utf-8") as fh:
            state["check"] = check_file(fh.read())
    return state["check"](rel, line)


sys.exit(naming_gate.walk_baselined(
    check=check,
    baseline=BASELINE,
    what="set one",
    pathspec=("test/",),
    prefilter=lambda text: "System.put_env" in text,
    headline="a test sets an OS variable a runner reads while it is running:",
    hint="State the quantity where the thing is built - `Hold.new(cap: ...)`, "
         "`start_relay(Op, hold_cap: ...)` - so no other holder in the run "
         "reads it. A module that cannot goes in " + BASELINE + " with the "
         "reason.",
    ok_message=f"live env in tests OK ({len(VARIABLES)} runner-read variables)",
    noun="site(s)",
))
PY
