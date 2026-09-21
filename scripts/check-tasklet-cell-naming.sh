#!/usr/bin/env bash
# Fails when `Procedure`, `Entity` or the `Proc*` abbreviation names a level of
# the workload model. The levels are Operative → Tasklet → Cell, and the
# abbreviation of Tasklet is `Task*`.
#
# Matching is by identifier COMPONENT, not by `\b` word: `entity_key`,
# `EntityRef`, `getEntityKey` and `ENTITY_KEY` all carry the word, and a `\b`
# anchor sees none of them. A component starts at a non-letter or a capital and
# ends before a lower-case letter, so `identity` never matches.
#
# The bare word `proc` is not policed: it also means an OS or BEAM process. What
# is policed is every shape the abbreviation took as a name: `ProcJournal`,
# `EchoProc`, `proc_idx`, `seq_in_proc`, `PROC_COMMITS`, `proc.journal`,
# `proc-journal`. Rust's `proc_macro`, Erlang's `proc_lib`, Linux `/proc` and
# `ProcParser` (which parses it) are out of scope by construction.
#
# Licence and third-party-notice text is out of scope by construction too: its
# "Legal Entity" is the licence's own word and is not ours to edit.
#
# THE RECORD, scripts/tasklet-cell-naming-baseline.txt, lists `<file>: <token>`
# pairs the tree still holds on purpose. It may only shrink: an entry outside it
# fails, and so does an entry it carries that the tree no longer holds.
#
# A line that MENTIONS a retired name rather than using it, as the requirement
# retiring the names has to, sits between `tasklet-cell-naming-record:begin` and
# `:end` markers; both on one line exempt that line alone.
#
# Scope is `git ls-files` plus what the SDK submodules carry, so .git/, target/,
# _build/ and openspec/changes/ (which holds the archive) are excluded by being
# untracked or ignored.
#
#   check-tasklet-cell-naming.sh              judge the tree
#   check-tasklet-cell-naming.sh --self-test  prove the gate fails on a planted
#                                             violation and passes once reverted
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - "$@" <<'PY'
import contextlib
import io
import os
import re
import subprocess
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.abspath("scripts"))
import naming_gate

BASELINE = "scripts/tasklet-cell-naming-baseline.txt"

RETIRED = [re.compile(p) for p in (
    # The two nouns, lower / Title / UPPER, singular and plural.
    r"(?:(?<![A-Za-z])|(?<=\\[ntr]))(?:entit(?:y|ies)|procedures?)(?![a-z])",
    r"(?:Entit(?:y|ies)|Procedures?)(?![a-z])",
    r"(?<![A-Z])(?:ENTIT(?:Y|IES)|PROCEDURES?)(?![A-Z])",
    # The abbreviation as a name.
    r"(?<![A-Za-z])Procs?(?=[A-Z])(?!Parser\b)",
    r"(?<=[a-z0-9])Procs?(?![a-z])",
    r"(?<![A-Za-z])procs?_(?!macro|lib\b|parser\b|self_|open\b|close\b)",
    r"(?<=[a-z0-9])(?<!read)_procs?(?![a-z])",
    r"(?<![A-Za-z])PROCS?_",
    r"(?<![A-Za-z/.])procs?[.-](?=journal|commit|inbox|boundar|index|idx|seq)",
)]
# A backslash stays in the token: in `"\tentity=..."` the `t` is an escape, not a letter.
TOKEN = re.compile(r"[A-Za-z0-9_.\\-]+")

LEGAL = ("LICENSE", "NOTICE", "CLA.md", "THIRD_PARTY_NOTICES.md")


def in_scope(rel):
    base = os.path.basename(rel)
    return not (base.startswith(LEGAL) or "/LICENSES/" in f"/{rel}")


def hits_in(line):
    """Every token on `line` carrying a retired name."""
    return sorted({tok.group(0) for tok in TOKEN.finditer(line)
                   if any(p.search(tok.group(0)) for p in RETIRED)})


def judge():
    code = naming_gate.walk_baselined(
        check=lambda rel, line: [(f"{rel}: {token}", f"`{token}` in {line.strip()}")
                                 for token in hits_in(line)],
        baseline=BASELINE,
        what="carry that token",
        path_filter=in_scope,
        prefilter=lambda text: re.search(r"(?i)entit|proc", text) is not None,
        self_files={"scripts/check-tasklet-cell-naming.sh", BASELINE},
        record_tag="tasklet-cell-naming-record",
        headline="a retired level name found (the levels are Operative → Tasklet → Cell, "
                 "abbreviated Task*):",
        hint="Rename it to Tasklet / Cell / Task*. A name another repository owns goes in "
             f"{BASELINE} as `<file>: <token>`.",
        ok_message="tasklet/cell naming OK",
    )
    # A path is a name too: a file called after a retired level is as wrong as a line.
    named = [f"{rel}: `{token}` in its path"
             for rel in naming_gate.tracked_files(carried=True) if in_scope(rel)
             for token in hits_in(rel.replace("/", " "))]
    paths = naming_gate.report(named, headline="a path names a retired level:",
                               hint="Rename the file or directory.", noun="path(s)")
    return code or paths


# Each planted file must fail the gate, and the tree must be clean again once it
# is gone: a line, a path, and a line whose escape letter sits against the word.
PLANTED = [("planted.md", f"{line}\n") for line in (
    "an entity", "ProcedureCtx", "cell.entity_key", "ProcJournal", "seq_in_proc",
    "EchoProc", "proc.journal", "WireSlowAProcedure", '"sink\\tentity=op/p/k1"',
)] + [("lib/entity_state.ex", ""), ("src/ProcJournal.java", "")]
CLEAN = {
    "clean.md": "identity runner_identity DlqIdentity excellent cancelled Cell Tasklet\n"
                "proc_macro2 :proc_lib /proc/self/stat ProcParser javac -proc:none\n",
    "LICENSE": "Legal Entity\n",
    BASELINE: "",
}


def self_test():
    def run():
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return judge()

    failures = []
    with naming_gate.scratch_tree(CLEAN):
        if run() != 0:
            failures.append("the clean tree was not clean")
        for rel, body in PLANTED:
            planted = f"`{body.strip() or rel}`"
            naming_gate.put(rel, body)
            if run() == 0:
                failures.append(f"a planted {planted} was not caught")
            subprocess.run(["git", "rm", "-q", "-f", rel], check=True)
            if run() != 0:
                failures.append(f"the tree was not clean after removing {planted}")
        # The record may only shrink: an entry the tree no longer holds fails.
        naming_gate.put(BASELINE, "clean.md: ProcJournal\n")
        if run() == 0:
            failures.append("a stale record entry did not fail the gate")
    if failures:
        print("tasklet/cell naming self-test FAILED", file=sys.stderr)
        print("\n".join(f"  {f}" for f in failures), file=sys.stderr)
        return 1
    print(f"tasklet/cell naming self-test OK ({len(PLANTED)} planted and removed, "
          "a stale record refused)")
    return 0


if sys.argv[1:] == ["--self-test"]:
    sys.exit(self_test())
if sys.argv[1:]:
    print("usage: check-tasklet-cell-naming.sh [--self-test]", file=sys.stderr)
    sys.exit(naming_gate.NO_ANSWER)
sys.exit(judge())
PY
