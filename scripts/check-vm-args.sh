#!/usr/bin/env bash
# Fails when the VM arguments the release ships and the configuration reference
# disagree.
#
# The Erlang VM is configured before the application starts, so no flag here can
# be a row of `lib/flezha/config/keys.ex` and nothing in the four configuration
# layers covers it: no compile step reads an args file and no suite does either.
# A flag added to the release and left undocumented is a behaviour change nobody
# can find, and a flag the reference presents as shipped that no template sets
# sends a user off overriding a default that was never there.
#
# The check is both directions and it is exact: the reference's block for a
# template must carry, as code spans, exactly the flag lines that template sets.
# A value change therefore has to be documented too, not just a new flag name.
#
# SCOPE IS THE FIRST COLUMN OF A TABLE ROW INSIDE THE MARKER BLOCK, not the
# document and not the whole block. Prose elsewhere naming a flag a user
# may set, and the reference names several it deliberately does not ship, is
# out of scope by construction, and so is a flag named in the row's own
# explanation. A gate that goes red on correct prose gets switched off, and this
# repository has paid for one.
#
# Takes no arguments. Exit 0 clean, 1 on disagreement, 3 when the gate cannot
# judge (a missing template, a missing or unclosed marker block).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import gate_support

REFERENCE = "docs/public/configuration/vm_arguments.md"
TEMPLATES = ["rel/vm.args.eex", "rel/remote.vm.args.eex"]

# The first column of a table row: `| `+sbwt none` | why |`. Anchored there so the
# why-column may name any flag it likes without being read as a claim that the
# release ships it. Whitespace is squashed either side, so reindenting a
# template cannot invalidate its own documentation.
SPAN = re.compile(r"^\|\s*`([+-][^`\n]*)`", re.M)


def squash(text):
    return " ".join(text.split())


def cannot_judge(reason):
    """This gate has no answer either way, which is not the same as a clean tree."""
    print(reason, file=sys.stderr)
    sys.exit(gate_support.NO_ANSWER)


def read(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError as exc:
        cannot_judge(f"cannot read {path}, so no flag can be judged: {exc}")


def shipped(path):
    """The flag lines a template sets. `#` opens a comment in vm.args."""
    lines = read(path).split("\n")
    return {squash(line) for line in lines if line.strip() and not line.lstrip().startswith("#")}


def documented(text, path):
    """The flag spans the reference's block for `path` carries."""
    begin, end = f"<!-- vm-args:{path}:begin -->", f"<!-- vm-args:{path}:end -->"
    if text.count(begin) != 1 or text.count(end) != 1:
        cannot_judge(f"{REFERENCE} does not carry exactly one {begin} … {end} block, "
                     f"so what it documents for {path} cannot be read")
    start, stop = text.index(begin) + len(begin), text.index(end)
    if stop < start:
        cannot_judge(f"{REFERENCE} closes the {path} block before it opens it")
    return {squash(span) for span in SPAN.findall(text[start:stop])}


reference = read(REFERENCE)

failures = []
judged = 0

for template in TEMPLATES:
    sets = shipped(template)
    says = documented(reference, template)
    judged += len(sets | says)
    for flag in sorted(sets - says):
        failures.append(f"{template} sets `{flag}`, which {REFERENCE} does not document")
    for flag in sorted(says - sets):
        failures.append(f"{REFERENCE} documents `{flag}` as shipped, which {template} does not set")

sys.exit(gate_support.report(
    failures,
    headline="the VM arguments the release ships and the configuration reference disagree:",
    hint=f"Every flag a template sets belongs in that template's block in {REFERENCE}, "
         f"as a code span, value included. A flag the reference only recommends belongs "
         f"outside the block.",
    noun="disagreement(s)",
    ok_message=f"VM arguments OK. {judged} flag(s) judged across {len(TEMPLATES)} template(s).",
))
PY
