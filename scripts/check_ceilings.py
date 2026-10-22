"""The size ceilings of docs/public/contributing/code_standards.md, measured: no
production file over 800 lines, no function over 50, and the controller's
compile cycles (`mix xref graph --format cycles`: how many, and how many files
the largest holds).

WHAT IS MEASURED. Production code as scripts/production_code.py defines it, the
definition the line budget reads; its Rust test code (a `#[cfg(test)]` item and
every file one declares) and generated files are not measured, and a new file
not yet staged is. A file is its newline count. A function runs from its head
to its closing line: a brace match with strings and comments blanked for Rust,
Java, TypeScript/JavaScript, PHP and Go; the `end` at the head's indent for Elixir;
indentation for Python and Ruby.

THE BASELINE. scripts/ceilings_baseline.txt records every breaker the tree held
when the gate landed, with its size, and the two cycle numbers. It is recorded
debt and MAY ONLY SHRINK (`load_baseline` in scripts/naming_gate.py): a breaker
outside it, or one that grew, is a finding; an entry the tree no longer holds at
that size is stale until it is lowered or deleted, so a commit that shrinks a
breaker lowers its line in the same commit.

    python3 scripts/check_ceilings.py [--write-baseline | --self-test]

Exit codes are the runner's (scripts/naming_gate.py): 3 when the baseline or
`mix xref` gave no answer.
"""

import os
import re
import subprocess
import sys

import naming_gate
import production_code

FILE_MAX, FN_MAX = 800, 50
BASELINE = "scripts/ceilings_baseline.txt"
BASELINE_HEADER = """\
# Ceiling breakers when scripts/check_ceilings.py recorded this tree: files
# over 800 lines, functions over 50, and the controller's compile cycles.
# Recorded debt, not an exemption: the gate fails on a breaker outside this
# list or larger than its entry, and on an entry the tree no longer holds at
# that size. Lower or delete a line in the commit that shrinks its breaker.
#
# One `<file> <lines>`, `<file>::<function> <lines>` or `xref <measure> <n>`
# per line, sorted.
"""

SRC_EXT = {".ex", ".exs", ".rs", ".java", ".ts", ".js", ".mjs", ".py", ".rb", ".php", ".go"}

RS_FN = re.compile(r'^\s*(?:pub(?:\([^)]*\))?\s+)?(?:default\s+)?(?:const\s+)?(?:async\s+)?'
                   r'(?:unsafe\s+)?(?:extern\s+"[^"]*"\s+)?fn\s+(\w+)')
JAVA_FN = re.compile(r'^\s*(?:@\w+(?:\([^)]*\))?\s+)*(?:(?:public|private|protected|static|final|'
                     r'abstract|synchronized|default|native|strictfp)\s+)*(?:<[^>]+>\s+)?'
                     r'(?:[\w.$]+(?:<.*>)?(?:\[\])*\s+)?(\w+)\s*\(')
TS_FN = re.compile(r'^\s*(?:export\s+)?(?:default\s+)?(?:(?:public|private|protected|static|async|'
                   r'readonly|override|get|set)\s+)*(?:function\s*\*?\s*(\w+)|'
                   r'(\w+)\s*(?:<[^>]*>)?\s*\(|'
                   r'(?:const|let|var)\s+(\w+)\s*(?::[^=]+)?=\s*(?:async\s+)?(?:\([^)]*\)|\w+)\s*'
                   r'(?::[^=]+)?=>)')
PHP_FN = re.compile(r'^\s*(?:(?:public|private|protected|static|final|abstract)\s+)*'
                    r'function\s+&?(\w+)\s*\(')
GO_FN = re.compile(r'^func\s+(?:\([^)]*\)\s*)?(\w+)')
BRACE_FN = {".rs": RS_FN, ".java": JAVA_FN, ".ts": TS_FN, ".js": TS_FN, ".mjs": TS_FN,
            ".php": PHP_FN, ".go": GO_FN}
NOT_FN = {"if", "for", "while", "switch", "catch", "return", "new", "else", "do", "try",
          "synchronized", "super", "this", "throw", "case", "function", "await", "typeof", "sizeof"}
EX_FN = re.compile(r'^(\s*)(def|defp|defmacro|defmacrop)\s+([\w?!]+)')
PY_FN = re.compile(r'^(\s*)(?:async\s+)?def\s+(\w+)')
RB_FN = re.compile(r'^(\s*)def\s+(?:self\.)?([\w?!=\[\]<>+\-*/%]+)')


def blank(text):
    return re.sub(r"[^\n]", " ", text)


def skip_literal(text, i, ext):
    """End offset of the string or comment opening at `i`, or None."""
    two = text[i:i + 2]
    if two == "//" or (ext == ".php" and text[i] == "#" and two != "#["):
        j = text.find("\n", i)
        return len(text) if j < 0 else j
    if two == "/*":
        j = text.find("*/", i + 2)
        return len(text) if j < 0 else j + 2
    if ext == ".java" and text[i:i + 3] == '"""':
        j = text.find('"""', i + 3)
        return len(text) if j < 0 else j + 3
    if text[i] in "\"`'":
        j = i + 1
        while j < len(text) and text[j] != text[i]:
            j += 2 if text[j] == "\\" else 1
        return min(j + 1, len(text))
    return None


def strip_code(text, ext):
    """`text` with strings and comments blanked, newlines kept, so braces can be
    matched. Rust is lexed as the production definition lexes it."""
    if ext == ".rs":
        lexer = production_code.Lexer()
        return "\n".join(lexer.line(line) for line in text.split("\n"))
    out, i = [], 0
    while i < len(text):
        end = skip_literal(text, i, ext)
        if end is None:
            out.append(text[i])
            i += 1
        else:
            out.append(blank(text[i:end]))
            i = end
    return "".join(out)


def brace_end(code, start):
    """Offset of the `}` closing the first `{` after `start`; None for a bodiless declaration."""
    nest, i = 0, start
    while i < len(code) and not (code[i] == "{" and nest <= 0):
        if code[i] in "([":
            nest += 1
        elif code[i] in ")]":
            nest -= 1
        elif code[i] == ";" and nest <= 0:
            return None
        i += 1
    depth = 0
    for j in range(i, len(code)):
        depth += {"{": 1, "}": -1}.get(code[j], 0)
        if depth == 0 and code[j] == "}":
            return j
    return None


def brace_functions(text, ext, test_lines=frozenset()):
    """(name, length) of every function in a brace language whose head is not on
    one of `test_lines` (1-based)."""
    code = strip_code(text, ext)
    line_of = lambda off: code.count("\n", 0, off)
    offset = 0
    for idx, line in enumerate(code.split("\n")):
        head, start, offset = BRACE_FN[ext].match(line), offset, offset + len(line) + 1
        if not head or idx + 1 in test_lines:
            continue
        name = next(group for group in head.groups() if group)
        if name in NOT_FN or re.search(rf"\b(record|class|interface|enum)\s+{name}\b", line):
            continue
        end = brace_end(code, start + head.end() - (line[head.end() - 1:head.end()] == "("))
        if end is not None:
            yield name, line_of(end) - idx + 1


def indent_functions(lines, head_rx, closes):
    """(name, length) of every function whose body is its deeper-indented lines."""
    indent = lambda s: len(s) - len(s.lstrip(" "))
    for idx, line in enumerate(lines):
        head = head_rx.match(line)
        if not head:
            continue
        depth, j = len(head.group(1)), idx + 1
        while j < len(lines) and (not lines[j].strip() or indent(lines[j]) > depth
                                  or lines[j].lstrip().startswith(("#", ")", "]", "}"))):
            j += 1
        end = j if j < len(lines) and closes(lines[j].strip()) else j - 1
        while end > idx and not lines[end].strip():
            end -= 1
        yield head.group(head.lastindex), end - idx + 1


def functions(text, ext, test_lines):
    if ext in BRACE_FN:
        return brace_functions(text, ext, test_lines)
    lines = text.split("\n")
    if ext in (".ex", ".exs"):
        return indent_functions(lines, EX_FN,
                                lambda s: s == "end" or s.startswith(("end ", "end)")))
    if ext == ".rb":
        return indent_functions(lines, RB_FN, lambda s: s == "end" or s.startswith("end "))
    return indent_functions(lines, PY_FN, lambda s: False) if ext == ".py" else []


def production_files():
    """(path from the root, text, Rust test line numbers) of every production
    source file, tracked or new and not ignored."""
    for _, repo, specs in production_code.rows():
        files = production_code.row_files(repo, specs, untracked=True)
        facts, tests, prefixes = production_code.classify(repo, files)
        for rel, (lines, generated, test_lines) in facts.items():
            if (generated or os.path.splitext(rel)[1] not in SRC_EXT
                    or production_code.is_test_file(rel, tests, prefixes)):
                continue
            path = rel if repo == "." else f"{repo}/{rel}"
            with open(path, "rb") as fh:
                yield path, fh.read().decode("latin-1"), test_lines


def breakers(files):
    """{entry key: size} of every file and function over its ceiling."""
    found = {}
    for path, text, test_lines in files:
        if text.count("\n") > FILE_MAX:
            found[path] = text.count("\n")
        seen = {}
        for name, length in functions(text, os.path.splitext(path)[1], test_lines):
            seen[name] = seen.get(name, 0) + 1
            if length > FN_MAX:
                key = f"{path}::{name}" + (f"#{seen[name]}" if seen[name] > 1 else "")
                found[key] = length
    return found


def cycles():
    return parse_cycles(subprocess.run(["mix", "xref", "graph", "--format", "cycles"],
                                       capture_output=True, text=True, check=True).stdout)


def parse_cycles(out):
    """{"xref cycles": n, "xref largest": files in the largest}; a member annotated
    (compile) or (export) is counted."""
    count = re.search(r"^(\d+) cycles? found", out, re.M)
    blocks = re.split(r"^Cycle of length \d+.*$", out, flags=re.M)[1:]
    largest = max((len(re.findall(r"^\s+\S+\.exs?\b", block, re.M)) for block in blocks), default=0)
    return {"xref cycles": int(count.group(1)) if count else 0, "xref largest": largest}


def judge(measured, recorded):
    """(findings, hits): a breaker outside the baseline or larger than its entry is a
    finding; `hits` is every entry the tree still holds, for `stale_baseline`."""
    base = {}
    for entry in recorded:
        key, _, size = entry.rpartition(" ")
        base[key] = int(size)
    findings, hits = [], set()
    for key, size in sorted(measured.items()):
        was = base.get(key)
        if was is None:
            findings.append(f"{key}: {size}, not in the baseline")
        elif size > was:
            findings.append(f"{key}: {size}, recorded at {was}")
            hits.add(f"{key} {was}")
        hits.add(f"{key} {size}")
    return findings, hits


def measure():
    return {**breakers(production_files()), **cycles()}


def main(args):
    if args == ["--self-test"]:
        return self_test()
    if args not in ([], ["--write-baseline"]):
        print("usage: check_ceilings.py [--write-baseline | --self-test]", file=sys.stderr)
        return naming_gate.NO_ANSWER
    try:
        measured = measure()
    except subprocess.CalledProcessError as exc:
        print(f"{' '.join(exc.cmd)} failed, so nothing can be judged:\n{exc.stderr}",
              file=sys.stderr)
        return naming_gate.NO_ANSWER
    if args:
        with open(BASELINE, "w", encoding="utf-8") as fh:
            fh.write(BASELINE_HEADER + "".join(f"{k} {v}\n" for k, v in sorted(measured.items())))
        print(f"recorded {len(measured)} entries in {BASELINE}")
        return naming_gate.OK
    try:
        recorded = naming_gate.load_baseline(BASELINE)
    except OSError as exc:
        print(f"cannot read {BASELINE}, so nothing can be judged: {exc}", file=sys.stderr)
        return naming_gate.NO_ANSWER
    findings, hits = judge(measured, recorded)
    found = naming_gate.report(
        findings, headline="over a ceiling (file 800, function 50, cycles may only fall):",
        hint="Split it, or shrink it back under its recorded size.", noun="breaker(s)",
        ok_message=None)
    stale = naming_gate.stale_baseline(BASELINE, recorded, hits,
                                       "hold at that size; lower or delete them")
    if not found and not stale:
        print(f"ceilings OK — {len(recorded)} recorded, 0 new or grown")
    return found or stale


def self_test():
    """The judgement over known cases: a new breaker, a grown one, a stale entry
    and a cycle count that rose are each a finding; the recorded tree is clean."""
    recorded = {"lib/a.ex 900", "lib/a.ex::run 60", "xref cycles 4", "xref largest 143"}
    clean = {"lib/a.ex": 900, "lib/a.ex::run": 60, "xref cycles": 4, "xref largest": 143}
    cases = {
        "recorded": (clean, 0, 0),
        "new breaker": ({**clean, "lib/b.ex": 801}, 1, 0),
        "grown breaker": ({**clean, "lib/a.ex::run": 61}, 1, 0),
        "stale entry": ({k: v for k, v in clean.items() if k != "lib/a.ex"}, 0, 1),
        "shrunk breaker": ({**clean, "lib/a.ex": 850}, 0, 1),
        "cycle rise": ({**clean, "xref cycles": 5}, 1, 0),
    }
    problems = []
    for name, (measured, new, stale) in cases.items():
        findings, hits = judge(measured, recorded)
        if (len(findings), len(recorded - hits)) != (new, stale):
            problems.append(f"  {name}: {len(findings)} finding(s), {len(recorded - hits)} stale")
    lines = ["fn ok() {", *["    x();"] * 3, "}", "#[cfg(test)]", "mod tests {",
             "    fn long() {", *["        y();"] * 60, "    }", "}"]
    xref = ("2 cycles found. Showing them in decreasing size:\n\nCycle of length 3 (1 compile):\n\n"
            "    lib/a.ex\n    lib/b.ex (compile)\n    lib/c.ex (export)\n\n"
            "Cycle of length 2:\n\n    lib/d.ex\n    lib/e.ex\n")
    if (parsed := parse_cycles(xref)) != {"xref cycles": 2, "xref largest": 3}:
        problems.append(f"  annotated cycle members: parsed {parsed}")
    test_lines, _ = production_code.scan_rust(lines)
    measured = dict(brace_functions("\n".join(lines), ".rs", test_lines))
    if measured != {"ok": 5}:
        problems.append(f"  rust test module: measured {measured}")
    with naming_gate.scratch_tree({"kept.rs": "fn f() {}\n", "gone.rs": "fn f() {}\n"}) as repo:
        os.remove("gone.rs")
        listed = production_code.row_files(repo, ["."], untracked=True)
        if listed != ["kept.rs"]:
            problems.append(f"  tracked file deleted, not staged: listed {listed}")
    return naming_gate.self_test_verdict("check_ceilings", problems, f" ({len(cases)} cases)")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.exit(main(sys.argv[1:]))
