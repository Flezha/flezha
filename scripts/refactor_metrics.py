#!/usr/bin/env python3
"""Measure the repository's structure per area and ratchet it (charter §11).

    refactor_metrics.py [--area NAME] [--json] [--lint]
    refactor_metrics.py --write-baseline [--timing NAME=SECONDS]... [--bench NAME=VALUE]...
    refactor_metrics.py --check [--timing NAME=SECONDS]...   # docs/refactor/baseline.json
    refactor_metrics.py --targets                             # docs/refactor/targets.md
    refactor_metrics.py --self-test

One area table, one classifier per language: a line is blank, code, a plain
comment or a doc comment; a file is production or test by its path, and a Rust
file is test inside each `#[cfg(test)] mod … { }` block. Duplicates are jscpd's (`.jscpd.json`),
cycles `mix xref graph --format cycles`, lint counts credo and clippy under `--lint` (slow,
opt-in). A ratcheted metric must never rise; timings tolerate 5 % and compare only on the
recording box; an area under `blocking` in the baseline fails `--check`, the rest only report.
"""

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASELINE = "docs/refactor/baseline.json"
TARGETS = "docs/refactor/targets.md"
JSCPD = "sdk/node/node_modules/.bin/jscpd"

FILE_CEILING = 800
FUNCTION_CEILING = 50
NAME_CEILING = 32
TIMING_TOLERANCE = 0.05

RATCHETED = (
    "prod_loc narrative_markers files_over_800 functions_over_50 long_names duplicate_pairs "
    "cycles async_false_share sleeps refutes credo_warnings clippy_warnings"
).split()
LISTED = {
    m: f"{m}_list" for m in "files_over_800 functions_over_50 long_names duplicate_pairs".split()
}
BY_FILE = {m: f"{m}_by_file" for m in "prod_loc narrative_markers sleeps refutes".split()}
COLUMNS = list(
    zip(
        RATCHETED[:1] + ["test_loc", "comment_density"] + RATCHETED[1:],
        "prod test cmt% narr >800 fn>50 name>32 dups cyc async_f sleep refute credo clippy".split(),
    )
)
LANG_EXTS = {
    "rust": "rs",
    "elixir": "ex exs",
    "java": "java",
    "ts": "ts mts js mjs cjs",
    "python": "py",
    "ruby": "rb gemspec",
    "php": "php",
}
LANG_BY_EXT = {f".{e}": lang for lang, exts in LANG_EXTS.items() for e in exts.split()}
GENERATED = tuple(f"{c}/src/generated/" for c in "flz_core flz_transport sdk/rust".split())
CRATES_AND_SDKS = (
    "flz_core flz_transport flz_nif flz_core_ffi flezha-macros sdk/rust sdk/java sdk/node "
    "sdk/python sdk/ruby sdk/php cli lib/mix"
).split()

# --- areas -------------------------------------------------------------------


def areas():
    """The area table: name -> (roots, excluded prefixes). Controller subtrees
    are read from lib/flezha so a new one becomes an area without an edit."""
    table = {name: ([f"{name}/"], []) for name in CRATES_AND_SDKS}
    lib = os.path.join(ROOT, "lib/flezha")
    subtrees = sorted(d for d in os.listdir(lib) if os.path.isdir(os.path.join(lib, d)))
    table["lib/flezha"] = (["lib/flezha.ex", "lib/flezha/"], [f"lib/flezha/{s}/" for s in subtrees])
    for sub in subtrees:
        table[f"lib/flezha/{sub}"] = ([f"lib/flezha/{sub}/"], [])
    table["test"] = (["test/"], ["test/apps/"])
    table["test/apps"] = (["test/apps/"], [])
    return table


def tracked_files():
    out = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True)
    return [p for p in out.stdout.decode().split("\0") if p]


def language(path):
    if os.path.basename(path) == "Rakefile":
        return "ruby"
    return LANG_BY_EXT.get(os.path.splitext(path)[1])


def in_area(path, roots, excluded):
    inside = any(path.startswith(r) or path == r.rstrip("/") for r in roots)
    return inside and not any(path.startswith(e) for e in excluded)


def is_test_path(path):
    """Test code by location: the language's test directory, or a test app."""
    name = os.path.basename(path)
    by_dir = path.startswith("test/") or any(
        m in path for m in "/tests/ /test/ /benches/ /types-tests/ src/test/".split()
    )
    return (
        by_dir
        or path.endswith(("_tests.rs", "_test.rb", "Test.java"))
        or (name.startswith("test_") and name.endswith(".py"))
    )


# --- line classifiers ----------------------------------------------------------


def _block(lines, line_comment):
    """C-family: `/** */` doc, `/* */` plain, `//` (and `#` for PHP) plain."""
    state = None
    for raw in lines:
        s = raw.strip()
        if state:
            yield state
            state = None if "*/" in s else state
        elif not s:
            yield "blank"
        elif s.startswith("/*"):
            state = "doc" if s.startswith("/**") and s != "/**/" else "comment"
            yield state
            state = None if "*/" in s[2:] else state
        elif s.startswith(line_comment):
            yield "comment"
        else:
            yield "code"


def _fenced(lines, opens_doc, comment):
    """Elixir and Python: a doc block runs from its opener to the closing fence."""
    fence = None
    for raw in lines:
        s = raw.strip()
        if fence:
            yield "doc"
            fence = None if fence in s else fence
            continue
        opener = opens_doc(s)
        if opener is not None:
            yield "doc"
            fence = opener
        elif not s:
            yield "blank"
        elif s.startswith(comment):
            yield "comment"
        else:
            yield "code"


DOC_ATTR = re.compile(r"^@(moduledoc|doc|typedoc)\b(.*)$")


def _elixir_doc(s):
    m = DOC_ATTR.match(s)
    if not m:
        return None
    return next((f for f in ('"""', "'''") if m.group(2).count(f) == 1), False)


def _python_doc(s):
    for fence in ('"""', "'''"):
        if s.startswith(fence) or s.startswith("r" + fence):
            return fence if s.count(fence) == 1 else False
    return None


def classify_rust(lines):
    for raw in lines:
        s = raw.strip()
        if not s:
            yield "blank"
        else:
            yield (
                "doc"
                if s.startswith(("///", "//!"))
                else "comment"
                if s.startswith("//")
                else "code"
            )


def classify_ruby(lines):
    in_block = False
    for raw in lines:
        s = raw.strip()
        if in_block:
            yield "comment"
            in_block = not s.startswith("=end")
        elif s.startswith("=begin"):
            yield "comment"
            in_block = True
        else:
            yield "blank" if not s else "comment" if s.startswith("#") else "code"


CLASSIFIERS = {
    "rust": classify_rust,
    "elixir": lambda lines: _fenced(lines, _elixir_doc, "#"),
    "python": lambda lines: _fenced(lines, _python_doc, "#"),
    "java": lambda lines: _block(lines, ("//",)),
    "ts": lambda lines: _block(lines, ("//",)),
    "php": lambda lines: _block(lines, ("//", "#")),
    "ruby": classify_ruby,
}

CFG_TEST = re.compile(r"^\s*#\[cfg\((?:test|all\(test\b[^)]*\))\)\]")
MOD_OPEN = re.compile(r"^\s*(pub(\([^)]*\))?\s+)?mod\s+\w+\s*\{")


def mod_close(lines, start):
    """Index past the `}` closing the module opened at `start`: the first one at its own
    indentation (the formatter's shape — a brace count closes early on a raw string), else EOF."""
    indent = lines[start][: len(lines[start]) - len(lines[start].lstrip())]
    closes = (k + 1 for k in range(start + 1, len(lines)) if lines[k].rstrip() == indent + "}")
    return next(closes, len(lines))


def rust_test_regions(lines):
    """(start, end) of every `#[cfg(test)]` that opens a `mod`."""
    regions, i = [], 0
    while i < len(lines):
        if CFG_TEST.match(lines[i]):
            j = i + 1
            while j < len(lines) and (not lines[j].strip() or lines[j].strip().startswith("#[")):
                j += 1
            if j < len(lines) and MOD_OPEN.match(lines[j]):
                regions.append((i, mod_close(lines, j)))
                i = regions[-1][1]
                continue
        i += 1
    return regions


# --- functions and names --------------------------------------------------------

KEYWORDS = set(
    "if for while switch catch else try return new do match unless case with throw".split()
)
MODIFIERS = (
    r"(?:(?:public|private|protected|static|final|synchronized|abstract|default|native)\s+)*"
)
DEF = {
    "rust": re.compile(
        r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:const\s+|async\s+|unsafe\s+|extern\s+\"C\"\s+)*fn\s+(\w+)"
    ),
    "elixir": re.compile(r"^(\s*)(?:def|defp|defmacro|defmacrop)\s+([a-z_]\w*[?!]?)"),
    "python": re.compile(r"^(\s*)(?:async\s+)?def\s+(\w+)"),
    "ruby": re.compile(r"^(\s*)def\s+(?:self\.)?([\w?!=\[\]<>+\-*/%]+)"),
    "php": re.compile(r"^\s*" + MODIFIERS + r"function\s+(\w+)\s*\("),
    "java": re.compile(
        r"^\s*"
        + MODIFIERS
        + r"(?:<[^>]+>\s+)?[\w<>\[\],.?]+\s+(\w+)\s*\([^;{]*\)?\s*(?:throws[\w., ]+)?\s*\{?\s*$"
    ),
    "ts": re.compile(
        r"^\s*(?:export\s+)?(?:(?:async|static|private|public|protected|get|set)\s+)*(?:function\s+)?(\w+)\s*(?:<[^>]*>)?\s*\([^)]*\)\s*(?::\s*[^{=]+)?\s*(?:=>)?\s*\{\s*$"
    ),
}
ARROW = re.compile(
    r"^\s*(?:export\s+)?(?:const|let)\s+(\w+)\s*=\s*(?:async\s*)?(?:\([^)]*\)|\w+)\s*(?::\s*[^=]+)?=>\s*\{\s*$"
)
STRING = re.compile(r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'')
LINE_COMMENT = re.compile(r"//.*$|#.*$")


def brace_extent(lines, start):
    """Lines from `start` to the brace that closes the first `{` after it."""
    depth, opened = 0, False
    for i in range(start, len(lines)):
        text = LINE_COMMENT.sub("", STRING.sub('""', lines[i]))
        if not opened and "{" not in text and ";" in text:
            return None
        for ch in text:
            depth += (ch == "{") - (ch == "}")
            opened |= ch == "{"
            if opened and depth == 0:
                return i - start + 1
    return None


def indent_extent(lines, start, indent, closes_with_end):
    """Lines to the `end` at the definition's indentation, or to the last
    non-blank line before the next one at that indentation."""
    last = start
    for i in range(start + 1, len(lines)):
        line = lines[i]
        if not line.strip():
            continue
        if len(line) - len(line.lstrip()) <= indent:
            if closes_with_end and line.strip().startswith("end"):
                return i - start + 1
            break
        last = i
    return last - start + 1


def functions(lines, lang):
    """(name, start_line, length) for every definition the shape finds."""
    found = []
    for i, line in enumerate(lines):
        m = DEF[lang].match(line) or (ARROW.match(line) if lang == "ts" else None)
        if not m or m.group(m.lastindex) in KEYWORDS:
            continue
        if lang in ("elixir", "python", "ruby"):
            one_liner = lang == "elixir" and re.search(r",\s*do:", line)
            length = 1 if one_liner else indent_extent(lines, i, len(m.group(1)), lang != "python")
        else:
            length = brace_extent(lines, i)
            if length is None:
                continue
        found.append((m.group(m.lastindex), i + 1, length))
    return found


NARRATIVE = re.compile(
    r"\b(used to|before this change|first draft|previously|originally|no longer|"
    r"the old (?:way|code|behaviou?r|design))\b|\b20\d\d-\d\d-\d\d\b|\btasks?\.md\b|"
    r"\bopenspec/changes\b|\bD\d{1,2}\b|\bPR\s*#\d+|\bproposal\b",
    re.IGNORECASE,
)
SLEEP = {
    "elixir": re.compile(r"\bProcess\.sleep\(|:timer\.sleep\("),
    "rust": re.compile(r"\bthread::sleep\(|\bsleep\(Duration"),
    "java": re.compile(r"\bThread\.sleep\("),
    "python": re.compile(r"\btime\.sleep\(|\basyncio\.sleep\("),
    "ruby": re.compile(r"(?<![\w.])sleep[ (]"),
    "php": re.compile(r"\b(?:u)?sleep\("),
    "ts": re.compile(r"setTimeout\(\s*(?:resolve|r)\b"),
}
REFUTE = re.compile(r"\brefute_receive\b")
ASYNC_CASE = re.compile(r"^\s*use\s+[\w.]*Case\b(.*)$", re.MULTILINE)


# --- measurement -------------------------------------------------------------


def read_lines(path):
    try:
        with open(os.path.join(ROOT, path), encoding="utf-8") as fh:
            lines = fh.read().split("\n")
    except (UnicodeDecodeError, OSError):
        return None
    return lines[:-1] if lines and lines[-1] == "" else lines


def measure_file(path):
    """Everything one file contributes; None when the file is not source."""
    lang = language(path)
    lines = read_lines(path) if lang and not path.startswith(GENERATED) else None
    return None if lines is None else measure_lines(path, lang, lines)


def measure_lines(path, lang, lines):
    """Test throughout for a test path; a Rust file only inside its `#[cfg(test)]` modules."""
    whole = is_test_path(path)
    regions = rust_test_regions(lines) if lang == "rust" and not whole else []
    in_test = [whole or any(s <= i < e for s, e in regions) for i in range(len(lines))]
    counts = defaultdict(int)
    for i, kind in enumerate(CLASSIFIERS[lang](lines)):
        if kind == "code":
            counts["test_loc" if in_test[i] else "prod_loc"] += 1
        elif kind != "blank":
            counts[f"{kind}_lines"] += 1
            counts["narrative_markers"] += bool(NARRATIVE.search(lines[i]))
        if in_test[i]:
            counts["sleeps"] += bool(SLEEP[lang].search(lines[i]))
            counts["refutes"] += bool(lang == "elixir" and REFUTE.search(lines[i]))
    fns = [f for f in functions(lines, lang) if not in_test[f[1] - 1]]
    is_exunit = lang == "elixir" and path.endswith("_test.exs")
    return {
        "lines": len(lines),
        "counts": counts,
        "long_functions": [f"{path}:{s} {n} ({ln})" for n, s, ln in fns if ln > FUNCTION_CEILING],
        "long_names": [f"{path}:{s} {n}" for n, s, _ in fns if len(n) > NAME_CEILING],
        "async_case": _async_case(lines) if is_exunit else None,
    }


def _async_case(lines):
    m = ASYNC_CASE.search("\n".join(lines))
    return None if not m else "async: true" in m.group(1)


def jscpd_pairs(roots):
    """Cross-file duplicate pairs from jscpd, as (a, b, 'a:s-e <-> b:s-e')."""
    binary = os.path.join(ROOT, JSCPD)
    if not os.access(binary, os.X_OK):
        return None
    with tempfile.TemporaryDirectory() as tmp:
        dirs = sorted({r.rstrip("/") for r in roots if os.path.isdir(os.path.join(ROOT, r))})
        cmd = [binary, "--config", ".jscpd.json", "--output", tmp, "--absolute", "--silent", *dirs]
        subprocess.run(cmd, cwd=ROOT, capture_output=True)
        report = os.path.join(tmp, "jscpd-report.json")
        if not os.path.exists(report):
            return None
        with open(report, encoding="utf-8") as fh:
            dups = json.load(fh)["duplicates"]
    pairs = []
    for d in dups:
        a, b = d["firstFile"], d["secondFile"]
        pa, pb = (os.path.relpath(x["name"], ROOT) for x in (a, b))
        where = f"{pa}:{a['start']}-{a['end']} <-> {pb}:{b['start']}-{b['end']}"
        pairs.append((pa, pb, f"{where} ({d.get('tokens', '?')} tokens)"))
    return pairs


def run(cmd):
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)


def xref_cycles():
    """Member file lists of every cycle `mix xref` reports, or None without mix."""
    out = run(["mix", "xref", "graph", "--format", "cycles"]) if shutil.which("mix") else None
    if out is None or out.returncode != 0:
        return None
    cycles = []
    for line in out.stdout.split("\n"):
        if line.startswith("Cycle of length"):
            cycles.append([])
        elif cycles and line.strip().startswith(("lib/", "test/")):
            cycles[-1].append(line.strip())
    return cycles


def credo_warnings():
    if not shutil.which("mix"):
        return None
    try:
        issues = json.loads(run(["mix", "credo", "--strict", "--format", "json"]).stdout)["issues"]
    except (ValueError, KeyError):
        return None
    return [
        (i["filename"], f"{i['filename']}:{i['line_no']} {i['check'].rsplit('.', 1)[-1]}")
        for i in issues
    ]


def clippy_warnings():
    if not shutil.which("cargo"):
        return None
    # The feature `make stabilize-test-check` builds with; two sdk/rust test targets need it.
    cmd = "cargo clippy --workspace --all-targets --features flezha_runner/bench-internals".split()
    out = run(cmd + ["--message-format=json"])
    seen = set()
    for line in out.stdout.split("\n"):
        msg = json.loads(line) if line.startswith("{") else {}
        m = msg.get("message", {})
        if (
            msg.get("reason") == "compiler-message"
            and m.get("level") == "warning"
            and m.get("spans")
        ):
            span = next((s for s in m["spans"] if s.get("is_primary")), m["spans"][0])
            code = (m.get("code") or {}).get("code", "warning")
            seen.add((span["file_name"], span["line_start"], code))
    return [(f, f"{f}:{ln} {c}") for f, ln, c in sorted(seen)]


def summarise(files, pairs, cycles, credo, clippy):
    """One area's entry from its files and the repo-wide facts."""
    a = defaultdict(int)
    by_file = {key: {} for key in BY_FILE.values()}
    for p, m in files.items():
        for k, v in m["counts"].items():
            a[k] += v
        for metric, key in BY_FILE.items():
            if m["counts"][metric]:
                by_file[key][p] = m["counts"][metric]
    lists = {
        "files_over_800": sorted(
            f"{p} ({m['lines']})" for p, m in files.items() if m["lines"] > FILE_CEILING
        ),
        "functions_over_50": sorted(x for m in files.values() for x in m["long_functions"]),
        "long_names": sorted(x for m in files.values() for x in m["long_names"]),
        "duplicate_pairs": sorted(
            {s for pa, pb, s in pairs or [] if pa != pb and (pa in files or pb in files)}
        ),
    }
    async_cases = [m["async_case"] for m in files.values() if m["async_case"] is not None]
    comments = a["comment_lines"] + a["doc_lines"]
    total = a["prod_loc"] + a["test_loc"] + comments
    entry = {
        k: a[k]
        for k in "prod_loc test_loc comment_lines doc_lines narrative_markers sleeps refutes".split()
    }
    entry.update(
        files=len(files),
        test_files=len(async_cases),
        comment_density=round(comments / total, 3) if total else 0.0,
        cycles=sum(any(f in files for f in c) for c in cycles) if cycles is not None else None,
        async_false_share=round(1 - sum(async_cases) / len(async_cases), 3)
        if async_cases
        else None,
        credo_warnings=sum(f in files for f, _ in credo) if credo is not None else None,
        clippy_warnings=sum(f in files for f, _ in clippy) if clippy is not None else None,
        **by_file,
    )
    for metric, items in lists.items():
        entry[metric] = None if metric == "duplicate_pairs" and pairs is None else len(items)
        entry[LISTED[metric]] = items
    return entry


def measure(area_filter=None, lint=False):
    """Every area's metrics plus the repo-wide facts they were derived from."""
    table = {k: v for k, v in areas().items() if not area_filter or k in area_filter}
    per_file = {}
    for path in tracked_files():
        m = measure_file(path) if any(in_area(path, r, e) for r, e in table.values()) else None
        if m:
            per_file[path] = m
    pairs = jscpd_pairs([r for roots, _ in table.values() for r in roots])
    cycles = xref_cycles() if any(k.startswith(("lib/", "test")) for k in table) else None
    credo = credo_warnings() if lint else None
    clippy = clippy_warnings() if lint else None
    result = {}
    for name, (roots, excluded) in table.items():
        files = {p: m for p, m in per_file.items() if in_area(p, roots, excluded)}
        result[name] = summarise(files, pairs, cycles, credo, clippy)
    repo = {
        "cycles": len(cycles) if cycles is not None else None,
        "cycle_sizes": sorted((len(c) for c in cycles), reverse=True)
        if cycles is not None
        else None,
        "credo_warnings": len(credo) if credo is not None else None,
        "clippy_warnings": len(clippy) if clippy is not None else None,
    }
    return {"areas": result, "repo": repo}


def box():
    cpu = [ln for ln in read_lines("/proc/cpuinfo") or [] if ln.startswith("model name")]
    cpu = cpu[0].split(":", 1)[1].strip() if cpu else "unknown"
    governor = read_lines("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor") or ["unknown"]
    return {"hostname": socket.gethostname(), "cpu": cpu, "governor": governor[0].strip()}


def parse_kv(items):
    return {name: float(value) for name, _, value in (item.partition("=") for item in items or [])}


# --- ratchet -------------------------------------------------------------------


def culprits_for(metric, then, now):
    if metric in LISTED:
        return sorted(set(now.get(LISTED[metric], [])) - set(then.get(LISTED[metric], [])))
    key = BY_FILE.get(metric)
    if not key:
        return []
    grown = {f: v - then.get(key, {}).get(f, 0) for f, v in now.get(key, {}).items()}
    return [f"{f} (+{d})" for f, d in sorted(grown.items(), key=lambda kv: -kv[1]) if d > 0][:5]


def compare(current, baseline, timings=None, current_box=None):
    """Regressions as (area, metric, baseline, now, culprits); a metric absent on
    either side is skipped, a timing is compared only on the recording box."""
    regressions = []
    for area, now in current["areas"].items():
        then = baseline["areas"].get(area, {})
        for metric in RATCHETED:
            b, c = then.get(metric), now.get(metric)
            if b is not None and c is not None and c > b:
                regressions.append((area, metric, b, c, culprits_for(metric, then, now)))
    same_box = current_box and baseline.get("box", {}).get("hostname") == current_box["hostname"]
    for name, seconds in (timings or {}).items() if same_box else ():
        then = baseline.get("timings", {}).get(name)
        if then is not None and seconds > then["seconds"] * (1 + TIMING_TOLERANCE):
            regressions.append(("timings", name, then["seconds"], seconds, []))
    return regressions


def exit_code(regressions, baseline):
    blocking = set(baseline.get("blocking", []))
    return 1 if any(area in blocking for area, *_ in regressions) else 0


def render_table(data):
    rows = [["area"] + [short for _, short in COLUMNS]]
    for name, m in data["areas"].items():
        rows.append([name] + ["-" if m[c] is None else str(m[c]) for c, _ in COLUMNS])
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    cells = lambda r: (
        v.ljust(w) if i == 0 else v.rjust(w) for i, (v, w) in enumerate(zip(r, widths))
    )
    return "\n".join("  ".join(cells(r)) for r in rows)


def render_targets(data, measured_head):
    def by_size(item):
        return -int(item[0].rsplit("(", 1)[1].split()[0].rstrip(")"))

    sections = (
        (f"Files over {FILE_CEILING} lines", "files_over_800_list", by_size),
        (f"Production functions over {FUNCTION_CEILING} lines", "functions_over_50_list", by_size),
        (f"Production names over {NAME_CEILING} characters", "long_names_list", None),
        ("Cross-file duplicate pairs (jscpd, ≥ 40 tokens)", "duplicate_pairs_list", None),
    )
    out = [f"# Refactor targets (measured at `{measured_head}`)", ""]
    out += ["Rendered by `scripts/refactor_metrics.py --targets`; the ceilings are charter §7.", ""]
    for title, key, order in sections:
        items = sorted(
            {(x, a) for a, m in data["areas"].items() for x in m[key]}, key=order or (lambda t: t)
        )
        out += ["", f"## {title} ({len(items)})", ""] + [f"- `{x}` — {a}" for x, a in items]
    return "\n".join(out) + "\n"


# --- self-test ----------------------------------------------------------------

KIND = {"b": "blank", "c": "code", "C": "comment", "d": "doc"}
# source, one kind letter per line, (name, length) per definition
FIXTURES = {
    "rust": (
        "//! crate doc\n/// fn doc\npub fn short(a: u32) -> u32 {\n    // plain\n    a\n}",
        "ddcCcc",
        [("short", 4)],
    ),
    "elixir": (
        'defmodule A do\n  @moduledoc """\n  Owns the thing.\n  """\n  # plain\n  @doc "one line"\n'
        "  def go(x), do: x\n  defp long_name_that_is_far_beyond_the_ceiling(x) do\n    Process.sleep(10)\n"
        "    x\n  end\nend",
        "cdddCdcccccc",
        [("go", 1), ("long_name_that_is_far_beyond_the_ceiling", 4)],
    ),
    "java": ("/** doc */\n// plain\npublic int f(int a) {\n  return a;\n}", "dCccc", [("f", 3)]),
    "ts": (
        "/** doc */\nexport function f(a: number): number {\n  return a;\n}\nconst g = async (x) => {\n  return x;\n};",
        "dcccccc",
        [("f", 3), ("g", 3)],
    ),
    "python": (
        '"""Module doc\nspanning."""\n# plain\ndef f(a):\n    """One-line doc."""\n    return a\n\nx = 1',
        "ddCcdcbc",
        [("f", 3)],
    ),
    "ruby": ("# plain\ndef f(a)\n  a\nend\n=begin\nblock\n=end", "CcccCCC", [("f", 3)]),
    "php": (
        "<?php\n/** doc */\n# plain\nfunction f(int $a): int {\n    return $a;\n}",
        "cdCccc",
        [("f", 3)],
    ),
}


def self_test_classifiers():
    for lang, (source, kinds, fns) in FIXTURES.items():
        lines = source.split("\n")
        assert list(CLASSIFIERS[lang](lines)) == [KIND[k] for k in kinds], (
            lang,
            list(CLASSIFIERS[lang](lines)),
        )
        assert [(n, ln) for n, _, ln in functions(lines, lang)] == fns, (
            lang,
            functions(lines, lang),
        )
    rust = FIXTURES["rust"][0].split("\n") + [
        "",
        "#[cfg(test)]",
        "mod tests {",
        "    fn t() { std::thread::sleep(d); }",
        "}",
        "pub fn after(x: u32) -> u32 {",
        "    x",
        "}",
        "#[cfg(all(test, unix))]",
        "mod more {",
        "}",
    ]
    assert rust_test_regions(rust) == [(7, 11), (14, 17)] and SLEEP["rust"].search(rust[9])
    assert not CFG_TEST.match('#[cfg(any(test, feature = "testing"))]')
    m = measure_lines("x.rs", "rust", rust)
    assert (m["counts"]["prod_loc"], m["counts"]["test_loc"], m["counts"]["sleeps"]) == (6, 7, 1)
    assert [n for n, _, _ in functions(rust, "rust")] == ["short", "t", "after"]
    assert measure_file("flz_core/src/generated/runner_commands.rs") is None
    assert len(functions(FIXTURES["elixir"][0].split("\n"), "elixir")[1][0]) > NAME_CEILING
    assert SLEEP["elixir"].search("    Process.sleep(10)") and REFUTE.search("refute_receive {:x}")
    assert (
        _async_case(["use ExUnit.Case, async: true"]) is True
        and _async_case(["use Flezha.TestCase"]) is False
    )
    assert NARRATIVE.search("// this used to be a list") and NARRATIVE.search("# see D3")
    assert NARRATIVE.search("// 2026-08-28 incident") and not NARRATIVE.search("// bounded at 3 ms")


def self_test_ratchet():
    def area(loc, over, listed):
        return {
            "prod_loc": loc,
            "files_over_800": over,
            "files_over_800_list": listed,
            "prod_loc_by_file": {"x": loc},
        }

    base = {
        "box": {"hostname": "here"},
        "timings": {"suite": {"seconds": 100.0}},
        "areas": {"a": area(10, 0, [])},
    }
    cur = {"areas": {"a": area(12, 1, ["big (900)"])}}
    regs = compare(cur, base, {"suite": 104.0}, {"hostname": "here"})
    assert [(r[1], r[4]) for r in regs] == [
        ("prod_loc", ["x (+2)"]),
        ("files_over_800", ["big (900)"]),
    ], regs
    assert compare(cur, base, {"suite": 106.0}, {"hostname": "here"})[-1][1] == "suite"
    assert not any(
        r[0] == "timings" for r in compare(cur, base, {"suite": 106.0}, {"hostname": "elsewhere"})
    )
    assert exit_code(regs, {"blocking": []}) == 0 and exit_code(regs, {"blocking": ["a"]}) == 1


# --- main --------------------------------------------------------------------


def parse_args(argv):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--area", action="append", help="restrict to this area (repeatable)")
    ap.add_argument("--json", action="store_true", help="print the full JSON instead of the table")
    ap.add_argument("--lint", action="store_true", help="also run credo and clippy (slow)")
    ap.add_argument("--timing", action="append", metavar="NAME=SECONDS", help="suite wall-clock")
    ap.add_argument("--bench", action="append", metavar="NAME=VALUE", help="bench headline number")
    ap.add_argument("--write-baseline", action="store_true", help=f"write {BASELINE}")
    ap.add_argument("--check", action="store_true", help=f"compare the tree to {BASELINE}")
    ap.add_argument("--targets", action="store_true", help=f"write {TARGETS}")
    ap.add_argument("--self-test", action="store_true")
    return ap.parse_args(argv)


def write_baseline(data, baseline, args):
    stamp = {**data["box"], "head": data["head"]}
    data["blocking"] = baseline.get("blocking", [])
    for key, unit, arg in (("timings", "seconds", args.timing), ("bench", "value", args.bench)):
        given = {n: {unit: v, **stamp} for n, v in parse_kv(arg).items()}
        data[key] = given or baseline.get(key, {})
    with open(os.path.join(ROOT, BASELINE), "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print(f"wrote {BASELINE} at {data['head']} on {data['box']['hostname']}")


def check(data, baseline, args):
    if not baseline:
        print(f"no {BASELINE}; run --write-baseline first", file=sys.stderr)
        return 2
    regressions = compare(data, baseline, parse_kv(args.timing), data["box"])
    blocking = set(baseline.get("blocking", []))
    for area, metric, then, now, culprits in regressions:
        tag = "BLOCKING" if area in blocking else "measuring"
        where = f"  [{', '.join(culprits)}]" if culprits else ""
        print(f"{tag} {area} {metric}: {then} -> {now}{where}")
    code = exit_code(regressions, baseline)
    print(f"{len(regressions)} regression(s) against {baseline.get('head')}; exit {code}")
    return code


def main(argv):
    args = parse_args(argv)
    if args.self_test:
        self_test_classifiers()
        self_test_ratchet()
        print("self-test OK")
        return 0
    baseline_path = os.path.join(ROOT, BASELINE)
    baseline = {}
    if os.path.exists(baseline_path):
        with open(baseline_path, encoding="utf-8") as fh:
            baseline = json.load(fh)
    data = measure(args.area, lint=args.lint)
    data["head"], data["box"] = run(["git", "rev-parse", "--short", "HEAD"]).stdout.strip(), box()
    if args.write_baseline:
        write_baseline(data, baseline, args)
    elif args.targets:
        with open(os.path.join(ROOT, TARGETS), "w", encoding="utf-8") as fh:
            fh.write(render_targets(data, data["head"]))
        print(f"wrote {TARGETS}")
    elif args.check:
        return check(data, baseline, args)
    elif args.json or args.area:
        print(json.dumps(data, indent=1, sort_keys=True))
    else:
        print(render_table(data))
        print(f"\nrepo: {data['repo']}  head {data['head']}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
