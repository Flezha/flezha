#!/usr/bin/env python3
"""Report guide code lines that are not in the tracked example they show.

    wip/tools/check-guide-code.py <example> <page.md>...
    wip/tools/check-guide-code.py --budget <page.md>...
    wip/tools/check-guide-code.py --basenames

<example> is the folder name under sdk/<lang>/examples/ (tradedesk, agentdesk,
echo), or several joined by commas for a page that excerpts more than one.
Every fenced code block inside a language tab of a page (### Java,
### Rust, ...) is compared line by line, whitespace stripped, against every
file of that language's example; a line found in none is printed with the page,
its line number and the tab. Shell, output, Dockerfile and YAML blocks are
skipped, and so are lines that are only an ellipsis or a closing bracket,
and a `// File.java: what it is` header naming the file.
Exit status 1 when anything is reported.

--budget prints each page's visible lines: non-blank lines outside tab blocks plus
the longest tab of each block, what a reader sees with one language selected. A page
under docs/public/tutorials/ over BUDGET fails.

--basenames fails when two extras of mix.exs share a basename, since ExDoc names
each page after its basename.
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
TABS = {"Java": "java", "Rust": "rust", "Node": "node", "Python": "python",
        "Ruby": "ruby", "PHP": "php", "Elixir": "elixir"}
SKIP_LANGS = {"sh", "output", "dockerfile", "yaml", "json", "text", "console", ""}
SKIP_DIRS = {"target", "node_modules", "vendor", "deps", "_build", ".git", "__pycache__"}
TRIVIAL = re.compile(r"^[\s\.\}\)\];,]*$|^(end|\.\.\.)$")
# A block's first line may name the file it comes from: page-only, by convention.
HEADER = re.compile(r"^(//|#) [\w/.]+\.\w+: ")
BUDGET = 200


def example_lines(lang, example):
    lines = set()
    base = ROOT / "sdk" / lang / "examples" / example
    for path in base.rglob("*"):
        if path.is_file() and not SKIP_DIRS.intersection(path.relative_to(base).parts):
            try:
                lines.update(l.strip() for l in path.read_text().splitlines())
            except (UnicodeDecodeError, OSError):
                pass
    return lines


def visible_lines(page):
    total, tabs, current = 0, None, None
    for line in pathlib.Path(page).read_text().splitlines():
        if line.startswith("<!-- tabs-open"):
            tabs, current = {}, None
        elif line.startswith("<!-- tabs-close"):
            total += max(tabs.values(), default=0)
            tabs = None
        elif tabs is not None and re.match(r"^### \w+\s*$", line):
            current = line
            tabs[current] = 0
        elif line.strip():
            if tabs is not None and current:
                tabs[current] += 1
            else:
                total += 1
    return total


def budget(pages):
    over = 0
    for page in pages:
        count = visible_lines(page)
        limited = "docs/public/tutorials/" in str(pathlib.Path(page).resolve())
        flag = " OVER" if limited and count > BUDGET else ""
        over += bool(flag)
        print(f"{count:5} {page}{flag}")
    sys.exit(1 if over else 0)


def basenames():
    extras = re.findall(r'"(docs/[^"]+\.md)"', (ROOT / "mix.exs").read_text())
    seen, clash = {}, 0
    for path in dict.fromkeys(extras):
        name = pathlib.Path(path).name
        if name in seen:
            print(f"{name}: {seen[name]} and {path}")
            clash += 1
        seen[name] = path
    sys.exit(1 if clash else 0)


def main():
    if sys.argv[1] == "--budget":
        budget(sys.argv[2:])
    if sys.argv[1] == "--basenames":
        basenames()
    example, pages = sys.argv[1], sys.argv[2:]
    known = {lang: set().union(*(example_lines(lang, e) for e in example.split(","))) for lang in TABS.values()}
    missing = 0
    for page in pages:
        tab, fence = None, None
        for number, line in enumerate(pathlib.Path(page).read_text().splitlines(), 1):
            if fence is None:
                heading = re.match(r"^### (\w+)\s*$", line)
                if heading:
                    tab = TABS.get(heading.group(1))
                elif line.startswith("<!-- tabs-close"):
                    tab = None
                elif line.startswith("```"):
                    fence = line[3:].strip().lower()
                continue
            if line.startswith("```"):
                fence = None
                continue
            text = line.strip()
            if tab and fence not in SKIP_LANGS and not TRIVIAL.match(text) and not HEADER.match(text) and text not in known[tab]:
                print(f"{page}:{number} [{tab}] {text}")
                missing += 1
    sys.exit(1 if missing else 0)


if __name__ == "__main__":
    main()
