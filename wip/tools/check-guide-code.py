#!/usr/bin/env python3
"""Report guide code lines that are not in the tracked example they show.

    wip/tools/check-guide-code.py <example> <page.md>...

<example> is the folder name under sdk/<lang>/examples/ (tradedesk, agentdesk,
echo). Every fenced code block inside a language tab of a page (### Java,
### Rust, ...) is compared line by line, whitespace stripped, against every
file of that language's example; a line found in none is printed with the page,
its line number and the tab. Shell, output, Dockerfile and YAML blocks are
skipped, and so are lines that are only an ellipsis or a closing bracket,
and a `// File.java: what it is` header naming the file.
Exit status 1 when anything is reported.
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


def main():
    example, pages = sys.argv[1], sys.argv[2:]
    known = {lang: example_lines(lang, example) for lang in TABS.values()}
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
