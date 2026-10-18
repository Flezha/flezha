"""Production code: the one definition the line budget and the ceiling gate read.

`wip/tools/prod-lines.sh` prints the production lines per repository and a total
from it; `scripts/check_ceilings.py` measures the size ceilings over the same
files. Left out of production:

- tests, fixtures, examples, benches and docs, by path (EXCLUDE);
- generated files: `@generated` in the first five lines (prost output);
- Rust test code: items under `#[cfg(test)]` or `#[cfg(all(test, ..))]`, and
  every file such an item declares as `mod x;` (with its `#[path]`, if any),
  together with that file's own submodule directory. `cfg(any(test, ..))` is
  production, since a feature can build it in.

Files are read as bytes (latin-1), so a count never depends on the locale.

    python3 scripts/production_code.py      # the table prod-lines.sh prints
"""

import os
import re
import subprocess
import sys

EXCLUDE = re.compile(r"(^|/)(test|tests|spec|fixtures|examples?|benches)/|_test\.|Test\.java$"
                     r"|\.md$|^flezha-sdk-examples/")

# Row name, repository, the pathspecs its production code lives under.
ROWS = [
    ("core-lib", ".", ["lib"]),
    ("core-flz_core", ".", ["flz_core/src"]),
    ("core-flz_core_ffi", ".", ["flz_core_ffi/src"]),
    ("core-flz_nif", ".", ["flz_nif/src"]),
    ("core-flz_transport", ".", ["flz_transport/src"]),
    ("core-cli", ".", ["cli/src"]),
    ("sdk-elixir", "sdk/elixir", ["lib"]),
    ("sdk-elixir-native", "sdk/elixir", [":(glob)native/*/src/**"]),
    ("sdk-java", "sdk/java", [":(glob)*/src/main/**"]),
    ("sdk-node", "sdk/node", ["src"]),
    ("sdk-php", "sdk/php", ["src"]),
    ("sdk-python", "sdk/python", ["src"]),
    ("sdk-ruby", "sdk/ruby", ["lib"]),
    ("sdk-rust", "sdk/rust", ["src"]),
    ("sdk-rust-macros", "sdk/rust", ["macros/src"]),
]

TEST_ITEM = re.compile(r"#\[cfg\((test\)|all\(test[ ,)])")
PATH_ATTR = re.compile(r'#\[path *= *"([^"]*)"\]')
MOD_DECL = re.compile(r"mod[ \t]+([A-Za-z0-9_]+)[ \t]*;")
RAW_PREFIX = re.compile(r"(^|[^A-Za-z0-9_])b?r(#*)$")
CHAR_LITERAL = re.compile(r"'(\\[^']+|\\'|[^\\']|[\x80-\xff]+)'")


def rows():
    """Every row: the fixed ones, then one per plugin package."""
    plugins = sorted(name for name in os.listdir("plugins") if os.path.isdir(f"plugins/{name}"))
    return ROWS + [(name, f"plugins/{name}", ["lib"]) for name in plugins]


def row_files(repo, specs, untracked=False):
    """The row's files, relative to `repo`: tracked, and with `untracked` also
    the new files git does not ignore. A tracked file deleted from the working
    tree but not staged is not listed: the tree no longer carries it."""
    extra = ["-co", "--exclude-standard"] if untracked else []
    listed = subprocess.run(["git", "-C", repo, "ls-files", "-z", *extra, "--", *specs],
                            capture_output=True, check=True).stdout.decode()
    return [rel for rel in listed.split("\0")
            if rel and not EXCLUDE.search(rel) and os.path.isfile(os.path.join(repo, rel))]


def read_lines(path):
    """The file's lines as awk reads them: split on newline, a last line without
    one still a line."""
    with open(path, "rb") as fh:
        text = fh.read().decode("latin-1")
    lines = text.split("\n")
    return lines[:-1] if text.endswith("\n") else lines


class Lexer:
    """One Rust line at a time with string, char and comment contents removed; a
    block comment, string or raw string is carried over to the next line."""

    def __init__(self):
        self.state, self.depth, self.closer = 0, 0, ""

    def line(self, s):
        out = ""
        while s:
            if self.state == 1:
                m = re.search(r"/\*|\*/", s)
                if not m:
                    break
                s = s[m.end():]
                self.depth += 1 if m.group() == "/*" else -1
                self.state = 0 if self.depth == 0 else 1
            elif self.state == 2:
                m = re.search(r'\\.|"', s)
                if not m:
                    break
                s = s[m.end():]
                self.state = 0 if m.group() == '"' else 2
            elif self.state == 3:
                at = s.find(self.closer)
                if at < 0:
                    break
                s, self.state = s[at + len(self.closer):], 0
            else:
                out, s, done = self._plain(out, s)
                if done:
                    return out
        return out

    def _plain(self, out, s):
        m = re.search(r"[\"'/]", s)
        if not m:
            return out + s, "", True
        out, s = out + s[:m.start()], s[m.start():]
        if s[0] == '"':
            raw = RAW_PREFIX.search(out)
            self.state, self.closer = (3, '"' + raw.group(2)) if raw else (2, "")
            return out, s[1:], False
        if s[0] == "/":
            if s.startswith("//"):
                return out, "", True
            if s.startswith("/*"):
                self.state, self.depth = 1, 1
                return out, s[2:], False
            return out + "/", s[1:], False
        char = CHAR_LITERAL.match(s)
        return (out, s[char.end():], False) if char else (out + "'", s[1:], False)


class TestItem:
    """The brace and bracket depth of one `#[cfg(test)]` item, and the `mod x;`
    it declares when it is a declaration."""

    def __init__(self):
        self.depth, self.nest, self.head, self.path, self.declared = 0, 0, "", "", None

    def ends(self, code):
        for c in code:
            if c == "{":
                self.depth += 1
            elif c in "([":
                self.nest += 1
            elif c in ")]":
                self.nest -= 1
            elif c == "}":
                self.depth -= 1
                if self.depth <= 0:
                    return True
            elif c == ";" and self.depth == 0 and self.nest <= 0:
                decl = MOD_DECL.search(self.head)
                self.declared = decl.group(1) if decl else None
                return True
        return False


def scan_rust(lines):
    """(line numbers inside `#[cfg(test)]` items, [(module, #[path]) declared])."""
    lexer, item, inside, declared = Lexer(), None, set(), []
    for number, raw in enumerate(lines, 1):
        code = lexer.line(raw)
        if item is None:
            start = TEST_ITEM.search(code)
            if not start:
                continue
            item, code = TestItem(), code[start.start():]
        inside.add(number)
        if item.depth == 0:
            attr = PATH_ATTR.search(raw)
            item.path = attr.group(1) if attr else item.path
            item.head += " " + code
        if item.ends(code):
            if item.declared:
                declared.append((item.declared, item.path))
            item = None
    return inside, declared


def resolve(declaring, name, path, files):
    """The file a `mod name;` in `declaring` loads, by Rust's rules."""
    folder, base = os.path.split(declaring)
    if path:
        return os.path.normpath(os.path.join(folder, path))
    if base not in ("mod.rs", "lib.rs", "main.rs"):
        folder = os.path.join(folder, base[:-3])
    candidate = os.path.join(folder, f"{name}.rs")
    return candidate if candidate in files else os.path.join(folder, name, "mod.rs")


def classify(repo, files):
    """{file: (lines as awk reads them, generated?, inline test line numbers)} and
    the set of path prefixes and files that are Rust test code."""
    facts, declarations = {}, []
    for rel in files:
        lines = read_lines(os.path.join(repo, rel))
        if not lines:
            continue
        generated = any("@generated" in line for line in lines[:5])
        inside = set()
        if rel.endswith(".rs") and not generated:
            inside, declared = scan_rust(lines)
            declarations += [(rel, name, path) for name, path in declared]
        facts[rel] = (lines, generated, inside)
    tests = {resolve(rel, name, path, facts) for rel, name, path in declarations}
    prefixes = {t[:-len("mod.rs")] if t.endswith("/mod.rs") else t[:-3] + "/" for t in tests}
    return facts, tests, prefixes


def is_test_file(rel, tests, prefixes):
    return rel in tests or any(rel.startswith(prefix) for prefix in prefixes)


def production_lines(repo, files):
    """The row's newline count, less every line that is not production."""
    facts, tests, prefixes = classify(repo, files)
    total = nonprod = 0
    for rel in files:
        with open(os.path.join(repo, rel), "rb") as fh:
            total += fh.read().count(b"\n")
    for rel, (lines, generated, inside) in facts.items():
        if generated or is_test_file(rel, tests, prefixes):
            # A file awk stops reading after five lines is short of its length here too.
            nonprod += len(lines) if generated or rel.endswith(".rs") else min(len(lines), 5)
        else:
            nonprod += len(inside)
    return total - nonprod


def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    total = 0
    for name, repo, specs in rows():
        count = production_lines(repo, row_files(repo, specs))
        total += count
        print(f"{name:<28} {count:7d}")
    print(f"{'total':<28} {total:7d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
