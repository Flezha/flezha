#!/usr/bin/env python3
"""Extract the controller functions a consumer checkout calls.

`flezha_console` reaches this controller over its HTTP API, naming each
operation by the module and function it calls, so nothing at compile time
links the two trees and a rename here breaks the console silently. This script
reads the consumer's own source and emits the one thing a gate in this
repository can check: `Module.function/arity`, one per line.

It extracts CALL SITES, never module names used as values. The distinction is
load-bearing: the consumer's fixtures carry controller modules as provider
atoms, and a resolver fed those would report two breaks for ever until the
reader learned to ignore it. So a name counts only where it is called —
`Mod.fun(...)`, `&Mod.fun/N`, or a name passed to one of the consumer's four
channel helpers — and only under the consumer's `lib/`, since a fixture is not
a call the console makes.

The four helpers and the arity each implies:

    authorized_call(token, Mod, :fun, [args])           -> length(args) + 1
    authorized_list_call(token, Mod, :fun, [args])      -> length(args) + 1
    authorized_rows_call(token, Mod, :fun, [args])      -> length(args) + 1
    authorized_stream(token, Mod, :fun, [args], pid)    -> length(args) + 1

Each sends the operation to the HTTP API, whose door runs the published
function with the caller context the token proves, so the arity is one more
than the site spells. A helper called at the end of a pipe takes its token
from the pipe, and an argument list built by a call on a written list, such as
`with_opts([namespace], opts)`, counts the written list: what the call adds
fills optional arguments. The stream helper's trailing pid receives the events
and is not an argument. The helper definitions take
`module` and `function` as parameters, which carry no `Flezha.` prefix and are
therefore skipped without needing a rule.

Usage:

    scripts/consumer_surface.py <consumer-checkout> [--out PATH]
"""

import argparse
import os
import re
import sys

MODULE = r"Flezha(?:\.[A-Z][A-Za-z0-9_]*)*"
FUN = r"[a-z_][A-Za-z0-9_]*[?!]?"

# The module argument's position, and how many arguments the helper adds.
HELPERS = {
    "authorized_call": (1, 1),
    "authorized_list_call": (1, 1),
    "authorized_rows_call": (1, 1),
    "authorized_stream": (1, 1),
}

HELPER_CALL = re.compile(r"\b(" + "|".join(HELPERS) + r")\s*\(")
# A paren is required, so a `Mod.fun/1` written in a doc or a comment is not a
# call. That exclusion is what keeps the receiver's moduledoc out of the manifest.
DIRECT_CALL = re.compile(r"(?<![\w.:@])(" + MODULE + r")\.(" + FUN + r")\s*\(")
CAPTURE = re.compile(r"&(" + MODULE + r")\.(" + FUN + r")/(\d+)")
MODULE_ARG = re.compile(r"^\s*(" + MODULE + r")\s*$")
ATOM_ARG = re.compile(r"^\s*:(" + FUN + r")\s*$")
LIST_ARG = re.compile(r"^\s*\[")
WRAPPED_LIST_ARG = re.compile(r"^\s*[a-z_][A-Za-z0-9_]*\(\s*\[")
PIPED = re.compile(r"\|>\s*$")

OPENERS, CLOSERS = "([{", ")]}"


def balanced(text, open_index):
    """The text between `text[open_index]` and its matching close bracket."""
    depth = 0
    for i in range(open_index, len(text)):
        char = text[i]
        if char in OPENERS:
            depth += 1
        elif char in CLOSERS:
            depth -= 1
            if depth == 0:
                return text[open_index + 1 : i]
    return None


def split_args(text):
    """`text` split on its top-level commas, brackets and strings respected."""
    parts, depth, current, quote = [], 0, [], None
    for char in text:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
            continue
        if char in "\"'":
            quote = char
        elif char in OPENERS:
            depth += 1
        elif char in CLOSERS:
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return [p for p in parts if p.strip()]


def line_of(text, index):
    return text.count("\n", 0, index) + 1


def helper_sites(text):
    """Every `Module.function/arity` reached through one of the helpers."""
    for match in HELPER_CALL.finditer(text):
        helper = match.group(1)
        module_at, added = HELPERS[helper]
        if PIPED.search(text, 0, match.start()):
            module_at -= 1
        args = balanced(text, match.end() - 1)
        if args is None:
            continue
        parts = split_args(args)
        if len(parts) < module_at + 3:
            continue
        module = MODULE_ARG.match(parts[module_at])
        function = ATOM_ARG.match(parts[module_at + 1])
        arg_list = parts[module_at + 2]
        listed = LIST_ARG.match(arg_list) or WRAPPED_LIST_ARG.match(arg_list)
        if not (module and function and listed):
            continue
        inner = balanced(arg_list, arg_list.index("["))
        arity = len(split_args(inner)) + added
        yield module.group(1), function.group(1), arity, match.start()


def direct_sites(text):
    """Every `Module.function(...)` and `&Module.function/N` written out."""
    for match in DIRECT_CALL.finditer(text):
        args = balanced(text, match.end() - 1)
        if args is None:
            continue
        yield match.group(1), match.group(2), len(split_args(args)), match.start()
    for match in CAPTURE.finditer(text):
        yield match.group(1), match.group(2), int(match.group(3)), match.start()


def extract(root):
    """`{"Mod.fun/arity": [(path, line), ...]}` for the consumer's own `lib/`."""
    lib = os.path.join(root, "lib")
    if not os.path.isdir(lib):
        sys.exit(f"{root} carries no lib/, so it is not a consumer checkout")

    sites = {}
    for dirpath, _dirnames, filenames in os.walk(lib):
        for filename in sorted(filenames):
            if not filename.endswith((".ex", ".exs")):
                continue
            path = os.path.join(dirpath, filename)
            with open(path, encoding="utf-8") as handle:
                text = handle.read()
            rel = os.path.relpath(path, root)
            found = list(helper_sites(text)) + list(direct_sites(text))
            for module, function, arity, index in found:
                key = f"{module}.{function}/{arity}"
                sites.setdefault(key, []).append((rel, line_of(text, index)))
    return sites


def render(sites, root):
    modules = {key.rsplit(".", 1)[0] for key in sites}
    total = sum(len(v) for v in sites.values())
    width = max(len(key) for key in sites) if sites else 0

    lines = [
        "# The controller functions flezha_console calls.",
        "#",
        "# Generated by scripts/consumer_surface.py; do not hand-edit. The console",
        "# names each operation it sends to this controller's HTTP API by module and",
        "# function, so no compiler links the two trees and a rename here breaks it",
        "# silently. test/flezha/api/consumer_surface_test.exs",
        "# resolves every line below against this tree's real exports and reports an",
        "# unresolved one as moved, privatised or withdrawn.",
        "#",
        "# The trailing comment on each line is a navigation aid into the consumer's",
        "# tree. Only the Module.function/arity set is compared when the manifest is",
        "# regenerated, so a line shifting in the consumer does not fail this tree.",
        "#",
        f"# {total} call sites, {len(sites)} distinct functions, {len(modules)} modules.",
        "#",
        "# Every name below is a call another repository makes, not a name this one",
        "# uses, so the whole payload is a record: a retired module path appearing",
        "# here is the break the gate exists to report.",
        "# module-naming-record:begin",
    ]
    for key in sorted(sites):
        where = sites[key]
        plural = "site" if len(where) == 1 else "sites"
        lines.append(f"{key.ljust(width)}  # {len(where)} {plural}, {where[0][0]}:{where[0][1]}")
    lines.append("# module-naming-record:end")
    lines.append(f"# Extracted from {os.path.basename(os.path.normpath(root))}/lib.")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("checkout", help="path to the consumer checkout")
    parser.add_argument("--out", help="write here instead of stdout")
    args = parser.parse_args()

    text = render(extract(args.checkout), args.checkout)
    if args.out:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
