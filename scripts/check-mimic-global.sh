#!/usr/bin/env bash
# Fails when a test source sets Mimic's global mode without releasing it.
#
# `Mimic.set_mimic_global/0` is process-group state. Mimic returns it to
# `:private` only when it sees the owner go down, and it monitors the owner
# only once that owner registers a stub — so a module whose last-run test
# stubs nothing leaves `:global` behind, and the next module to call `stub/3`
# from its own pid dies with "only the global owner is allowed". Every test in
# it fails, and which module pays depends on ExUnit's order, so the blame lands
# on whichever change last added or removed a test module.
#
# `FlezhaTest.Helper.MimicGlobal.set_mimic_global_until_exit/1` sets the mode
# and releases it in `on_exit`. Its own definition is the one place that must
# still call the raw function, and it is exempt BY PATH — the patterns stay
# narrow rather than being weakened to spare it.
#
# `Mimic.set_mimic_from_context/1` is the same door with the mode chosen from
# `async:`, and every module here is `async: false`, so it is refused too: say
# which mode you mean instead of deriving it.
#
# Scope is `git ls-files test/`, so only test sources are policed; the raw call
# has no place in `lib/` at all. Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import gate_support

WRAPPER = "test/helpers/mimic_global.ex"

# `set_mimic_global_until_exit` shares the prefix, so the patterns are pinned
# to what follows it: a call's `(`, or the atom's word boundary. Neither can
# match the wrapper's name.
PATTERNS = [re.compile(p) for p in (
    # `Mimic.set_mimic_global()`, and the bare call under `import Mimic`.
    r"\bset_mimic_global\s*\(",
    # `setup :set_mimic_global`, and any other apply-by-atom.
    r":set_mimic_global\b",
    # The context-derived door: `async: false` takes the global arm.
    r"\bset_mimic_from_context\b",
)]

sys.exit(gate_support.walk(
    check=gate_support.pattern_check(PATTERNS),
    pathspec=("test/",),
    path_filter=lambda rel: rel != WRAPPER,
    prefilter=lambda text: "set_mimic" in text,
    headline="a test source sets Mimic's stub mode directly, so nothing "
             "releases it:",
    hint="Use `import FlezhaTest.Helper.MimicGlobal` and "
         "`setup :set_mimic_global_until_exit`, which releases the mode in "
         "`on_exit`; a module that wants private mode says "
         f"`setup :set_mimic_private`. Only {WRAPPER} may call the raw "
         "function.",
    ok_message="mimic global mode OK",
))
PY
