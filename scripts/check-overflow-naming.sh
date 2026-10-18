#!/usr/bin/env bash
# Fails when `Block` is used to name an overflow policy.
#
# The policies are `DropNothing`, `DropNewest` and `DropOldest` — each names
# what it discards. "Block" named the mechanism instead, which is why the docs
# had to gloss it, and which stops being true on an inter-tasklet channel
# once the push no longer parks a thread.
#
# "block" is ordinary English and a common programming term, so this gate
# matches only the policy's own identifiers and literals rather than the word.
# `block_parker`, `BLOCK_PARK_TIMEOUT`, `pushes_blocked_total`, the
# `*_block_steps` fixtures and every blocking call name the parking
# mechanism, which still exists on the entry path and is honestly named: all
# are out of scope by construction, not by exemption.
#
# Scope is `git ls-files`, so .git/, target/, _build/ and openspec/changes/
# (which holds the archive) are excluded by being untracked or ignored; dist/
# build artifacts and binaries are excluded explicitly. Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

PATTERNS = [re.compile(p) for p in (
    # The enum, in every SDK's spelling — `::` Rust, `.` Java, `#` javadoc link.
    r"\bOverflowPolicy[:.#]+(Block|BLOCK)\b",
    r"\bHostOverflow[:.#]+Block\b",
    # The variant standing alone where the enum is the subject: the Rust enum
    # body, the macro's expansion, Java's wire and annotation mappings.
    r"^\s*(#\[default\]\s*)?Block,\s*$",
    r"quote!\s*\{\s*Block\s*\}",
    r"\bcase\s+BLOCK\s*->",
    r'\bcase\s+"block"\s*->',
    # The declaration surface: `.on_full(...)` / `.onFull(...)` naming it.
    r"\bon_?[Ff]ull\b.{0,40}:block\b",
    r"\bonFull\s*\([^)]*\bBLOCK\b",
    r'\bon_?[Ff]ull\s*=\s*"block"',
    # The wire literal, and the atom lists that enumerate the policies. The
    # list runs both ways: the retired name is as often the last member as the
    # first, and requiring one order is how a policy list stays unread.
    r'"overflow"\s*[:,)][^"]*"block"',
    r":block\s*[,|]\s*:drop_(newest|oldest)",
    r":drop_(newest|oldest)\s*[,|]\s*:block\b",
    # The Kafka producer's `:queue_overflow`, whose value set is the same three
    # policies. Anchored on that option's own name, so the observability bus's
    # per-sink `overflow:` — a different mechanism, keeping its own vocabulary
    # — stays out of scope by construction rather than by exemption.
    r'\bqueue_overflow\b.{0,80}[:"]block\b',
    # The clause or call that takes the policy as an argument. `overflow(` with
    # the paren is the function; `overflow:` the map key is the bus's.
    r"\boverflow\([^)]*:block\b",
    # The conformance fixture and its per-SDK classes.
    r"\bbounded_mailbox_block\b",
    r"\bBoundedMailboxBlock",
    # Prose that names the policy rather than the act of blocking.
    r"`?Block`?[- ]polic(y|ies)",
)]

# The observability bus's per-sink overflow (`:drop` / `:block`) is a separate
# mechanism with its own vocabulary — its policy really is named `:block`, and
# renaming it here would widen the diff without making anything truer. Masked
# by scope so the prose pattern does not claim it.
ALLOWED = [
    ("openspec/specs/observability-foundation", re.compile(r"Block policy is opt-in")),
    # `docs/known_issues.md` records retired vocabulary as evidence: an entry
    # proving the old name is gone has to quote it. A QUOTED occurrence is a
    # mention, not a use — bare prose naming the policy is still caught, which
    # is the only shape that could reintroduce it.
    ("docs/known_issues.md", re.compile(r"""['"`]OverflowPolicy::Block['"`]""")),
    # `docs/refactor/STATE.md` is the programme board: it has to quote the
    # retired spelling to name the work that retires it, and a backticked span
    # is a quotation. Bare prose naming the policy is still caught.
    ("docs/refactor/STATE.md", re.compile(r"`[^`\n]*:block[^`\n]*`")),
]

# Inside the Kafka producer's page and spec, a bare `:block` / `"block"` names
# the queue-overflow policy and nothing else: the parking mechanism there is
# spelled `queue_block_timeout_ms`, `blocked` and `:block_timeout`, none of which
# is a bare `:block` atom. Scoping the broad shape to those paths reaches the
# prose and the scenario titles that carry no `:queue_overflow` on their own
# line — the shapes a repo-wide pattern cannot claim, because elsewhere `:block`
# is the observability bus's own policy. The producer's code is the plugin
# package's.
KAFKA_SCOPE = (
    "docs/public/plugins/kafka_producer.md",
    "openspec/specs/kafka-producer-plugin/",
)
KAFKA_POLICY = [re.compile(p) for p in (
    r":block\b",
    r'"block"',
    r"[Bb]lock[- ]mode\b",
)]

policy = naming_gate.pattern_check(PATTERNS, ALLOWED)
kafka_policy = naming_gate.pattern_check(KAFKA_POLICY)

sys.exit(naming_gate.walk(
    check=lambda rel, line: policy(rel, line)
    or (kafka_policy(rel, line) if rel.startswith(KAFKA_SCOPE) else []),
    prefilter=lambda text: "lock" in text or "LOCK" in text,
    # The gate's own files carry the retired names as data.
    self_files={"scripts/check-overflow-naming.sh", ".github/workflows/naming.yml"},
    record_tag="overflow-naming-record",
    headline="`Block` used as an overflow-policy name "
             "(the policies are DropNothing / DropNewest / DropOldest):",
    hint="A policy names what it discards. If the line means the parking "
         "mechanism rather than the policy, say `park` or `blocked` without "
         "the policy's shape (`OverflowPolicy::…`, `on_full(…)`, `onFull(…)`).",
    ok_message="overflow naming OK",
))
PY
