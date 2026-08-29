# Refactor baseline, gates and formatters — implementation notes

Gotchas and load-bearing decisions from `refactor-0-baseline`: the measuring
script (`scripts/refactor_metrics.py`), its baseline and targets under
`docs/refactor/`, the veneer formatters behind `scripts/format-apply.sh`, the
lint-in-measuring-mode configs and the naming-gate fixes. What
`docs/refactor/charter.md` says is not repeated; what is here is what the
next session would otherwise rediscover.

## The measuring script

**A Rust file is test code from the `#[cfg(test)]` that opens a `mod`, to its
end.** Rust keeps unit tests inside the production file, so a path rule alone
counts every `mod tests` as production LOC. `rust_test_start` looks for a
`#[cfg(test)]` whose next non-attribute line is `mod … {` and treats everything
from that attribute to EOF as test. A `#[cfg(test)]` on a single `use` or `fn`
does not start the region — only the module does — and nothing after the test
module is ever production again in this tree, which is what makes "to EOF"
safe. Every other language is classified by path (`tests/`, `test/`,
`benches/`, `types-tests/`, `src/test/`, `*_tests.rs`, `*_test.rb`,
`*Test.java`, `test_*.py`, and all of `test/`).

**Ratchet semantics.** A metric in `RATCHETED` must never rise. `--check`
compares the current tree to `docs/refactor/baseline.json` per area and prints
every regression; it exits non-zero only for an area listed under the
baseline's `blocking` array, which is how an area flips from measuring to
blocking (edit the array, nothing else). A metric that is `null` on either
side is skipped, so a box without `mix` (no cycle count) or a run without
`--lint` (no credo/clippy count) never trips the gate. Culprits come from two
shapes: the four list-backed metrics (`files_over_800`, `functions_over_50`,
`long_names`, `duplicate_pairs`) diff their `*_list`, and the four per-file
metrics (`prod_loc`, `narrative_markers`, `sleeps`, `refutes`) diff their
`*_by_file` map and name the five files that grew most — which is why the
baseline carries a per-file production LOC map and is ~190 KB. Timings
tolerate 5 % and are compared only when `box.hostname` matches the baseline's;
bench numbers are recorded with the box beside them and never ratcheted — the
charter's interleaved A/B is the gate for a hot path.

**`baseline.json`'s `head` names the parent of the commit that carries it.**
The script stamps `git rev-parse HEAD` at measurement time, and the baseline
is measured on the working tree of the change before that change is committed.
Read `head` as "measured on this commit plus the change that introduced the
file", never as the commit the file lives in.

**Function and name shapes are approximate on purpose.** Elixir/Python/Ruby
extents are indentation-based (an Elixir `, do:` one-liner is length 1, a
multi-clause function counts per clause); the C-family uses brace matching
with strings and line comments stripped. The numbers are stable between runs,
which is all a ratchet needs; they are not a parser.

**The script is exactly 800 lines and every function is under 50.** It is the
tool that enforces both ceilings, so it obeys them; `ruff format` at width 100
expands a magic trailing comma into one-element-per-line, and that alone cost
~300 lines on the first draft. `scripts/` is not a formatter scope, so the
script is ruff-formatted by hand (`sdk/python/.venv/bin/ruff format
--line-length 100 scripts/refactor_metrics.py`), and a future edit that adds a
trailing comma to a collection literal will push it over the ceiling.

## Duplication

**jscpd 5 tokenises Elixir only through an explicit extension mapping.**
`elixir` is in `jscpd --list`, but a run with `--format elixir` scans zero
sources; `.jscpd.json` carries `"formatsExts": {"elixir": ["ex", "exs"], …}`
and then `lib/flezha/plugin/provider/` yields 24 sources and 12 clones. jscpd
5 is a Rust binary (`cpd 5.0.16`, no `@jscpd/*` packages), pinned in
`sdk/node/package.json` because that is the repo's one Node toolchain root;
the script calls `sdk/node/node_modules/.bin/jscpd --config .jscpd.json
--absolute` and reports `null` duplicates when the binary is absent. Cross-file
pairs are the ratcheted count; a same-file pair is in the JSON but not counted,
per D3's "≥ 2 files".

**The nine `convert_value` copies are structural twins, not textual ones.**
Each provider's `convert_value` head carries its own field list, so a 40-token
window finds the `on_input`/`on_output` forward trio and the supervisor pairs
around them and touches `convert_value` in one fragment only. The cluster the
board names is a pattern to fold (`refactor-5b-plugin`), and jscpd will not
count its removal as a duplicate-pair drop; the LOC ratchet will.

## Lint in measuring mode

**Clippy needs `--features flezha_runner/bench-internals`.** Two `sdk/rust`
test targets (`nonparking_holdback_bound`, `nonparking_backpressure`) do not
compile without it, so `cargo clippy --workspace --all-targets` is red at HEAD
for the same reason `cargo test --workspace` is; the script passes the feature
`make stabilize-test-check` builds with. Every crate carries `[lints]
workspace = true`; the two crates that had their own `[lints.rust]` table
(`flz_transport`, `sdk/rust`) lost it, because Cargo refuses a crate-local lint
table beside `workspace = true`, and the union of their `check-cfg` lists lives
in the root `[workspace.lints.rust]`.

**The credo count is dominated by two style checks the codebase contradicts.**
`.credo.exs` runs the strict default set plus `CyclomaticComplexity` at 10
through `checks.extra` (`checks.enabled` REPLACES the default set and leaves
only the one check — the first draft counted 14 issues for that reason). Of the
1349 issues, `AliasUsage` (540) and `ParenthesesOnZeroArityDefs` (427) flag
house conventions; only 14 are complexity. The count is ratcheted as measured;
the first controller area decides whether those two checks are disabled or
adopted, and either decision re-baselines.

## Formatters

**`sdk/node/types-tests/` is excluded from prettier, and the exclusion is
load-bearing.** A `// @ts-expect-error` directive binds to the next line; the
first prettier run wrapped three chained calls under their directives and
`npm run typecheck` failed with TS2578 ("unused directive") plus the errors
the directives were meant to absorb. `.prettierignore` names the directory
with that reason; a "format everything" pass must keep it.

**A containerised PHP is reached through `FLEZHA_PHP`.** This box has no
`php` or `composer`; the PHP SDK runs in `docker/dev/Dockerfile.php`'s
`flezha-php` image. `scripts/format-apply.sh` word-splits
`${FLEZHA_PHP:-php}`, so
`FLEZHA_PHP="docker run --rm -u $(id -u):$(id -g) -v $PWD:/work -w /work flezha-php php"`
runs php-cs-fixer from the repo root inside the container with the same
relative paths; `composer require` for the pin was run the same way (with
`-e HOME=/tmp`, or composer cannot create its cache). php-cs-fixer prints an
advisory warning every run — the config is at the repo root so it cannot read
`sdk/php/composer.json`, and with it readable it would warn instead that PHP
8.4 exceeds the 8.1 floor — and exits 0 either way.

**RuboCop runs Layout cops only, and `Layout/LineLength` is off.** With every
cop enabled `rubocop -a` is a linter that rewrites code; `.rubocop.yml` sets
`DisabledByDefault: true` and enables `Layout`, so `-a` moves whitespace and
nothing else. `LineLength` is a Layout cop that cannot autocorrect a comment
table (`sdk/ruby/lib/flezha/codec.rb`), which made the first apply run exit 1;
width is a lint judgement, not layout. Ruby on this box is rbenv's 3.4.10
(`~/.rbenv/versions/3.4.10/bin`), not the system 3.2.3 the gem refuses.

**The formatter widths follow `rustfmt.toml`'s 100** (`.prettierrc.json`,
`ruff.toml`); at ruff's default 88 the Python set would have been 26 files
instead of 12, half of them re-wrapping lines the SDK had wrapped at 100.

## Naming gates

<!-- kind-naming-record:begin -->
<!-- overflow-naming-record:begin -->
**Fixed at the source, with one allowlist row widened rather than added.** The
kind gate's 24 offences were wording (`TTL-bounded` → `TTL-expiring`,
`Unbounded:` → `No cap:`, a test renamed `…has_a_time_limit…`, the PHP fixture
classes renamed to the Rust runner's `BoundedMailboxDrop*Operative`, the Node
fixture helper to `boundedMailbox`), except the crossbeam sense of "an
unbounded coordinator send", which three glue files share: that row's scope
went from `ruby_glue.rs` to `flz_core_ffi/src/`. The overflow gate's five were
four spec lines (`Block`-policy → `DropNothing`-policy in
`python-sdk-concurrency`, edited directly with no delta) and one dev note that
MENTIONS the retired header, wrapped in an `overflow-naming-record` block.
<!-- overflow-naming-record:end -->
<!-- kind-naming-record:end -->

## Stale claims, refuted by measurement

- **"`mix format` disagrees between local 1.20 and CI 1.19"** — false at
  `17bdcb9c`: `mix format --check-formatted` exits 0 under both 1.20.2-otp-29
  and 1.19.2-otp-28 (`mise exec elixir@1.19.2-otp-28 erlang@28.1.1 -- …`) on
  the same tree. CI moved to 1.20.2 anyway (seven `elixir-version` lines in six
  workflows) because the prod and dev-toolchain images are already
  `hexpm/elixir:1.20.2-erlang-28.5.0.5` and the local pin is the user's global
  mise default outside the repo; `otp-version: "28"` stays.
- **"`cargo fmt --all` reformats files nobody touched"** — false at
  `17bdcb9c`: 0 files under both `+nightly` and the CI-pinned
  `+nightly-2026-05-07`; Java's `spotless:check` was also clean. The three
  "formatting-only commits" for Rust, Elixir and Java were therefore empty and
  nothing was added to `.git-blame-ignore-revs` for them; only the four veneer
  commits exist.
- **"33 files over 800 lines"** — 52 once tests and test apps are counted the
  way the ceiling reads.

## Measurement notes

- The Elixir controller has no layering today: one 55-file `mix xref` cycle
  spans `state`, `runner`, `operative`, `service`, `convergence`, `shutdown`,
  `plugin`, `transport` and `dlq`; charter §3 now records the measured edge
  counts and calls the stated order the target.
- `mix bench.roundtrip --engine flezha` in its default shape (`-p 4 -m 5000`,
  20 000 records) is setup-dominated — 53 844 rec/s end to end over 371 ms —
  and is recorded as that shape, not as a throughput ceiling; it needs only the
  `kafka` service of `bench/flink/compose.yml` (`start`/`stop`, never `down`).
- `.git/info/exclude` on this box excludes all of `bench/flink/`, so the bench
  reports never show in `git status` here and will on any other box.
