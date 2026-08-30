# `format-gate-tells-the-truth` — dev notes

## The shape: a preflight that runs before any check, not before its own check

`scripts/format-apply.sh` opened with a `need` loop that tested **every**
formatter's presence before running **any** of them:

```bash
need rubocop "install the development dependency in sdk/ruby/flezha.gemspec"   # :33
...
cargo +nightly fmt --all                                                        # :43
```

On a box without `rubocop` the script exited at line 33. It said something true
about Ruby and nothing at all about Rust, Elixir, Java, Node, Python or PHP —
and it printed **not one `==>` line**, so there was no visible evidence that
nothing had run. That is how six unformatted production files reached commit
`d6f6e16f`: a worker read "format-check.sh exits 0" and it had never run.

The lesson generalises past this script. **A command that reports "nothing
wrong" must be able to report something.** A gate whose failure mode is silence
is worse than no gate, because it is trusted.

## "Tool unavailable" and "tree is dirty" are different findings

Both exit non-zero, and the old script conflated them by having only one way to
end. `scripts/gate_lib.sh` is the whole fix and it is 70 lines:

- `gate_run [--nested] <label> [<tool>:<hint> …] -- <cmd…>` — the install hint is
  the preflight's useful half and survives verbatim; only the abort goes. A
  missing tool records the label plus the hint and **returns 0**, so the loop
  continues.
- `gate_report <heading>` — prints the two lists under separate headings and
  returns the status.

The distinction has to survive a **process boundary**, because `format-check.sh`
runs `format-apply.sh` as a child. So it lives in the exit code:

    0  everything ran, nothing to report
    1  a check ran and failed
    2  nothing failed, but a tool was missing so some check did not run

with failure winning when both are true. `format-check.sh` reads that with
`gate_run --nested`.

**`--nested` cannot be made implicit.** prettier, ruff and rubocop all use exit
**2** for "the tool errored" — a failure. Only a command known to be another
`gate_lib` gate may have its 2 read as "a tool was missing", so the caller has
to say so.

## What the proof is, and why this box is the right place to run it

`rubocop` is genuinely absent here and PHP lives only in the `flezha-php` image,
so the defect reproduces with no setup. Three arms, all drilled:

| arm | exit | what it says |
|---|---|---|
| every tool present, tree clean | **0** | seven formatters, `every check ran and had nothing to report` |
| `rubocop` and `php` absent, tree clean | **2** | five formatters reported, Ruby and PHP named under `NOT RUN`, and `reuse lint` + the notices diff still ran behind them |
| every tool present, one unformatted `.exs` | **1** | `UNFORMATTED: mix format`, and the four formatters *after* Elixir still ran |

The middle row is the entry's whole point: before the change that invocation
printed one line about Ruby and stopped.

The full-toolchain arm needs `PATH="$HOME/.rbenv/versions/3.4.10/bin:$PATH"`
(rubocop 1.90.0 is installed there; system ruby 3.2 cannot build it — `prism`
needs headers this box lacks) and `FLEZHA_PHP="docker run --rm -u $(id -u):$(id
-g) -v $PWD:/work -w /work flezha-php php"`. That combination is the first time
all seven formatters have run together on this machine.

## CI keeps no list of formatters

`.github/workflows/format.yml` ran spotless and nothing else, so prettier, ruff,
rubocop and php-cs-fixer had **never executed in CI**, while `format-check.sh`
claimed to cover them. Two lists drift; the fix is to have one. CI now installs
the four veneer toolchains and runs **`scripts/format-apply.sh --check`**.

Every version comes out of the SDK's own manifest — `npm ci` reads
`package-lock.json`, `composer install` reads `composer.lock`, and the ruff and
rubocop pins are grepped out of `pyproject.toml` and `flezha.gemspec` — so no
pin is written a second time in the workflow.

Two follow-on decisions, both the same principle one level up:

- The four installs are **`continue-on-error: true`**. An install that fails
  must cost one language, not the report; the check step still fails the job and
  now names the tool it could not find.
- `paths:` became **`paths-ignore: [docs/**, openspec/**]`**. An allow-list that
  forgets a source tree is a gate that silently stops covering it — which is
  exactly how four of the seven languages went unchecked. An ignore-list fails
  safe: a new tree is covered by default.

The one pin that is still written twice is the nightly (`nightly-2026-05-07`):
the workflow *installs* it and the script *invokes* it. `rust-toolchain.toml`
names it in prose as the place to bump from, and all three comments point at
each other.

## `ruff check` is a decision, written in the script

It is **not** gated, and the reason sits beside the `ruff format` call rather
than in a tracker nobody opens. 77 errors, 55 auto-fixable, 19 rules — but
`DTZ005`/`DTZ001` are the ambient-clock family the Python SDK guards on purpose
(`docs/dev_notes/python-sdk.md`), and `RUF022`/`RUF023` reorder `__all__` and
`__slots__`. Adopting it is a Python SDK source change with judgement in it, so
it is its own change; KI-6 stays open holding the count.

Turning it on later is one `gate_run "ruff check" …` line, and it reaches CI in
the same edit, because CI runs this script.

## `.formatter.exs`: the widening and the reflow are one commit

Widening `inputs` without reformatting turns the Elixir check red; reformatting
without widening lets the files drift again.

`examples/**` is deliberately narrowed to `examples/plugins/**`.
`examples/coding-fleet/` is gitignored, has its own repo, and is unformatted —
the wide glob would fail `mix format --check-formatted` locally on a file CI
cannot see. The tracked `.exs` outside the old `inputs` were **four**, not the
two filed: `.credo.exs` and `examples/plugins/hello_world/manifest.exs` were
already formatted.

The reflow is proved semantics-neutral twice, because a 102-line whitespace diff
nobody reviewed for meaning is exactly the sort of thing that carries one:

- **AST equality** — `Code.string_to_quoted!` on HEAD's copy and the reformatted
  copy, with all metadata stripped by `Macro.prewalk`, compares **identical**
  for both files.
- **Output equality** — `summarize.exs` driven against the repo's own
  `bench/acceptance/results.quick.tsv` and `cells.quick.tsv` produces
  byte-identical stdout, `summary.quick.md` and saved baseline on both the
  report arm and the `--save-baseline` arm. (The title line carries
  `DateTime.utc_now()`, so a second-boundary crossing between the two runs is
  the only legitimate diff.)

`scripts/auto_restore_persistence_smoke.exs` needs Docker, Kafka and a live
controller and is recorded MANUAL, so AST equality is the whole proof there.

## Left alone, deliberately

- **`.github/workflows/naming.yml` has the same shape.** Its four gate steps run
  in sequence with no `if: always()`, so a failing `check-kind-naming` hides
  every verdict behind it. Same defect, different gate, out of this change's
  scope — worth a follow-up.
- **`notices_fresh` writes a fixed `${TMPDIR:-/tmp}/third_party_notices.md`**, so
  two concurrent runs collide. Pre-existing; `mktemp` would fix it.
- **The five `scripts/check-*.sh` gates do not use `gate_lib.sh`** and should
  not: each runs exactly one check with one tool, so there is nothing behind a
  failure for it to hide. `gate_lib` exists for the multi-check case.
