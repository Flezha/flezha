# Four small fixes that share nothing (`small-mechanical-fixes`)

Four triaged defects, batched only because forty separate changes was what the
triage existed to avoid. They share no mechanism, so each carries its own proof
and each is written up on its own here. One of the four could not be fixed the
way its entry asked, and that is the most useful thing in this note.

## KI-5 — a metric that depends on the scan now names it

`duplicate_pairs` is a **pair** count over whatever files the run scanned, and
`jscpd_pairs` is handed only the roots of the areas *this* invocation covers. So
a pair whose two halves straddle a scanned and an unscanned area is invisible,
and the number is an undercount that grows with scope. Measured on this tree at
`50304acb`, identical source, three runs:

    --area sdk/rust                                     169
    --area sdk/rust --area sdk/java --area sdk/node     169
    (no --area, whole repo)                             215

The middle row is the one worth keeping: `sdk/java` and `sdk/node` share nothing
with `sdk/rust`, so adding them adds nothing. *Which* siblings you scan is the
whole point, not how many.

The fix is not to the count. `measure()` records `scan` — the sorted area set —
next to the numbers it produced, `docs/refactor/baseline.json` carries it, and
`scan_mismatch/2` makes `--check` **refuse** rather than difference two
recordings made over different sets. A warning would have been a number nobody
reads, which is the same defect the requirement exists to prevent.

Refusing on *any* difference is deliberate and is not over-strict: adding an
area to the table adds files to the jscpd run, which can add cross-area pairs to
areas that were already there. Every area's `duplicate_pairs` really does move.
The response is `--write-baseline`, and the charter now says so.

A baseline with no `scan` key is refused too — an unknown scan is exactly the
comparison the requirement forbids, and there is one baseline in this repo, to
which the key was added by hand so that no metric value moved (`+38` lines, `-0`).

**Proved** by running both arms: `--check` over the whole repo compares normally
(exit 1 on six pre-existing regressions, see below), and `--check --area
sdk/rust` prints

    refusing to compare against docs/refactor/baseline.json: the baseline
    scanned 36 areas, this run 1 (only in the baseline: cli, flezha-macros, …)

and exits 2. The `--self-test` covers the three arms as a unit test, including
the legacy no-`scan` baseline.

**Not this change**: `refactor_metrics.py --check` is red at `50304acb` with six
regressions — `flz_core prod_loc` and `test narrative_markers` blocking, four
`lib/flezha/*` areas measuring. Every regressed file has a commit between the
baseline's `head` (`c5bdc26b`) and HEAD, and this change touches no file inside
any area. Re-recording the baseline is the refactor programme's call at an area
close, not a side effect of a metrics fix.

## KI-15 — the bindgen guard has no build-time home, and that is the finding

The proposal (D2) says to move `BINDGEN_EXTRA_CLANG_ARGS` out of
`scripts/build-ruby-sdk.sh` and into `flz_core_ffi`'s own build, so every caller
is protected by construction. **That cannot be built.** `rb-sys` is a
*dependency* of `flz_core_ffi`, so cargo runs `rb-sys`'s build script — the one
that runs bindgen — before `flz_core_ffi/build.rs` exists to say anything. The
reproduction shows it directly: the build dies at `Compiling rb-sys v0.9.130`,
and this crate's build script never runs. There is no `cargo::` metadata channel
in that direction either; metadata flows from a dependency to its dependents.

The only mechanism that reaches a bare `cargo test -p flz_core_ffi --features
ruby-embed` is `.cargo/config.toml`'s `[env]`, and it takes a **literal** value.
The value needed here is `-I/usr/lib/gcc/x86_64-linux-gnu/13/include`: gcc 13,
`x86_64`, Debian layout. Committing that would set the variable on every box,
point most of them at a directory that does not exist, and leave the same
confusing failure behind a guard that looks general. That is worse than none, so
the wrapper stays and the gap is recorded rather than papered over.

**`cargo test -p flz_core_ffi --features ruby-embed` remains unprotected** on a
box without clang's builtin headers. Proved after `cargo clean -p rb-sys`, with
rbenv 3.4.10 on PATH and the variable unset: exit 101, `'stdarg.h' file not
found`. CI is unaffected — `ruby-sdk.yml` installs `libclang-dev`, which brings
the builtin headers with it, and the workflow is the only other caller of the
ruby features anywhere in the repo (`build-dist.sh` does not build them).

What did change is the diagnosis. The wrapper now refuses **before** cargo when
the Ruby on PATH has no development headers, because that failure surfaces from
rb-sys as `'ruby.h' file not found` and reads as the same missing-header family
as the stdarg.h case while wanting the opposite remedy. With the system Ruby
(3.2.3, no `ruby-dev`) it now says so by name and says it is *not* the include
path problem. The reason no build-time home exists is written beside the guard,
where the next reader will look for it.

## KI-24 — the UI snapshots are ignored by the formatter, and the count was 5

`sdk/rust/tests/ui/*.rs` are compiled out-of-tree by trybuild, so `cargo fmt`
never reaches them and the per-file `rustfmt` loop this box's convention forces
does. `rustfmt.toml`'s `ignore` array now carries `sdk/rust/tests/ui`, which
makes a stray `rustfmt +nightly --edition 2021 sdk/rust/tests/ui/foo.rs` a
no-op — `ignore` is nightly-only, which is already the pinned toolchain.

Measured both ways on scratchpad copies before touching the tree: without the
entry the per-file loop rewrites **5 of 25** files (`procedure_ctx_with_key`,
`sealed_disallows_on_full`, `sealed_disallows_wiring`,
`stream_handle_cannot_escape`, `wire_wrong_input`), and with it, 0 of 25. Then
run for real over the working tree: `git status` on the directory is empty.

The 20 unchanged files are why this hid — a loop over the whole list reports
clean. `CLAUDE.md` and `docs/dev_notes/refactor-3-rust-sdk.md` both said "breaks
three at once"; the number is five, and both now say so.

## KI-16 — one advisory, no exposure, one line of lock

`mix hex.audit` before: exactly one entry, `postgrex 0.22.3`, EEF-CVE-2026-66838
(MEDIUM) — SQL injection via the `:comment` option of `Postgrex.stream/4`.
`lib/` never calls `Postgrex.stream` and never passes `:comment`; the callers are
`Postgrex.query/4`, `child_spec/1`, `start_link/1` and an `after_connect:
{Postgrex, :query!, …}`. A lock bump, not an exposure. After: *No retired or
security advisory packages found*.

**`mix deps.update postgrex` moved two lines, not one** — the second was
`telemetry 1.3.0 => 1.4.2`, which nothing asked for: postgrex 0.22.4 does not
depend on telemetry at all (`db_connection`'s `~> 0.4 or ~> 1.0` does), so the
resolver simply took the newest satisfying version while it was re-resolving.
telemetry was put back to 1.3.0 in the lock and `mix deps.get` accepted it, so
the committed diff is one line: `postgrex 0.22.3 → 0.22.4`. Holding it back is
not a pin — `mix.exs` never mentions telemetry — it just leaves everything this
change was not about where it was.

## None of the four left the batch

Each stayed within a few lines. KI-15 is the one that raised a design question,
and the answer was that its intended fix does not exist rather than that it is
larger than it looked: the escape the task list offers (2.3) is what it took.
