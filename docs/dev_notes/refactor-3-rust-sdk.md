# `sdk/rust` + `flezha-macros` refactor (`refactor-3-rust-sdk`)

Area 3 of the refactor programme, six worker segments (A–F) with a supervisor
commit at each checkpoint. Zero behaviour change. What follows is what a later
session would otherwise rediscover; the plan itself is in the archived change.

## The module narrative charter §5 moved out of the crates

**`sdk/rust/src` is a facade over one poll core.** `lib.rs` is the authoring
surface and nothing else — module tree, re-exports, the `AutoOperative`
inventory harvest, `register_stream_class` / `register_workflow_class`, and
`__dispatch_internals`, which must stay at the crate root because the benches
and eight test binaries reach the dispatcher through it. The runner *handle*
(`Flezha`, `FlezhaRunner`, `Started`, `ActiveRunner` and the process-wide
singleton slot) lives in `runner_handle.rs`; the crate `//!`'s worked example is
now `examples/append_operative.rs`, which `cargo check --all-targets` compiles —
`Cargo.toml` sets `doctest = false`, so before this area that example had never
been compiled by any gate.

**The typed tail is a type alias, not a second implementation.**
`TypedBuffer<S>` is `EffectBuffer<TypedBodies<S>>` over `flz_core::effect`'s
`Bodies` marker, so every `downcast_mut::<TypedBuffer<S>>()` site compiles
unchanged and the settlement rule lives once, in `settle_effects`. What stays
with the caller is deliberate and small: the `NoChange`-class inline flush, the
`park_tier_off` decision, and the timer apply that `run_activation` performs
outside the slot locks. `dispatcher.rs` keeps the activation path only; the two
sink adapters live in `dispatcher/emit_sink.rs` and the invocation machinery in
`dispatcher/invocation.rs`, split by OWNER rather than by size.

**`channel/builder/` is five modules by owner** — `mod` (the builder and the
entry channels), `issue` (the 28 `TopologyIssue` variants), `state` (the
builder's mutable half and the config writers), `stream` (the handles), `merge`.
`TopologyIssue::TypeMismatch` had no producer anywhere in the repo and is gone.

**`flezha-macros` is four expanders and no `util` bucket.** Each entry point is
`expand` → classify → refuse → emit. `procedure.rs` is the one to read first:
`classify_params`/`classify_arg` turn the `run` signature into `Params`,
`refuse_shape` holds every shape the sugar cannot express, and
`expand_procedure` assembles the `Procedure` impl from `expand_state`,
`expand_run`, `expand_modes` and `expand_auto_wire`. `auto_operative` lives in
`operative.rs` because it is *emission*, shared by both entry points; `util.rs`
holds `generic_args` and the ident casing, which are `syn` inspection.

## Gotchas, in the order they cost time

**`sdk/rust/tests/ui/*.rs` must never be rustfmt-formatted.** Their line numbers
are pinned by the `.stderr` snapshots. `cargo fmt` never reaches them (trybuild
compiles them out-of-tree), which is exactly why a per-file `rustfmt` loop —
the only safe way to format this tree, since `cargo fmt --all` is forbidden here
— DOES reach them and breaks three snapshots at once. Exclude `tests/ui/` from
every formatting loop. Segment E hit this and restored the files with
`git show HEAD:<path> > <path>`.

**A file split moves a `.stderr` snapshot, and that is legal; a macro edit
cannot.** Splitting `channel/builder.rs` into a directory moved 7 lines across 3
of the 24 snapshots — the `--> src/channel/builder.rs` source paths and one
module path inside a type name (`builder::BuilderState` →
`builder::state::BuilderState`). Those are now the baseline. The macro split in
segment F moved **none**: a macro edit can only move a snapshot by changing a
diagnostic, so a moved snapshot there means the change is not neutral.

**`refactor_metrics.py` counted every `quote!`-heavy function as tiny.**
`brace_extent` stripped Rust `#` as a shell-style comment, so every `#ident`
interpolation ended the brace count early: `procedure.rs::expand` measured as if
it closed at its first emission and was invisible at 348 lines. Fixed at
`5d53f7f5`, mid-area — which is why batch 10b exists and why `state.rs`'s three
expanders (72 / 60 / 101) surfaced only at the close.

**The classifier reads `/tests/` or a `_tests.rs` suffix, and nothing else.**
`src/channel/route_latest/tests.rs` matched neither, so 265 test lines and nine
test names counted as *production* at the baseline — moving it to
`route_latest_tests.rs` is a **measurement correction, not a deletion**. The
same rule bites in reverse: a `poll_core_tests/` directory does not classify as
test, so that split went to `src/runner/tests/poll_core/` reached by `#[path]`,
which keeps the module path `crate::runner::poll_core_tests::fixtures` and its
two importers untouched.

**`duplicate_pairs` is a pair count, and splitting a file multiplies it.**
`benches/workflow_scaling.rs` paired 5 times at the baseline against the single
4 280-line `workflow/mod.rs`; against the seven fixture files that file became,
it pairs **65** times for the same duplicated source, and the duplicated-token
total rose with it (251 → 3 181) because each clone instance is now reported
against a separate file. Excluding that one binary, the area's pairs fell
178 → 150 and its duplicated tokens 9 770 → 8 344. Read the token totals, not
the pair count, whenever a split is in the diff — and always name the scan mode,
because `--area sdk/rust` and the full-repo run disagree on identical source.

**A `tests/common::prelude` is worth ~18 pairs and no more.** Fifteen new test
binaries each repeated the same `use` header; one `pub mod prelude` in
`tests/common/mod.rs` plus `use common::prelude::*;` removed 121 test lines and
18 duplicate pairs (233 → 215 whole-repo). It cannot touch the
`workflow_scaling` group above, and the report must say so rather than claiming
the rise was cured.

**`cargo test --workspace` was red with no flags, and now is not.**
`sdk/rust/Cargo.toml`'s `bench-internals` did not forward to
`flz_core/bench-internals`, and three test targets did `mod common;` without a
`required-features` entry, so a bare `cargo check --workspace --all-targets`
failed with three `E0432: unresolved import flezha_runner::__dispatch_internals`.
Both halves are fixed; keep the `[[test]]` entries in step when adding a binary
that uses `common`.

**`workflow_driver_scenarios` is ONE `#[test]` on purpose.** The scenarios share
ordered process-global state — a reconnect trigger installed once,
`state_sink::SINK_TEST_LOCK` held for the whole test, 36 registry insertions —
so splitting the `#[test]` would make the ordering a hope. It is 17 phase
*functions* under one `#[test]`, grouped into `workflow/tests/driver/` by owner.
Four of the first twelve phases came out over clippy's cognitive-complexity 25
and were split again at their own `--- ---` markers.
`tests/runner_lifecycle.rs` is the same shape for the same reason (one
process-wide runner slot) and was split into three phase functions at the close.

**Three flakes, three mechanisms, and a regime that reproduces them.** The plain
regime — 8 threads on an idle box — reproduces NONE of the three, 20/20 before
and after, so a green proof there is worthless. The regime that works is
`--test-threads=32` under `nproc` busy loops.
`tier_replay_skips_inputs_at_or_below_the_consumed_floor` takes `>= 1` on the
positive wait (the strict `== 1` is re-asserted after the quiet window) and pins
`FLEZHA_WORKER_POOL_SIZE`; `stream_transforms`' panicking-transform case moved
from `SINK_LOCK` to the `CHAIN_LOCK` its siblings hold, so its `park_blocking`
worker cannot starve the replay drain; and
`overflow_policies::drop_oldest_evicts_head_and_bumps_oldest_counter` asserts a
delta on the **process-wide** `entity::drops_oldest_total()`, so the four cases
that produce or measure a shed share a file-local `DROP_AGGREGATE_LOCK`.

**Three sanctioned `thread::sleep` survive**, each named:
`tests/common/mod.rs`'s `wait_until` (the suite's one polling primitive, ~110
call sites depend on its predicate shape), `runner/tests/poll_core/mod.rs`'s
10 ms poll interval, and `benches/workflow_scaling.rs`'s measurement window.

## Performance: the seam costs ~4 % on the typed dispatch rows

Measured once, at the close, over the arms every checkpoint kept (charter §8's
"build every checkpoint, measure once"). Rows 1 and 2 — the typed dispatch gates
— sit **+3.9 %** and **+4.8 %** above A′ (the end of segment A), and both bisect
to **B2, the end of segment B**: the batch that ported the typed tail onto
`settle_effects`/`TypedBodies`. B3, B4 and B5 are flat against B2 (+0.31 % and
+0.08 %). Allocations are flat across that step, so the cost is instruction
path, not allocation. The tier-off row went the other way: **−4.9 % and −2.00
allocs/elem**, which is `coalesce`'s lazy map+mask no longer allocating on a
keyless buffer.

**Two arms with identical production code are the only honest floor.** B4 and B5
differ solely in `#[cfg(test)]` modules, `tests/`, comments and two private
function names, so the bench binaries execute the same code — yet they separated
by **1.93 %** on row 1 in the same interleaved rounds. Within one arm, A′ spanned
5.6 % on row 1 and 15 % on row 2 across the session. The batch-0 floors (0.33 %
and 0.95 %) were taken on a cold idle box and do not survive a session that has
just run two workspace test passes and a 175-second system suite. Measure on a
cold box, or carry a same-code control arm and compare against it.

**An unchanged binary is not re-measured.** `flz_core_ffi`'s `host_loop` (row 5)
is byte-identical at B3, B4 and B5, so only A′ vs B5 was run: +1.53 %, inside
the row's 4.13 % floor. Keep building an arm at every checkpoint even when
nothing is measured — the attribution above was a seven-minute run because five
arms were on disk.

## Zero-behaviour-change proof

Every string literal over twelve characters in `sdk/rust` (all of it, so the
comparison is closed under this area's file moves) and `flezha-macros/src`,
comments excluded, compared against `refactor-baseline-3`: **1 893 → 1 904
occurrences, 22 lost and 33 gained, every one accounted for.** The lost ones are
four deduplications the plan ordered (`unexpected reply from the runner thread`
2→1, `expected a bare identifier` 2→1, `postcard serialization of Change failed`
3→2, `operative does not support handle_message` 2→1), one planned deletion
(`TypeMismatch`'s text, a variant with no producer), six `UnixStream::pair`
expects that became `bench_support::stream_pair`, and the four
`run`/`compensate` error templates. Those four are D11's core:
`format!("procedure {} failed: {}", …)` became
`format!("{} {} failed: {}", shape.prefix, …)` with `prefix` `"procedure"` /
`"compensation for procedure"` — **the rendered text is byte-identical**, and
four new assertions in `dispatcher_tests.rs` pin it. The gains are the compiled
example, the `#[path]` strings of the moved test modules, and test assertions.

The 24 `tests/ui/*.stderr` snapshots are unchanged since segment C, and the five
byte-pinning contract tests named in `design.md` — including
`effect_record_body_postcard_bytes_are_the_cross_language_fixture` and
`the_json_shape_matches_the_java_sdk_byte_for_byte` — are byte-identical to the
tag, two of them merely relocated to `tests/output_identity.rs`.

## The line count went up, and why

`sdk/rust` production 5 938 → 6 001 and `flezha-macros` 1 004 → 1 166. Both
rises are charter §7's ceilings: twelve functions over 50 and nine files over
800 became zero, and every extraction costs a signature, a closing brace and
often a rebinding `let`. `procedure.rs::expand` alone went 348 lines in one
function to 14 functions and +92 production lines. Charter §1 permits this
exactly when the reason is another rule of the charter, and here it is; the
counterweight is on the other side of the ledger — narrative markers 20 → 2,
`files_over_800` 9 → 0, `functions_over_50` 12 → 0, `long_names` 9 → 0, sleeps
9 → 3, clippy 0 across `src/` *and* `tests/`.

The two surviving narrative markers are **false positives on the word**:
`route_latest.rs` and `route_latest_tests.rs` say "a key this member may no
longer own", which is runtime ownership after a rescale, not history. A gate
that read them as offences would be wrong twice.
