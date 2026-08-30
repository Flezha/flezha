# Mimic's global mode is released by the test that took it — implementation notes

Gotchas and load-bearing decisions from `close-mimic-global-leak`: the 28
modules that set Mimic's `:global` mode raw, the wrapper they now use, and
`scripts/check-mimic-global.sh`. The proposal's account of the mechanism is
half right, and the half that is wrong is the half that decides which module
pays — so the correction is the note.

## What actually leaks, and what does not

Mimic's mode lives in one ETS row owned by `Mimic.Server`
(`{:mode, :global, owner_pid}`). `valid_mode?/2` lets a `stub/3` through only
when the caller **is** that owner, so a stale `:global` row fails every stub in
every later module until something puts the row back to `:private`.

Two things put it back, and the proposal only knew one:

1. **The stub monitor.** `monitor_if_not_verify_on_exit/2` runs inside the
   `{:stub, …}` / `{:expect, …}` handlers — *only* there. On the owner's
   `:DOWN`, `clear_data_from_pid/2` resets the mode. A test that registers no
   stub is therefore never monitored, exactly as
   `test/helpers/mimic_global.ex`'s `@moduledoc` says.
2. **`verify_on_exit!`.** `use Mimic` injects `setup :verify_on_exit!`, whose
   `on_exit` casts `Server.exit(pid)` — the same `clear_data_from_pid/2`, with
   no stub and no monitor required. `import Mimic` injects nothing.

So the leak needs **both** halves to be missing: a module with no
`verify_on_exit!` **and** whose *last-run* test registers no stub. Granularity
is the test, not the module: `setup` runs per test, so each test becomes the
owner in turn and only the last one's fate matters.

**Measured at HEAD, seed 0, one module per `mix test` run**, reading
`:ets.lookup(Mimic.Server, :mode)` from a temporary `ExUnit.after_suite`
callback: of the 28 raw-call modules, **exactly one** —
`test/flezha/service/cli_manager_state_get_test.exs` — left the mode `:global`.
Its last test in definition order ("a malformed path is refused naming the
shape it expects") stubs nothing, and it reaches Mimic through `import Mimic`.
Only four of the 28 lack `verify_on_exit!` at all (`cli_manager_state_get`,
`cli_manager_send`, `cli_manager_rewind` — all `import Mimic` — and
`control_channel_proc_commit`, which fully-qualifies `Mimic.stub`); the other
24 were protected **by accident**, by a callback they take from `use Mimic` for
an unrelated reason. That accident is what the gate replaces with a decision.

**REFUTED, and not to be re-investigated:** the design's two named proof cases,
`flezha/runner/fleet_manager_sdk_lang_test.exs` and
`plugin/kafka/producer_worker_timestamp_test.exs`, do not leak. Both register
no stub — and both `use Mimic`, so `verify_on_exit!` releases the mode for
them. Paired with a stubbing module in either argument order they are green
(23 and 11 passed). "Registers no stub" is a necessary condition, never a
sufficient one.

The leak's frequency follows from the same reading: `state_get` has one
no-stub test out of ten, so a non-zero seed shuffles it last about one run in
ten. Across seeds 0/1/7/42/12345 it leaked on seed 0 only — which is why
KI-43's field sighting was a full-suite run on seed `682679` and looked like a
regression in whatever change happened to reorder the modules.

## The reproduction, and why a green run proves nothing

    mix test test/flezha/service/cli_manager_state_get_test.exs \
             test/flezha/service/cli_manager_schedule_test.exs --seed 0

Before: **11/20 passed, 9 failed**, every failure
`(ArgumentError) Stub cannot be called by the current process. Only the global
owner is allowed.` in `CliManagerScheduleTest`. The schedule module alone:
**10 passed**. After: **20 passed**. (KI-43 said ten failures; it is nine —
one of the schedule module's ten tests registers no stub of its own.)

**ExUnit's module order is not the command line's, and no seed changes it.**
Both argument orders of that pair put `StateGet` first and fail identically,
and `--trace` on `state_get + cli_manager_cluster` puts `ClusterTest` first on
seeds 0/1/2/3 alike. The seed shuffles *tests within a module*, not the module
sequence — so "run it the other way round" is not an experiment you can
perform from the CLI, and the order-independence claim has to be proved by
reading the mode directly rather than by permuting arguments.

## Two modules did not need global mode at all

`plugin/kafka/producer_worker_timestamp_test.exs` and
`flezha/runner/fleet_manager_sdk_lang_test.exs` register zero stubs, and
nothing they call does either — `FlezhaTest.Kafka.ProducerFakes`,
`FlezhaTest.Helper.FleetStateHelper` and `Flezha.State.TestHelpers` contain no
Mimic reference at all. Both inject their doubles as plain function values
(`send_fn:`, `init_producer_id_fn:`) or seed real state. Their global-mode call
was dropped rather than converted, green on seeds 0 and 7. The sdk-lang
module's `setup` still carried the comment that justified the call — "the
filter runs INSIDE the FleetManager process, so a per-process stub would never
reach it" — which is true of a stub that no longer exists; it went with the
call.

## The gate

`scripts/check-mimic-global.sh` reuses `scripts/naming_gate.py`'s
`walk` + `pattern_check`, like the two gates already there. Three narrow
patterns, no allowlist:

- `\bset_mimic_global\s*\(` — the qualified call and the bare one under
  `import Mimic`.
- `:set_mimic_global\b` — `setup :set_mimic_global` and any apply-by-atom.
- `\bset_mimic_from_context\b` — the same door with the mode derived from
  `async:`. Nothing uses it today, and every module here is `async: false`, so
  it would take the global arm and leak identically. Refusing it costs one
  pattern and forces the intent to be spelled.

`set_mimic_global_until_exit` shares the prefix, so both `set_mimic_global`
patterns are pinned to what *follows* it — a call's `(`, or the atom's word
boundary. Neither can match the wrapper's name, which is why the gate needs no
allowlist masking at all and `naming_gate.pattern_check`'s destructive,
in-file-order rule application (see `rename-block-to-drop-nothing`) cannot bite
here: there are no rules to apply.

**The one exemption is by path, and it is load-bearing.**
`test/helpers/mimic_global.ex` must keep calling `Mimic.set_mimic_global/0`; it
is skipped through `walk`'s `path_filter`, and with that filter removed the
gate flags it (drilled). Scope is `git ls-files test/` — the raw call has no
place in `lib/`, and `pathspec` keeps the walk off the rest of the tree.

Drilled RED on all four shapes (`setup :set_mimic_global`,
`Mimic.set_mimic_global()`, a bare `set_mimic_global()`, and
`setup :set_mimic_from_context`) by re-adding each to `schedule/hub_test.exs`,
and GREEN with it removed.

## Pre-existing, found on the way

`test/plugin/kafka_test.exs:285` asserts `elapsed_us < 1_000` around one
`on_output` call. Run alone it measures 1.09–1.28 ms and fails every time; in a
large run it usually passes. A/B against the file's HEAD content on the same
seed: 1193/1126/1190 µs — identical, so the budget is simply below this box's
cold-path cost. Not this change's, and not a Mimic ordering effect.
