# `green-the-suite-log-gate` — three log lines, and what each one was

## The starting state

`.github/workflows/elixir.yml:150`–`:163` runs `mix test | tee /tmp/mix-test.log`
and then `grep -nE '\[(error|critical)\]'` over it, exiting 1 on any hit. It is
unconditional, and it failed at HEAD on a suite that is 1836/1836 green — so the
merge gate reported nothing, because nobody could tell a new uncaptured error
from the standing ones.

Reproduced verbatim before anything changed (`mix test` redirected to a file,
never `| tail` — the failure block prints long before the summary):

| line | source | outcome |
|---|---|---|
| `[Overwatch] restore REFUSED entity={"refused_…"}` | `restore_refusal_test.exs` `await_restore/2` case | now captured **and asserted** |
| `[Overwatch] restore REFUSED entity={"tier_refused_…"}` | the same module's tier-bind case | now captured **and asserted** |
| `Child Flezha.Runner.ControlUnit … shut down abnormally` | a `RunnerSupervisor` terminate sweep | **production defect — KI-48, not fixed here** |

## The one judgement: the abnormal shutdown is (b)

Design D2 made diagnosing this a task rather than an assumption, because the two
readings have opposite treatments. The finding is (b) — the control unit does not
stop cleanly — so per D2 it is filed (KI-48) and left alone.

The evidence, in the order it settled the question:

- **The exit reason names the mechanism.** `RunnerSupervisor` lists
  `ControlChannel` before `ControlUnit`, so the reverse-order terminate sweep
  stops the channel first, and a `ControlUnit` blocked in
  `GenServer.call(control_channel, {:send_control, …}, 5000)` exits
  `{:shutdown, {GenServer, :call, …}}`. OTP's `supervisor:shutdown/2` accepts
  only a bare `shutdown`, so the tuple becomes `{error, Reason}` and
  `do_terminate/2` reports `shutdown_error` — the exact wording Elixir's
  translator renders as "shut down abnormally". Note there is **no accompanying
  GenServer crash report**: a `{:shutdown, _}` reason is in GenServer's quiet
  set, which is why the log carries one line and not two.
- **The in-flight command is a `Ping`** — field 19 of `CommandRequest`, decoded
  straight out of the reported call's payload. Its 16-hex nonce is minted only
  in `ControlUnit.handle_call({:ping_runner, …})`, whose only production caller
  is `Flezha.Operative.Lifecycle` (`attempt_ping_and_start/3`, `runner_ready?/1`).
  The single test caller lives in another module and namespace.
- **Everything after the test's one call is production code**:
  `NodeTestHelper.stop_runner/2` → `FleetManager.disconnect_runner/2` (the same
  public entry `Convergence.Heal` uses) → the runner closes →
  `FleetManager.runner_disconnected` → `do_handle_runner_disconnect/3` →
  `FleetSupervisor.stop_runner_sup/1` → `Supervisor.terminate_child/2`. The test
  stops no child itself and names no order, so there is no "tore down out of
  order" to fix.
- **The code already half-knows.** `ControlChannel.send_control/2` guards the
  channel being *gone* (`GenServer.whereis` → `{:error, :no_control_channel}`)
  but not its dying mid-call, and `Lifecycle.timeout_class?/1` explicitly names
  "the ControlUnit having exited on such a call" as one of three shapes a busy
  runner takes. The caller is defended; the unit is left to die.
- The only test-side ingredient — `test/flezha_test.exs:592` starting
  `Convergence.Loop`, which `config/test.exs` leaves off — makes the run *more*
  production-like, not less: the Loop is on in production.

**Attribution cost a `--trace` run.** KI-21 could not name the owning test. It is
`test/flezha_test.exs:592`, "runner disconnect preserve", and the line lands
*inside* the test rather than in a later `on_exit`. It is intermittent: absent
from the baseline full run, present in the post-fix one, present in 2 of 2 runs
of that file alone. So the gate is intermittently red on it, not permanently —
which is worth knowing, because a single green full run does not prove it gone.

## The gate was proved, not assumed

A gate that has only ever been red and is now green has been shown to be quiet,
not to work (D3). A scratch test emitting one uncaptured `Logger.error` turned
CI's own grep **RED while `mix test` still exited 0** — which is precisely the
case the gate exists for, a passing suite with an unasserted failure path. The
same test wrapped in `capture_log` with an assertion turned it **GREEN**. The
scratch test was then deleted.

## `capture_log` plus an assertion, never `capture_log` alone

Both refusal cases run inside `ExUnit.CaptureLog.with_log/1` (the house form —
`signal_handler_test.exs` uses the same import) and assert on the returned
string, not on a received message, so there is no timing to lose. What is
asserted is what the line is *for*: that it says `restore REFUSED`, names the
Entity it left unbound, and carries the SDK's own reason.

- The assertions are load-bearing rather than decorative: `with_log` returning
  `""` would fail all three, so the tests passing *is* the proof the line was
  emitted and captured.
- The 78-character refusal string was already duplicated verbatim in both cases
  under two names (`reason`, `refusal`). It is now one `@refusal` attribute that
  the stubs, the return-value assertions and the log assertions all read, and the
  three log assertions are one `assert_refusal_logged/2`.
- `@moduletag capture_log: true` was rejected on purpose: it swallows the whole
  module including a line a future test emits by accident, and asserts nothing.

## The temp-dir leak: four sites, and why a count is the wrong measurement

KI-19 attributed 11 of 13 leaked `/tmp/flezha_*` dirs per run to three modules.
The fourth site is `DurabilityBackendStub.isolate_store_dirs/0`, which mints one
root **per `install/0`** while a module `uninstall/0`s once — so fixing it in
`uninstall/0` would still have leaked one per test. Each site now registers
`on_exit` beside the line that mints the directory (D4), which for the stub means
`import ExUnit.Callbacks, only: [on_exit: 1]` — the convention `auth_test_helper`
and `tier_e2e_harness` already use, and safe because every `install/0` call site
is a test or setup body.

**Counting dirs cannot prove this.** `System.unique_integer/1` restarts low in
each BEAM, so run N+1 re-mints run N's names: a fresh leak can land on an
existing name and never show up as new, and a *fixed* module reclaims an old
leak and shows up as a net loss. The honest measurement is a timestamp marker —
`find /tmp -maxdepth 1 -name 'flezha_*' -newer <marker>` — which after the fix
returns only `/tmp/flezha_sockets`, the configured socket root, for both a run of
the four sites and a full `mix test`. (The counts, for what they are worth:
the three named modules +11 → 0; a full run 336 → 323, the 13 reclaimed being
earlier leaks whose names this run re-minted and then cleaned.)

**`tmp_app_data_dir/0` owning its own `on_exit` was considered and not done.**
It would delete ~30 duplicated `File.rm_rf!(base)` lines across the ~48 callers
and make the omission structurally impossible — and `state_test_helpers.ex`
already calls `ExUnit.Callbacks.start_supervised/1`, so it is already
test-process-bound and would take on no new constraint. It is out of this
change's Impact (three modules), against task 4.2's letter, and unnecessary:
the measurement shows no other caller leaks. Recorded here for whoever next
touches that helper.

## The gate's population is bigger than three lines

KI-21 counted three lines from one run. Across three full plain runs (one
baseline, two after the fix) plus targeted module runs, the set is **four
sources, none of them reliably present**:

| source | seen |
|---|---|
| the two restore refusals | every run before the fix, none after |
| `ControlUnit … shut down abnormally` (KI-48) | 2 of 3 full runs; 2 of 2 runs of `flezha_test.exs` alone |
| a task `overwatch_suspension_test` leaks onto its own next test's stub (KI-49) | 1 of 3 full runs; **1 of 5 runs of that module alone** |

KI-49 was found by the third full run and is the change's own gate working: two
`[error]` lines while all 20 tests in the module pass, from a migrate task still
in the retry ladder when its test ended, arriving at the next test's
`stub(OperativeManager, :assign_runner, fn _ns, "chat", ^inst -> …)` carrying the
previous instance id. It is test-side and small, but it is a fourth line outside
this change's task list, so it is filed and not fixed — and the honest reading is
that **the gate cannot be relied on to be green until KI-48 and KI-49 are both
closed**. What this change bought is that a new uncaptured error is now visible
against a known, named set of two rather than an unexplained three.

## What this change did not do

- **KI-48 is not fixed.** The suite's third `[error]` line stands, so the gate
  can still go red on it. Any fix is a production change (guard the call inside
  `ControlUnit`, or stop it before `ControlChannel` in the child list) and is its
  own decision.
- **KI-49 is not fixed** — found here, filed with a 1-in-5 single-module
  reproduction, out of this change's scope.
- **KI-19's DLQ half is untouched**, as triage directed: it does not grow in a
  plain run and needs `--include system`.
- **`mix test --include system` is not green on this box**: two runs, **1914/1915**
  each, a *different* KI-18 member failing each time (`flezha_test.exs:383`, then
  `workflow_crash_resume_e2e_test.exs:249` — the one KI-18 reproduced at
  `refactor-baseline-2`). Its log carries **1639** `[error]` lines, dominated by
  KI-17's `sun_path` overflow family, so the gate would not pass there either —
  it runs on the plain suite.
- **`scripts/format-check.sh` cannot run on this box** — `rubocop` is absent and
  `format-apply.sh`'s `need` preflight aborts before any formatter runs. That is
  KI-4, unrelated to this change; the four touched files are
  `mix format --check-formatted` clean.
