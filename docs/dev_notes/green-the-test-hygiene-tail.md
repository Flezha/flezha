# `green-the-test-hygiene-tail` — implementation notes

Phase 1 row 4. Four `docs/known_issues.md` entries about *tests that lie or
leak* — KI-54, KI-21, KI-23, KI-18 — plus two the row's own runs found (KI-66,
KI-67). Three segments, **nothing in `lib/` changed**, and the row's product is
a plain `mix test` that is quiet on every run.

## Read this first: two of the four entries did not survive verification

**KI-21 no longer reproduces, and that is the row's most useful finding.** The
entry said three uncaptured `[error]` lines and "the gate fails on this suite at
HEAD today". Six full `mix test` runs at random seeds, each grepped with CI's own
command (`.github/workflows/elixir.yml:159`–`:163`), carried **none of the
three**. Both restore refusals had already been wrapped in `with_log/1` *with
assertions* by `green-the-suite-log-gate` (`restore_refusal_test.exs:20`, `:75`,
`:118`); the third was re-filed as KI-48 and closed by row 2. The only hits in
six runs were two KI-54 lines in one of them.

**KI-54's filed remedy is refuted, and the entry's landing site was wrong.** It
named `fake_start_registry.ex:186` and an ETS `ArgumentError`. At this row's HEAD
the same defect landed as a `FunctionClauseError` in a *narrow Mimic global stub
owned by a different module* (`operative_manager_request_stop_test.exs:71`,
`:113`). So "make the registry lookup answer *gone* instead of raising" closes
one door in an open corridor: the leaked worker calls `HubManager.get_pid/2`
under Mimic **global** mode and whatever stub is installed at that instant is
what raises. The only remedy that covers the family is row 2's answer to KI-49 —
**the background work stops with the test that started it.**

## KI-54 — the acceptance is the registry, never the log

`ForwardSup` is in the **application** supervision tree (`application.ex:90`),
so `operative_forward_test.exs`'s `unless Process.whereis(ForwardSup), do:
start_supervised!(ForwardSup)` was always false and nothing ExUnit tears down
owned the workers: 26 tests created one, one stopped one, and a refused delivery
arms `retry_timer` (`forward_worker.ex:266`) and retries for ever by design
(`:17`, "payloads are never dropped"). The raise is uncaptured because the
delivery is a bare `spawn` whose `try` catches `:exit` only
(`forward_worker.ex:206`–`:212`).

- **The defect shows through the log 1 run in 6 and through the registry every
  time.** Converting a sampled symptom into a deterministic one is the whole
  reason the fix can be believed: detector red at **1 of 28** tests, green at 28
  after. Never count runs when the state is readable.
- **The design's snapshot sweep is racy.** `WorkerSup.ensure_worker/4` drops a
  key before it re-registers the replacement, so one `Registry.select` can miss a
  live worker — 8 of 28 still failed. The sweep repeats until the registry stays
  empty, reusing the module's own `wait_until/1`.
- **`on_exit` is armed BEFORE the assertion.** In the design's order the first
  leak raises in `setup` before `on_exit` is registered, disarming the sweep for
  every test behind it — one key appeared in **20 consecutive failures** that way.
- **`deliver_next/1`'s `try` is deliberately left catching `:exit` and not
  `:error`** (`forward_worker.ex:176`–`:177`: a dead delivery process is
  indistinguishable from an unreachable target, so it is retryable). Widening it
  turns a real raise into a silent retry loop. `ForwardSup` also stays in the
  application tree: a test's hygiene is not a reason to change a supervision tree.

## KI-18 — one missing barrier with two faces, and one unpinned node-global read

- `worker_registry_test.exs:304`: `handle_call({:swap, …})`
  (`worker_registry.ex:185`) checks `state.entries[key]` first, so a monitor
  `:DOWN` still queued behind the three swaps leaves the dead pid in the entry.
  Two faces, one cause: `:385`'s `assert_receive {:starter_called, …}, 100`, or
  `:390`'s `assert entry.callers == 3` with `2`. **The window is forced, not
  sampled** — the registry's `starter` is a *test-supplied fun*, so a scratch
  instrument blocks the registry inside `handle_call`, queues the three swaps,
  kills the worker behind them and releases; unfixed it fails every run. The
  barrier is the positive settle the registry already publishes
  (`worker_registry.ex:378`–`:380`), which proves the DOWN was **processed** —
  the `eventually/2` poll at `:440` would only prove a side effect became
  visible, and needs a timeout to tune.
- `watchdog_test.exs:395`: `DurabilityTier.breaches/0` folds node-global
  `Shipper.status()`, and the closing `assert breaches() == []` asserted about
  every emitter on the node. **The forced red is a deliberate in-test foreign
  breach that stays in the test** — it is what keeps the pin load-bearing, and
  removing only the pin is then the negative control. The pin is needed **twice**:
  the closing assertion and the `wait_until` above it, which acquires the same
  exposure the moment a foreign entry exists.
- **Six of eight coordinates had drifted** (`591→598`, `383→389`, `413→419`,
  `303→304`, `390→395`, `384→440` — the survey predicted `430` and segment A's
  own barrier moved it again). A `file:line` that rots is how an entry stops
  being checkable; every citation now carries the test's name as well.
- The entry's "most urgent thing, and it is not about tests at all" — re-point
  the seven `refactor-baseline-*` tags — was **already done**; the paragraph was
  stale and is deleted, with the ancestor check recorded in its place.

## KI-23 — the bound goes in the generator because the call sites cannot hold it

The filed remedy ("hold prefixes to ≤ 23") is unenforceable: **three of the five
sites do not spell a literal prefix** — two truncate a factory id by hand at two
different widths, and one computes `unique("pipe_#{factory}")` with no limit.
`TierE2EHarness.unique/2` takes the counter as an argument and slices the prefix
against `Validator.id_byte_limit()`, so the result fits at *any* counter width
(a reserved digit count only moves the cliff) and `32` is never written down.

- **Its red-first is a negative control, not a natural red, and the note says
  so.** The natural red needs a counter ≥ 10⁷ ≈ 281,250 `unique_integer` calls —
  ~167× a full plain suite at this box's stride of 32.
- **Inertness was proved over the whole tree**: all 40 literal `unique("…")`
  prefixes × widths 1–7 = **280 pairs, 280 byte-identical**. That test is green
  in *both* arms of the negative control, which is what makes it an inertness
  assertion rather than a second bound assertion.
- Exactly **5** ids change, all from the two over-conservative hand truncations,
  all getting *longer*, all still inside 32 by construction.
- Deletion-funded: one private `unique/1`, two hand truncations, and **four**
  private `unique_id_path/0` copies (the design named one) collapse onto
  `unique_id_path/1` with a neutral default, 15 call sites untouched.

## KI-66 — the mint becomes a claim, and KI-23 is deliberately not shared with

`System.unique_integer([:positive])` restarts low in every BEAM, so run *N+1*
re-mints run *N*'s `/tmp` names and `File.mkdir_p!/1` is silent on a directory
that already exists; `WalWriter.append/4` then answers `:duplicate` for a seq it
has already persisted. `File.mkdir/1` retried on `:eexist` is the claim — an
existing atomic primitive, no new naming scheme, no call site changed, and no
dependence on `/tmp` being clean, which was the binding constraint.

**A shared mechanism with KI-23 was refused and the refusal is right.** An
operative id **cannot be claimed at mint time** — nothing exists on disk when the
generator returns, so there is no `mkdir` to fail. And the one scheme that would
serve both, a cross-BEAM-monotonic `System.os_time(:microsecond)` suffix,
**spends KI-23's entire budget**: 16 digits plus a separator leaves 15 bytes of
32. Two problems that look identical; one primitive that fits one of them.

The claim is `NodeTestHelper.claim_tmp_dir/1` with five callers — placed there
because it is the **lowest layer among the test helpers** (every other helper
depends on it, it depends on none) and it already owns the sibling concept
`peer_dir/2`. `peer_dir/2` itself is deliberately excluded, with a comment: its
name carries no per-BEAM counter to re-mint. `unique_id_path/1` claims a
*directory* and returns a path inside it, because there the file itself must be
absent — a runner that finds an id at `FLEZHA_RUNNER_ID_PATH` adopts it.

## KI-67 — the regression test that passed on the code it existed to catch

**This is the row's real lesson.** KI-66's first regression test planted a
512-wide band at `counter+1 … counter+512` and asserted the mint was not in it.
Measured on the *defective* helper: **15 red of 20** (worker) and **7 red of 12**
(supervisor, independently). It green-lit the defect roughly two times in five.

- **Mechanism**: that stream is node-global **and per-scheduler striped**, so the
  value the test draws and the value the mint draws afterwards are not adjacent —
  the whole node draws while the 512 directories are being written, and a
  scheduler migration moves the draw to a different stripe. Drift measured
  `+6365`, `+829`, `+509`, `+29` and **`−260`**; a negative drift puts the mint
  *below* the band entirely.
- **Its vacuity guard could not see that.** `assert minted > counter + band` is
  true both when the claim walked the whole band and when the first candidate was
  already past it, so it separated nothing. **A guard that is true in both the
  caught and the missed case is not a guard**, and it is exactly what made the
  test look rigorous.
- **The fix is a real second BEAM**, which is what the defect is: a child
  `elixir -pa <flezha ebin> -e …` calls `tmp_app_data_dir/0` in a process whose
  counter genuinely starts low. No retry loop, no probabilistic argument.
- **The child must advance its counter to a known floor first.** A bare boot
  lands anywhere in **99..1417** on this box (25 runs) because of the same
  striping, so a band sized on "where a fresh BEAM starts" still misses — the
  first arm of this fix did, at 1 of 12 seeds. The child draws until it is past
  `4096`; the parent plants `4096..4608`.
- **The new guard discriminates, and that was demonstrated rather than argued.**
  `assert (head + stride) in @band` names the claim's first candidate. On the
  deliberately mis-sized arm one seed drew `325` against `1024..2048` and the
  test reported *"the claim asked for 325 first, outside the planted band"* — the
  exact case in which the old test returned a green.
- **Acceptance is 12 of 12 red, never a majority**, at the supervisor's twelve
  seeds, and every one of them on `refute Map.has_key?(planted, base)` rather
  than on the guard. Restored: 12 of 12 green, 0.4 s each. It is also robust to
  `/tmp` residue in both directions — 292 foreign trees planted immediately
  adjacent to and past the band, still green — which the old test was not.
- **The process lesson**: a regression test is proved by its negative control and
  by nothing else. Segment A's checkpoint accepted the worker's claim without
  running it, which is what let a defective proof ship inside a correct fix.

## Standing facts and traps

- **`check-kind-naming.sh` is red on prose in `docs/known_issues.md` and
  `docs/refactor/STATE.md` (KI-65), and the count climbs whenever the board
  discusses it.** Nine hits at this row's HEAD, nine after — this row's prose was
  written to avoid the token rather than to add a tenth.
- **A red `--include system` run is not a regression until its first crash line
  is read.** One `driver init failed` (`sun_path`, KI-57) cascaded into 471
  failures and passed clean on re-run.
- **`workflow_crash_resume_e2e_test.exs:296` fails deterministically** and is
  KI-68, pre-existing since `a9787a0f`: `ControlChannel`'s `@drain_owed_ms
  60_000` waits the full minute for direct outputs a resumed run never delivers.
  Ruled out of this row — row 4 changes nothing in `lib/`, and a 60-second
  production drain cap is a behaviour decision.
- **`refactor_metrics.py --check` and the naming gates walk `git ls-files`**, so
  a new, untracked test file is invisible to them. `git add -N` every new file
  before reading a gate's exit code — segment A's `--check` was exit 0 for that
  wrong reason.
- **ExUnit's module order is neither the command line's nor seed-shuffled**, so
  running module A then module B to reproduce a cross-module leak is
  corroboration and never the proof.
