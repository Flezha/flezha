# `lib/flezha/plugin` — four cycles cut by six named edges, and the one ceiling that stays

## The edge ledger — `cycles` 4 → 0, and the metric alone does not show how

Four cycles, six edges, each named:

**The 55-file cycle** (it held `plugin/extension.ex` and `plugin/extension/table.ex`)
- `extension/table.ex → service/plugin_manager.ex` — the two
  `PluginManager.unregister_plugin/1` casts at `teardown_all/1` and
  `do_unsubscribe/3`. The table is a data structure, so it now REPORTS the ids
  it dropped and `Operative.Plugins.unregister/1` casts. 55 → 53 files, and no
  plugin file is in it.

**kafka 3-cycle** (`kafka.ex ↔ producer_sup.ex ↔ producer_worker.ex`)
- `producer_worker.ex → producer_sup.ex` — `ProducerWorker.via/3` deleted;
  `WorkerSup.ensure_worker/4` puts `via/2` into the child's opts, so a worker
  never names the supervisor that started it.
- `producer_worker.ex → kafka.ex` — the seven telemetry emitters moved PRIVATE
  into their one caller.

**operative_forward 3-cycle** (`operative_forward.ex ↔ forward_sup.ex ↔ forward_worker.ex`)
- `forward_worker.ex → forward_sup.ex` — same `:via` inversion.
- `forward_worker.ex → operative_forward.ex` — the four emitters moved private
  into their one caller.

**postgres 2-cycle** (`sink_sup.ex ↔ sink_worker.ex`)
- `sink_worker.ex → sink_sup.ex` — same `:via` inversion.

**The `:via` inversion alone does NOT cut the kafka and operative_forward
cycles.** With the worker→sup edge gone the cycle runs
`provider.ex → sup.ex → worker.ex → provider.ex`: the supervisor becomes an
intermediate node, not the back-edge. Only the telemetry edge closes it. That
is why postgres fell to the inversion and the other two needed the emitters.

**The telemetry placement was decided by caller count, per family, not by
symmetry.** `operative_forward`: one caller (`forward_worker.ex`, 5 sites, no
test), and the file is 268 lines and is not split — so the emitters went down
into it and `055054f2`'s retirement of a `Telemetry` sibling stands. `kafka`:
also **one production caller today**, so the same. A three-caller reading
derived from D6's *proposed* split was refused as evidence for a module,
because that split is itself under re-decision — if it ever lands, the module
is earned at that moment with the split as its evidence.

## The three splits were all cyclic AS DESIGNED, and none shipped that way

Measured before a file moved, on code lines with `@doc`/`@spec`/comment text
stripped — an earlier pass counted a `@spec` line as a call and over-reported.

- **D5 (`extension/table.ex`)** — `S↔C` (4/1), `S↔H` (1/3), `T↔H` (5/4): all
  four groups in one SCC. Landed instead as
  **`Health (leaf) ← Subscription ← Chain ← Table`**, needing two moves: the
  four `rebuild_chain`/`mark_active` calls came out of
  `add_subscription`/`remove_subscription` into `Chain.refresh/2` at the caller
  (the same "the data structure reports, the owner acts" cut as the
  `PluginManager` edge), and `attempt_restart`, `handle_down`, `retry` and
  `fire_timer` moved from Health to Table because they are lifecycle
  operations TRIGGERED BY health rather than health itself.
  **No sub-module names `Extension.Table`**: they take the table as a plain map
  and use map-update syntax. Writing `%Table{} = table` in a companion — the
  natural thing — re-forms the exact cycle the split removes.
- **D6 (`producer_worker.ex`)** — 8 cyclic pairs, `Window→ProducerWorker` 14
  edges and the reverse 11. §7 says split by *mutation × SCC*; D6 grouped by
  mutation only. See the ceiling section.
- **D7 (`websocket.ex`)** — 4 edges. Landed as
  **`Subscription (leaf) ← Client ← Websocket`**; `established/2` and
  `schedule_subscription_poll/1` went down to Subscription (one caller in each
  group, so the common lower layer is the leaf), `compile_ignore_pattern/1`,
  `idle_tick_interval/1` and `@min_tick_interval_ms` down to Client, with
  `Websocket.idle_tick_interval/1` delegating because it is public API with
  three test callers.

**D7's real cost was not its line count.** `use Fresh` means moving
`start_link/2` re-points the callback module — `Fresh.start_link(uri,
__MODULE__, …)` now names `Client` — which pinned **58 test call sites across 8
files** to `Websocket.handle_*`. All rewired. Anything that moves a Fresh
callback pays that.

## `files_over_800` finishes at 1, deliberately

`producer_worker.ex` stays at **1 322** and it is the recorded outcome, not a
miss. The leaf `Wire` extraction took the 11 pure builders/classifiers;
`do_send/2` stays with the state because its two non-leaf edges
(`Broker.cancel_dispatch_timer`, `Window.put_batch`) are inside it — so D6's
goal of making its non-re-entrance "structural rather than documented" is not
reachable by putting it in a stateless module.

**There is no struct to pull.** That state is a bare `%{}` map, not a
`defstruct` — the material difference from `WalWriter.State`, which is what
dissolved the same shape in `lib/flezha/state`. Enumerating all 17
`Window → ProducerWorker` calls and reading each:

- **10 are shared accessors** — `partition_meta` ×3, `elapsed_ms` ×3,
  `retry_backoff_ms` ×2, `tag`, `caller_tag` — pure readers over the state map
  and its clock. A `Producer.State` dissolves all ten.
- **3 are the telemetry emitters**, pure given their arguments.
- **4 are genuine mutual lifecycle calls**: `drain`, `maybe_kick_batch`,
  `process_blocked`, `reply_to_record` — the window finishing a batch and then
  kicking the queue or releasing a parked caller.

**13 of 17 collapse; 4 remain, and 4 mutual edges is still a cycle.** A future
area takes the second cut by giving those four the reports-owner-acts shape, or
by extracting `Producer.State` first and then facing only those four. A
recorded, explained 1 322 beats a split that re-forms the cycle this area just
removed.

## `duplicate_pairs` — say which scan, every time

**Full scan (the number `--check` compares): baseline 14 → 13.**
Area scan (`--area lib/flezha/plugin`): 13 → 10. Both are correct for their
scan and they are not the same scale — `scan_mismatch` refuses a baseline taken
over a different set precisely because they cannot be mixed. A close-out
quoting the area figure against a full-scan baseline reads as 13 → 10 against
14, which is nonsense.

What is left, and why: three `Ctx.*` struct pairs (public structs plugin authors
pattern-match), two installer directory-walk pairs (leaf modules with no
dependency either way, so folding needs a new module to save 11 lines), two
`on_input`/`on_output` dispatch pairs (the matched tokens are the two dispatch
lines; the bodies differ entirely), two `*_sup.ex` wrapper pairs (the shared
core already exists — `WorkerSup` — and what remains is the per-family key
shape), and one worker pair. Each priced and refused rather than merged.

## Things that cost a turn to learn

- **`refactor_metrics.py` cannot see untracked files**, and a refactor adds
  them, so the error is one-directional. Every number in this area's reports
  was quoted only after naming the new paths for `git add -N`.
- **`async_false_share` had zero headroom** (0.811 against a 0.811 baseline),
  and every `async: false` candidate in this area NAMES a global that is
  genuinely true — three say "global telemetry handlers", one "the primary
  Logger", one "the live controller and its fleet". A `:telemetry` handler is
  process-global: a concurrent test emitting the same event reaches this test's
  handler. So no conversion could be justified, and the folds had to be free:
  three async:false folds took `test` 228 → 223 files with 43 `async: true`
  untouched, landing **0.807**.
- **All three test files over 800 share a large helper preamble** (http_poll's
  is a whole fake HTTP origin server, ~250 lines; websocket_subscription_source's
  ~120; producer_worker's second half needs `start_worker` 16 times, `attach` 8,
  `dispatched?` 7 and six aliases). Splitting any of them needs a shared case
  module under `test/support/`; duplicating the preamble would add a large jscpd
  pair to a ratcheted metric. Held, not forced — and **owed to area 7**, which
  owns `test/support/` and can build the case module all three need. `test`
  `files_over_800` holds at its baseline 7, so nothing regressed by waiting.
  **Discharged**: area 7 moved `test/support/` into `test/helpers/` (`5e3b32c6`)
  and built the case modules — `http_poll_case.ex`, `outbox_case.ex`,
  `installer_case.ex`, `state_case.ex`, `system_case.ex` — so the prediction's
  path is stale and its substance is met.
- **24 credo `Refactor.Nesting` extractions in one batch, and two were wrong** —
  a `&verify_entry(&1, dir)` capture is arity-1 where `reduce_while` needs 2,
  and that first version read the file TWICE where the original read once; and
  an `http_poll` `with` merged two error shapes and dropped the `"http_poll "`
  prefix from a refusal. The suite caught both. Two-in-twenty-four is the rate
  to assume; a batch claiming zero is the one to distrust.
- **Check the operator docs before cutting a moduledoc.** Twelve were over 15
  lines; `docs/plugins/` carried the option tables for all of them except
  http_poll's `:idle_after_ms`, which existed nowhere else and therefore stayed.
- **`build/1` raised where its own `@callback` says it returns** (KI-93): 44
  option fields across 7 of 9 providers. The `cond` arm the author wrote for
  that case was unreachable because `missing_fields` was computed first and
  died in `Map.has_key?/2` on a binary.
- **The provider parameter order was VM atom-table order** (KI-95), so it moved
  when unrelated code changed the modules' atom chunks. Pinned to `defstruct`
  declaration order via `__info__(:struct)` — which also deleted a
  `required_fields/1` that read `@enforce_keys` by REGEX-MATCHING the text of
  an `ArgumentError`.
- **`mix bench.extension` has not run since `1926bd28`** (KI-97): that commit
  renamed `partition:` → `entity:` in the bench's producer opts, but it is a
  Kafka partition, not a Flezha Entity. `ProducerWorker` still `fetch!`es
  `:partition`. The survey's observation that "no floor for this instrument
  exists anywhere in the programme" was, read afterwards, the first symptom.
  Nothing past is invalidated — no `baseline.json` bench or timing row comes
  from this task — the loss is prospective. P1 is `direction` and NOT run.
