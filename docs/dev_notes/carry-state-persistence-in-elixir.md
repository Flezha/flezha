# `state_persistence` in the Elixir SDK — the kernel's rules re-expressed in its own core, and the first persisted capability the programme made universal

`carry-state-persistence-in-elixir`: the record stream, the restore fold, compaction,
the live read, the credit window and the shutdown notification in `lib/flezha/sdk`,
which inherits nothing from `flz_core`. What `proposal.md` and `design.md` say is not
repeated; this is what the next person touching an Elixir Entity's records, a
`Flezha.SDK.Commands` handler or the fixture twins needs.

## The slot is the load-bearing decision, and its red-first run says why

An Elixir Entity is its process. The kernel's `EntitySlot` outlives any worker; here the
Operative recreates a killed Entity fresh, and a fresh changeful Entity starts at
`change_seq = 0` — from which `WalWriter` (`wal_writer.ex:286`, `seq <=
last_persisted_seq → :duplicate`) drops every later record while the runner keeps
dispatching. The Operative therefore owns one `:public` ETS table of slots, a
changeful Entity writes `{state, change_seq}` into it **after** its records are on the
wire, and `Entity.seed/4` reads it in `init/1`. The test that pins it ("an Entity killed
untrappably comes back on its seq line from the slot") was red with the seed held back —
`right: {0, {:delta, 4}, …}` — and green with it. The alternative, a deliberate seq gap
that makes the controller resync, was refused by the supervisor: recovery through an
error path that folds inputs into a cold state until the resync lands is the silent loss
the slot exists to prevent.

## One process writes one stream, so ordering is by construction — per dispatch

The Entity ships its own records on one secondary chosen by `phash2(identity)`, then hands
its outputs to the Operative, which writes them on the stream it picks by `phash2(oid)`.
Within a dispatch the record precedes the output (flush-before-emit). Across dispatches a
later record can overtake an earlier output, because the output takes one more hop; the
first version of the ordering test assumed strict interleaving and flaked 1 in 4. Assert
per dispatch. The compaction test has the same shape: request the snapshot after the
outputs have been *observed*, or it lands between two dispatches (legal, at seq 0).

## Two CRCs, and the on-disk one is the fold's

The wire frame's trailing CRC covers the body (`Wire.encode_frame/4`); the controller's
on-disk record — what `RestoreInitState.change_log_bytes` carries — is
`[seq:64][len:32][header:16][body][crc32 over header <> body]`. `Record.fold/6` checks
the on-disk one (`:erlang.crc32(:erlang.crc32(header), body)`), then resolves the header
**language → format → schema** with the core's wordings, because
`schema_evolution_e2e_test:187` asserts one of them verbatim and `replay.ex:225` forwards
the SDK's `errors` as the refusal reason. The `sdk_lang mismatch:` text names both
languages by the core's table; `format_id 7` is allocated in `flz_core/src/header.rs`
beside PHP's JSON, one constant and one line in `format_ids_are_distinct` — the only
Rust this change touches.

## `named/2` names the channel too, so two nodes of one changeful module are refused

The reference gives a named node its own identity (`state.rs:214`,
`node_name.unwrap_or_else(S::name)`), so two `into_named` nodes of one Procedure never
share a WAL there. Here `named/2` after `into/2` names the node **and** is what the
reference spells `.named("sink")` on the channel, and `compaction_e2e_test:39` pins
`store_input_procedure` — the module's `name/0` — as the WAL segment. So the WAL identity is
`module.name()`, and `Topology.build/1` refuses a class with two nodes of one changeful
module: `{:DuplicatePersistentProcedure, "counter"}`, an issue name the reference has no
need for.

## The credit window is a pid, not a name

The first `Flezha.SDK.Credit` was a named singleton like the RunnerServer. A `:normal`
stop of the RunnerServer does not kill a linked non-trapping process, so the next
runner's `start_link` met `{:error, {:already_started, …}}` — in an existing test. The
window is unnamed, started in `RunnerServer.initial_state/1`, and handed down by pid
through the Operative to every Entity; `acquire/2` on a window that is gone returns
`:ok` (un-metered), which is what a dying runner wants. Tests start their own.
`credit_parked` counts iterations the way `credit.rs:40–47` does: one per park, one more
per grant that wakes the head without covering it.

## SIGTERM is the BEAM's own `init:stop`

In a runner-only boot no `SignalHandler` runs (`application.ex:121–125` is the controller
tree), so `sigterm` → `init:stop` → application stop → the supervisor's shutdown →
`RunnerServer.terminate/2` → `Commands.shutdown/2`, and the acks arrive on the terminating
process's own mailbox. Proved by `kill -TERM` on a spawned standalone runner
(`{"do_nothing_procedure":null}` reached the Overwatch, then the VM exited) and in-node
by `Flezha.SDK.close/1` (`transport_modes_test.exs`). `child_spec/1` sets
`shutdown: 10_000` — the 5 s serialise wait plus the 2 s ack wait, inside a container's
grace period. The standalone app's stdin-EOF `System.halt/1` skips `terminate` by design:
that is the harness killing the port.

## The gate that printed instead of failing

`mix compile --warnings-as-errors` compiles `lib/` and never the test files; the
checkpoint's claim "warnings-as-errors clean" was false on two unused aliases the harness
extraction left behind, and `mix test <dir> --warnings-as-errors` prints `Result: 75
passed` **and then** `ERROR! Test suite aborted … due to warnings`. Prove it by exit code
(`; echo $?`), never by the result line.

## Where the ratchet bit, and what each cost

`init/1` crossed 50 lines when it started the window (→ `initial_state/1`); four codec
names crossed 32 characters (`encode_restore_response/3`, `encode_compact_response/3`,
`encode_credit_ack/1`, `shutdown_notification_command/2`); `persistence_test.exs`
crossed 800 (→ the fixtures in `test/helpers/sdk_persistence_fixtures.ex`, the frame
helpers on `SdkOperativeHarness`, the commands in `commands_test.exs`); one `AliasOrder`
in a test. `duplicate_pairs` read differently under `--area` subsets than under the
whole-repo `--check`, exactly as the charter says — read the check, name the pairs from
the rows.

## Small things that will bite

- `Ctx.new/3` takes the module; `Ctx.apply/3` needs it for `apply_change/2`.
- A `state_from_json/1` is needed by every Procedure whose state keys are atoms — the
  default hands the string-keyed object back, and `%{s | value: …}` then raises on the
  next dispatch. The fixtures declare it.
- `Entity.read/2` answers `{:error, :busy}` on the 5 s gate and `{:error, :gone}` for a
  dead pid; a walk omits both, a read that names one Entity answers `entity_busy` /
  `entity_not_found`.
- `Operative.lookup/4` materialises a dynamic key only with `materialise: true` — a
  restore's case; a live read never mints an Entity.
- The elixir_runner app's `_build` is its own: rebuild it through the manifest's build
  command (`MIX_ENV=dev mix compile` in the app dir) before any arm, or the arm proves the
  old SDK.
- KI-134 leaks follow the *multi-module* elixir e2e run; five per-module runs leaked
  nothing, the supervisor's combined run leaked one. Check with an anchored `ps` for
  `mix run --no-halt --no-deps-check -- ` after every arm.

## What the change found that it did not fix

KI-135 — `Procedure.max_entities/0` is declared and never enforced (reproduced at
runtime, three live Entities against a cap of two); owed to the row's change 2 with
`dynamic_entity`. The Elixir output frame carries the plugin name where every other SDK
carries the declared channel name (`operative.ex:161` against `emit_sink.rs:150`,
consumed by `operative_forward.ex:222`) — the documented deviation `keyed_channels` must
close. `D5`'s sentence that an Entity at `change_seq = 0` answers `{0, 0}` with an error
does not match the core, which ships a snapshot at seq 0; the code follows the core.
