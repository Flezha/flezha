# The veneer fixture port — queue row R5, S2–S5

`port-durable-fixtures-to-veneers`: the `state_persistence`, `schema_evolution`,
`entity_timers`, `effectively_once` and `correction_window` fixture twins for
Node, Ruby and PHP, 42 fixture implementations under one change. What the
proposal and `design.md` say is not repeated; this is what the next person
porting a fixture — or reading a red arm — needs.

## The manifest flip is the ratchet, and the red half is a skip count

A capability leaves `deferred` and enters `fixtures` with exactly the
reference's ids (`gate_test.exs:290` refuses any other set), and the spec says a
carried capability *runs and passes; there is no third state*. So a segment's
proof is never "the fixtures register": it is the tagged scenarios green on
each runtime, with the same command run **before** the flip to record the
skips — `18 passed, 10 skipped` with the deferral line naming the capability is
the red half, `23 passed, 5 skipped` the green one. Re-derive the tagged files
with `grep -ln 'capability: :<cap>' test/system/*.exs`; the list in a plan goes
stale.

## Where the fixtures live: the template's partition, forced by PHP

`test/apps/php_runner/fixtures.php` was 692 lines against a `test/apps`
ceiling of 800, so the persisted-state family went to a companion in every app
— `fixtures_state.{mjs,rb,php}` — with exactly `fixtures_state.py`'s
partition: the keyed upsert, the six persistence fixtures, schema evolution
and the four tier fixtures; timers and `candle` stay in the main file. Node's
main file re-exports it (`export * from './fixtures_state.mjs'`; `specsFrom`
reads `Object.values(module)`, which includes re-exports), Ruby's reopens
`module Fixtures` and splats a `STATE` list into `ALL`, PHP's is a sibling
`require` in `runner.php` so the Swoole worker bootstrap loads it too.

**Node needs a third file, and it is not optional.** `defineOperative` builds
the graph eagerly (`authoring.ts:412`), so a companion that imported the
`text`/`bytes` helpers back from `fixtures.mjs` would evaluate first in the ESM
cycle and hit the temporal dead zone. `fixture_support.mjs` is
`fixture_support.py`'s layout for that reason.

## The erased-host live read is not a persisted-seq signal

Every erased host publishes an Entity's state before its changes reach the
core: Ruby's `invoke` writes the state back in its `ensure`
(`dispatch.rb:158-160`), Node's in `finally`, PHP's likewise, and only when the
work returns does `complete` hand the change bodies to the core's flusher. A
`live_state_read` served in that window — by another of Ruby's eight workers,
or between awaits on Node's loop — answers a total whose changes are not yet
framed. `schema_evolution_e2e_test.exs` used the live read as its settle
signal before a `stop_operative(preserve_state: true)` and read `0/2` on ruby
in four of six runs; it now awaits the persisted seq with the
`CreditTestHelper` functions `credit_flow` already used (KI-129 is the seq
stamp's silence about this). A test that stops an operative after a live read
has the same race on any host with more than one loop.

## Fidelity choices, both against Python

Rust and Java name `persistence_off_counter`'s procedures
`persistence_off_entry_procedure` / `persistence_off_counter_procedure`
(`state.rs:533,557`); Python reuses the classes under the `counter_*` names.
All three veneer builders take a node-name override (`into(klass, name)`), so
the ports reuse the classes **and** keep the reference's names. And
`coalescing_counter` ships uncoalesced in every veneer — the coalesce key is a
Rust `Change` trait method with no SDK seam — which is a throughput property,
not a semantic one: the fold is idempotent per `(interval, ts)`.

## What the tier goldens proved

`tier_store.json` and `checkpoint_batched_store.json` (the second pins
`"ack_granularity": "checkpoint"` with no `checkpoint_every`) were the first
end-to-end assertion that `imply-checkpoint-granularity-in-veneers`' seam
works; nothing under `test/system/` reached that code until S4 flipped the
manifests. They matched on all three runtimes.

## Traps that cost time, so nobody pays twice

- **Ruby needs `PATH="$HOME/.rbenv/versions/3.4.10/bin:$PATH"` on every command**
  — `mix test` arms, `rake test`, `rubocop`, and `scripts/build-ruby-sdk.sh`,
  which otherwise dies on `no ruby.h under /usr/include/ruby-3.2.0`. Without it
  the runner never boots and a test times out at 60 s looking like a hang.
- **`pgrep -f` matches the shell running it.** A cleanup loop that kills
  `pgrep -f 'php_runner/runner.php wedge'` kills itself; anchor on the
  interpreter (`^php /home`).
- **A wedged PHP `sync` loop cannot be stopped.** SIGTERM and stdin EOF are
  polled between polls, so a loop inside a body never observes either; the
  `control_plane_wedge` drill leaves two such processes per run, and
  `exit_terminally`'s graceful sequence waits on the wedged Entity's gate as
  well. Kill them by pid after the drill.
- **A manifest written with `json.dumps(..., ensure_ascii=False)` rewrites the
  file's `—` escapes** and produces a hunk that is not the flip.

## What the row found that it did not fix

KI-123 (the erased hosts never shipped their state on shutdown — fixed first,
its own commit), KI-126 (the same entry re-registered a stopping runner —
fixed with it), KI-127 (Java's `store_input` twin spelled its state camelCase
— fixed), KI-125 (Python aborts at exit after SIGTERM: a daemon thread on
stdin and CPython's finalisation), KI-128 (`credit_flow` under php, 1 in 20,
unreproduced), KI-129 (the live-read seq stamp). Each was found by a scenario
running on a runtime for the first time, which is the point of the row.
