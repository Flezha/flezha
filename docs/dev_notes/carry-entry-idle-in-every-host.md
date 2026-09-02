# `entry_idle` in every host — one latch in the core, five ticks, and the first capability the programme made universal

`carry-entry-idle-in-every-host`: the policy half of the entry-idle signal in
Node, Ruby, PHP, Python and Elixir, over a latch that moved down into
`flz_core` so no host re-derives it. What `proposal.md` and `design.md` say is
not repeated; this is what the next person touching an idle observer, a
veneer's tick or the Elixir cells needs.

## The observer is three parts, and the row's answer is "share one of them"

Accessor, latch, tick-and-delivery. The **latch** (`evaluate(now)` over the
stamp, with the `fired_at` that makes a resume exact) is pure and any-thread,
so it lives once in `flz_core::entry_input::IdleWatch` and crosses the C ABI by
watch id — `flz_core_idle_watch_new` / `_poll` / `_free`, context-free like the
two clock reads beside them. The **tick and the delivery** stay per host,
because a native tick calling back into the host is impossible on two of them:
MRI cannot adopt a thread it did not start (the reason `ruby_glue.rs` veneers no
log callback), and PHP has no thread for an FFI callback at all. A veneer's tick
is therefore *poll the id, deliver the answer*, and its unit tests prove wiring
against a scripted core; the state machine is proved once, in `flz_core`, and
once over the ABI. Java keeps its own copy of the latch — its test injects
clocks — under the named debt `move-java-entry-idle-latch-onto-the-core`.

## Where each host ticks, and what that costs the contract

| host | tick | delayed by |
| --- | --- | --- |
| Node | `setInterval(...).unref()` on the main loop | a synchronous body under `workers(0)` |
| Ruby | one thread on `Queue#pop(timeout:)` | a pure-Ruby body holding the GVL |
| Python | one daemon thread on `Event.wait` | a pure-Python body holding the GIL |
| PHP | between polls of the thread that started the runner: `Runner::serve` (`sync`), `SwooleExecutor::supervise` (`swoole`) | a body under `sync`; nothing under `swoole` |
| Elixir | `Process.send_after` in `Flezha.SDK.EntryIdle` | nothing |

The `sleeps` ratchet has zero headroom in `test`, `test/apps` and
`flz_core_ffi`, and `refactor_metrics.py` counts `sleep(` in Ruby and PHP and
`time.sleep` in Python as production sleeps — which is why no tick sleeps:
the queue pop, the event wait, the poll loop, the timer. The ABI test drives
the latch with a **zero threshold and a bounded spin**: the first poll after a
stamp reads a later nanosecond and fires.

## Two hosts dispatch where the observer cannot see

The observer runs where it was registered. Under `FLEZHA_NODE_WORKERS` the
procedure runs on a worker isolate that imported `fixtures.mjs` afresh, so the
Node twin's counters cross on a `BroadcastChannel` and every isolate reads
`absent_at_boot` itself at import (before any frame can exist). Under
`SwooleExecutor` a static is one value per interpreter — the reason
`PoisonProbe`'s antidote is KI-133 — so the PHP twin publishes
`<absent>:<episodes>:<resumes>` to a pid-keyed file the procedure reads whole; a
torn read fails the scenario's parse, which polls again. The entry script
re-runs in every Swoole worker, so `Runner::onEntryIdle` is a **no-op on an
attached handle**: only the thread that started the runner owns its observers,
as it owns its signals (`Shutdown::watch`'s rule). The twin's file is seeded
only when absent, so a worker's re-run cannot clobber the counts.

**The Swoole arm ran, for the first time, through a `php` shim** that executes
the harness's own launch line inside `flezha-php-zts` (`docker run --rm -i
--network host -u $(id -u):$(id -g)`, the repository and `/tmp/flezha_sockets`
mounted at their host paths, `FLEZHA_*` forwarded): `runner up … swoole
executor`, the `[php_runner]` line twice, `1 passed`. One container outlived
the run by two seconds and was stopped by hand. KI-132 is CI's gap, not the
arm's.

## A watch id before a context

Every veneer's `last_entry_delivery` and `on_entry_idle` are legal before the
runner starts, and each needed one seam for it: `Core.context_free` (Ruby —
`Native` after `load_extension`), `Core::contextFree()` (PHP — a `Core` with a
null context whose context-bound calls refuse), `load_extension()` (Python —
the module's own functions), `loadNative()` (Node — cached). PHP's `Core` also
implements `EntryIdleBridge`, the `SpawnBridge` shape, so a test scripts the
five reads without the FFI.

## The Elixir clock is nanoseconds since VM start, and a young VM has few

`Flezha.SDK.EntryInput.now_nanos/0` anchors `System.monotonic_time/1` at
`:erlang.system_info(:start_time)` so `0` can mean "never delivered" as it does
in the core. A test that stamps `now − 2 s` on a VM that has been up 1.6 s
clamps to the sentinel floor and every gap reads as the uptime — three tests
failed that way. Stamp instant `1` (the gap is then the uptime, past any
threshold) and prove the accessor through the pure `EntryInput.instant/3`,
Java's `deliveryInstant`. The suite logs at `:error`
(`config/test.exs:165`), so the one test that asserts the contained-observer
warning raises the level for `Flezha.SDK.EntryIdle` alone with
`:logger.set_module_level/2` — `test_helper.exs`'s own idiom — and unsets it on
exit. Interning is a `:persistent_term.put` and therefore a global GC scan: the
RunnerServer interns an Operative's cells before starting it and a
registration's cell before forwarding it, one process, never on delivery.

## The Elixir twin does not `named/2` its second edge

Unnamed mentions of one module unify into one node; a named mention is a
different node. The reference's `.named("report")` is therefore dropped in
Elixir, whose output frame carries the plugin name anyway — which is what the
harness's `forward_sink/2` binds.

## Small things that will bite

- `native-types.test.mjs` pins every `#[napi(object)]` to a TypeScript mirror;
  a new object struct needs its `MIRRORS` row or the package suite is red.
- A `private const` is not visible to an anonymous class in PHP; `phpunit`
  says "Cannot access private constant" from inside the closure.
- `cargo test -p flz_core_ffi --features ruby-embed` needs
  `BINDGEN_EXTRA_CLANG_ARGS="-I$(gcc -print-file-name=include)"` on a box
  without clang's builtin headers — `scripts/build-ruby-sdk.sh`'s own rule.
- The Rust SDK's integration tests need `--features bench-internals,testing`.
- `BindingTest` pins `Core::live()`'s refusal text; a context-free `Core`
  refuses through the same message.

## What the row found that it did not fix

KI-132 (no Swoole e2e arm anywhere — CI builds the zts image and prints a
version line) and KI-133 (`PoisonProbe`'s per-interpreter static), both filed
by the supervisor from the survey. The five credo `--strict` findings in
`lib/flezha/sdk` (`route_from`, `establish_sockets`, `inbound_channel`,
`define_stream`, `add_operative`) predate this change and did not move.
