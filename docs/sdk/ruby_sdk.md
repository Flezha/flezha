# Ruby SDK

The `flezha` gem is a poll host over `flz_core`, the runner kernel the Rust,
Java and Node SDKs also drive. The kernel owns transport, ordering, per-entity
exclusivity, durability, retries, timers, credit, routing and the workflow
driver. This gem supplies threads and user code, and nothing else —
[`poll_core.md` → "The core decides, the SDK brings the threads"](poll_core.md#the-core-decides-the-sdk-brings-the-threads).

- **Gem**: `flezha`.
- **Source**: `sdk/ruby/`; the native veneer is `flz_core_ffi/src/ruby_glue.rs`,
  behind that crate's `ruby` feature.
- **Scope**: `test/apps/ruby_runner/conformance.json` is the scope matrix — the
  capabilities this SDK carries and the ones it defers, checked against the
  Rust reference by `test/sdk_conformance/gate_test.exs`. This page does not
  restate it, because a restatement drifts.

## Supported Ruby versions

Ruby **3.3 or newer**. Two things fix the floor: `Process.warmup` is 3.3+ and
the startup order below calls it unconditionally, and Ruby 3.2 is past end of
life. YJIT is enabled where the running Ruby has it, and reported either way.

## Installing

```bash
gem install flezha
```

A precompiled platform gem carries the native library, so installation needs no
Rust toolchain, no cargo invocation and no network fetch of Rust dependencies.
A platform with no prebuilt gem falls back to the source gem, which does need a
stable Rust toolchain and this repository's `flz_core_ffi` crate.

From a checkout, `scripts/build-ruby-sdk.sh` builds the veneer and installs it
where the gem loads it from.

## Authoring

Classes, registered explicitly. There is no autoload, no load-path walk and no
package scan: what runs is what you named.

```ruby
require "flezha"

class Upcase < Flezha::Procedure
  def initial_state(_entity_key) = { seen: 0 }
  def apply_change(state, change) = state[:seen] += change[:by]

  def process(input, ctx)
    ctx.change({ by: 1 })
    input.upcase
  end
end

class UpcaseOperative < Flezha::StreamOperative
  factory_id "upcase"
  channels do |c|
    c.input("input", decode: ->(bytes) { bytes.dup.force_encoding("UTF-8") })
     .into(Upcase).named("out")
     .sink("upcase_sink", encode: ->(value) { value.to_s.b })
  end
end

Flezha.create_runner("default").define_stream(UpcaseOperative).run
```

Which optional hooks a procedure declares is read from the methods it defines —
`apply_change` present means it folds changes, `on_timer` present means it may
register timers, `entities_for` present means it fans out over a static key set
— so a declaration and its implementation cannot disagree.

Everything a type system would have caught is refused at registration instead,
naming the class, the handler and the defect. That is the same set, for the
same reasons, as the Rust builder's `TopologyIssue`.

### State and Change

State and Change serialize as MessagePack. A plain `Hash` needs nothing; a
class declares `state_type` / `change_type` and supplies `to_h` and `.from_h`:

```ruby
class Basket < Flezha::Procedure
  state_type Cart          # Cart#to_h and Cart.from_h
  change_type Added
end
```

**`Marshal` is never used**, and a unit test keeps it that way. `Marshal`
embeds class names and instance-variable layout, so renaming a State class
would break the restore of every record written before the rename — and the
first anyone would learn of it is a restart. Encoding the Hash instead is what
makes the rename survivable.

Ruby's integers are arbitrary-precision, so the unsigned 64-bit offsets,
sequence numbers and timestamps in the schema need no width policy: there is no
`2^53` cliff and no `PHP_INT_MAX` ceiling to work around.

Declared state is mutated only through `ctx.change`. A field assigned directly
is live in this process and gone after a restart, and nothing would report the
difference until the restart. Ruby has no cheap deep read-only view — the Node
SDK's guard is a `Proxy`, which has no counterpart here that does not cost an
allocation per field read — so this is a rule rather than a runtime check.

## Startup order

```
declare classes  ->  warm up  ->  fork (if you fork at all)  ->  start
```

`Process.warmup` runs after the last declaration and before the first `poll`.
It compacts the heap and promotes the declared object graph to the old
generation, which is what makes a subsequent fork copy-on-write-friendly.

**The core must not be started before a fork.** It owns native threads —
transport, flusher, the runner loop — and `fork(2)` does not carry them, so a
child would connect to nothing and serve nothing. The SDK records the process
that started the core and raises, naming this rule, on any core call from
another process; a documented rule alone would not survive contact with Puma
cluster mode. `Runner#boot` and `Runner#run` both take the core, so an operator
who forks starts one in each child.

The SDK spawns no processes. Process count is the orchestrator's decision, and
each process is one runner — which is what HRW placement spreads across.

## Parallelism, as arithmetic

- **Ruby-level handler execution is bounded to one core per process.** That is
  the GVL, and it is not worked around.
- **Everything below the poll seam is genuinely parallel.** Transport, framing,
  durability, retries, credit, routing and the activation codec all run in Rust
  with the GVL released. The lock bounds the handler body and nothing else.
- **Work underway is capped by `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`** (default
  1024), regardless of pool size. The pool is a servicing detail, never the
  concurrency bound.
- **The lever for CPU-bound handlers is process count, not thread count**:
  `flz operative start -p N`.

Worked example. Handlers that spend 90% of their time on I/O and 10% on Ruby,
at 5 ms each: one process saturates its core at roughly 2,000 activations per
second, and the pool grows to cover the I/O wait. Handlers that are pure Ruby
at 5 ms each: one process serves about 200 per second no matter how large the
pool is, and four processes serve about 800.

## The threading host

A base pool of Ruby threads, each looping `poll -> dispatch -> complete` on
itself, plus one supervisor parked in the core's starvation wait. Each
starvation observation starts one more worker up to a ceiling; workers above
the base retire after an idle streak.

| Knob | Default | Meaning |
| ---- | ------- | ------- |
| `FLEZHA_WORKER_POOL_SIZE` | the machine's processor count | base workers |
| `FLEZHA_WORKER_POOL_MAX` | ten times the base, never below it | the ceiling |

These are the Rust host's knobs, deliberately: this host is the Rust host's
model, and a second vocabulary for the same decision makes a fleet harder to
reason about. The SDK introduces no additional pool knob.

Java's hand-off model is not used here, and the reason is the reason behind
both shipped hosts. Java hands each activation to a virtual thread because a
parked virtual thread costs a stack and nothing else — which is also exactly
why Java never starves and never calls `wait_starvation`. An MRI thread is a
real OS thread with a real stack, so hand-off would buy nothing and cost a
thread per activation up to the core's in-flight cap.

### The GVL, and the interrupt bound

Every blocking core call — `poll`, `wait_starvation`, `complete` and the
workflow inbox receive — runs with the GVL released, through one function in
the veneer. `workflow_recv` is the sharp one: a workflow stage waiting seconds
on its inbox with the lock held would freeze every other thread in the VM,
fetchers included.

`flz_core_shutdown` wakes *every* poller, so the ABI has no per-thread wake and
none is in scope to add. Each `poll` is therefore bounded by a timeout, and
that timeout — **250 ms** — is the documented upper bound on how quickly
`Thread#kill`, `Interrupt` and runner close are observed. A stated bound, not
an accident.

### The drain is bounded

Shutdown — a signal, closed standard input, or the controller's own stop —
drains what is in flight and then exits. That drain is bounded at **5 seconds**,
and the bound is not a nicety: a handler that never returns holds its worker
forever, so an unbounded drain means a runner that never leaves, and a harness
or orchestrator that stops runners by closing their pipes leaks a process per
wedge. What is in flight gets the bound, the runner reports what it abandoned,
and then it goes. Unanswered work is re-handed by the core on the next bind,
which is the guarantee that makes leaving safe.

A runner that gave up on part of its drain does not release the core context:
the worker it gave up on is inside user code and may yet call in, and freeing
the context under it would be a use-after-free in native code. The process is
on its way out, so the kernel reclaims what that skips.

### The executor seam

| Executor | Selected by | For |
| -------- | ----------- | --- |
| `:threads` | the default, or `FLEZHA_RUBY_EXECUTOR=threads` | everything |
| `:fibers` | `FLEZHA_RUBY_EXECUTOR=fibers` | I/O-heavy handlers |

The fiber executor keeps a real thread fetching — `poll` blocks in native code,
and fetching on a runtime's shared I/O pool starves everything else that pool
serves — and hands activations to fibers through a `Thread::Queue`, which is
scheduler-aware. It needs a `Fiber::Scheduler`: add the `async` gem, or pass
`executor(:fibers, scheduler: -> { MyScheduler.new })`.

**No executor may observe or affect per-entity ordering.** Exclusivity is the
core's handout guarantee — one outstanding activation per Entity — and an
activation is completed only after its dispatch has returned, so ordering holds
for any executor that does not complete early. That invariant is the seam's
entire contract, and the gem's unit suite asserts it under both members.

### The Ractor executor is deferred

Ractors are the only path to one-process-all-cores in MRI, and the architecture
suits them unusually well: `poll_core::global()` is a process-global and only
bytes cross the seam, so N Ractors polling it is structurally identical to N
threads. What is not ready is Ruby-side — Ractor is still experimental, the
extension would have to be marked `rb_ext_ractor_safe`, and non-shareable
handler state and unsafe gems break on crossing.

It is therefore **deferred by name to `ruby-sdk-ractor-executor`**, and one
discipline lands now so it stays a swap rather than a rewrite: all per-VM SDK
state resolves per Ractor from day one. The gem's unit suite asserts that no
bare mutable global holds any of it.

## Deferred fixtures

The manifest defers eight capabilities' end-to-end fixture sets —
`state_persistence`, `effectively_once`, `entity_timers`, `correction_window`,
`workflow`, `dynamic_entity`, `entry_idle` and `schema_evolution` — to
**`sdk-durable-fixture-parity`**, the shared follow-up that also owes them for
the Node and Elixir apps. The Elixir app defers a ninth, `keyed_channels`,
because its SDK is its own core and mints no identity stamps yet.

This is a deferral of **fixtures, not mechanism**: the dispatcher answers every
activation kind the boundary defines — work, restore fold, snapshot, live state
read, apply state, migration serialize, key request, workflow run and
compensate run — and the gem's unit suite sweeps all of them. What is owed is
the end-to-end coverage, and the conformance gate holds the deferral honest in
both directions.

`keyed_channels` left that list when the gem began minting the output identity
stamps (`Flezha::Ctx#stamp_identity`): the capability is carried with the
reference's `partition_relay` fixture and proved end to end.

## The runner image

The reference runner image preloads **jemalloc**. MRI's allocator fragments
under long-lived workers — the exact shape a runner is — and jemalloc is the
standard answer. It is documented and shipped in the image; it is **not** a
hard runtime requirement, and the gem installs and runs without it.

## Diagnostics

The default logger writes to standard error at `warn` and above; set
`FLEZHA_LOG_LEVEL` or pass `runner.logger(your_logger)`. A logger needs only
`debug` / `info` / `warn` / `error`.

The startup line reports the executor, the worker count and the resulting JIT
state, so a measurement can never be ambiguous about which it was taken under.
