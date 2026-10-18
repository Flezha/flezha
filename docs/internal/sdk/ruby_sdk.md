# Ruby SDK

The `flezha` gem is a poll host over `flz_core`, the runner kernel the Rust,
Java and Node SDKs also drive. The kernel owns transport, ordering, per-cell
exclusivity, durability, retries, timers, credit, routing and the steps relay
driver. This gem supplies threads and user code, and nothing else —
[`poll_core.md` → "The core decides, the SDK brings the threads"](poll_core.md#the-core-decides-the-sdk-brings-the-threads).

**Support tier:** [tier 2](../../public/publishing/maturity.md#sdk-ruby), passes the full
conformance run in CI exactly as tier 1 does, with every capability present, and is
permanently supported; it is not yet run by the maintainer in a live deployment, so a
defect specific to it is likely to be found by its users first.

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
stable Rust toolchain and fetches the core at the git tag
`ext/flz_core_ffi/Cargo.toml` names.

From a checkout, `scripts/build-ruby-sdk.sh` builds the veneer and installs it
where the gem loads it from.

## Authoring

Classes, registered explicitly. There is no autoload, no load-path walk and no
package scan: what runs is what you named. This is
`sdk/ruby/examples/local_word_count.rb`'s graph:

```ruby
class Split < Flezha::Tasklet
  tasklet_name "split"

  def run(line, ctx)
    line.split.each { |word| ctx.emit(word.downcase) }
    nil
  end
end

class Tally < Flezha::Tasklet
  tasklet_name "tally"
  # One Cell per word, because the edge into it is keyed; an idle word's Cell
  # is evicted after five minutes.
  idle_ttl_seconds 300

  def init_state(_key) = { seen: 0 }
  def apply_change(state, change) = state[:seen] = change[:seen]

  def run(word, ctx)
    seen = ctx.state[:seen] + 1
    ctx.change({ seen: seen })
    ctx.emit("#{word} #{seen}")
  end
end

class WordCount < Flezha::Relay
  factory_id "local_word_count_example"
  channels do |c|
    c.input("lines", codec: :text)
     .into(Split)
     # One Cell per word: the hop from `Split` to `Tally` is the runtime's,
     # not this file's.
     .key_by { |word| word }
     .into(Tally).keyed
     .output("totals")
  end
end
```

**A value leaves a tasklet only through its context**: `ctx.emit`, with its
`key:`, `channel:` and `metadata:` keywords, or `ctx.emit_to(channel, value)`.
What `run`, `on_timer` and `compensate` answer is ignored. Every Ruby method
answers its last expression, so a `run` whose last line computes a value is
ordinary Ruby, and the SDK neither emits nor refuses it; `Split` above could
drop its `nil`.

**A part previews the output while `run` is still working**: `ctx.part(value)`
sends `value` on the tasklet's own output channel, encoded as `ctx.emit(value)`
would encode it, and returns without waiting. It works in `run` and in
`on_timer`, in a flow relay and in a steps relay's stage. The
relay's listeners receive the parts before the output; an output never does,
and a part is never durable or replayed. Parts are numbered from 0 within each
attempt, so a retry starts its preview again.

```ruby
def run(prompt, ctx)
  %w[he hel hell].each { |partial| ctx.part(partial) }
  ctx.emit("hello")
end
```

A part that cannot leave at once, such as in a local run, is dropped. A
channel that does not end in an output (it only feeds another tasklet, or ends in
`response`) raises a `Flezha::Error` coded `part.no_output`, and a context kept
after its invocation returned raises `part.outside_invocation`. The testing
harness sends nothing.

**Every tasklet writes `tasklet_name`**, and a class wired without one is
refused when its relay is declared, naming the class. The name is the
tasklet's identity: its Cells' state is saved under it, so a class can be
renamed freely and the name is chosen once. It is not inherited, so a
subclass writes its own. It is spelled `tasklet_name` rather than `name`
because `name` is `Class#name`, which every Ruby tool reads as the constant's
name.

Which optional hooks a tasklet declares is read from the methods it defines —
`apply_change` present means it folds changes, `on_timer` present means it may
register timers, `cells_for` present means it fans out over a static key set
— so a declaration and its implementation cannot disagree. A method from an
older form, `initial_state`, is refused naming `init_state`, and a retired
class-body declaration raises `NoMethodError` naming what replaced it.

- **State.** A new Cell starts from `init_state(cell_key)` when the class
  defines it, else from `state_type.new` when the class declares `state_type`,
  else it has no state. A `state_type` whose `new` needs an argument, with no
  `init_state`, is refused at registration, and so is an `apply_change` with
  neither `init_state` nor `state_type`.
- **Changes.** A change is any value the codec carries, such as a `Hash`,
  recorded with `ctx.change` and folded by `apply_change(state, change)`, which
  mutates the state in place. No change class is needed; `change_type` is
  there for an author who wants one.
- **Cell mode.** The wiring decides it. A tasklet reached through `key_by`,
  on an edge or a merge, has one Cell per key; one writing `cell_keys` has that
  static set; anything else has a single Cell. One class can be keyed in one
  relay and single in another. A `cell_keys` tasklet fed by `key_by` is
  refused as `topology.key_by_on_unkeyable_target`.
- **Eviction.** `idle_ttl_seconds`, a positive whole number of seconds, evicts
  a keyed Cell that has been idle that long; unset, none is evicted.
  `max_cells` caps a keyed tasklet's Cells.
- **Clock.** Defining `event_time(input)` makes the clock event time, and
  `ctx.now` then answers what it extracted. The `logical_clock` flag in the
  class body makes it logical, which needs a state to count. Otherwise it is
  the wall clock. `event_time` beside `logical_clock` is refused naming both.
- **Migrations.** A tasklet whose `min_readable_schema_version` is below its
  `schema_version` defines `migrate_state(from_version, bytes)` or
  `migrate_change(from_version, bytes)`, which receive the stored bytes and
  answer the value; with neither it is refused at registration.

Everything a type system would have caught is refused at registration instead,
naming the class, the handler and the defect. That is the same set, for the
same reasons, as the Rust builder's `TopologyIssue`.

### The short form

A `Flezha::Relay` subclass that defines `run` instead of `channels` is a
whole relay: the class carries its one tasklet's body, and no
`Flezha::Tasklet` stands beside it. `factory_id` is required; `output`, `input`
and `codec` are optional, each its own call, and `codec` is the entry codec the
builder's `input` takes. An `output` written on a `Flezha::Tasklet` is refused,
naming the relay base. The class-body `run :steps` is a class setting and
the instance method `run` the tasklet's body; Ruby keeps the two apart.

```ruby
class Upcase < Flezha::Relay
  factory_id "upcase"
  output "out"
  codec :text

  def run(value, ctx) = ctx.emit(value.upcase)
end

class Place < Flezha::Relay
  factory_id "place"
  run :steps
  output "placed"
  input "order"
  codec :text

  def run(order, ctx) = ctx.emit(order)
end

Flezha.create_runner("default").register_factory(Upcase).register_factory(Place).run
```

The generated relay is the one you would have written by hand with that
single tasklet: an entry channel named `input`, or the `input` value,
decoded by `codec`, the tasklet, a plugin output named by `output` when one is
written, a factory id equal to `factory_id`, and the class settings it
writes, as the full form takes them. The tasklet is named after the factory id, so a full form that
names its tasklet the same keeps every cell where it was. A steps relay short
form is a `once` steps relay, run once over its first message: `flz relay
start --text '<payload>'`, or a start followed by `flz relay send`; a
second message to a `once` instance is accepted and never read. With
`each` written it loops, still one class:

```ruby
class Chat < Flezha::Relay
  factory_id "chat"
  output "replies"
  codec :text
  run steps: { each: :per_input }

  def run(turn, ctx) = ctx.emit(turn)
end
```

With no `output`, the relay declares the entry channel and the tasklet and
no plugin channel: the relay that only keeps state.

```ruby
class Counter < Flezha::Relay
  factory_id "counter"
  input "ticks"
  codec :text

  def init_state(_cell_key) = { seen: [] }
  def apply_change(state, change) = state[:seen] << change[:push]

  def run(tick, ctx) = ctx.change({ push: tick })
end
```

The tasklet's settings and hooks are written on the class and handed to the
generated tasklet: settings such as `cell_keys`, `idle_ttl_seconds`,
`max_cells`, `logical_clock`, `mailbox_capacity`, `schema_version` and
`state_type`, the ones a `Flezha::Tasklet` class body declares, and hook
methods such as `init_state`, `apply_change`, `cells_for` and `event_time`. A
short form's `run` emits through its `ctx` exactly as a tasklet's does, and
what it answers is ignored.

Anything more is the full form: a second tasklet, a fan-out, a merge, a keyed
edge or an edge's retry budget. An `output`, `input`, `codec` or tasklet setting
written beside `channels` is refused rather than ignored.

`factory_id` must be written: the factory id is an address a user types,
and a class rename must not move it. A class without one is refused at
`register_factory`, where this SDK places every refusal. So is a class defining
both `run` and `channels`, naming `topology.short_form_with_topology`, and one defining
neither. A second relay under a factory id the runner already holds, short
or full, fails naming `topology.duplicate_factory_id` when the runner declares its
classes.

### Class settings

The class body takes the class settings, in either form, written as the
tree's own data:

```ruby
class Orders < Flezha::Relay
  factory_id "orders"
  run steps: { each: :per_input, on_failure: :keep_progress }
  durable local: { sleep_after: 10_000 }
  max_duration_ms 600_000
  channels { |c| c.input("order", codec: :text).into(Place).output("placed") }
end
```

- `run` is `:flow` (the default) or `:steps`, bare or with its members.
  `steps` takes `each` (`:once`, the default, `:per_input` or `:chained`) and
  `on_failure` (`:undo_all`, the default, or `:keep_progress`).
- A flow takes `exactly`: `:at_least_once` (the default), `:effectively_once`,
  or `{ effectively_once: { ack:, correction_ms:, beyond_window: } }`, with
  `ack` either `:per_input` or `:checkpoint`, or
  `{ checkpoint: { every:, max_delay_ms:, outputs: } }`.
- `durable` is `false`, `:local` (the default) or `:remote`, and
  `{ local: { sleep_after: } }` or `{ remote: { ... } }` with milliseconds or
  `false`. A kept instance sleeps after six idle hours by default.
- `max_duration_ms` stops an instance that long after it starts; `description`
  is free text.

The core judges the tree when the runner declares its classes: a setting under
a parent that does not carry it is `topology.unknown_field`, naming the setting
and the parent, and a value outside its choice is `topology.invalid_setting`.

A steps relay retries a failing tasklet on the edge that feeds it, as a flow
does: `retry_max` and `retry_backoff_ms` on the channel (3 attempts, 50 ms base
by default), so `retry_max(1)` is fail-fast.

A steps tasklet registers timers as a flow tasklet does, and a firing
runs as a pass of its own ([`../../public/topics/core/timers.md`](../../public/topics/core/timers.md));
only inside a `compensate` hook do `ctx.timer_after`, `ctx.timer_at_event_time`
and `ctx.cancel_timer` raise a `Flezha::Error` with the code `runtime.timers_unavailable`. A `ctx.spawn_steps`
naming a flow factory resolves its handle to a `Flezha::Error` with the code `relay.factory_not_steps`.

A call to anything outside the instance goes through `ctx.effect("name") { value }`, which
keeps its result so a re-run reads it instead of calling again
([`effectively_once.md`](../../public/topics/core/effectively_once.md#recorded-effects)).

### Channel payloads

A channel names the format of its payload with `codec:` on `input` and on
`output`, as a symbol:

| Codec | An entry hands the tasklet | An output sends |
| --- | --- | --- |
| `:text` | a UTF-8 `String`; invalid UTF-8 fails the decode | a `String` as its bytes if it is valid UTF-8; any other value fails the emit |
| `:bytes` | a binary `String`, the bytes unchanged | any `String` unchanged; any other value fails the emit |
| `:json` | the parsed JSON, with symbol keys | any value as JSON, a `String` quoted |
| `:msgpack` | the value MessagePack decodes to | any value as MessagePack |

An entry with no codec and no `decode:` reads JSON: `{"a":1}` arrives as
`{ a: 1 }`, and plain text fails its decode and rides the retry and dead-letter
ladder. An output with no codec and no `encode:` follows the value: a `String` is
sent as its bytes and anything else as JSON. So an output needs no codec for a
string or a JSON value, and names one only to pick another format, `:json` to
quote a string or `:msgpack`. A `decode:` or `encode:` callable takes the place
of a codec for anything else. A codec name outside the four, or a codec named
beside a callable on one channel, is refused when the class is declared, naming
the class and the channel.

`.response` ends a channel on the answer to an outside call instead of an output,
and takes the output's `encode:` and `codec:`: [`sending_and_calling.md`](../../public/topics/core/sending_and_calling.md#calls).
`.call("pricing", deadline_ms: 500)` or `.call(factory: "agent", deadline_ms: ..., create: false, codec: ...)`
ends a chain on a call to another relay and answers an edge of `Flezha::Response`s:
[`sending_and_calling.md`](../../public/topics/core/sending_and_calling.md#calls).

Ruby's integers survive JSON at any width. A `Float` that is `NaN` or infinite,
or a `String` that is not valid UTF-8, cannot be sent as JSON and fails the emit
naming the channel.

A value passed between two tasklets of one relay never crosses a channel
codec: it stays MessagePack.

### State and Change

State and Change serialize as MessagePack. A plain `Hash` needs nothing; a
class declares `state_type` / `change_type` and supplies `to_h` and `.from_h`:

```ruby
class Basket < Flezha::Tasklet
  tasklet_name "basket"
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
cluster mode. `Runner#boot` and `Runner#run` both take the core, so a user
who forks starts one in each child.

`Runner#run` is `Runner#boot` followed by `Runner#await_termination`, which
waits for shutdown and then drains. A host that wants the runner connected
before it does work of its own, control calls for one, calls the two apart.

The SDK spawns no processes. Process count is the orchestrator's decision, and
each process is one runner — which is what HRW placement spreads across.

## Local mode: run a graph with no controller

`Flezha::LocalRelay.start` takes the relay class you deploy and runs
its declared graph in this process — no controller, no socket, no local-mode
member on the class:

```ruby
run = Flezha::LocalRelay.start(WordCount)
begin
  run.on_output("totals") { |output| puts output.payload }
  run.feed("lines", "the quick brown fox")
  run.run_until_idle(10_000)
ensure
  run.close
end
```

`on_output` registers against the output's **channel** name, which is the producing
tasklet's name unless the declaration named it. The run brings the same
thread executor a booted runner brings, so the tasklets run on its workers
while the caller waits in `run_until_idle`; `close` stops those workers on the
executor's own flag rather than by shutting the core, so a process may start
another local run afterwards. Worked example:
[`sdk/ruby/examples/local_word_count.rb`](../../../sdk/ruby/examples/local_word_count.rb).

**State is held in memory and nothing is durable** — no write-ahead log, no
snapshots, no dead-letter queue. Durability, effectively-once delivery, plugins,
placement and migration, the dead-letter queue, observability export, authz and
multi-runner scale need a controller; a declaration asking for one of those is
refused at startup, naming it. Walk-through:
[`running_and_testing.md`](../../public/topics/core/running_and_testing.md#local-mode).

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
  run more runner processes, and start the relay with `--partitions N` so
  its keys spread across them.

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

- `FLEZHA_WORKER_POOL_SIZE`: Base workers.
- `FLEZHA_WORKER_POOL_MAX`: The ceiling.

Their types and defaults are in [Runner environment variables](../../public/configuration/runner_environment.md#every-variable).

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

Every blocking core call — `poll`, `wait_starvation` and `complete` — runs
with the GVL released, through one function in the veneer. `poll` is the sharp
one: a poller waiting on an idle core with the lock held would freeze every
other thread in the VM, fetchers included.

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

**No executor may observe or affect per-cell ordering.** Exclusivity is the
core's handout guarantee — one outstanding activation per Cell — and an
activation is completed only after its dispatch has returned, so ordering holds
for any executor that does not complete early. That invariant is the seam's
entire contract, and the gem's unit suite asserts it under both members.

### The Ractor executor is deferred

Ractors are the only path to one-process-all-cores in MRI, and the architecture
suits them unusually well: `poll_core::global()` is a process-global and only
bytes cross the seam, so N Ractors polling it is structurally identical to N
threads. What is not ready is Ruby-side — Ractor is not yet stable in Ruby itself, the
extension would have to be marked `rb_ext_ractor_safe`, and non-shareable
handler state and unsafe gems break on crossing.

It is therefore **deferred by name to `ruby-sdk-ractor-executor`**, and one
discipline lands now so it stays a swap rather than a rewrite: all per-VM SDK
state resolves per Ractor from day one. The gem's unit suite asserts that no
bare mutable global holds any of it.

## Fixtures

The manifest at `test/apps/ruby_runner/conformance.json` defers nothing: every
capability is carried with the reference's fixtures, the persistence, schema
evolution and effectively-once set in `test/apps/ruby_runner/fixtures_state.rb`,
the steps relay set in `fixtures_steps.rb`, and the rest in `fixtures.rb`.

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
