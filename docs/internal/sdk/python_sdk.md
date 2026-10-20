# Python SDK

The `flezha` package is a poll host over `flz_core`, the runner kernel the
Rust, Java, Node and Ruby SDKs also drive. The kernel owns transport, ordering,
per-cell exclusivity, durability, retries, timers, credit, routing and
steps runs. This package supplies threads and user code, and nothing else —
[`poll_core.md` → "The core decides, the SDK brings the threads"](poll_core.md#the-core-decides-the-sdk-brings-the-threads).

**Support tier:** [tier 2](../../public/publishing/maturity.md#sdk-python), passes the full
conformance run in CI exactly as tier 1 does, with every capability present, and is
permanently supported; it is not yet run by the maintainer in a live deployment, so a
defect specific to it is likely to be found by its users first.

- **Package**: `flezha` (PyPI).
- **Source**: `sdk/python/`; the native veneer is
  `flz_core_ffi/src/python_glue.rs`, behind that crate's `python` feature.
- **Installing**: the supported Python versions, the wheel tiers and how the
  native module reaches the host are in the package's
  [`README.md`](../../../sdk/python/README.md);
  [local mode](../../public/topics/core/running_and_testing.md#local-mode) and the
  [test harness](../../public/topics/core/running_and_testing.md#check-a-tasklet-on-its-own) are on the Running relays page.
  From a checkout, `scripts/build-python-sdk.sh` builds the veneer, installs it
  where the package loads it from, and creates the virtual environment the e2e
  app runs in.
- **Scope**: `test/apps/python_runner/conformance.json` is the scope matrix —
  the capabilities this SDK carries and the ones it defers, checked against the
  Rust reference by `test/sdk_conformance/gate_test.exs`. This page does not
  restate it, because a restatement drifts.

## Authoring

Modules and classes, registered explicitly. There is no autoload, no path walk
and no package scan: what runs is what you named.

A tasklet is a class decorated `@flezha.tasklet(name=...)`, whose
`run(self, value, ctx)` is the handler and whose other methods are the hooks.
From [`sdk/python/examples/local_word_count.py`](../../../sdk/python/examples/local_word_count.py):

```python
@tasklet(name="split")
class Split:
    def run(self, line: str, ctx: Ctx[None, str]) -> None:
        for word in line.split():
            ctx.emit(word.lower())


@state
@dataclass
class Count:
    seen: int = 0


@dataclass
class Seen:
    seen: int


@tasklet(name="tally", idle_ttl_seconds=300)
class Tally:
    def run(self, word: str, ctx: Ctx[Count, str]) -> None:
        seen = ctx.state.seen + 1
        ctx.change(Seen(seen=seen))
        ctx.emit(f"{word} {seen}")

    def apply_change(self, state: Count, change: Seen) -> None:
        state.seen = change.seen


@relay("local_word_count_example")
def word_count(c: Channels) -> None:
    # One Cell per word: the hop from `Split` to `Tally` is the runtime's,
    # not this file's.
    c.input("lines", codec="text").into(Split).key_by(lambda word: word).into(Tally).keyed().output(
        "totals"
    )
```

- **`name` is required.** It is the tasklet's durable identity: its state is
  saved under it, so a renamed class keeps its state. A tasklet with no name,
  and a function handed to `flezha.tasklet`, are refused with
  `FlezhaError` (`topology.tasklet_name_missing`, `topology.invalid_tasklet`); a whole one-tasklet relay written as a function is
  [the short form](#the-short-form).
- **A value leaves only through `ctx`.** `run`, `on_timer` and `compensate`
  take the context as their last parameter and return `None`, plain or
  `async def`. A hook written without the context, or annotated to return a
  type, is refused at declaration, naming the hook. One that returns a value
  anyway fails that input without a retry, naming the tasklet, the hook and
  `ctx.emit`.
- **The context names the state and the output**, `Ctx[State, Output]`, with
  `None` in either position for none. `ctx.emit` takes an `Output`, so a type
  checker reports an emit of the wrong type.
- **`ctx.part(value)` sends a preview of the output while `run` is still
  working.** It previews the tasklet's own output channel, is encoded as
  `ctx.emit(value)` would encode it there, and never waits: a part that cannot
  leave at once is dropped. A listener receives it, numbered from 0 within each
  attempt; an output never does, and nothing about it is durable. It works in
  `run` and `on_timer`, for a flow tasklet and a steps stage alike. It
  raises `FlezhaError` coded `part.no_output` when the output channel does not
  end in an output (a call end is not one), and `part.outside_invocation`
  through a context kept past its invocation.
- **State starts from the state class.** It is `state=`, else the state
  argument of `run`'s context hint, else what `init_state` returns. A new
  cell's state is `init_state(cell_key)` when the class writes one, and
  otherwise the state class called with no arguments, the origin for a
  parameterised builtin, so `Ctx[dict[str, int], ...]` starts at `{}`. A state
  class that needs arguments, with no `init_state`, is refused at declaration,
  naming it. `Tally` starts from `Count()`. A class that still writes
  `initial_state` is refused, naming `init_state`.
- **A change is plain data.** `ctx.change(value)` journals it and
  `apply_change(state, change)` folds it, live and again on every restore. A
  change with no state to fold into is refused at declaration.
- **The clock follows from what the class writes**: event time when it writes
  `event_time(value) -> int`, logical with `logical_clock=True`, the wall clock
  otherwise. Both at once is refused, naming both.
- **Cells come from the wiring.** `cell_keys=[...]`, with a `cells_for` method,
  gives the tasklet those cells; a `key_by` on the edge that feeds it gives it
  a cell per key, created on first sight; anything else gives it one cell.
  `Tally` is dynamic in `word_count` because of the `key_by` before it, and
  would be single in a relay that wired it without one.

The keywords `flezha.tasklet` takes beside `name`:

| Keyword | What it declares |
| --- | --- |
| `cell_keys` | a fixed set of cells, routed by `cells_for` |
| `idle_ttl_seconds` | how long an idle dynamic cell lives, a positive whole number of seconds; unset keeps it |
| `max_cells`, `mailbox_capacity` | the cap on cells, and each cell's mailbox |
| `max_timers_per_cell` | the pending timers one cell may hold, 10 000 unset; an item past it fails |
| `logical_clock` | a clock that counts the tasklet's changes |
| `schema_version`, `min_readable_schema_version` | the saved state's version, and the oldest it reads through `migrate_state` / `migrate_change` |
| `state`, `change` | the state and change classes, when the hints do not name them |

### The short form

`@flezha.relay` declares a whole single-tasklet relay when it decorates
a tasklet's body instead of a channels body: one declaration, no
`flezha.tasklet` beside it. `id` is the factory id and is always written; `output`, `input`
and `codec` (the entry codec `Channels.input` takes) are optional, and so are
the tasklet's own keywords (`cell_keys`, `logical_clock`, `idle_ttl_seconds`,
`mailbox_capacity` and the rest `flezha.tasklet` takes, except `name`).

```python
@flezha.relay(id="upcase", output="out", codec="text")
def upcase(value: str) -> str:
    return value.upper()


@flezha.relay(id="place", run="steps", output="placed", input="order", codec="text")
def place(order: str) -> str:
    return order


runner = flezha.create_runner("default")
runner.register_factory(upcase).register_factory(place).run()
```

The body is one of two:

- **A function** whose one parameter is the input and whose return value is
  emitted, `None` emitting nothing. It takes no context: a function with a
  second parameter is refused, naming it and pointing at the class body. The
  function stays an ordinary callable, `upcase.tasklet("a")`, so a plain test
  calls it with no runtime present.
- **A class** that follows the everyday form: `run(self, value, ctx) -> None`,
  emitting through the context, with state, hooks, clock and settings exactly
  as a `flezha.tasklet` class declares them, and no `name`.

A function is a channels body when it takes one parameter annotated
`flezha.Channels`, or not annotated at all; any other function is a tasklet
body.

A class body is how a one-tasklet relay with state is written, from
`sdk/python/test_app/fixtures.py`:

```python
@flezha.relay(id="sugar_store_input", output="sugar_store_sink", codec="text")
class SugarStoreInput:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], str]) -> None:
        ctx.change({"push": value})
        ctx.emit(value)

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"text_inputs": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["text_inputs"].append(change["push"])
```

With no `output`, the relay declares its entry channel and the tasklet and no
plugin channel, which is how a relay that only keeps state is written.

The generated relay is the one you would have written by hand with that
single tasklet: an entry channel named `input` (or the `input` keyword's
value) decoded by `codec`, the tasklet, a plugin output named by `output` when one
is written, a factory id equal to `id`, and the class settings it writes, as
the full form takes them. The tasklet is named `id` verbatim, so growing out of the short form
keeps every cell's state where it was, and `upcase.tasklet` wires it into
another relay's channels body. A steps short form is a `once` steps relay,
run once over its first message: `flz relay start --text '<payload>'`, or
a start followed by `flz relay send`; a second message to a `once`
instance is accepted and never read. With `each` written it loops, still
one declaration:

```python
@flezha.relay(id="chat", run={"steps": {"each": "per_input"}}, output="replies", codec="text")
def chat(turn: str) -> str:
    return turn
```

Anything more is the full form: a second tasklet, a fan-out, a merge, a keyed
edge, an edge's retry budget. The short form declares
none of them, and nothing is harvested: a relay a runner declares is one
you registered.

Three refusals name themselves. `topology.short_form_with_topology` (a class with `run`
that also declares `channels`) and `topology.output_without_type` (an `output` on a function
with no return annotation, or `None`, or on a class whose context names no
output) are raised where the decorator runs, and
`topology.duplicate_factory_id` where a second relay under one factory id is
registered. A missing `id`, and `output`, `input`, `codec` or a tasklet keyword
on a channels body, are refused where the decorator runs too, as is `run`,
`durable`, `output`, `input` or `codec` on `flezha.tasklet`.

### Class settings

The decorator takes the class settings as keywords, in either form, written as
the tree's own data:

```python
@flezha.relay(
    "orders",
    run={"steps": {"each": "per_input", "on_failure": "keep_progress"}},
    durable={"local": {"sleep_after": 10_000}},
    max_duration_ms=600_000,
)
def orders(c: flezha.Channels) -> None:
    c.input("order", codec="text").into(Place).output("placed")
```

- `run` is `"flow"` (the default) or `"steps"`, bare or with its members.
  `steps` takes `each` (`"once"`, the default, `"per_input"` or `"chained"`)
  and `on_failure` (`"undo_all"`, the default, or `"keep_progress"`).
- A flow takes `exactly`: `"at_least_once"` (the default), `"effectively_once"`,
  or `{"effectively_once": {"ack": ..., "correction_ms": ..., "beyond_window": ...}}`,
  with `ack` either `"per_input"` or `"checkpoint"`, or
  `{"checkpoint": {"every": ..., "max_delay_ms": ..., "outputs": ...}}`.
- `durable` is `False`, `"local"` (the default) or `"remote"`, and
  `{"local": {"sleep_after": ...}}` or `{"remote": {...}}` with milliseconds or
  `False`. A kept instance sleeps after six idle hours by default.
- `max_duration_ms` stops an instance that long after it starts;
  `description` is free text.

The `Run` and `Durable` type aliases spell the tree with `Literal` unions for a
type checker. The core judges the tree when the runner declares its relays,
before it connects: a setting under a parent that does not carry it is
`topology.unknown_field`, naming the setting and the parent, and a value
outside its choice is `topology.invalid_setting`.

A steps relay retries a failing tasklet on the edge that feeds it, as a flow
does: `retry_max` and `retry_backoff_ms` on the channel (3 attempts, 50 ms base
by default), so `retry_max(1)` is fail-fast.

A steps tasklet registers timers as a flow tasklet does, and a firing
runs as a pass of its own ([`../../public/topics/core/timers.md`](../../public/topics/core/timers.md));
only inside a `compensate` hook do `ctx.timer_after`, `ctx.timer_at_event_time`
and `ctx.cancel_timer` raise a `FlezhaError` with the code `runtime.timers_unavailable`. A `ctx.spawn_steps`
naming a flow factory resolves its handle to a `FlezhaError` with the code `relay.factory_not_steps`.

### Type hints are the declaration

The graph is wired from the annotations, and the package ships `py.typed`, so a
wrong edge is a **type error before it is a runtime error**:

```python
c.input("orders").into(Bill).named("invoices").into(Dispatch).output()
#                                              ^^^^ Bill emits Invoice,
#                                                   Dispatch takes Shipment
```

A tasklet's input type is its `run`'s value annotation and its output type
the output argument of its context hint, `Ctx[None, Invoice]`; a function
short form's output is its return annotation.

pyright and mypy report that in the editor. What a type checker cannot see — a
cycle, a missing entry channel, an unservable clock, a keyed channel with no
plugin wire — is refused at the `register_factory` call site, naming the handler
and the defect, before any input is served. That pairing is what this SDK has
in place of the Rust SDK's `tests/ui` compile-fail pins.

The state type is what the context hint names, or `state=`, and the change
type is what `apply_change` folds, so a declaration cannot disagree with the
code. A `TypedDict` is a plain dict at run time and is stored as one.

### Declared state is mutated only through changes

`@flezha.state` rejects direct attribute assignment, naming the field and the
change-based path:

```python
state.count += 1
# FlezhaError: Seen.count was assigned directly. Declared state is rebuilt from
# its change log, so an assignment outside that log is live in this process and
# gone after a restart. Express it as a change: `ctx.change(...)`.
```

Rust forecloses this at compile time by withholding `DerefMut`; Python's level
is runtime rejection. `docs/internal/sdk/authoring_contract.md` records the level per
SDK, because three different enforcement levels honestly stated is correct and
three identical claims of which one is true is not.

The guard covers assignment, not mutation *through* a container a state field
holds (`state.items.append(x)`). Rust has the same gap via interior mutability.
`flezha.testing.TaskletTestHarness.replay_equals()` is what catches it.

### Determinism

`ctx.now()` and `ctx.stable_id()` are computed by the engine, once per
invocation, and reproduce on replay. `time.time()`, `datetime.now()` and
`uuid.uuid4()` do not — so calling one inside a handler emits a warning naming
the file and line, and the invocation completes normally. Detection is armed
only while an invocation runs and only on the thread running it.

A call to anything outside the instance goes through `ctx.effect(name, fn)`,
which keeps its result so a re-run reads it instead of calling again
([`effectively_once.md`](../../public/topics/core/effectively_once.md#recorded-effects)).

## Channel payloads

A channel names the format of its payload with `codec=` on `input` and on
`output`, a string typed as `flezha.ChannelCodec`:

| Codec | An entry hands the tasklet | An output sends |
| --- | --- | --- |
| `"text"` | a `str`; invalid UTF-8 fails the decode | a `str` as UTF-8; any other value fails the emit |
| `"bytes"` | `bytes`, unchanged | `bytes`, `bytearray` or `memoryview` unchanged; any other value fails the emit |
| `"json"` | the parsed JSON | any value as compact JSON, a `str` quoted |
| `"msgpack"` | the value MessagePack decodes to | any value as MessagePack |

An entry with no codec and no `decode=` reads JSON: `{"a":1}` arrives as
`{"a": 1}`, and plain text fails its decode and rides the retry and dead-letter
ladder. An output with no codec and no `encode=` follows the value: bytes pass
through, a `str` is sent as UTF-8, and anything else as JSON. So an output needs no
codec for a string, bytes or a JSON value, and names one only to pick another
format, `"json"` to quote a string or `"msgpack"`. A `decode=` or `encode=`
callable takes the place of a codec for anything else. A codec name outside the
four, or a codec named beside a callable on one channel, is refused when the
relay is declared, naming the relay and the channel.

A called input's first output answers its caller and still reaches the output's plugins:
[`sending_and_calling.md`](../../public/topics/core/sending_and_calling.md#calls).
`.call("pricing", deadline_ms=500)` or `.call(factory="agent", deadline_ms=..., create=False, answer_channel="quotes", codec=...)`
ends a chain on a call to another relay and answers an edge of `Response`s:
[`sending_and_calling.md`](../../public/topics/core/sending_and_calling.md#calls).

Python's integers survive JSON at any width. A `datetime`, a set, a dataclass,
`NaN` or an infinity has no JSON form, so sending one to an output that names no
codec fails the emit naming the channel; `codec="msgpack"` or an encoder carries
it.

A value passed between two tasklets of one relay never crosses a channel
codec: it stays MessagePack.

## Encoding

State and change bodies are MessagePack, through one codec protocol with
built-in adapters for dataclasses, Pydantic models and attrs classes, plus a
hand-written `to_dict` / `from_dict` escape. Field evolution is additive: a body
is decoded into the fields the class declares *now*, so a field added with a
default takes its default when an older record does not carry it.

`pickle` appears nowhere on the persistence or wire paths, for two independent
reasons: loading a pickle executes code, and its payloads embed class paths so a
refactor breaks the restore of every record written before it.
`sdk/python/tests/test_codec.py` asserts the absence over the loaded source, so
a later change cannot reintroduce it.

## Concurrency, as arithmetic

The default host is a base pool of Python threads, each looping
`poll → dispatch → complete` **on itself**, plus one supervisor thread parked in
the core's starvation wait. It is `sdk/rust/src/relay/poll_pool.rs`
transliterated, and it reads that host's environment rather than inventing a
vocabulary:

- `FLEZHA_WORKER_POOL_SIZE`: Base worker threads.
- `FLEZHA_WORKER_POOL_MAX`: The ceiling the supervisor grows to.
- `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`: The core's in-flight cap.
- `FLEZHA_PYTHON_EXECUTOR`: `thread` (default) or `asyncio`.

Their types and defaults are in [Runner environment variables](../../public/configuration/runner_environment.md#every-variable).

State it plainly:

- **On the GIL tier, Python-level handler execution is bounded to one core per
  process.** On the free-threaded tier it is not.
- **Everything below the poll seam runs with the GIL released** — transport,
  framing, durability, retries, credit, routing and the activation decode all
  run in Rust — so a thread host is genuinely parallel everywhere except user
  Python code, and parallel there too for I/O-bound handlers, which release the
  GIL while waiting.
- **Work underway is capped by `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` regardless of
  pool size.** The pool is a servicing detail, never the concurrency bound.
- **Throughput per loop ≈ 1 ÷ handler latency.**
- The levers for CPU-bound handlers are, in order: **the free-threaded tier**,
  **process count**, and **native libraries that release the GIL**.

### A worked sizing example

An agent-orchestration handler that spends 180 ms in a model call and 20 ms in
Python, and 4 000 inputs per second to serve.

1. Throughput per loop ≈ 1 ÷ 0.2 s = **5 activations/s per loop**.
2. 4 000 ÷ 5 = **800 concurrent activations** — under the 1024 in-flight cap, so
   the cap is not the binding constraint.
3. Python-level work is 20 ms of each 200 ms, so **10% of 800 = 80 activations'
   worth of CPU**. On the GIL tier one process gives one core, which serves
   about 50 activations of that shape (1 core ÷ 0.02 s = 50/s × 0.2 s window).
4. So: **two processes on the GIL tier**, or **one on the free-threaded tier**,
   with `FLEZHA_WORKER_POOL_SIZE` raised well above the processor count because
   the pool is a *waiting* resource here — the supervisor will grow it either
   way, and starting there only removes the first growth episode's latency.

The failure mode this arithmetic prevents is silent: sizing a deployment as
though handler bodies overlapped on a GIL build.

### The starvation supervisor is required, not an optimisation

A completion can block. When a tasklet emits onto a full `DropNothing`
consumer mailbox, the routing push parks the completing thread — and
`routing::install_inline_serve`, the hook that rescues the in-process Rust host
by draining the consumer inline, takes a Rust function pointer and is **not
reachable over the C ABI**. A host whose only thread is parked in a completion
is therefore not entering `poll`, which is exactly what the core reports as
starvation, and the worker the supervisor starts is what drains the consumer
and releases the producer.

So there is no pool configuration without a supervisor, and no single-threaded
mode. Both executors run one, and under both the worker it starts is a real
fetch-and-run **thread**: a stalled event loop cannot rescue itself.

### The executor seam

| Executor | Fetches on | Dispatches on | For |
| --- | --- | --- | --- |
| `thread` (default) | its own worker threads | the same thread | everything, unless measurement says otherwise |
| `asyncio` | real fetcher threads | one shared event loop | I/O-dense handlers |

The fetcher is a real thread under both. `poll` blocks in native code, and
borrowing a runtime's shared I/O pool for it starves everything else that pool
serves. Under the asyncio executor completion happens on the loop thread, which
the ABI documents as expected rather than merely tolerated.

`async def` handlers work under **both**, and users never declare which style
they wrote. A synchronous user never sees asyncio: off an event loop the
dispatch never suspends, so the thread executor drives an entire synchronous
activation without creating a loop at all.

**No executor may observe or affect per-cell ordering.** Exclusivity is the
core's handout guarantee — it will never hand out two activations for the same
Cell concurrently, however many threads poll — and an activation is completed
only after its dispatch has returned. Safety comes from the core, not from the
adapter: an executor can affect throughput and latency; it cannot affect
correctness.

The asyncio executor's real trade-off: one blocking call stalls the loop. That
is why `thread` is the default and not the reverse.

### The interrupt bound

`flz_core_shutdown` wakes *every* poller, so the ABI has no per-thread wake.
Each `poll` is therefore bounded by a timeout — **250 ms** — and that timeout is
the documented upper bound on how quickly `KeyboardInterrupt`, a termination
signal and runner close are observed.

The process's main thread is never a poll worker: CPython runs signal handlers
only on the main thread, between bytecodes, so a main thread inside a handler
would delay them by the length of that handler.

## Startup order, and never forking after `start`

```
declare classes → (fork, if you fork at all) → start() in each child
```

`flz_core_start` must not be called before a fork. The core owns Rust threads —
transport, flusher, the runner loop — and `fork(2)` carries none of them, so a
child that inherited a started core would connect to nothing and serve nothing,
silently. `start()` records the owning process and every core call from another
one raises, naming the rule. A documented rule alone would not survive contact
with `multiprocessing`, whose default start method on Linux is fork.

The SDK spawns no processes. A runner is one transport connection and a
connection lives in one process, so N processes are N runners — which is what
HRW placement spreads across, and the answer to CPU-bound handler bodies.

## Diagnostics

Standard-library `logging`, under the `flezha` logger hierarchy. The SDK never
configures the root logger. When a runner or a local run starts and neither
the `flezha` logger nor the root logger has a handler, it gives the `flezha`
logger one writing to standard error at `FLEZHA_LOG_LEVEL` (`warn` when unset),
the level every other SDK starts at; an application's own configuration wins.
`flezha.configure_logging()` sets that default explicitly. The core's notices
reach the `flezha.core` logger, written by the starvation supervisor on each
wake and when a run stops; the local-mode banner at `ERROR`.

No OpenTelemetry: no SDK ships spans, and adding them to one binding is the
exact asymmetry the lockstep rule exists to prevent. When they land, they land
everywhere in one change.

## Deferred, by name

- **Sub-interpreter execution** (PEP 684/734), owed to `sdk-python-subinterpreters`.
  PyO3's multi-interpreter support is not there yet, and an interpreter boundary
  between the binding and handler state is a larger question than a build flag.
  The discipline that keeps it a swap rather than a rewrite is in place today:
  the binding handle is an opaque `#[pyclass]` reached only through the
  interpreter holding it, never a process-wide `static`, and the registry view
  and the active executor resolve per interpreter.
- **Declarative routing keys and edge transforms** — evaluating a key spec or a
  simple transform in Rust so the common path never enters Python at all. The
  largest remaining performance lever; owed to `sdk-declarative-keys`.
