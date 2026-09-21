# Python SDK

The `flezha` package is a poll host over `flz_core`, the runner kernel the
Rust, Java, Node and Ruby SDKs also drive. The kernel owns transport, ordering,
per-cell exclusivity, durability, retries, timers, credit, routing and the
workflow driver. This package supplies threads and user code, and nothing else —
[`poll_core.md` → "The core decides, the SDK brings the threads"](poll_core.md#the-core-decides-the-sdk-brings-the-threads).

- **Package**: `flezha` (PyPI).
- **Source**: `sdk/python/`; the native veneer is
  `flz_core_ffi/src/python_glue.rs`, behind that crate's `python` feature.
- **Scope**: `test/apps/python_runner/conformance.json` is the scope matrix —
  the capabilities this SDK carries and the ones it defers, checked against the
  Rust reference by `test/sdk_conformance/gate_test.exs`. This page does not
  restate it, because a restatement drifts.

## Supported Python versions

Python **3.11 or newer**, CPython only. The native module needs the CPython C
API, so PyPy and GraalPy are out of scope — a constraint, not a gap waiting to
be closed.

Two wheel tiers ship from one source with one API and one set of semantics:

| Tier | Wheels | What it is for |
| --- | --- | --- |
| Compatibility | one `abi3` wheel per platform, targeting 3.11 | every supported interpreter at or above the floor |
| Performance | per-version free-threaded wheels, from `cp314t` | handler bodies that execute in parallel |

`abi3` has no free-threaded form before CPython 3.15's `abi3t`, which is why the
performance tier is per-version rather than one wheel. It starts at **3.14t**
and not 3.13t: PyO3 refuses the free-threaded build of CPython below 3.14, so
`cp313t` is not a target this SDK has. The runner reports which
tier it is running in its startup line, so a benchmark or a bug report is never
ambiguous about it.

## Installing

```bash
pip install flezha
```

Prebuilt wheels carry the native library, so installation needs no Rust
toolchain, no cargo invocation and no network fetch of Rust dependencies. A
platform with no prebuilt wheel falls back to the source distribution, which
does need a stable Rust toolchain and fetches the core at the git tag
`native/Cargo.toml` names.

From a checkout, `scripts/build-python-sdk.sh` builds the veneer, installs it
where the package loads it from, and creates the checkout-local virtual
environment the e2e app runs in.

Optional extras: `flezha[pydantic]`, `flezha[attrs]`, `flezha[uvloop]`. The SDK
imports none of them unconditionally.

## Authoring

Modules and classes, registered explicitly. There is no autoload, no path walk
and no package scan: what runs is what you named.

```python
from dataclasses import dataclass, field

import flezha


@flezha.state
@dataclass
class Seen:
    count: int = 0


@dataclass
class Bumped:
    by: int


class Upcase:
    def run(self, value: str, ctx: flezha.Ctx[Seen]) -> str:
        ctx.change(Bumped(by=1))
        return value.upper()

    def initial_state(self, cell_key: str | None) -> Seen:
        return Seen()

    def apply_change(self, state: Seen, change: Bumped) -> None:
        state.count += change.by


upcase = flezha.tasklet(Upcase)


@flezha.stream_operative("upcase")
def upcase_operative(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(upcase).sink("out")


flezha.create_runner("default").register_factory(upcase_operative).run()
```

A tasklet is a function when it is a transform and a class when it has state,
timers, a clock extractor or a compensation. Both produce the same
`Tasklet`, and either way the handler stays an ordinary callable a plain
`pytest` test can invoke with no runtime present:

```python
@flezha.tasklet
def price(tick: Tick) -> Quote: ...


def test_price() -> None:
    assert price(Tick(...)) == Quote(...)
```

### The short form

`@flezha.stream_operative` and `@flezha.workflow_operative` declare a whole
single-tasklet operative when they decorate a tasklet's body instead of a
channels body: one declaration, no `flezha.tasklet` beside it. The decorator
names the kind. `id` is the factory id and is always written; `sink`, `input`
and `codec` (the entry codec `Channels.input` takes) are optional, and so are
the tasklet's own keywords (`cell_keys`, `clock`, `mailbox_capacity`,
`schema_version` and the rest `flezha.tasklet` takes, except `name`).

```python
@flezha.stream_operative(id="upcase", sink="out", codec="text")
def upcase(value: str) -> str:
    return value.upper()


@flezha.workflow_operative(id="place", sink="placed", input="order", codec="text")
def place(order: str) -> str:
    return order


runner = flezha.create_runner("default")
runner.register_factory(upcase).register_factory(place).run()
```

The body is a function, or a class whose `run` is the handler and whose other
methods are the hooks. A function is a channels body when it takes one
parameter annotated `flezha.Channels`, or not annotated at all; any other
function is a tasklet body.

With no `sink`, the operative declares its entry channel and the tasklet and
no plugin channel, which is how an operative that only keeps state is written:

```python
@flezha.stream_operative(id="counter", input="ticks", codec="text")
class Counter:
    def run(self, value: str, ctx: flezha.Ctx[Seen]) -> None:
        ctx.change(Bumped(by=1))

    def initial_state(self, cell_key: str | None) -> Seen:
        return Seen()

    def apply_change(self, state: Seen, change: Bumped) -> None:
        state.count += change.by
```

The generated operative is the one you would have written by hand with that
single tasklet: an entry channel named `input` (or the `input` keyword's
value) decoded by `codec`, the tasklet, a plugin sink named by `sink` when one
is written, a factory id equal to `id`, and every class-level setting at its
default. The tasklet is named `id` verbatim, so growing out of the short form
keeps every cell's state where it was, and `upcase.tasklet` wires it into
another operative's channels body. A workflow short form is a `once` workflow,
seeded on its entry channel from `flz operative start --kind workflow
--init-values`.

Anything more is the full form: a second tasklet, a fan-out, a merge, a keyed
edge, a loop mode, a retry ladder, the effectively-once tier, a correction
window. The short form declares none of them, and nothing is harvested: an
operative a runner declares is one you registered.

Three refusals name themselves. `ShortFormWithTopology` (a class with `run`
that also declares `channels` or `tasklets`) and `SinkWithoutOutput` (a `sink` on a body with
no return annotation, or `None`) are raised where the decorator runs, and
`DuplicateFactoryId` where a second operative under one factory id is
registered. A missing `id`, a class-level setting on a short form, and `sink`,
`input`, `codec` or a tasklet keyword on a channels body are refused where the
decorator runs too, as is `kind`, `sink`, `input` or `codec` on
`flezha.tasklet`.

> A workflow short form started by a *spawn* does not see its payload yet
> (`KB-381`). Start it with `--init-values` until that is fixed.

### Type hints are the declaration

The graph is wired from the annotations, and the package ships `py.typed`, so a
wrong edge is a **type error before it is a runtime error**:

```python
c.input("orders").into(bill).named("invoices").into(dispatch)
#                                              ^^^^ bill produces Invoice,
#                                                   dispatch takes Shipment
```

pyright and mypy report that in the editor. What a type checker cannot see — a
cycle, a missing entry channel, an unservable clock, a keyed channel with no
plugin wire — is refused at the `register_factory` call site, naming the handler
and the defect, before any input is served. That pairing is what this SDK has
in place of the Rust SDK's `tests/ui` compile-fail pins.

The state type is what `initial_state` returns and the change type is what
`apply_change` folds, so a declaration cannot disagree with the code.

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
is runtime rejection. `docs/sdk/authoring_contract.md` records the level per
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

## Channel payloads

A channel names the format of its payload with `codec=` on `input` and on
`sink`, a string typed as `flezha.ChannelCodec`:

| Codec | An entry hands the tasklet | A sink sends |
| --- | --- | --- |
| `"text"` | a `str`; invalid UTF-8 fails the decode | a `str` as UTF-8; any other value fails the emit |
| `"bytes"` | `bytes`, unchanged | `bytes`, `bytearray` or `memoryview` unchanged; any other value fails the emit |
| `"json"` | the parsed JSON | any value as compact JSON, a `str` quoted |
| `"msgpack"` | the value MessagePack decodes to | any value as MessagePack |

An entry with no codec and no `decode=` reads JSON: `{"a":1}` arrives as
`{"a": 1}`, and plain text fails its decode and rides the retry and dead-letter
ladder. A sink with no codec and no `encode=` follows the value: bytes pass
through, a `str` is sent as UTF-8, and anything else as JSON. So a sink needs no
codec for a string, bytes or a JSON value, and names one only to pick another
format, `"json"` to quote a string or `"msgpack"`. A `decode=` or `encode=`
callable takes the place of a codec for anything else. A codec name outside the
four, or a codec named beside a callable on one channel, is refused when the
operative is declared, naming the operative and the channel.

Python's integers survive JSON at any width. A `datetime`, a set, a dataclass,
`NaN` or an infinity has no JSON form, so sending one to a sink that names no
codec fails the emit naming the channel; `codec="msgpack"` or an encoder carries
it.

A value passed between two tasklets of one operative never crosses a channel
codec: it stays MessagePack. An Elixir entry declared with no decoder hands its
tasklet the binary instead, since an Elixir string is its binary, so the same
bytes on an undeclared entry are a `dict` here and a string there.

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
the core's starvation wait. It is `sdk/rust/src/operative/poll_pool.rs`
transliterated, and it reads that host's environment rather than inventing a
vocabulary:

| Variable | What it sets | Default |
| --- | --- | --- |
| `FLEZHA_WORKER_POOL_SIZE` | base worker threads | the machine's processor count |
| `FLEZHA_WORKER_POOL_MAX` | the ceiling the supervisor grows to | ten times the base, never below it |
| `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` | the core's in-flight cap | 1024 |
| `FLEZHA_PYTHON_EXECUTOR` | `thread` (default) or `asyncio` | `thread` |

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

Standard-library `logging`, under the `flezha` logger hierarchy. No handler is
installed by default — a library that configures the root logger takes a
decision that belongs to the application — and `flezha.configure_logging()` is
the one opt-in that turns diagnostics on at a documented default
(`FLEZHA_LOG_LEVEL`, or `warning`, to standard error).

No OpenTelemetry: no SDK ships spans, and adding them to one binding is the
exact asymmetry the lockstep rule exists to prevent. When they land, they land
everywhere in one change.

## Local mode: run a graph with no controller

`LocalOperative.start` takes the operative you deploy and runs its declared
graph in this process — no controller, no socket, no local-mode member on the
class:

```python
from flezha import LocalOperative

with LocalOperative.start(word_count) as run:
    run.on_sink("totals", lambda out: print(out.payload.decode()))
    run.feed("lines", "the quick brown fox")
    run.run_until_idle(10_000)
```

`on_sink` registers against the sink's **channel** name, which is the producing
tasklet's name unless the declaration named it. The run brings the same
executor a started runner brings, so the tasklets run on its worker threads
while the caller waits in `run_until_idle`. Worked example:
[`sdk/python/examples/local_word_count.py`](../../sdk/python/examples/local_word_count.py).

**State is held in memory and nothing is durable** — no write-ahead log, no
snapshots, no dead-letter queue. Durability, effectively-once delivery, plugins,
placement and migration, the dead-letter queue, observability export, authz and
multi-runner scale need a controller; a declaration asking for one of those is
refused at startup, naming it. Walk-through:
[`../quickstart.md`](../quickstart.md).

## Testing your handlers

```python
from flezha.testing import TaskletTestHarness

harness = TaskletTestHarness(upcase)
harness.apply_inputs(["a", "b"])
assert harness.state.count == 2
assert harness.replay_equals()
```

The harness drives the same `invoke` every activation kind goes through, and
`replay_equals()` folds through the same `fold_restore` a real restore calls —
so what it proves is what the runner does, rather than something that agrees
with it. It also drives `async def` handlers, so a test needs no event loop.

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
- **Post-start factory resync to a live controller.** `register_factory` after
  `start()` reaches the core immediately and the controller at the next bind,
  because the ABI has no entry point that ships `UpdateFactories` or retracts a
  declaration — only the in-process Rust host, which owns the runner channel
  directly, can notify a live controller. Java carries the same gap; owed to
  `sdk-abi-factory-resync`.
