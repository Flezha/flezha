"""The concurrency host: what runs ``poll -> dispatch -> complete``, and on what.

Two members in v1, and the seam between them may never observe or affect
per-entity ordering. Exclusivity is the core's handout guarantee — one
outstanding activation per Entity — and an activation is completed only after
its dispatch has returned, so ordering holds for any executor that does not
complete early. That invariant is this seam's entire contract: an executor can
affect throughput and latency; it cannot affect correctness.

Three things are shared rather than written once per executor, because they are
properties of the host and not of either member: the threads and their bounded
drain (:class:`_Pool`), the fetch-and-run worker (:func:`_fetch_and_run`), and
the starvation supervisor (:class:`_Supervisor`) — whose answer to an
observation is one more fetch-and-run **thread** under either executor, because
a thread is what a parked completion needs and a stalled loop cannot supply.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from typing import Any, Callable, Protocol

from .diagnostics import logger
from .dispatch import DispatchDeps, close_thread_loop, dispatch, dispatch_now
from .errors import FlezhaError

__all__ = [
    "Executor",
    "ThreadExecutor",
    "AsyncioExecutor",
    "POLL_TIMEOUT_MS",
    "base_size",
    "ceiling_size",
    "build",
    "EXECUTORS",
]

_log = logger("executor")

#: How long a worker parks in one ``poll``.
#:
#: ``flz_core_shutdown`` wakes *every* poller, so the ABI has no per-thread wake
#: and none is in scope to add. This value is therefore the documented upper
#: bound on how quickly ``KeyboardInterrupt``, a termination signal and runner
#: close are observed: a stated bound, not an accident. It is the Java and Ruby
#: hosts' value, for the same reason.
POLL_TIMEOUT_MS = 250

#: How long the supervisor parks in one starvation wait. Shorter than the poll
#: timeout would only spin; longer would delay the first grown worker.
STARVATION_WAIT_MS = 250

#: How long a drain has before the runner stops waiting for it.
#:
#: A handler that never returns holds its worker forever — the ``wedge`` fixture
#: is exactly that, and so is any real handler that blocks on something that
#: never answers. A drain that joined it unconditionally would never finish, and
#: a runner stopped by closing its pipes would leak a process per wedge rather
#: than exiting. So what is in flight gets this long, and then the runner goes
#: away regardless: the alternative is not "no work is lost", it is "the runner
#: never leaves".
DRAIN_TIMEOUT_S = 5.0

#: Consecutive idle polls after which an above-base worker retires. At the poll
#: timeout above this is two seconds of no work, which is long enough that a
#: burst does not churn the pool and short enough that a spike does not leave
#: threads parked for the life of the process.
IDLE_RETIREMENT_STREAK = 8

#: How far the asyncio executor's fetchers may run ahead of its loop. The core's
#: in-flight cap bounds the total either way; this only stops one fetcher
#: monopolising the hand-off.
QUEUE_DEPTH = 64

#: The ABI's three answers to ``wait_starvation``.
STARVATION_OBSERVED = 1
STARVATION_SHUTTING_DOWN = 2

#: What ``poll`` answers when every poller has been released.
SHUTTING_DOWN = "shutting_down"


class Executor(Protocol):
    """Drive the poll loop; ``start`` MAY drive it on N threads."""

    @property
    def name(self) -> str: ...

    @property
    def size(self) -> int: ...

    def describe(self) -> str: ...

    def start(self) -> Executor: ...

    def stop(self) -> int: ...


def base_size() -> int:
    """The base pool size: the machine's processor count unless told otherwise.

    The same knob the Rust host reads, deliberately — this host is the Rust
    host's model, and a second vocabulary for the same decision is what makes a
    fleet hard to reason about.
    """
    declared = _positive_env("FLEZHA_WORKER_POOL_SIZE")
    return declared or (os.cpu_count() or 1)


def ceiling_size(base: int) -> int:
    """The ceiling: ten times the base unless told otherwise, never below it.

    A ceiling under the floor would retire the pool the runner was told to keep.
    """
    declared = _positive_env("FLEZHA_WORKER_POOL_MAX")
    return max(declared, base) if declared else base * 10


def _positive_env(name: str) -> int:
    try:
        value = int(os.environ.get(name, "") or 0)
    except ValueError:
        return 0
    return value if value > 0 else 0


def _deadline() -> float:
    return time.monotonic() + DRAIN_TIMEOUT_S


def _remaining(deadline: float) -> float:
    """How long is left of a drain, never negative."""
    return max(deadline - time.monotonic(), 0.0)


def _report_undrained(count: int) -> None:
    """Say what the drain did not finish.

    Silence here is the failure mode the bound exists to avoid reintroducing: a
    runner that leaves without reporting what it abandoned looks identical to
    one that drained.
    """
    if not count:
        return
    _log.warning(
        "%d activation(s) were still running after the %.0fs drain; the runner is exiting "
        "anyway. The core re-hands unanswered work on the next bind — find the handler that "
        "does not return.",
        count,
        DRAIN_TIMEOUT_S,
    )


def fetch(
    core: Any,
    stopping: Callable[[], bool],
    on_activation: Callable[[Any], Any],
    on_core_shutdown: Callable[[], None] = lambda: None,
) -> None:
    """One fetch loop, and the one place ``poll``'s three answers are read.

    Both executors fetch, so what a timeout, an activation and a shutdown each
    mean lives here rather than in one correct copy per executor. The callback
    is handed each activation, and ``None`` on a timeout so a caller that
    retires on an idle streak can count them; answering ``"stop"`` ends the
    loop, as does ``stopping()``.

    A core that says it is shutting down ends the loop AND reports it: a
    controller-initiated close is not the host's own stop, and a runner that
    only watched its own would keep waiting for a termination nothing was going
    to signal.
    """
    while not stopping():
        activation = core.poll(POLL_TIMEOUT_MS)
        if activation == SHUTTING_DOWN:
            on_core_shutdown()
            return
        if on_activation(activation) == "stop":
            return


def _fetch_and_run(
    core: Any,
    deps: DispatchDeps,
    stopping: Callable[[], bool],
    *,
    retire_when_idle: bool,
    on_exit: Callable[[], None] = lambda: None,
    on_core_shutdown: Callable[[], None] = lambda: None,
) -> None:
    """One worker: ``poll -> dispatch -> complete``, on this thread.

    The body of both the thread executor's base pool and the worker either
    executor's supervisor starts on a starvation observation, because those are
    the same worker — what differs is only whether it retires when the work
    stops.
    """
    idle = 0

    def served(activation: Any) -> Any:
        nonlocal idle
        if activation is None:
            idle += 1
            return "stop" if retire_when_idle and idle >= IDLE_RETIREMENT_STREAK else None
        idle = 0
        dispatch_now(activation, core, deps)
        return None

    try:
        fetch(core, stopping, served, on_core_shutdown)
    except Exception as error:  # noqa: BLE001 - a silent death is the defect
        # A worker that dies silently is a pool that shrinks with no report; the
        # supervisor grows it back only if the core starves, which it will not
        # while the remaining workers keep up.
        _log.error("a worker thread ended on %s", error, exc_info=error)
    finally:
        close_thread_loop()
        on_exit()


class _Pool:
    """The threads an executor owns, and the bounded drain that ends them."""

    def __init__(self) -> None:
        self._threads: list[threading.Thread] = []
        self._lock = threading.Lock()

    def spawn(self, name: str, body: Callable[[], None]) -> threading.Thread:
        thread = threading.Thread(target=body, name=name, daemon=True)
        with self._lock:
            # Retired workers are dropped here rather than at the drain: a
            # long-lived runner that grows and retires through many starvation
            # episodes would otherwise accumulate a dead Thread per episode for
            # the life of the process.
            self._threads = [t for t in self._threads if t.is_alive()]
            self._threads.append(thread)
        thread.start()
        return thread

    def alive(self) -> int:
        with self._lock:
            return sum(1 for thread in self._threads if thread.is_alive())

    def join(self, deadline: float) -> int:
        """Join every thread within the deadline; answer how many outlived it."""
        with self._lock:
            threads = list(self._threads)
        for thread in threads:
            thread.join(_remaining(deadline))
        undrained = sum(1 for thread in threads if thread.is_alive())
        with self._lock:
            self._threads = [thread for thread in self._threads if thread.is_alive()]
        return undrained


class _Supervisor:
    """Parked in the core's starvation wait; one worker per observation.

    Not an optimisation and not optional. A completion can block: an emit onto a
    full ``DropNothing`` consumer mailbox parks the completing thread, and
    ``routing::install_inline_serve`` — the hook that rescues the in-process
    Rust host by draining the consumer inline — takes a Rust function pointer
    and is not reachable over the C ABI. A host whose only thread is parked in a
    completion is therefore not entering ``poll``, which is exactly what the
    core reports as starvation, and the worker started here is what drains the
    consumer and releases the producer.

    Both executors run one, and under both the worker it starts is a real
    fetch-and-run thread: a stalled event loop cannot rescue itself, so growing
    the loop's concurrency would rescue nothing.
    """

    def __init__(
        self,
        core: Any,
        *,
        base: int,
        ceiling: int,
        grow: Callable[[], None],
    ) -> None:
        self._core = core
        self._base = base
        self._ceiling = ceiling
        self._grow = grow
        self._grown = 0
        self._lock = threading.Lock()

    @property
    def grown(self) -> int:
        with self._lock:
            return self._grown

    def worker_retired(self) -> None:
        with self._lock:
            self._grown -= 1

    def run(self, stopping: Callable[[], bool]) -> None:
        at_ceiling_logged = False
        try:
            while not stopping():
                status = self._core.wait_starvation(STARVATION_WAIT_MS)
                if status == STARVATION_SHUTTING_DOWN:
                    return
                if status != STARVATION_OBSERVED:
                    continue
                with self._lock:
                    room = self._base + self._grown < self._ceiling
                    if room:
                        self._grown += 1
                if room:
                    self._grow()
                    at_ceiling_logged = False
                elif not at_ceiling_logged:
                    _log.warning(
                        "the core is starving and the worker pool is at its ceiling of %d; "
                        "raise FLEZHA_WORKER_POOL_MAX, or find what is holding the handlers",
                        self._ceiling,
                    )
                    at_ceiling_logged = True
        except Exception as error:  # noqa: BLE001 - the pool would stop growing
            _log.error("the starvation supervisor ended on %s", error, exc_info=error)


class _Host:
    """What both executors are: a pool, a supervisor and a bounded drain."""

    def __init__(
        self,
        core: Any,
        deps: DispatchDeps,
        *,
        base: int | None = None,
        ceiling: int | None = None,
        on_core_shutdown: Callable[[], None] = lambda: None,
    ) -> None:
        self._core = core
        self._deps = deps
        # Resolved here rather than in each member: the defaults are a property
        # of the host, and both members were spelling the same two lines.
        self.base = base or base_size()
        self.ceiling = ceiling or ceiling_size(self.base)
        # What a controller-initiated close reaches. Shutdown is observed three
        # ways — a signal, closed standard input, and the core's own stop — and
        # this is the third: the runner's own wait must end on it too.
        self._on_core_shutdown = on_core_shutdown
        self._pool = _Pool()
        self._stopping = False
        self._supervisor = _Supervisor(
            core, base=self.base, ceiling=self.ceiling, grow=self._spawn_rescue_worker
        )

    @property
    def name(self) -> str:
        """The name ``FLEZHA_PYTHON_EXECUTOR`` selects this member by."""
        raise NotImplementedError

    @property
    def size(self) -> int:
        """How many threads are alive now.

        Reported in the startup line and read by the pool-growth tests.
        """
        return self._pool.alive()

    def describe(self) -> str:
        """This host's conditions, for the runner's startup line.

        The executor knows its own sizing, so it says it — a caller reaching in
        for `base` and `ceiling` had to guess whether they were there.
        """
        return f"{self.name} executor, {self.size} threads, base {self.base} ceiling {self.ceiling}"

    def _start_supervisor(self) -> None:
        self._pool.spawn("flezha-starvation", lambda: self._supervisor.run(self._is_stopping))

    def _is_stopping(self) -> bool:
        return self._stopping

    def _spawn_worker(self, name: str, *, retire_when_idle: bool) -> None:
        self._pool.spawn(
            name,
            lambda: _fetch_and_run(
                self._core,
                self._deps,
                self._is_stopping,
                retire_when_idle=retire_when_idle,
                on_exit=self._supervisor.worker_retired if retire_when_idle else (lambda: None),
                on_core_shutdown=self._on_core_shutdown,
            ),
        )

    def _spawn_rescue_worker(self) -> None:
        self._spawn_worker(f"flezha-rescue-{self._supervisor.grown}", retire_when_idle=True)

    def stop(self) -> int:
        """Stop fetching and drain.

        A worker inside a handler finishes it, completes it, and only then
        observes the shutdown on its next poll. Bounded by
        :data:`DRAIN_TIMEOUT_S` — a handler that never returns must not keep the
        runner alive forever.
        """
        self._stopping = True
        undrained = self._pool.join(_deadline())
        _report_undrained(undrained)
        return undrained


class ThreadExecutor(_Host):
    """The default: a base pool of threads each looping ``poll -> dispatch ->
    complete`` on itself, plus the supervisor that grows it.

    This is the Rust host's model rather than Java's hand-off, and the reason is
    the reason behind both shipped hosts: Java hands off because a parked
    virtual thread costs a stack and nothing else — which is also exactly why
    Java never starves and never calls ``wait_starvation``. A CPython thread is
    a real OS thread with a real stack, so hand-off would buy nothing and cost a
    thread per activation up to the core's in-flight cap.

    Concurrency is bounded by the core's ``FLEZHA_MAX_INFLIGHT_ACTIVATIONS``,
    never by the pool size. The pool is a servicing detail.
    """

    @property
    def name(self) -> str:
        return "thread"

    def start(self) -> ThreadExecutor:
        for index in range(self.base):
            self._spawn_worker(f"flezha-worker-{index}", retire_when_idle=False)
        self._start_supervisor()
        return self


class AsyncioExecutor(_Host):
    """The opt-in executor for I/O-dense handlers: real-thread fetchers hand
    each activation to one shared event loop, which dispatches and completes on
    its own thread.

    The fetcher stays a real thread here too. ``poll`` blocks in native code,
    and ``docs/sdk/poll_core.md`` is explicit that fetching on a runtime's
    shared I/O pool starves everything else that pool serves — so the loop's
    default executor is never borrowed for it.

    Completion happens on the loop thread, not on the fetcher's: the ABI
    documents a completion from a thread other than the poller's as expected,
    and the ordering guarantee is the core's handout rather than this
    executor's. ``uvloop`` is used when it is installed.
    """

    def __init__(
        self,
        core: Any,
        deps: DispatchDeps,
        *,
        loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
        **sizing: Any,
    ) -> None:
        super().__init__(core, deps, **sizing)
        self._loop_factory = loop_factory or _default_loop_factory
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()
        self._room = threading.Semaphore(QUEUE_DEPTH)

    @property
    def name(self) -> str:
        return "asyncio"

    def start(self) -> AsyncioExecutor:
        self._pool.spawn("flezha-loop", self._run_loop)
        if not self._ready.wait(timeout=10.0):
            raise FlezhaError("the asyncio executor's event loop did not start")
        for index in range(self.base):
            self._pool.spawn(f"flezha-fetcher-{index}", self._run_fetcher)
        self._start_supervisor()
        return self

    # -- internals ---------------------------------------------------------

    def _run_loop(self) -> None:
        loop = self._loop_factory()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._ready.set()
        try:
            loop.run_until_complete(self._serve())
        except Exception as error:  # noqa: BLE001 - the loop is the executor
            _log.error("the asyncio executor's loop ended on %s", error, exc_info=error)
        finally:
            loop.close()

    async def _serve(self) -> None:
        """Keep the loop alive until the drain, then let its dispatches finish.

        Closing the loop with tasks still running would abandon activations the
        core is holding entities for, so the drain waits for them the way the
        pool's join does — and is bounded the same way.
        """
        while not self._stopping:
            await asyncio.sleep(POLL_TIMEOUT_MS / 1000)
        pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        if pending:
            await asyncio.wait(pending, timeout=DRAIN_TIMEOUT_S)

    def _run_fetcher(self) -> None:
        def served(activation: Any) -> Any:
            if activation is None:
                return None
            self._room.acquire()
            loop = self._loop
            assert loop is not None
            asyncio.run_coroutine_threadsafe(self._dispatch(activation), loop)
            return None

        try:
            fetch(self._core, self._is_stopping, served, self._on_core_shutdown)
        except Exception as error:  # noqa: BLE001
            _log.error("a fetcher thread ended on %s", error, exc_info=error)

    async def _dispatch(self, activation: Any) -> None:
        try:
            await dispatch(activation, self._core, self._deps)
        finally:
            self._room.release()


def _default_loop_factory() -> asyncio.AbstractEventLoop:
    try:
        import uvloop  # type: ignore[import-not-found]
    except ImportError:
        return asyncio.new_event_loop()
    return uvloop.new_event_loop()


#: The executor seam's members, by the name ``FLEZHA_PYTHON_EXECUTOR`` selects.
EXECUTORS: dict[str, Callable[..., Any]] = {
    "thread": ThreadExecutor,
    "asyncio": AsyncioExecutor,
}


def build(kind: str, core: Any, deps: DispatchDeps, **options: Any) -> Executor:
    """The executor named by ``kind``, or a refusal listing the seam's members.

    Every member runs the starvation supervisor: there is no configuration that
    omits it, because a host whose threads are all parked in a completion has no
    other way back.
    """
    factory = EXECUTORS.get(kind)
    if factory is None:
        raise FlezhaError(f"unknown executor `{kind}`; the seam has {', '.join(sorted(EXECUTORS))}")
    return factory(core, deps, **options)
