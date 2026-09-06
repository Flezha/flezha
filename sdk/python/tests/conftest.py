"""Shared fixtures: a fake core, and the declarations the tests drive.

The fake core answers the ABI's own vocabulary — ``poll`` gives an activation, a
timeout or ``"shutting_down"``, ``wait_starvation`` gives the three-way status —
so a test of the executor tests the executor rather than a mock of it.
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import pytest

import flezha
from flezha import codec, native

#: The process context ``graph.validate`` submits a record through. One per test
#: session: a check registers nothing, so every suite shares it.
CHECK_CORE = native.Core.create()


def declare(operative: flezha.Operative) -> dict[str, Any]:
    """Submit one operative's record, and answer what the core derived from it."""
    from flezha import graph

    return graph.validate(operative.spec, CHECK_CORE)


@flezha.state
@dataclass
class Counter:
    """The reference declared state: a scalar and a list."""

    count: int = 0
    seen: list[str] = field(default_factory=list)


@dataclass
class Bumped:
    """The reference declared change."""

    by: int


class CountUp:
    """A stateful procedure in the class spelling."""

    def process(self, value: str, ctx: flezha.Ctx[Counter]) -> None:
        ctx.change(Bumped(by=len(value)))
        ctx.emit({"total": ctx.state.count})

    def initial_state(self, key: str | None) -> Counter:
        return Counter()

    def apply_change(self, state: Counter, change: Bumped) -> None:
        state.count += change.by
        state.seen.append(str(change.by))


count_up = flezha.procedure(CountUp)


@flezha.procedure
def echo(value: str) -> str:
    """A stateless procedure in the function spelling."""
    return value


@flezha.stream_operative("demo")
def demo(c: flezha.Channels) -> None:
    c.input("input").into(echo).named("mid").into(count_up).named("out").sink("demo_sink")


class FakeCore:
    """A core that answers the ABI's vocabulary, with no native library.

    Every executor test drives this: what it proves is that the host reads
    ``poll`` and ``wait_starvation`` the way the ABI defines them.
    """

    def __init__(
        self,
        activations: list[Any] | None = None,
        *,
        starve_after: float | None = None,
    ) -> None:
        self._activations: queue.Queue[Any] = queue.Queue()
        for activation in activations or []:
            self._activations.put(activation)
        self._lock = threading.Lock()
        self._starve_after = starve_after
        self._shutting = threading.Event()
        self.completed: list[dict[str, Any]] = []
        self.abandoned: list[int] = []
        self.polls = 0
        self.pollers_inside = 0
        self.peak_pollers = 0
        self.shut_down = False
        self.freed = False
        self.declared: list[bytes] = []
        self.started: str | None = None
        self.complete_gate: threading.Event | None = None
        self.dispatch_threads: set[int] = set()
        self.idle_watches = 0
        self.idle_freed: list[int] = []
        self.stdin_watch_grace_ms: int | None = None

    # -- the ABI ----------------------------------------------------------

    def declare_class(self, json_bytes: bytes) -> None:
        self.declared.append(json_bytes)

    def check_class(self, json_bytes: bytes) -> bytes:
        # The one entry point the fake does not fake: a check registers nothing
        # and mutates nothing, so the real core is what a test wants an answer
        # from — a fake verdict would only prove the fake.
        return CHECK_CORE.check_class(json_bytes)

    def start(self, namespace: str) -> None:
        self.started = namespace

    def offer(self, activation: Any) -> None:
        """Queue one activation for the next poller, as a controller would."""
        self._activations.put(activation)

    def poll(self, timeout_ms: int) -> Any:
        with self._lock:
            self.polls += 1
            self.pollers_inside += 1
            self.peak_pollers = max(self.peak_pollers, self.pollers_inside)
            shutting = self.shut_down
        try:
            if shutting:
                return "shutting_down"
            try:
                return self._activations.get(timeout=min(timeout_ms, 20) / 1000)
            except queue.Empty:
                return None
        finally:
            with self._lock:
                self.pollers_inside -= 1

    def wait_starvation(self, timeout_ms: int) -> int:
        """The core's condition, not an unconditional yes.

        Starvation is *work queued with no thread inside poll*. A fake that
        answered `observed` forever would make the supervisor grow, retire and
        grow again in perpetuity — which is a defect of the fake, and would make
        a retirement assertion true only inside a window.
        """
        if self.shut_down:
            return 2
        waited = timeout_ms / 1000 if self._starve_after is None else self._starve_after
        if self._shutting.wait(timeout=waited):
            return 2
        if self._starve_after is None:
            return 0
        return 1 if not self._activations.empty() else 0

    def complete(self, result: dict[str, Any]) -> None:
        self.dispatch_threads.add(threading.get_ident())
        if self.complete_gate is not None:
            self.complete_gate.wait(timeout=5)
        with self._lock:
            self.completed.append(result)

    def abandon(self, token: int) -> None:
        with self._lock:
            self.abandoned.append(token)

    def shutdown(self) -> None:
        self.shut_down = True
        self._shutting.set()

    def watch_stdin_eof(self, grace_ms: int) -> None:
        self.stdin_watch_grace_ms = grace_ms

    def free(self) -> None:
        self.freed = True

    def guard(self) -> None:
        pass

    def spawn_workflow(self, *args: Any) -> str:
        return "spawned"

    def workflow_emit(self, instance_id: str, payload: bytes) -> None:
        pass

    def workflow_recv(self, instance_id: str, timeout_ms: int) -> Any:
        return None

    def workflow_cancelled(self, instance_id: str) -> bool:
        return False

    def monotonic_nanos(self) -> int:
        return time.monotonic_ns()

    def entry_last_delivery_nanos(self, channel: str) -> int:
        return 0

    def idle_watch_new(self, channel: str, threshold_nanos: int) -> int:
        with self._lock:
            self.idle_watches += 1
            return self.idle_watches

    def idle_watch_poll(self, watch_id: int) -> tuple[int, int]:
        return (0, 0)

    def idle_watch_free(self, watch_id: int) -> None:
        with self._lock:
            self.idle_freed.append(watch_id)


def work_activation(
    token: int, *payloads: Any, factory: str = "demo", procedure: int = 0
) -> dict[str, Any]:
    """One work activation, as the veneer produces it.

    Variadic because a batch is the interesting case — a failure mid-batch stops
    it — and a one-payload call is the same builder with one argument.
    """
    return {
        "token": token,
        "operative": "op",
        "procedure": procedure,
        "change_seq": 0,
        "factory": factory,
        "kind": "work",
        "items": [
            {
                "kind": "input",
                "channel": 0,
                "payload": codec.encode(payload),
                "metadata": [],
                "consumed_offset": token + index,
                "replay": False,
                "now": 0,
            }
            for index, payload in enumerate(payloads)
        ],
    }


def deps_for(operative: flezha.Operative, core: FakeCore) -> Any:
    """A validated, registered `DispatchDeps` over one operative.

    Twelve tests across two files spelled `Registry() + validate + add +
    DispatchDeps` out longhand; this is that, once, so a change to what a
    dispatch loop needs is one edit.
    """
    from flezha.dispatch import DispatchDeps, Registry

    registry = Registry()
    declare(operative)
    registry.add(operative.spec)
    return DispatchDeps(
        registry=registry,
        bridge=core,
        workflow_ctx=lambda instance_id: flezha.WorkflowCtx(instance_id, core),
    )


def compensating(seen: list[Any]) -> Any:
    """A procedure whose compensation appends the outputs it is driven over.

    Two suites drive the same shape — the dispatcher's `compensate_run` arm and
    the harness's `compensate` — so the procedure is declared once.
    """

    class Compensating:
        def process(self, value: str, ctx: flezha.Ctx[None]) -> None:
            ctx.emit(value)

        def compensate(self, outputs: list[Any], ctx: flezha.Ctx[None]) -> None:
            seen.extend(outputs)

    return flezha.procedure(Compensating)


def solo_deps(proc: flezha.Procedure[Any, Any], core: FakeCore, factory: str = "solo") -> Any:
    """The same, over a one-procedure operative wrapped around `proc`."""

    @flezha.stream_operative(factory)
    def operative(c: flezha.Channels) -> None:
        c.input("in").into(proc).sink("out")

    return deps_for(operative, core)


def pytest_configure(config: pytest.Config) -> None:
    """Fail any test that trips a determinism warning.

    Declared here and not in ``pyproject.toml``: pytest resolves an ini-time
    category by importing its package, which happens before coverage starts
    tracing and leaves every module executed at import unmeasured.
    """
    config.addinivalue_line("filterwarnings", "error::flezha.DeterminismWarning")


@pytest.fixture
def fake_core() -> FakeCore:
    return FakeCore()


@pytest.fixture(autouse=True)
def _quiet_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the executor tests off the machine's processor count."""
    monkeypatch.setenv("FLEZHA_WORKER_POOL_SIZE", "2")
    monkeypatch.delenv("FLEZHA_WORKER_POOL_MAX", raising=False)
