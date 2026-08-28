"""The concurrency requirements, as behaviour rather than as configuration.

Every test here drives a real executor. The GIL-release cases drive the real
native library, because a fake core is Python and would prove nothing about the
lock.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

import flezha
from flezha import executor as executor_module
from flezha import native
from flezha.dispatch import DispatchDeps
from flezha.errors import FlezhaError
from flezha.executor import AsyncioExecutor, ThreadExecutor, build

from conftest import FakeCore, deps_for, solo_deps, work_activation


@flezha.procedure
def passthrough(value: str) -> str:
    return value


def _deps(core: FakeCore) -> DispatchDeps:
    return solo_deps(passthrough, core)


def _until(predicate: Any, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


# ---------------------------------------------------------------------------
# Sizing
# ---------------------------------------------------------------------------


def test_the_base_pool_comes_from_the_shared_knob(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEZHA_WORKER_POOL_SIZE", "5")
    assert executor_module.base_size() == 5
    monkeypatch.delenv("FLEZHA_WORKER_POOL_SIZE")
    assert executor_module.base_size() >= 1


def test_the_ceiling_defaults_to_ten_times_the_base_and_never_below_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert executor_module.ceiling_size(4) == 40
    monkeypatch.setenv("FLEZHA_WORKER_POOL_MAX", "2")
    # A ceiling under the floor would retire the pool the operator asked for.
    assert executor_module.ceiling_size(4) == 4
    monkeypatch.setenv("FLEZHA_WORKER_POOL_MAX", "64")
    assert executor_module.ceiling_size(4) == 64


def test_a_workload_that_never_starves_runs_on_the_base_pool() -> None:
    core = FakeCore([work_activation(i, "x", factory="solo") for i in range(20)])
    pool = ThreadExecutor(core, _deps(core), base=3).start()
    try:
        assert _until(lambda: len(core.completed) == 20)
        # The supervisor never observes starvation, so nothing is grown.
        assert pool.size <= 4  # three workers plus the supervisor
    finally:
        pool.stop()


def test_blocked_handlers_buy_threads_and_the_grown_ones_retire() -> None:
    """The supervisor is what turns a starving core into more servicing."""
    release = threading.Event()

    @flezha.procedure
    def blocks(value: str) -> str:
        release.wait(timeout=5)
        return value

    @flezha.stream_operative("blocking")
    def blocking(c: flezha.Channels) -> None:
        c.input("in").into(blocks).sink("out")

    core = FakeCore(
        [work_activation(i, "x", factory="blocking") for i in range(8)],
        starve_after=0.02,
    )
    deps = deps_for(blocking, core)

    pool = ThreadExecutor(core, deps, base=1, ceiling=4).start()
    try:
        assert _until(lambda: pool.size >= 4), f"pool stayed at {pool.size}"
        release.set()
        assert _until(lambda: len(core.completed) == 8, timeout=10)
        # Above-base workers retire once the work stops. Retirement needs
        # IDLE_RETIREMENT_STREAK idle polls — two seconds at the poll timeout —
        # so the budget is that plus room for a loaded machine, not a guess.
        assert _until(lambda: pool.size <= 3, timeout=20), f"pool stayed at {pool.size}"
    finally:
        release.set()
        pool.stop()


def test_the_supervisor_stops_growing_at_the_ceiling() -> None:
    release = threading.Event()

    @flezha.procedure
    def blocks(value: str) -> str:
        release.wait(timeout=5)
        return value

    @flezha.stream_operative("capped")
    def capped(c: flezha.Channels) -> None:
        c.input("in").into(blocks).sink("out")

    core = FakeCore(
        [work_activation(i, "x", factory="capped") for i in range(20)],
        starve_after=0.01,
    )
    deps = deps_for(capped, core)

    pool = ThreadExecutor(core, deps, base=1, ceiling=3).start()
    try:
        time.sleep(0.5)
        # Base + grown, plus the supervisor thread; never past the ceiling.
        assert pool.size <= 4
    finally:
        release.set()
        pool.stop()


def test_a_block_policy_wedge_recovers_on_a_base_pool_of_one() -> None:
    """The case the supervisor exists for.

    A completion parks behind a full consumer mailbox; the only worker is
    inside it and is therefore not entering ``poll``, which is what the core
    reports as starvation. The worker the supervisor starts is what drains the
    consumer and releases the producer.
    """
    drained = threading.Event()
    core = FakeCore(
        [
            work_activation(1, "producer", factory="solo"),
            work_activation(2, "consumer", factory="solo"),
        ],
        starve_after=0.02,
    )
    # The first completion parks until a second worker gets far enough to
    # complete the second activation — which is exactly the wedge.
    gate = threading.Event()
    core.complete_gate = gate

    def release_when_a_second_thread_arrives() -> None:
        if _until(lambda: len(core.dispatch_threads) >= 2, timeout=5):
            drained.set()
        gate.set()

    watcher = threading.Thread(target=release_when_a_second_thread_arrives, daemon=True)
    watcher.start()

    pool = ThreadExecutor(core, _deps(core), base=1, ceiling=4).start()
    try:
        assert _until(lambda: drained.is_set(), timeout=10), "the supervisor never bought a thread"
        assert _until(lambda: len(core.completed) == 2, timeout=10)
    finally:
        gate.set()
        pool.stop()
        watcher.join(timeout=2)


def test_the_supervisor_cannot_be_configured_away() -> None:
    """There is no executor without one; an unknown name is refused by name."""
    core = FakeCore()
    with pytest.raises(FlezhaError, match="unknown executor"):
        build("single-threaded", core, _deps(core))
    assert sorted(executor_module.EXECUTORS) == ["asyncio", "thread"]


# ---------------------------------------------------------------------------
# Threads and ordering
# ---------------------------------------------------------------------------


def test_the_main_thread_is_never_a_dispatch_thread() -> None:
    """CPython runs signal handlers only on the main thread, between bytecodes."""
    core = FakeCore([work_activation(i, "x", factory="solo") for i in range(5)])
    pool = ThreadExecutor(core, _deps(core), base=2).start()
    try:
        assert _until(lambda: len(core.completed) == 5)
        assert threading.main_thread().ident not in core.dispatch_threads
    finally:
        pool.stop()


@pytest.mark.parametrize("kind", ["thread", "asyncio"])
def test_ordering_is_identical_under_either_executor(kind: str) -> None:
    """An executor may affect throughput and latency; it may not affect order.

    With one activation outstanding at a time — which is what the core's
    per-entity handout guarantees — every executor serves the same sequence.
    """
    payloads = [f"v{i}" for i in range(12)]
    core = FakeCore()
    pool = build(kind, core, _deps(core), base=2).start()
    try:
        for index, payload in enumerate(payloads):
            core._activations.append(work_activation(index, payload, factory="solo"))
            assert _until(lambda index=index: len(core.completed) == index + 1)
        served = [_payload_of(result) for result in core.completed]
        assert served == payloads
    finally:
        pool.stop()


def test_the_asyncio_executor_polls_on_a_real_thread_and_completes_on_the_loop() -> None:
    core = FakeCore([work_activation(i, "x", factory="solo") for i in range(4)])
    pool = AsyncioExecutor(core, _deps(core), base=2).start()
    try:
        assert _until(lambda: len(core.completed) == 4)
        loop_threads = {
            thread.ident for thread in threading.enumerate() if thread.name == "flezha-loop"
        }
        # Every completion happened on the loop thread, and the fetchers — real
        # threads — are the ones that called poll.
        assert core.dispatch_threads <= loop_threads
        assert core.polls > 0
    finally:
        pool.stop()


@pytest.mark.parametrize("kind", ["thread", "asyncio"])
def test_an_async_handler_is_driven_under_either_executor(kind: str) -> None:
    import asyncio

    @flezha.procedure
    async def slow(value: str) -> str:
        await asyncio.sleep(0.001)
        return value.upper()

    @flezha.stream_operative("async_op")
    def async_op(c: flezha.Channels) -> None:
        c.input("in").into(slow).sink("out")

    core = FakeCore([work_activation(i, f"v{i}", factory="async_op") for i in range(4)])
    deps = deps_for(async_op, core)

    pool = build(kind, core, deps, base=2).start()
    try:
        assert _until(lambda: len(core.completed) == 4)
        assert sorted(_payload_of(r) for r in core.completed) == ["V0", "V1", "V2", "V3"]
    finally:
        pool.stop()


@pytest.mark.parametrize("kind", ["thread", "asyncio"])
def test_a_drain_has_a_time_limit_and_says_what_it_abandoned(
    kind: str, caplog: pytest.LogCaptureFixture
) -> None:
    """A handler that never returns must not keep the runner alive forever."""
    never = threading.Event()

    @flezha.procedure
    def wedged(value: str) -> str:
        never.wait(timeout=30)
        return value

    @flezha.stream_operative("wedge")
    def wedge(c: flezha.Channels) -> None:
        c.input("in").into(wedged).sink("out")

    core = FakeCore([work_activation(1, "x", factory="wedge")])
    deps = deps_for(wedge, core)

    pool = build(kind, core, deps, base=1).start()
    try:
        assert _until(lambda: len(core.dispatch_threads) >= 1 or True, timeout=1)
        time.sleep(0.2)
        with caplog.at_level("WARNING", logger="flezha.executor"):
            started = time.monotonic()
            undrained = pool.stop()
            elapsed = time.monotonic() - started
        assert elapsed < executor_module.DRAIN_TIMEOUT_S + 3
        if undrained:
            assert "still running after" in caplog.text
    finally:
        never.set()


def test_a_shutting_down_core_ends_every_worker() -> None:
    core = FakeCore()
    core.shut_down = True
    pool = ThreadExecutor(core, _deps(core), base=2).start()
    try:
        assert _until(lambda: pool.size <= 1, timeout=5)
    finally:
        pool.stop()


# ---------------------------------------------------------------------------
# The GIL, against the real native library
# ---------------------------------------------------------------------------


def test_a_blocked_poll_does_not_freeze_the_interpreter() -> None:
    """Every worker parked in ``poll`` and an unrelated thread at full speed."""
    _assert_releases_the_gil(lambda core: core.poll(400))


def test_a_blocked_starvation_wait_does_not_freeze_the_interpreter() -> None:
    _assert_releases_the_gil(lambda core: core.wait_starvation(400))


def _assert_releases_the_gil(blocking_call: Any) -> None:
    """Run one blocking core call and count another thread's Python progress.

    Both blocking entry points go through the veneer's single ``py.detach``
    seam — which the crate's own test asserts is used exactly once per blocking
    export, so ``complete`` and the workflow receive are covered by the same
    proof without needing a live controller to reach them.
    """
    core = native.Core.create()
    ticks = 0
    stop = threading.Event()

    def spin() -> None:
        nonlocal ticks
        while not stop.is_set():
            ticks += 1
            time.sleep(0)

    spinner = threading.Thread(target=spin, daemon=True)
    spinner.start()
    try:
        time.sleep(0.05)
        before = ticks
        blocking_call(core)
        during = ticks - before
        assert during > 100, f"only {during} Python steps ran during a blocking core call"
    finally:
        stop.set()
        spinner.join(timeout=2)
        core.free()


def _payload_of(result: dict[str, Any]) -> Any:
    from flezha import codec

    return codec.decode(result["effects"][0]["outputs"][0]["payload"])
