"""The observer's host half — registration, the tick, contained delivery and
the accessor's mapping — against a scripted core. The latch itself is the
core's and is proved there; what this proves is the wiring over it."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime, timedelta

import pytest

import flezha
from conftest import FakeCore, demo
from flezha.entry_idle import EntryIdleWatcher, IdleObserver, last_entry_delivery, tick_period_s
from flezha.errors import FlezhaError

GAP_NANOS = 1_500_000_000


class ScriptedCore:
    """A core whose polls answer a script — the ABI's statuses in order, then none."""

    def __init__(self, answers: list[int], *, stamped: int = 0) -> None:
        self.answers = answers
        self.stamped = stamped
        self.freed: list[int] = []
        self.next = 0

    def monotonic_nanos(self) -> int:
        return 10**12

    def entry_last_delivery_nanos(self, channel: str) -> int:
        return self.stamped if channel == "fed" else 0

    def idle_watch_new(self, channel: str, threshold_nanos: int) -> int:
        if not channel:
            return 0
        self.next += 1
        return self.next

    def idle_watch_poll(self, watch_id: int) -> tuple[int, int]:
        status = self.answers.pop(0) if self.answers else 0
        return status, GAP_NANOS if status > 0 else 0

    def idle_watch_free(self, watch_id: int) -> None:
        self.freed.append(watch_id)


class Recorder(IdleObserver):
    def __init__(self) -> None:
        self.seen: list[tuple[str, float]] = []

    def idle(self, gap_ms: float) -> None:
        self.seen.append(("idle", gap_ms))

    def resumed(self, gap_ms: float) -> None:
        self.seen.append(("resumed", gap_ms))


def test_one_idle_and_one_resumed_reach_the_observer_in_the_order_the_core_answers() -> None:
    watcher = EntryIdleWatcher(ScriptedCore([0, 1, 0, 2]), ticking=False)
    recorder = Recorder()
    watcher.register("trades_in", 1_000, recorder)
    for _ in range(4):
        watcher.tick()
    assert recorder.seen == [("idle", 1500.0), ("resumed", 1500.0)]
    watcher.stop()


def test_a_bare_callable_is_an_observer_whose_resume_does_nothing() -> None:
    watcher = EntryIdleWatcher(ScriptedCore([1, 2, 1]), ticking=False)
    gaps: list[float] = []
    watcher.register("trades_in", 1_000, gaps.append)
    for _ in range(3):
        watcher.tick()
    assert gaps == [1500.0, 1500.0]
    watcher.stop()


def test_a_raising_observer_is_logged_and_the_next_episode_still_reaches_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls = 0

    class Raising(IdleObserver):
        def idle(self, gap_ms: float) -> None:
            nonlocal calls
            calls += 1
            raise RuntimeError("observer under test")

    watcher = EntryIdleWatcher(ScriptedCore([1, 2, 1]), ticking=False)
    watcher.register("book_in", 1_000, Raising())
    with caplog.at_level(logging.WARNING, logger="flezha.entry_idle"):
        for _ in range(3):
            watcher.tick()
    assert calls == 2, "the episode after the raise still reaches the observer"
    lines = [record.getMessage() for record in caplog.records]
    assert len(lines) == 2
    assert (
        "entry-idle observer raised on the idle episode of channel 'book_in': observer under test"
        == lines[0]
    )
    watcher.stop()


def test_stop_frees_every_watch_the_core_handed_out_and_is_idempotent() -> None:
    core = ScriptedCore([])
    watcher = EntryIdleWatcher(core, ticking=False)
    watcher.register("a", 1_000, lambda gap: None)
    watcher.register("b", 2_000, lambda gap: None)
    watcher.stop()
    assert core.freed == [1, 2]
    watcher.stop()
    assert core.freed == [1, 2]


def test_the_first_registration_starts_the_tick_thread_and_stop_ends_it() -> None:
    watcher = EntryIdleWatcher(ScriptedCore([]))
    watcher.register("a", 1_000, lambda gap: None)
    thread = next(t for t in threading.enumerate() if t.name == "flezha-entry-idle")
    assert thread.daemon, "the tick never keeps the process alive"
    watcher.stop()
    assert not thread.is_alive(), "stop ends the tick thread"


def test_a_refused_name_a_non_positive_threshold_and_a_non_observer_are_refused() -> None:
    watcher = EntryIdleWatcher(ScriptedCore([]), ticking=False)
    with pytest.raises(FlezhaError, match="refused the channel name"):
        watcher.register("", 1_000, lambda gap: None)
    with pytest.raises(ValueError, match="positive number of milliseconds"):
        watcher.register("x", 0, lambda gap: None)
    with pytest.raises(TypeError, match="implement `idle` or be callable"):
        watcher.register("x", 1_000, object())  # type: ignore[arg-type]


def test_the_tick_is_a_quarter_of_the_threshold_within_its_bounds() -> None:
    assert tick_period_s(60_000) == 1.0
    assert tick_period_s(2_000) == 0.5
    assert tick_period_s(10) == 0.25


def test_the_accessor_is_absent_before_the_first_frame_and_an_instant_from_the_gap_after() -> None:
    core = ScriptedCore([], stamped=10**12 - 2 * 10**9)
    assert last_entry_delivery(core, "never") is None
    before = datetime.now(UTC)
    at = last_entry_delivery(core, "fed")
    after = datetime.now(UTC)
    assert at is not None
    assert before - timedelta(seconds=2) <= at <= after - timedelta(seconds=2)


def test_the_runner_frees_its_watches_on_drain() -> None:
    """The runner wires the watcher: a registration after start reaches the
    core the runner started with, and ``drain`` frees it."""
    core = FakeCore()
    runner = flezha.create_runner("test").define_stream(demo)
    runner.start(core=core)  # type: ignore[arg-type]
    try:
        assert runner.on_entry_idle("input", 1_000, lambda gap: None) is runner
        assert runner.last_entry_delivery("input") is None
    finally:
        runner.stop()
        runner.drain()
    assert core.idle_freed == [1]
