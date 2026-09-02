"""Idle observation for entry channels: the policy half of the input-liveness
signal the core records.

The core knows when a frame last arrived and owns the episode latch over it
(``flz_core::entry_input::IdleWatch``, reached by watch id); only the
application knows whether the silence since then is anomalous — a quiet
overnight market is not a dead feed. So the SDK ships no default threshold and
no default action: :meth:`Runner.on_entry_idle` takes both from the caller and
reports episodes.

The tick is one daemon thread per runner, woken by an event wait — never
``time.sleep`` — so ``drain`` ends it by setting the event. It runs while the
GIL is free: a pure-Python body holding the lock past the tick delays the
report by its length and never changes the count, because the latch compares
stamps, not clock readings.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from .errors import FlezhaError
from .logger import logger

__all__ = ["EntryIdleWatcher", "IdleObserver", "last_entry_delivery", "tick_period_s"]

_log = logger("entry_idle")

#: Tick floor and ceiling, the same in every SDK. The floor bounds how often a
#: fleet of observers wakes; the ceiling bounds how late an episode is reported.
TICK_MIN_S = 0.25
TICK_MAX_S = 1.0

#: The two poll answers that are episode boundaries; ``0`` is none and ``-1``
#: an id no watch has.
_IDLE = 1
_RESUMED = 2


class IdleObserver:
    """What an application does about an entry channel that has gone quiet.

    Registered per channel with a threshold; the SDK reports **episodes**, not
    ticks: :meth:`idle` fires once when the silence first exceeds the threshold
    and not again while it continues, and :meth:`resumed` fires once when a
    frame finally arrives, re-arming the next episode. Silence before the very
    first frame is never an episode — an operative waiting for its
    subscriptions to take is not an incident.

    Both calls run on the SDK's tick thread. A raise is caught and logged;
    delivery and later episodes are unaffected. A bare callable is an observer
    whose ``resumed`` does nothing.
    """

    def idle(self, gap_ms: float) -> None:
        """The channel has been silent for ``gap_ms``, which first exceeded the threshold."""
        raise NotImplementedError

    def resumed(self, gap_ms: float) -> None:
        """A frame arrived, ending an episode that had lasted ``gap_ms``.

        Does nothing unless overridden — an alarm that only needs raising
        implements :meth:`idle` alone. ``gap_ms`` is measured to the newest
        frame the tick saw, so a burst that resumes and continues within one
        tick reports the silence plus that burst, bounded by the tick.
        """
        return


class _CallableObserver(IdleObserver):
    def __init__(self, fn: Callable[[float], object]) -> None:
        self._fn = fn

    def idle(self, gap_ms: float) -> None:
        self._fn(gap_ms)


def tick_period_s(threshold_ms: float) -> float:
    """The tick a ``threshold_ms`` asks for: a quarter of it, so an episode is
    reported within 25% of the threshold, bounded at both ends."""
    return min(TICK_MAX_S, max(TICK_MIN_S, threshold_ms / 4000.0))


def last_entry_delivery(core: Any, channel: str) -> datetime | None:
    """When ``channel`` last received a frame, or ``None`` before its first frame
    (and for a name no declared operative has — both mean "nothing has arrived
    here").

    The core's stamp is monotonic; the wall-clock instant is derived from the
    gap, so a clock step between then and now cannot skew it.
    """
    last = int(core.entry_last_delivery_nanos(channel))
    if last == 0:
        return None
    gap = max(int(core.monotonic_nanos()) - last, 0)
    return datetime.now(UTC) - timedelta(microseconds=gap / 1_000)


class _Watch:
    __slots__ = ("channel", "id", "observer")

    def __init__(self, channel: str, watch_id: int, observer: IdleObserver) -> None:
        self.channel = channel
        self.id = watch_id
        self.observer = observer


class EntryIdleWatcher:
    """One runner's idle observers and the thread that evaluates them.

    ``ticking=False`` leaves the tick to the caller — a test's seam, the way
    the core's ``stamp_at`` is one.
    """

    def __init__(self, core: Any, *, ticking: bool = True) -> None:
        self._core = core
        self._ticking = ticking
        self._watches: list[_Watch] = []
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stopping = False
        self._thread: threading.Thread | None = None
        self._period_s: float | None = None

    def register(
        self,
        channel: str,
        threshold_ms: float,
        observer: IdleObserver | Callable[[float], object],
    ) -> None:
        """Register ``observer`` against ``channel``.

        Legal before and after :meth:`Runner.start`: the core interns the
        observation by name, so a registration made before the class is
        declared watches the same channel it later gets. Several observers may
        watch one channel.
        """
        if isinstance(threshold_ms, bool) or not (
            isinstance(threshold_ms, (int, float)) and threshold_ms > 0
        ):
            raise ValueError(
                "on_entry_idle: the threshold must be a positive number of milliseconds, "
                f"got {threshold_ms!r}"
            )
        if isinstance(observer, IdleObserver):
            admitted = observer
        elif callable(observer):
            admitted = _CallableObserver(observer)
        else:
            raise TypeError("on_entry_idle: the observer must implement `idle` or be callable")
        watch_id = int(self._core.idle_watch_new(channel, round(threshold_ms * 1_000_000)))
        if watch_id == 0:
            raise FlezhaError(f"on_entry_idle: the core refused the channel name `{channel}`")
        wanted = tick_period_s(threshold_ms)
        with self._lock:
            self._watches.append(_Watch(channel, watch_id, admitted))
            if self._period_s is None or wanted < self._period_s:
                self._period_s = wanted
            if self._ticking and self._thread is None:
                self._stopping = False
                self._thread = threading.Thread(
                    target=self._run, name="flezha-entry-idle", daemon=True
                )
                self._thread.start()
        # A shorter period re-arms the wait at once rather than after the old one.
        self._wake.set()

    def tick(self) -> None:
        """Evaluate every watch once and deliver the episodes the core found."""
        with self._lock:
            watches = list(self._watches)
        for watch in watches:
            status, gap_nanos = self._core.idle_watch_poll(watch.id)
            if status == _IDLE:
                self._deliver(watch, "idle", gap_nanos)
            elif status == _RESUMED:
                self._deliver(watch, "resumed", gap_nanos)

    def stop(self) -> None:
        """Stop the tick and drop every registration, in the core too. Idempotent."""
        with self._lock:
            thread, self._thread = self._thread, None
            self._stopping = True
        self._wake.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        with self._lock:
            watches, self._watches = self._watches, []
            self._period_s = None
        for watch in watches:
            self._core.idle_watch_free(watch.id)

    def _run(self) -> None:
        while True:
            with self._lock:
                period = self._period_s or TICK_MAX_S
            self._wake.wait(period)
            self._wake.clear()
            with self._lock:
                if self._stopping:
                    return
            self.tick()

    def _deliver(self, watch: _Watch, episode: str, gap_nanos: int) -> None:
        """Invoke the observer, contained: application code runs on the SDK's
        own thread, so a raise must become a log line rather than a dead
        ticker."""
        gap_ms = int(gap_nanos) / 1_000_000
        try:
            if episode == "idle":
                watch.observer.idle(gap_ms)
            else:
                watch.observer.resumed(gap_ms)
        except Exception as error:  # noqa: BLE001 - the tick must outlive the observer
            _log.warning(
                "entry-idle observer raised on the %s episode of channel '%s': %s",
                episode,
                watch.channel,
                error,
            )
