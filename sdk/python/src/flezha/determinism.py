"""Warn when a handler reads ambient time or identity.

``ctx.now()`` and ``ctx.stable_id()`` are replay-stable; ``time.time()``,
``datetime.now()`` and ``uuid.uuid4()`` are not. A handler that reads one of the
latter produces a different value on the live run and on the replay, and nothing
else reports the difference — the record is written, the fold succeeds, and the
two states quietly disagree.

So the calls are detected and warned about, naming the file and line that made
them. A warning and not a refusal: the call is legal Python, an author may have
a reason, and aborting an invocation over a diagnostic would be a worse failure
than the one it diagnoses.

Detection is armed only while an invocation is running and only on the thread
running it, so a fetcher, a supervisor or an unrelated library thread reading
the clock is silent. It is installed when a runner boots and never at import:
importing this SDK must change nothing about a process that has not started one.
"""

from __future__ import annotations

import datetime as _datetime
import threading
import time as _time
import uuid as _uuid
import warnings
from typing import Any, Callable

from .errors import DeterminismWarning

__all__ = ["install", "invocation", "armed"]

# The real ones, captured before anything is replaced, so the wrappers call
# through to what they wrapped rather than to themselves.
_REAL_DATETIME = _datetime.datetime

_state = threading.local()
_installed = False
_install_lock = threading.Lock()


def armed() -> bool:
    """Whether this thread is inside an invocation."""
    return getattr(_state, "depth", 0) > 0


class _Invocation:
    """Arm ambient detection for the duration of one invocation, on this thread."""

    __slots__ = ()

    def __enter__(self) -> None:
        _state.depth = getattr(_state, "depth", 0) + 1

    def __exit__(self, *exc: object) -> None:
        _state.depth -= 1


_INVOCATION = _Invocation()


def invocation() -> _Invocation:
    """The scope inside which ambient reads warn. Re-entrant, thread-local."""
    return _INVOCATION


def _warn(what: str, alternative: str) -> None:
    if not armed():
        return
    # `stacklevel` steps past this function and the wrapper that called it, so
    # the warning names the handler's own file and line.
    warnings.warn(
        f"{what} was called inside an invocation; its value differs between the live run and "
        f"the replay. Use {alternative}, which the engine computes once per invocation and "
        f"reproduces on replay.",
        DeterminismWarning,
        stacklevel=3,
    )


def _wrap(original: Callable[..., Any], what: str, alternative: str) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        _warn(what, alternative)
        return original(*args, **kwargs)

    wrapper.__wrapped__ = original  # type: ignore[attr-defined]
    wrapper.__name__ = getattr(original, "__name__", what)
    wrapper.__qualname__ = getattr(original, "__qualname__", what)
    wrapper.__doc__ = original.__doc__
    return wrapper


class _DatetimeMeta(type):
    """Keeps ``isinstance`` and ``issubclass`` answering about the real type.

    ``datetime.now`` is a C-level classmethod that cannot be reassigned, so the
    only way to observe it is to put a subclass in the module's place — and a
    subclass would otherwise make ``isinstance(a_real_datetime, datetime)``
    answer ``False`` for every value produced before the swap, which is a far
    worse defect than the one being diagnosed. Both checks are delegated back.
    """

    def __instancecheck__(cls, instance: Any) -> bool:
        return isinstance(instance, _REAL_DATETIME)

    def __subclasscheck__(cls, subclass: type) -> bool:
        return issubclass(subclass, _REAL_DATETIME)


class _WarningDatetime(_REAL_DATETIME, metaclass=_DatetimeMeta):
    """``datetime`` whose ambient constructors warn while an invocation runs."""

    @classmethod
    def now(cls, tz: Any = None) -> Any:
        _warn("datetime.now()", "ctx.now()")
        return _REAL_DATETIME.now(tz)

    @classmethod
    def utcnow(cls) -> Any:
        _warn("datetime.utcnow()", "ctx.now()")
        return _REAL_DATETIME.now(_datetime.timezone.utc)

    @classmethod
    def today(cls) -> Any:
        _warn("datetime.today()", "ctx.now()")
        return _REAL_DATETIME.today()


def install() -> None:
    """Arm the detection. Idempotent, and called once when a runner boots."""
    global _installed
    with _install_lock:
        if _installed:
            return
        _time.time = _wrap(_time.time, "time.time()", "ctx.now()")  # type: ignore[assignment]
        _time.time_ns = _wrap(_time.time_ns, "time.time_ns()", "ctx.now()")  # type: ignore[assignment]
        _uuid.uuid4 = _wrap(_uuid.uuid4, "uuid.uuid4()", "ctx.stable_id()")  # type: ignore[assignment]
        _uuid.uuid1 = _wrap(_uuid.uuid1, "uuid.uuid1()", "ctx.stable_id()")  # type: ignore[assignment]
        _datetime.datetime = _WarningDatetime  # type: ignore[misc]
        _installed = True
