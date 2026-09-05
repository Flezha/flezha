"""The compiled veneer over ``flz_core``'s C ABI, and the host rules around it.

:class:`flz_core_ffi.Core` itself is defined by the extension
(``python_glue.rs``) and is an opaque object rather than a process-wide handle —
which is what keeps a sub-interpreter executor a later swap. This module adds no
ABI surface: every method below forwards to exactly one veneer call through
:meth:`Core._call`, which is the **one** site that touches the native object.
What it adds is the fork guard, which is a host rule and not a core one, and the
translation of the extension's errors into this SDK's one exception root.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from .errors import CoreError, FlezhaError, ForkError, SchemaMismatchError

if TYPE_CHECKING:
    from . import flz_core_ffi

__all__ = ["Core", "ACTIVATION_SCHEMA_VERSION", "load_extension", "build_tier"]

#: The activation schema version this package was written against. The
#: extension refuses a mismatch at context creation, naming both versions,
#: before any controller connection is attempted.
ACTIVATION_SCHEMA_VERSION = 3


def load_extension() -> Any:
    """Import the compiled veneer, or say what to do about its absence."""
    try:
        from . import flz_core_ffi as module
    except ImportError as error:  # pragma: no cover - exercised by a broken install
        raise FlezhaError(
            f"the flezha native extension could not be loaded ({error}). Install a prebuilt "
            f"wheel, or build from a checkout of the flezha repository, where "
            f"scripts/build-python-sdk.sh produces it."
        ) from error
    return module


def build_tier() -> str:
    """Which wheel tier is running: the GIL build or the free-threaded one.

    Reported in the runner's startup line, so a benchmark or a bug report can
    never be ambiguous about the conditions it was produced under.
    """
    return "free-threaded" if load_extension().free_threaded() else "gil"


class Core:
    """The process's core context, with the fork rule enforced around it.

    The core owns native threads — transport, flusher, the runner loop — and
    ``fork(2)`` carries none of them, so a child that inherited a started core
    would connect to nothing and serve nothing, silently.

    Every method below is one line through :meth:`_call`, which is the only
    place in this package that reaches the native object: the fork guard and the
    error translation therefore apply to all of them by construction, and
    ``test_native.py`` asserts there is no second site.
    """

    #: Every ABI entry point a running host reaches, in the veneer's own
    #: spelling. Read by ``test_native.py`` against what the veneer exports, so
    #: an entry point added there and not here is a red build rather than a
    #: method that quietly does not exist.
    ENTRY_POINTS = (
        "declare_class",
        "check_class",
        "start",
        "poll",
        "wait_starvation",
        "complete",
        "abandon",
        "shutdown",
        "workflow_emit",
        "workflow_recv",
        "workflow_cancelled",
        "spawn_workflow",
        "last_error",
    )

    __slots__ = ("_native", "_owner_pid", "_module")

    def __init__(self, native: flz_core_ffi.Core, module: Any) -> None:
        self._native = native
        self._module = module
        # Recorded at construction rather than at `start`, so a fork between
        # the two is caught by the same guard.
        self._owner_pid = os.getpid()

    @classmethod
    def create(cls) -> Core:
        """Create the context, negotiating the activation schema version."""
        module = load_extension()
        try:
            native = module.Core(ACTIVATION_SCHEMA_VERSION)
        except module.SchemaMismatch as error:
            raise SchemaMismatchError(str(error)) from error
        except module.NativeError as error:
            raise CoreError(str(error)) from error
        return cls(native, module)

    # -- the one crossing --------------------------------------------------

    def _call(self, entry: str, *args: Any) -> Any:
        """Guard, cross, and translate. The only site that touches the veneer."""
        if os.getpid() != self._owner_pid:
            raise ForkError(
                f"the flezha core was started in process {self._owner_pid} and this is "
                f"{os.getpid()}: the core owns native threads that fork(2) does not carry, so "
                f"a child cannot serve. Declare classes, fork, and start the core in each child."
            )
        try:
            return getattr(self._native, entry)(*args)
        except self._module.NativeError as error:
            # The engine's own detail, in this SDK's one exception root, with
            # the extension's exception chained so nothing is lost. A refusal
            # the core also answered as data carries that verdict across.
            raise CoreError(str(error), getattr(error, "verdict", None)) from error

    # -- lifecycle ---------------------------------------------------------

    def free(self) -> None:
        """Release what the context creation made."""
        self._call("free")

    def declare_class(self, decl_json: bytes) -> None:
        """Declare one host class from its JSON declaration."""
        self._call("declare_class", decl_json)

    def check_class(self, decl_json: bytes) -> bytes:
        """Judge one declaration without registering it; answers the derived one."""
        answer: bytes = self._call("check_class", decl_json)
        return answer

    def start(self, namespace: str) -> None:
        """Connect to the controller and start the runner loop."""
        self._call("start", namespace)

    def shutdown(self) -> None:
        """Ship every Entity's state to the controller, then wake every blocked
        poller and stop the runner loop."""
        self._call("shutdown")

    # -- the poll loop -----------------------------------------------------

    def poll(self, timeout_ms: int) -> Any:
        """One activation, ``None`` on timeout, or ``"shutting_down"``."""
        return self._call("poll", timeout_ms)

    def wait_starvation(self, timeout_ms: int) -> int:
        """The ABI's three-way starvation status: 1 observed, 0 timeout, 2 closing."""
        return self._call("wait_starvation", timeout_ms)

    def complete(self, result: dict[str, Any]) -> None:
        """Answer one activation."""
        self._call("complete", result)

    def abandon(self, token: int) -> None:
        """Re-hand a polled activation's untouched items and release its entity."""
        self._call("abandon", token)

    # -- workflow bodies ---------------------------------------------------

    def workflow_emit(self, instance_id: str, payload: bytes) -> None:
        self._call("workflow_emit", instance_id, payload)

    def workflow_recv(self, instance_id: str, timeout_ms: int) -> Any:
        return self._call("workflow_recv", instance_id, timeout_ms)

    def workflow_cancelled(self, instance_id: str) -> bool:
        return bool(self._call("workflow_cancelled", instance_id))

    def spawn_workflow(
        self, instance_id: str, factory_id: str, payload: bytes, if_running: str
    ) -> str:
        return str(self._call("spawn_workflow", instance_id, factory_id, payload, if_running))

    # -- diagnostics -------------------------------------------------------

    def last_error(self) -> str | None:
        """The context's last recorded error, or ``None``."""
        answer = self._call("last_error")
        return None if answer is None else str(answer)

    @property
    def native(self) -> Any:
        """The veneer object itself, for a test that drives it directly."""
        return self._native

    # -- context-free entry points ------------------------------------------
    #
    # These take no context, so they take no guard: they read a process-global
    # clock and a process-global channel table, neither of which a fork breaks.

    def monotonic_nanos(self) -> int:
        """The core's monotonic clock in nanoseconds."""
        return int(self._module.monotonic_nanos())

    def entry_last_delivery_nanos(self, channel: str) -> int:
        """When the named entry channel last received a frame, on the same clock."""
        return int(self._module.entry_last_delivery_nanos(channel))

    def idle_watch_new(self, channel: str, threshold_nanos: int) -> int:
        """Register an episode latch over the named entry channel in the core.

        The id, or ``0`` when the name is refused. The tick that advances it is
        a thread of this host's own.
        """
        return int(self._module.idle_watch_new(channel, threshold_nanos))

    def idle_watch_poll(self, watch_id: int) -> tuple[int, int]:
        """Advance a watch's latch to now: ``(status, gap_nanos)``."""
        status, gap = self._module.idle_watch_poll(watch_id)
        return int(status), int(gap)

    def idle_watch_free(self, watch_id: int) -> None:
        """Drop a watch."""
        self._module.idle_watch_free(watch_id)
