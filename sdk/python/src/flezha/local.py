"""Running a declared operative graph in this process, with no controller.

:meth:`LocalOperative.start` takes the operative you deploy and runs the
``channels`` graph it already declares — no local-mode member on the class, no
second declaration. What runs is the deployed machinery: entry decode, edge
transforms and keying, one Entity per resolved key with its own mailbox, the
retry ladder, and inter-procedure delivery performed by the runtime.

State is held in memory and nothing is durable. Durability, plugins, placement
and migration, the dead-letter queue, observability export, authz and
multi-runner scale all need a controller; the run says so on stderr every time
it starts. ``docs/quickstart.md`` walks through it.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Self

from . import graph
from .dispatch import DispatchDeps, Registry
from .errors import CoreError
from .executor import build as build_executor
from .native import Core
from .operative import Operative

__all__ = ["LocalOperative", "SinkOutput"]

#: Bounded so a stopped run's drain thread leaves promptly.
_OUTPUT_POLL_MS = 100


@dataclass(slots=True)
class SinkOutput:
    """One value a graph released on a declared sink."""

    channel: str
    payload: bytes
    #: What a deployed run would have put on the wire, ``flz.*`` stamps included.
    metadata: dict[str, str] = field(default_factory=dict)


class LocalOperative:
    """One operative graph running in this process."""

    __slots__ = ("_core", "_executor", "_sinks", "_drain", "_closed")

    def __init__(self, core: Core, executor: Any) -> None:
        self._core = core
        self._executor = executor
        self._sinks: dict[str, Callable[[SinkOutput], None]] = {}
        self._closed = threading.Event()
        self._drain = threading.Thread(
            target=self._drain_outputs, name="flz-local-sink", daemon=True
        )
        self._drain.start()

    @classmethod
    def start(cls, operative: Operative) -> LocalOperative:
        """Register ``operative``'s declared graph and start it locally.

        Raises naming the capability when the declaration needs a controller.
        """
        core = Core.create()
        try:
            spec = operative.spec
            registry = Registry()
            graph.declare(spec, core, registry)
            # Not `start`, which is the one call that dials a controller.
            core.local_start(spec.factory_id, f"{spec.factory_id}-local")
            deps = DispatchDeps(registry=registry, bridge=core, workflow_ctx=None)
            executor = build_executor(
                os.environ.get("FLEZHA_PYTHON_EXECUTOR", "thread"), core, deps
            )
            executor.start()
        except BaseException:
            core.free()
            raise
        return cls(core, executor)

    def on_sink(self, channel: str, callback: Callable[[SinkOutput], None]) -> Self:
        """Deliver every value released on ``channel`` to ``callback``.

        A sink channel takes the producing procedure's name unless the
        declaration named it.
        """
        # Registered with the core first: a channel the declaration does not
        # carry is refused there, naming it, rather than silently accepted.
        self._core.local_on_sink(channel)
        self._sinks[channel] = callback
        return self

    def feed(self, channel: str, payload: bytes | str) -> Self:
        """Feed one value's wire bytes to a declared entry channel."""
        self._core.local_feed(channel, payload.encode() if isinstance(payload, str) else payload)
        return self

    def run_until_idle(self, budget_ms: int) -> None:
        """Block until the graph has nothing left to do.

        A budget that elapses first is reported, never passed off as done.
        """
        self._core.local_run_until_idle(budget_ms)

    def close(self) -> None:
        """End the run and release its Entities. Idempotent."""
        if self._closed.is_set():
            return
        self._closed.set()
        self._core.local_stop()
        self._drain.join(timeout=_OUTPUT_POLL_MS / 1000 * 20)
        # Not `Core.shutdown`: that stops the process-wide poll core, and a
        # process may start another local run after this one. The workers
        # leave on the flag instead, which is why it is raised before the
        # drain rather than after it.
        self._executor.stop_fetching()
        undrained = self._executor.stop()
        # As the runner does: a worker the bounded drain gave up on may yet
        # call the core, and freeing the context under it is a use-after-free.
        if undrained == 0:
            self._core.free()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _drain_outputs(self) -> None:
        """One released value per pass, handed to whoever registered its channel."""
        while not self._closed.is_set():
            try:
                output = self._core.local_next_output(_OUTPUT_POLL_MS)
            except CoreError:
                # The run ended under us; the close path owns the teardown.
                # Narrow on purpose: a `ForkError` is a defect, not a stop.
                return
            if output is not None:
                self._deliver(output)

    def _deliver(self, output: dict[str, Any]) -> None:
        if output.get("target") != "plugin":
            # The core queues what a sink released and nothing else; an edge
            # output is an in-process mailbox push that never leaves.
            raise RuntimeError(f"local mode released a {output.get('target')} output")
        channel = str(output["plugin_channel"])
        callback = self._sinks.get(channel)
        if callback is None:
            return
        payload = bytes(output["payload"])
        callback(SinkOutput(channel, payload, {k: v for k, v in output.get("metadata", ())}))
