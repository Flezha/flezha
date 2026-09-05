"""The operative decorators: what an author declares and what a runner registers.

An operative is its channel graph, so the decorated object is the function that
builds one:

.. code-block:: python

    @flezha.stream_operative("echo")
    def echo(c: flezha.Channels) -> None:
        c.input("input").into(echo_proc).named("out").sink("echo_sink")

A workflow operative is either the same shape — a structured chain over a graph
— or a run-sugar body over ``(args, ctx)``. Which one it is is read from the
declaration's own annotations, not from a flag: a parameter annotated
:class:`~flezha.builder.Channels` builds a graph, and one annotated
:class:`~flezha.ctx.WorkflowCtx` is a run body. That keeps the same rule the
rest of this SDK follows — the hints are the declaration — and a function
annotated as neither is refused naming both shapes.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import codec as _codec
from .builder import Channels, ClassSpec
from .ctx import WorkflowCtx
from .errors import DeclarationError
from .procedure import positional_parameters

__all__ = ["Operative", "stream_operative", "workflow_operative"]


class Operative:
    """One declared operative class, resolved to its spec.

    Registering performs no I/O: the decorator records the declaration and the
    spec is built on first use, so importing a declaring module opens no socket
    and starts no thread.
    """

    __slots__ = ("factory_id", "kind", "declaration", "_options", "_spec", "__name__", "doc")

    def __init__(
        self,
        factory_id: str,
        kind: str,
        declaration: Callable[..., Any],
        options: dict[str, Any],
    ) -> None:
        self.factory_id = factory_id
        self.kind = kind
        self.declaration = declaration
        self._options = options
        self._spec: ClassSpec | None = None
        self.__name__ = getattr(declaration, "__name__", factory_id)
        #: The declaration's own docstring. Not `__doc__`, which `__slots__`
        #: may not name on a class that has one of its own.
        self.doc = declaration.__doc__

    def __repr__(self) -> str:
        return f"<flezha {self.kind} operative {self.factory_id}>"

    @property
    def spec(self) -> ClassSpec:
        if self._spec is None:
            self._spec = self._build_spec()
        return self._spec

    def _build_spec(self) -> ClassSpec:
        options = dict(self._options)
        persistent = options.pop("persistent", True)
        if self.kind == "stream":
            return self._stream_spec(persistent)
        return self._workflow_spec(persistent, options)

    def _channels(self) -> Channels:
        builder = Channels(self.factory_id)
        self.declaration(builder)
        return builder

    def _stream_spec(self, persistent: bool) -> ClassSpec:
        if _is_run_body(self.declaration):
            raise DeclarationError(
                self.factory_id,
                self.__name__,
                "is declared as a stream operative but its parameters read as a run body "
                "(args, WorkflowCtx); a stream class is fed by the controller through entry "
                "channels",
            )
        builder = self._channels()
        return ClassSpec(
            factory_id=self.factory_id,
            kind="stream",
            persistent=persistent,
            procedures=builder.procedures,
            channels=builder.channels,
            effectively_once=builder.effectively_once_flag,
            checkpoint_ack=builder.checkpoint_ack_flag,
            checkpoint_every=builder.checkpoint_every_value,
            checkpoint_outputs=builder.checkpoint_outputs_value,
            checkpoint_max_delay_ms=builder.checkpoint_max_delay_ms_value,
            correction_window_ms=builder.correction_window_ms_value,
            beyond_window_policy=builder.beyond_window_policy_value,
            compact_after_bytes=builder.compact_after_bytes_value,
            sync_durability=builder.sync_durability_flag,
        )

    def _workflow_spec(self, persistent: bool, options: dict[str, Any]) -> ClassSpec:
        run = self.declaration if _is_run_body(self.declaration) else None
        builder = Channels(self.factory_id) if run else self._channels()
        return ClassSpec(
            factory_id=self.factory_id,
            kind="workflow",
            persistent=persistent,
            procedures=builder.procedures,
            channels=builder.channels,
            effectively_once=builder.effectively_once_flag,
            sync_durability=bool(options.pop("sync_durability", False))
            or builder.sync_durability_flag,
            workflow={
                "loop_mode": options.pop("loop_mode", "once"),
                "rollback_mode": options.pop("rollback_mode", "all_or_nothing"),
                "retry_attempts": options.pop("retry_attempts", None),
                "retry_backoff_ms": options.pop("retry_backoff_ms", None),
                "barrier_retry_attempts": options.pop("barrier_retry_attempts", None),
                "max_duration_ms": options.pop("max_duration_ms", None),
                "wake_after_ms": options.pop("wake_after_ms", None),
            },
            run=run,
        )


def _is_run_body(fn: Callable[..., Any]) -> bool:
    """Whether a workflow declaration is a run-sugar body rather than a graph.

    Read from the last parameter's annotation: a run body takes ``(args, ctx)``
    with the context annotated :class:`WorkflowCtx`.
    """
    names = positional_parameters(fn, skip_self=False)
    if not names:
        return False
    annotation = _codec.type_hints(fn).get(names[-1])
    return isinstance(annotation, type) and issubclass(annotation, WorkflowCtx)


def _declares_channels(fn: Callable[..., Any]) -> bool:
    names = positional_parameters(fn, skip_self=False)
    if len(names) != 1:
        return False
    annotation = _codec.type_hints(fn).get(names[0])
    return annotation is None or (isinstance(annotation, type) and issubclass(annotation, Channels))


def _operative(
    factory_id: str, kind: str, options: dict[str, Any]
) -> Callable[[Callable[..., Any]], Operative]:
    def decorate(declaration: Callable[..., Any]) -> Operative:
        if kind == "workflow" and not (
            _is_run_body(declaration) or _declares_channels(declaration)
        ):
            raise DeclarationError(
                factory_id,
                getattr(declaration, "__name__", "<declaration>"),
                "is neither a channels graph nor a run body; annotate its single parameter "
                "`flezha.Channels` for a structured workflow, or its second parameter "
                "`flezha.WorkflowCtx` for a run-sugar body",
            )
        return Operative(factory_id, kind, declaration, options)

    return decorate


def stream_operative(
    factory_id: str, *, persistent: bool = True
) -> Callable[[Callable[..., Any]], Operative]:
    """Declare a stream operative: fed by the controller through entry channels.

    ``persistent`` is whether change bytes are flushed to the per-entity WAL.
    """
    return _operative(factory_id, "stream", {"persistent": persistent})


def workflow_operative(
    factory_id: str,
    *,
    persistent: bool = True,
    loop_mode: str = "once",
    rollback_mode: str = "all_or_nothing",
    retry_attempts: int | None = None,
    retry_backoff_ms: int | None = None,
    barrier_retry_attempts: int | None = None,
    max_duration_ms: int | None = None,
    wake_after_ms: int | None = None,
    sync_durability: bool = False,
) -> Callable[[Callable[..., Any]], Operative]:
    """Declare a workflow operative: a run-sugar body, or a structured chain."""
    return _operative(
        factory_id,
        "workflow",
        {
            "persistent": persistent,
            "loop_mode": loop_mode,
            "rollback_mode": rollback_mode,
            "retry_attempts": retry_attempts,
            "retry_backoff_ms": retry_backoff_ms,
            "barrier_retry_attempts": barrier_retry_attempts,
            "max_duration_ms": max_duration_ms,
            "wake_after_ms": wake_after_ms,
            "sync_durability": sync_durability,
        },
    )
