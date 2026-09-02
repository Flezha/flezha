"""The Flezha Python SDK: a poll host over ``flz_core``.

The SDK contains no kernel code. Transport, framing, ordering, per-entity
exclusivity, durability, retries, the retry-to-park ladder, timers, credit,
routing, the workflow driver and reconnect all live in the core; this package
brings the authoring surface, the codec for user types, the threading host and
idiomatic errors.

.. code-block:: python

    import flezha

    @flezha.procedure
    def echo(value: str) -> str:
        return value

    @flezha.stream_operative("echo")
    def echo_operative(c: flezha.Channels) -> None:
        c.input("input").into(echo).named("out").sink("echo_sink")

    flezha.create_runner("default").define_stream(echo_operative).run()
"""

from __future__ import annotations

from .builder import DROPPED, Channels, Edge
from .codec import TypeCodec, ValueCodec
from .ctx import Ctx, WorkflowCtx
from .entity_state import state
from .entry_idle import IdleObserver
from .logger import configure as configure_logging
from .errors import (
    CoreError,
    DeclarationError,
    DeterminismWarning,
    EmitError,
    FlezhaError,
    ForkError,
    NonRetryableError,
    SchemaMismatchError,
    UnrepresentableValueError,
)
from .operative import Operative, stream_operative, workflow_operative
from .procedure import Procedure, procedure
from .runner import ACTIVATION_SCHEMA_VERSION, Runner, create_runner
from .version import __version__

__all__ = [
    # authoring
    "procedure",
    "state",
    "stream_operative",
    "workflow_operative",
    "Channels",
    "Edge",
    "Ctx",
    "WorkflowCtx",
    "Procedure",
    "Operative",
    "DROPPED",
    # runner
    "create_runner",
    "Runner",
    "IdleObserver",
    "configure_logging",
    # codec
    "TypeCodec",
    "ValueCodec",
    # errors
    "FlezhaError",
    "CoreError",
    "SchemaMismatchError",
    "DeclarationError",
    "EmitError",
    "UnrepresentableValueError",
    "NonRetryableError",
    "ForkError",
    "DeterminismWarning",
    # versions
    "__version__",
    "ACTIVATION_SCHEMA_VERSION",
]
