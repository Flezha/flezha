"""Everything this SDK raises, so a host can catch one thing."""

from __future__ import annotations

__all__ = [
    "FlezhaError",
    "CoreError",
    "SchemaMismatchError",
    "DeclarationError",
    "EmitError",
    "UnrepresentableValueError",
    "NonRetryableError",
    "ForkError",
    "DeterminismWarning",
]


class FlezhaError(Exception):
    """Base of every error this SDK raises."""


class CoreError(FlezhaError):
    """A failure the native core reported, with its own detail preserved.

    The veneer raises the extension's own exception type; :class:`~flezha.native.Core`
    translates it here so a host catches one thing, and chains the original so
    nothing about the engine's answer is lost.
    """

    def __init__(self, message: str, verdict: bytes | None = None) -> None:
        super().__init__(message)
        #: The refusal as the core's own JSON, when the entry point answered one
        #: beside the rendered message — the topology check does. ``None``
        #: everywhere else.
        self.verdict = verdict


class SchemaMismatchError(CoreError):
    """The native library and this package speak different activation schemas.

    Raised at context creation, naming both versions, before any controller
    connection is attempted.
    """


class DeclarationError(FlezhaError):
    """A registration refusal: the class, the handler and the defect.

    Registration-time refusal is what Python has in place of the Rust SDK's
    compile-time one for the defects a type checker cannot see — a cycle, a
    missing entry channel, an unservable clock — so the message carries what a
    compiler would have pointed at.
    """

    def __init__(
        self,
        factory_id: str,
        handler: str | None,
        defect: str,
        refusals: list[str] | None = None,
    ) -> None:
        self.factory_id = factory_id
        self.handler = handler
        self.defect = defect
        #: The core's own names for what it refused, when the verdict came from
        #: it. Empty for a refusal this SDK made on its own surface.
        self.refusals = refusals or []
        super().__init__(
            f"{factory_id}: {handler}: {defect}" if handler else f"{factory_id}: {defect}"
        )


class EmitError(FlezhaError):
    """An emit the declaration cannot carry.

    A channel the procedure does not produce on, a reserved metadata key, a
    keyed channel with no key.
    """


class UnrepresentableValueError(FlezhaError):
    """A value the durable codec cannot represent, named by type and field."""

    def __init__(self, type_name: str, path: str, reason: str) -> None:
        self.type_name = type_name
        self.path = path
        super().__init__(f"{type_name}.{path} cannot be encoded for durable state: {reason}")


class NonRetryableError(FlezhaError):
    """A handler error the retry ladder must not retry.

    Raise this — or set ``non_retryable = True`` on any exception — to park an
    item immediately instead of consuming its remaining budget.
    """

    non_retryable = True


class ForkError(FlezhaError):
    """A core call from a process that did not start the core."""


class DeterminismWarning(UserWarning):
    """An ambient clock or identity read inside an invocation.

    A warning and not an error: the call is legal Python and the invocation
    completes, but its value differs between the live run and the replay, which
    is a defect nothing else reports.
    """
