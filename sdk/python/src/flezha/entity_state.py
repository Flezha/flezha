"""Declared state: how it is guarded, how it is built and how it is restored.

Declared state is mutated only through declared changes, because the change log
IS how the state is rebuilt: a field assigned directly is live in this process
and gone after a restart, and nothing would report the difference until the
restart.

Rust forecloses that at compile time by withholding ``DerefMut``. Python has no
such lever, so the class ``@state`` decorates rejects the assignment at runtime,
naming the field and the change-based path. The runtime keeps its own ability to
apply changes and restore — that is what :func:`runtime_mutation` opens, for the
duration of one call, on the calling thread only.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TypeVar

from .errors import FlezhaError

if TYPE_CHECKING:
    from .procedure import Procedure

__all__ = ["state", "runtime_mutation", "is_state", "fold_restore", "decode_body"]

T = TypeVar("T")

# Per thread, because workers dispatch concurrently and the permission belongs
# to one invocation and not to the process. A counter rather than a flag so a
# restore that folds a change through `apply_change` nests correctly.
_permitted = threading.local()


def _mutation_permitted() -> bool:
    return getattr(_permitted, "depth", 0) > 0


@contextmanager
def runtime_mutation() -> Iterator[None]:
    """Permit direct assignment on declared state, on this thread, in here.

    The runtime's own path: applying a change and folding a restore. Nothing in
    user code opens it, which is what makes the guard mean anything.
    """
    _permitted.depth = getattr(_permitted, "depth", 0) + 1
    try:
        yield
    finally:
        _permitted.depth -= 1


def is_state(cls: type) -> bool:
    """Whether ``cls`` was declared with :func:`state`."""
    return getattr(cls, "__flz_state__", False)


def state(cls: type[T]) -> type[T]:
    """Declare a class as durable Entity state.

    Works over a dataclass, a Pydantic model, an attrs class or a plain class
    with ``to_dict`` / ``from_dict``: the guard replaces ``__setattr__`` and
    delegates to whatever the class had, so validation and ``__pydantic_fields_set__``
    behave exactly as they did.

    Construction is not mutation — a state object has to be built before it can
    be guarded — so the guard arms itself at the end of ``__init__``.
    """
    if is_state(cls):
        return cls

    original_init = cls.__init__
    original_setattr = cls.__setattr__

    def __init__(self: Any, *args: Any, **kwargs: Any) -> None:
        object.__setattr__(self, "__flz_sealed__", False)
        original_init(self, *args, **kwargs)
        object.__setattr__(self, "__flz_sealed__", True)

    def __setattr__(self: Any, name: str, value: Any) -> None:
        if getattr(self, "__flz_sealed__", False) and not _mutation_permitted():
            raise FlezhaError(
                f"{cls.__name__}.{name} was assigned directly. Declared state is rebuilt from "
                f"its change log, so an assignment outside that log is live in this process and "
                f"gone after a restart. Express it as a change: `ctx.change(...)`, folded by the "
                f"procedure's apply_change."
            )
        original_setattr(self, name, value)

    cls.__init__ = __init__  # type: ignore[method-assign]
    cls.__setattr__ = __setattr__  # type: ignore[method-assign]
    cls.__flz_state__ = True  # type: ignore[attr-defined]
    return cls


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------


def decode_body(
    definition: Procedure[Any, Any],
    which: str,
    body: dict[str, Any],
    procedure: str,
) -> Any:
    """Decode one durable body, choosing between the three things it can be.

    A payload from an older schema version, one the procedure declared its own
    codec for, or the MessagePack default. One function rather than the same
    three-way choice written at each of the places a body is decoded — the two
    halves of a restore fold, on the dispatch path and in the test harness.
    """
    from_version = body.get("from_version")
    payload = body.get("bytes")
    if from_version is not None:
        migration: Callable[[int, Any], Any] | None = (
            definition.migrate_state if which == "state" else definition.migrate_change
        )
        if migration is None:
            raise FlezhaError(
                f"{procedure} was handed a {which} record written at schema version "
                f"{from_version} and declares no migrate_{which}; declare one, or raise "
                f"min_readable_schema_version so the record is refused rather than misread"
            )
        return migration(from_version, payload)

    return (
        definition.decode_state(payload) if which == "state" else definition.decode_change(payload)
    )


def fold_restore(
    definition: Procedure[Any, Any],
    payload: dict[str, Any],
    entity_key: str | None,
    procedure: str,
) -> Any:
    """Rebuild an entity's state from its snapshot and change log.

    This IS the restore, and both callers are the real thing — the dispatcher
    answering a restore fold, and the test harness constructing from a payload —
    which is why the harness proves what a warm restart does rather than
    something that agrees with it.
    """
    state_value = definition.build_initial_state(entity_key)
    snapshot = payload.get("snapshot")
    if snapshot is not None:
        state_value = decode_body(definition, "state", snapshot, procedure)

    for body in payload.get("changes") or ():
        if definition.apply_change is None:
            raise FlezhaError(
                f"{procedure} was handed a change log but declares no apply_change; a "
                f"procedure that records no changes should have no durable records to fold"
            )
        with runtime_mutation():
            definition.apply_change(state_value, decode_body(definition, "change", body, procedure))
    return state_value
