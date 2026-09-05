"""The record the core is handed, and the verdict it answers.

Every rule a declared topology can be refused under is one function in
``flz_core::host::register::check``, and :func:`validate` is the call that
submits to it: this SDK records what the author declared, hands it over, and
renders the answer as its own exception. Nothing here restates a rule the core
holds — a refusal added there reaches Python with no edit to this file.

What a type checker *can* see — a wrong value type on a wired edge — is caught
earlier still, by the generic :class:`~flezha.builder.Edge`, and what one
procedure's own declaration can be refused for is caught earlier still by
:class:`~flezha.procedure.Procedure` when the decorator builds it.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .builder import ChannelSpec, ClassSpec, ProcNode
from .errors import CoreError, DeclarationError
from .version import __version__

if TYPE_CHECKING:
    from .native import Core

__all__ = ["validate", "to_host_class_decl", "SDK_LANG_PYTHON", "FORMAT_ID_MSGPACK"]

# The Python SDK's byte in the core's `sdk_lang` allocation table.
SDK_LANG_PYTHON = 7

# MessagePack, shared with every SDK whose codec is MessagePack.
FORMAT_ID_MSGPACK = 5


def validate(spec: ClassSpec, core: Core) -> dict[str, Any]:
    """Submit the record and answer what the core derived from it.

    On a refusal this raises the verdict — every name the core returned, in one
    :class:`~flezha.errors.DeclarationError`. On success the derived channel
    names are written back, so the name this host addresses a channel by is the
    one the core registered.
    """
    _check_workflow(spec)
    try:
        answer = core.check_class(json.dumps(to_host_class_decl(spec)).encode())
    except CoreError as refusal:
        raise _refusal_of(spec, refusal) from refusal
    derived: dict[str, Any] = json.loads(answer)
    for channel in derived["channels"]:
        declared = spec.channels[channel["index"]]
        declared.name = channel["name"]
        # The runtime edges ARE the core's answer: it says which chains
        # produced a consumer and in what order, and the host attaches the
        # closures that chain declared.
        declared.consumers = [declared.chain_edges[edge["chain"]] for edge in channel["consumers"]]
    return derived


def _refusal_of(spec: ClassSpec, refusal: CoreError) -> DeclarationError:
    """The core's verdict as this SDK's own exception.

    The names are read off the verdict the core answered with — data, not the
    rendered prose — and kept apart on ``refusals``.
    """
    issues = json.loads(refusal.verdict)["issues"] if refusal.verdict else []
    return DeclarationError(
        spec.factory_id, None, str(refusal), [issue["name"] for issue in issues]
    )


def _check_workflow(spec: ClassSpec) -> None:
    """The one defect the record cannot carry: a workflow that declares nothing.

    A run body is a Python callable the declaration never names, so a class with
    neither it nor a graph reaches the core looking exactly like the run-sugar
    shape it accepts.
    """
    if spec.kind == "workflow" and not spec.run and not spec.channels:
        raise DeclarationError(
            spec.factory_id, None, "declares neither a run body nor a channels graph"
        )


# ---------------------------------------------------------------------------
# The declaration the core deserializes
# ---------------------------------------------------------------------------


def _compact(fields: dict[str, Any]) -> dict[str, Any]:
    """Drop the keys whose value is None, so the JSON omits them."""
    return {key: value for key, value in fields.items() if value is not None}


def _channel_json(channel: ChannelSpec) -> dict[str, Any]:
    return _compact(
        {
            "name": channel.name,
            "kind": channel.kind,
            "producer": channel.producer,
            "chains": channel.chains,
            "retry_max": channel.retry_max,
            "retry_backoff_ms": channel.retry_backoff_ms,
            "overflow": channel.overflow,
            "keyed": channel.keyed,
        }
    )


def _procedure_json(spec: ClassSpec, node: ProcNode) -> dict[str, Any]:
    proc = node.proc
    return _compact(
        {
            "name": node.name,
            "entity_mode": proc.entity_mode,
            "entity_keys": proc.entity_keys or [],
            "idle_ttl_ms": proc.idle_ttl_ms,
            "max_entities": proc.max_entities,
            "entities_for_overridden": proc.entities_for is not None,
            "compensate_overridden": proc.compensate is not None,
            "clock": proc.clock,
            # `is None`, not a falsy test: a declared 0 is what the core
            # refuses as `MailboxCapacityZero`, and rewriting it to the default
            # would hide the very declaration the author made.
            "mailbox_capacity": 1024 if proc.mailbox_capacity is None else proc.mailbox_capacity,
            # A procedure that folds no change has the empty Change type, so a
            # logical clock over it would never advance; a class that journals
            # nothing has it for every procedure.
            "change_is_nochange": not spec.persistent or proc.apply_change is None,
            "schema_version": proc.schema_version or 1,
            "min_readable_schema_version": proc.min_readable_schema_version,
        }
    )


def _stream_tier_json(spec: ClassSpec) -> dict[str, Any]:
    """The tier, checkpoint and correction keys, which only a stream class sends.

    The core refuses ``effectively_once`` and ``correction_window_ms`` on a
    workflow class outright, and the checkpoint family rests on a checkpoint
    acknowledgement that refusal leaves unreachable there.

    The spec keeps the flag either way: :meth:`Ctx._identity_stamps` reads it to
    decide whether a keyed plugin frame carries its own identity, so gating the
    declaration must not gate the in-process tier.
    """
    if spec.kind != "stream":
        return {}
    return {
        "effectively_once": spec.effectively_once,
        "checkpoint_ack": spec.checkpoint_ack,
        "checkpoint_every": spec.checkpoint_every,
        "checkpoint_outputs": spec.checkpoint_outputs,
        "checkpoint_max_delay_ms": spec.checkpoint_max_delay_ms,
        "correction_window_ms": spec.correction_window_ms,
        "beyond_window_policy": spec.beyond_window_policy,
    }


def to_host_class_decl(spec: ClassSpec) -> dict[str, Any]:
    """The declaration the core deserializes.

    Every key here is pinned by ``test/fixtures/host_class_decl_keys.txt``,
    which this package's own test reads: a key the core learns that Python
    cannot spell fails that test.
    """
    return _compact(
        {
            "factory_id": spec.factory_id,
            "persistent": spec.persistent,
            "sdk_lang": SDK_LANG_PYTHON,
            "sdk_version": __version__,
            "change_format_id": FORMAT_ID_MSGPACK,
            **_stream_tier_json(spec),
            "compact_after_bytes": spec.compact_after_bytes,
            "sync_durability": spec.sync_durability,
            "channels": [_channel_json(channel) for channel in spec.channels],
            "procedures": [_procedure_json(spec, node) for node in spec.procedures],
            "workflow": _compact(spec.workflow) if spec.workflow else None,
        }
    )
