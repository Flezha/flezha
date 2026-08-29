"""Registration-time refusals, and the ``HostClassDecl`` the core is handed.

Python has no compile step, so everything the Rust builder refuses at compile
time is refused here instead, naming the class, the handler and the specific
defect — the same set, for the same reasons, as the Rust builder's
``TopologyIssue``, so an author moving between SDKs meets one vocabulary. What a
type checker *can* see — a wrong value type on a wired edge — is caught earlier
still, by the generic :class:`~flezha.builder.Edge`.

The core validates again on its side. That is not redundancy: a hand-written
declaration reaching the ABI directly must not slip past, and an author who
declared through this surface deserves the error before any data moves.
"""

from __future__ import annotations

from typing import Any

from .builder import ChannelSpec, ClassSpec, ProcNode
from .errors import DeclarationError

__all__ = ["validate", "to_host_class_decl", "SDK_LANG_PYTHON", "FORMAT_ID_MSGPACK"]

# The Python SDK's byte in the core's `sdk_lang` allocation table.
SDK_LANG_PYTHON = 7

# MessagePack, shared with every SDK whose codec is MessagePack.
FORMAT_ID_MSGPACK = 5


def _refuse(spec: ClassSpec, handler: str | None, defect: str) -> None:
    raise DeclarationError(spec.factory_id, handler, defect)


def validate(spec: ClassSpec) -> None:
    """Refuse every graph defect a type system would have caught.

    Only defects that need the *graph* live here — a channel's shape, the
    keying agreement between an edge and its consumer, the topology, and the
    class-level tier and window declarations. A defect visible in one
    procedure's own declaration is refused by :class:`~flezha.procedure.Procedure`
    when the decorator builds it, which is earlier and puts the traceback on the
    handler rather than on the operative.
    """
    _check_workflow(spec)
    _check_tier(spec)
    _check_window(spec)
    _check_channels(spec)
    _check_keying(spec)
    _check_topology(spec)


def _check_channels(spec: ClassSpec) -> None:
    names: set[str] = set()
    sinks: set[str] = set()

    for channel in spec.channels:
        _check_channel_names(spec, channel, names, sinks)
        _check_channel_wiring(spec, channel)
        _check_overflow(spec, channel)

        if channel.kind == "entry" and not channel.consumers:
            _refuse(spec, channel.name, "is an entry channel that names no consumer")
        if channel.kind == "inter_proc" and not channel.consumers:
            _refuse(spec, channel.name, "is an inter-procedure channel that names no consumer")


def _check_channel_names(
    spec: ClassSpec, channel: ChannelSpec, names: set[str], sinks: set[str]
) -> None:
    if channel.name in names:
        _refuse(spec, channel.name, "this channel name is declared more than once")
    names.add(channel.name)

    if channel.kind != "plugin":
        return
    if channel.name in sinks:
        _refuse(spec, channel.name, "declares a plugin sink more than once")
    sinks.add(channel.name)


def _check_channel_wiring(spec: ClassSpec, channel: ChannelSpec) -> None:
    if channel.kind != "plugin" and channel.keyed:
        _refuse(
            spec,
            channel.name,
            "is declared keyed but has no plugin wire; output identity stamps only exist "
            "on plugin-bound frames, so add a sink(...) or drop the keyed",
        )

    if spec.effectively_once and channel.kind == "plugin" and not channel.keyed:
        _refuse(
            spec,
            channel.name,
            "has a plugin wire but is not declared keyed; the effectively-once tier "
            "deduplicates by output identity, so every plugin-bound channel of a class "
            "that declares effectively_once must opt in with keyed",
        )


def _check_overflow(spec: ClassSpec, channel: ChannelSpec) -> None:
    if channel.overflow != "drop_nothing":
        if spec.effectively_once:
            _refuse(
                spec,
                channel.name,
                f"declares on_full({channel.overflow}) on a class that declares "
                f"effectively_once; a shed input's offset is still advanced past by the "
                f"tier's watermark, so the loss is permanent and reported nowhere — "
                f"declare one or the other, not both",
            )
        if spec.correction_window_ms:
            _refuse(
                spec,
                channel.name,
                f"declares on_full({channel.overflow}) on a class that declares a "
                f"correction window; a correction re-folds a replayed slice and its answer "
                f"may not depend on which items a full mailbox happened to keep",
            )

    if channel.retry_max == 0:
        _refuse(
            spec,
            channel.name,
            "declares retry_max(0); at least one attempt is required — raise a "
            "non-retryable error to park without retries",
        )


def _check_keying(spec: ClassSpec) -> None:
    dynamic = {node.index for node in spec.procedures if node.proc.entity_mode == "dynamic"}

    for channel in spec.channels:
        for consumer in channel.consumers:
            node = spec.procedures[consumer]
            if channel.key_by and node.proc.entity_mode == "static":
                _refuse(
                    spec,
                    channel.name,
                    f"declares key_by into procedure `{node.name}`, which declares a static "
                    f"key set; static keying routes by entities_for and may name several "
                    f"entities, edge keying names exactly one — declare one or the other, "
                    f"not both",
                )
            if not channel.key_by and consumer in dynamic:
                _refuse(
                    spec,
                    channel.name,
                    f"feeds dynamic procedure `{node.name}` without a key_by; every edge into "
                    f"a dynamic procedure must name the entity its inputs reach",
                )


def _check_tier(spec: ClassSpec) -> None:
    if spec.effectively_once and not spec.persistent:
        _refuse(
            spec, None, "declares effectively_once without persistence: the tier IS durable state"
        )
    if spec.ack_granularity == "checkpoint" and not spec.effectively_once:
        _refuse(
            spec,
            None,
            "declares checkpoint acknowledgement without effectively_once; the checkpoint is "
            "where the tier's consumed offset advances, and a class without the tier has no "
            "durable offset to hold back",
        )
    if spec.checkpoint_every == 0:
        _refuse(
            spec,
            None,
            "declares checkpoint_every(0); an interval that closes every zero inputs is not an "
            "interval — omit it to close only on an explicit ctx.checkpoint",
        )
    if spec.checkpoint_outputs == "at_checkpoint" and spec.checkpoint_max_delay_ms is None:
        _refuse(
            spec,
            None,
            "declares outputs_at_checkpoint with no maximum delay; an interval that never "
            "closes would hold its outputs forever on a quiet entity",
        )


def _check_window(spec: ClassSpec) -> None:
    if spec.correction_window_ms is None:
        return
    if spec.correction_window_ms == 0:
        _refuse(
            spec, None, "declares a correction window of zero; a zero window can correct nothing"
        )
    if not spec.effectively_once:
        _refuse(
            spec,
            None,
            "declares a correction window without effectively_once; correction replays the "
            "input log against a rewound baseline, and only the tier journals inputs before "
            "forwarding them",
        )
    if not any(node.proc.clock == "event_time" for node in spec.procedures):
        _refuse(
            spec,
            None,
            "declares a correction window but no procedure declares an event_time clock; "
            "correction orders a replayed slice by event time",
        )


def _check_topology(spec: ClassSpec) -> None:
    """Every procedure reachable from an entry channel, and no cycle among them."""
    entries = [channel for channel in spec.channels if channel.kind == "entry"]
    if spec.kind == "stream" and not entries:
        _refuse(spec, None, "declares no entry channel; a stream class is fed by the controller")
    if not spec.procedures:
        if spec.kind == "stream":
            _refuse(spec, None, "declares no procedure")
        return

    successors: dict[int, list[int]] = {node.index: [] for node in spec.procedures}
    for channel in spec.channels:
        if channel.kind == "inter_proc" and channel.producer is not None:
            successors[channel.producer].extend(channel.consumers)

    reachable: set[int] = set()
    frontier = [consumer for channel in entries for consumer in channel.consumers]
    while frontier:
        node_index = frontier.pop()
        if node_index in reachable:
            continue
        reachable.add(node_index)
        frontier.extend(successors[node_index])

    # A cycle is found before unreachability is reported: a node on a cycle with
    # no path from an entry is both, and the cycle is the more useful answer.
    colour: dict[int, str] = {}

    def visit(index: int) -> None:
        seen = colour.get(index)
        if seen == "black":
            return
        if seen == "grey":
            _refuse(
                spec,
                spec.procedures[index].name,
                "is on a cycle; an operative topology is a DAG — a node's output may never "
                "reach its own input",
            )
        colour[index] = "grey"
        for next_index in successors[index]:
            visit(next_index)
        colour[index] = "black"

    for node in spec.procedures:
        visit(node.index)

    for node in spec.procedures:
        if node.index not in reachable:
            _refuse(spec, node.name, "is not reachable from any entry channel")


def _check_workflow(spec: ClassSpec) -> None:
    if spec.kind != "workflow":
        if spec.sync_durability:
            _refuse(
                spec,
                None,
                "declares sync_durability on a stream class; a stream class acknowledges at "
                "input rates, and a remote round trip per acknowledgement is not a guarantee "
                "it can keep",
            )
        return

    if spec.effectively_once:
        _refuse(spec, None, "is a workflow class and cannot declare effectively_once")
    if spec.correction_window_ms:
        _refuse(spec, None, "is a workflow class and cannot declare a correction window")
    if spec.run and spec.channels:
        _refuse(
            spec,
            None,
            "declares a run body and channels; a run-sugar workflow is one imperative body",
        )
    if not spec.run and not spec.channels:
        _refuse(spec, None, "declares neither a run body nor a channels graph")
    if spec.sync_durability and not spec.persistent:
        _refuse(spec, None, "declares sync_durability without persistence")


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
            "consumers": channel.consumers,
            "retry_max": channel.retry_max,
            "retry_backoff_ms": channel.retry_backoff_ms,
            "overflow": channel.overflow,
            "keyed": channel.keyed,
        }
    )


def _procedure_json(node: ProcNode) -> dict[str, Any]:
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
            "mailbox_capacity": proc.mailbox_capacity or 1024,
            "schema_version": proc.schema_version or 1,
            "min_readable_schema_version": proc.min_readable_schema_version,
        }
    )


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
            "change_format_id": FORMAT_ID_MSGPACK,
            "effectively_once": spec.effectively_once,
            "ack_granularity": spec.ack_granularity,
            "checkpoint_every": spec.checkpoint_every,
            "checkpoint_outputs": spec.checkpoint_outputs,
            "checkpoint_max_delay_ms": spec.checkpoint_max_delay_ms,
            "correction_window_ms": spec.correction_window_ms,
            "beyond_window_policy": spec.beyond_window_policy,
            "compact_after_bytes": spec.compact_after_bytes,
            "sync_durability": spec.sync_durability,
            "channels": [_channel_json(channel) for channel in spec.channels],
            "procedures": [_procedure_json(node) for node in spec.procedures],
            "workflow": _compact(spec.workflow) if spec.workflow else None,
        }
    )
