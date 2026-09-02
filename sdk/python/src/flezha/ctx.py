"""The invocation context: every deterministic effect a handler is allowed.

The context accumulates one item's effects — recorded changes, outputs, timer
operations, the checkpoint flag — and the dispatcher turns them into the item
effect the core folds. Nothing here reaches the wire on its own; an emit is a
value in a list until the completion carries it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Iterable, Mapping, TypeVar

from . import codec as _codec
from .builder import DROPPED, ChannelSpec, ClassSpec, ConsumerEdge, ProcNode, apply_transforms
from .errors import EmitError
from .identity import entity_identity, stable_id

if TYPE_CHECKING:
    from .native import Core

__all__ = ["Ctx", "WorkflowCtx", "RESERVED_PREFIX"]

S = TypeVar("S")

#: Metadata keys the runtime owns. A handler that sets one is refused.
RESERVED_PREFIX = "flz."

#: The identity stamps a non-tier keyed plugin emit carries.
#:
#: Under the effectively-once tier the controller derives them at flush time
#: from the effect record, so none ride on the frame; off the tier the frame is
#: written straight to the wire and the stamps have nowhere else to come from.
#: Byte-shape and condition both mirror the Java host, because a sink
#: deduplicates on them and two SDKs disagreeing about the shape is a sink that
#: silently stops deduplicating.
ENTITY_STAMP = "flz.entity"
SEQ_STAMP = "flz.seq"
KEY_STAMP = "flz.key"


class Ctx(Generic[S]):
    """What a handler is handed alongside its input."""

    __slots__ = (
        "_spec",
        "_node",
        "_outgoing",
        "_operative",
        "_change_seq",
        "_now",
        "_bridge",
        "_identity",
        "_ordinal",
        "_emit_ordinal",
        "changes",
        "outputs",
        "timers",
        "entity_key",
        "replay",
        "state",
        "checkpointed",
    )

    def __init__(
        self,
        *,
        spec: ClassSpec,
        node: ProcNode,
        outgoing: list[ChannelSpec],
        operative: str,
        entity_key: str | None,
        change_seq: int,
        now: int,
        replay: bool,
        state: S,
        bridge: Core | Any,
    ) -> None:
        self._spec = spec
        self._node = node
        self._outgoing = outgoing
        self._operative = operative
        self._change_seq = change_seq
        self._now = now
        self._bridge = bridge
        self._identity: bytes | None = None
        self._ordinal = 0
        # Separate from the `stable_id` ordinal on purpose: one counts
        # identifiers a handler asked for, the other counts frames it produced,
        # and a handler that does one and not the other must not shift the
        # sequence the sink deduplicates on.
        self._emit_ordinal = 0
        self.entity_key = entity_key
        self.replay = replay
        #: The entity's state. Mutated only through :meth:`change`: a field
        #: assigned directly is live in this process and gone after a restart,
        #: which is what the state guard refuses.
        self.state = state
        self.changes: list[bytes] = []
        self.outputs: list[dict[str, Any]] = []
        self.timers: list[dict[str, Any]] = []
        self.checkpointed = False

    # -- deterministic reads ----------------------------------------------

    def now(self) -> int:
        """The core-evaluated declared clock for this item. Never ambient time."""
        return self._now

    def stable_id(self) -> str:
        """A replay-stable identifier, distinct per call within the invocation."""
        if self._identity is None:
            # Lazily, because most invocations never ask and the identity is
            # the one part of the derivation that costs an allocation.
            self._identity = entity_identity(self._operative, self._node.name, self.entity_key)
        answer = stable_id(self._identity, self._change_seq, self._ordinal)
        self._ordinal += 1
        return answer

    # -- effects ------------------------------------------------------------

    def change(self, change: Any) -> None:
        """Record one declared change: journaled, then folded into the state."""
        proc = self._node.proc
        if proc.apply_change is None:
            raise EmitError(
                f"{self._node.name}: recorded a change but declares no apply_change; a change "
                f"that cannot be folded cannot rebuild the state it describes"
            )
        self.changes.append(proc.encode_change(change))
        # Fold it live through the same path the restore fold uses, so the live
        # path and the replay path cannot drift.
        proc.fold_change(self.state, change)

    def emit(
        self,
        value: Any,
        *,
        key: str | None = None,
        channel: str | None = None,
        metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None = None,
    ) -> None:
        """Emit one output value on the procedure's declared output.

        ``channel`` selects one when the procedure produces on several.
        """
        targets = [c for c in self._outgoing if c.name == channel] if channel else self._outgoing
        if not targets:
            raise EmitError(
                f"{self._node.name}: emits on channel `{channel}`, which it does not produce on"
                if channel
                else f"{self._node.name}: emits but declares no output channel; wire its edge "
                f"with into(...) or sink(...)"
            )
        for target in targets:
            self._release(target, value, key, metadata)

    def timer_after(self, milliseconds: int) -> int:
        """Register a processing-time timer ``milliseconds`` from ``now``.

        Anchored on ``now`` and never on an ambient read: that is what makes a
        replayed invocation register the timestamp the live one did.
        """
        at = self._now + int(milliseconds)
        self.timers.append({"kind": "processing_time", "scheduled_at": at, "register": True})
        return at

    def timer_at_event_time(self, at: int) -> int:
        """Register an event-time timer at an absolute time."""
        at = int(at)
        self.timers.append({"kind": "event_time", "scheduled_at": at, "register": True})
        return at

    def cancel_timer(self, kind: str, at: int) -> None:
        """Cancel a registered timer by the value its registration answered."""
        self.timers.append({"kind": kind, "scheduled_at": int(at), "register": False})

    def checkpoint(self) -> None:
        """Close the open checkpoint interval (checkpoint ack-granularity classes)."""
        self.checkpointed = True

    def spawn_workflow(
        self,
        instance_id: str,
        factory_id: str,
        payload: Any,
        *,
        if_running: str = "deliver",
    ) -> str:
        """Spawn or deliver to a workflow instance."""
        # A spawn is an external side effect. A replayed invocation must not
        # issue it twice, and a timer must — a timer is internal state.
        if self.replay:
            return "skipped_on_replay"
        return self._bridge.spawn_workflow(
            instance_id, factory_id, _codec.encode(payload), if_running
        )

    # -- what the dispatcher collects ---------------------------------------

    def effect(self, consumed_offset: int | None) -> dict[str, Any]:
        """The effect record for this item, as the core folds it."""
        return {
            "verdict": "ok",
            "changes": self.changes,
            "outputs": self.outputs,
            "consumed_offset": consumed_offset,
            "event_time": self._now,
            "timers": self.timers,
            "checkpoint": self.checkpointed,
        }

    # -- internals -----------------------------------------------------------

    @property
    def _structured_workflow(self) -> bool:
        """A structured workflow releases ONE value per emit.

        The driver fans it out to every consumer itself, from the journaled
        handoff, so the host names the producing channel and its first consumer
        and applies neither the edge's transforms nor its key.
        """
        return self._spec.kind == "workflow" and bool(self._spec.channels)

    def _release(
        self,
        channel: ChannelSpec,
        value: Any,
        key: str | None,
        metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None,
    ) -> None:
        # A channel with no consumer releases nothing on the edge path: on a
        # workflow that is the plugin-bound terminal, and seeding the next
        # stage from it would hand the driver a terminal output.
        if self._structured_workflow:
            if channel.consumers:
                self.outputs.append(self._workflow_output(channel, channel.consumers[0], value))
        else:
            for edge in channel.consumers:
                output = self._edge_output(channel, edge, value, key, metadata)
                if output is not None:
                    self.outputs.append(output)
        if channel.sink is not None:
            self.outputs.append(self._plugin_output(channel, value, key, metadata))

    def _workflow_output(
        self, channel: ChannelSpec, edge: ConsumerEdge, value: Any
    ) -> dict[str, Any]:
        return {
            "target": "edge",
            "channel": channel.index,
            "consumer": edge.procedure,
            "plugin_channel": None,
            "key": None,
            "event_time": None,
            "metadata": [],
            "payload": channel.encode(value) if channel.encode else _codec.encode(value),
        }

    def _edge_output(
        self,
        channel: ChannelSpec,
        edge: ConsumerEdge,
        value: Any,
        key: str | None,
        metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None,
    ) -> dict[str, Any] | None:
        """One consumer's frame: its own transforms, its own key, its own clock."""
        # Producer-side, in declaration order: the core routes on `(edge, key)`
        # and never evaluates a host closure.
        transformed = apply_transforms(edge.transforms, value)
        if transformed is DROPPED:
            return None

        resolved = key
        if resolved is None and edge.key_by is not None:
            resolved = edge.key_by(transformed)
        if resolved is None:
            resolved = self.entity_key

        consumer_proc = self._spec.procedures[edge.procedure].proc
        return {
            "target": "edge",
            "channel": channel.index,
            "consumer": edge.procedure,
            "plugin_channel": None,
            "key": None if resolved is None else str(resolved),
            "event_time": (
                consumer_proc.event_time_of(transformed)
                if consumer_proc.event_time is not None
                else None
            ),
            "metadata": self._meta_list(metadata),
            "payload": channel.encode(transformed)
            if channel.encode
            else _codec.encode(transformed),
        }

    def _plugin_output(
        self,
        channel: ChannelSpec,
        value: Any,
        key: str | None,
        metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None,
    ) -> dict[str, Any]:
        """The plugin frame, serialized once from the value as emitted.

        An edge's transforms belong to that edge, so they never reach the sink.
        """
        resolved = key if key is not None else self.entity_key
        if channel.keyed and resolved is None:
            raise EmitError(
                f"{self._node.name}: emits on keyed channel `{channel.name}` with no key, and "
                f"has no entity key of its own to fall back on; pass `key=` on the emit"
            )
        resolved = None if resolved is None else str(resolved)
        stamps = self._meta_list(metadata) + self._identity_stamps(channel, resolved)
        self._emit_ordinal += 1
        return {
            "target": "plugin",
            "channel": None,
            "consumer": None,
            "plugin_channel": channel.name,
            "key": resolved,
            "event_time": None,
            "metadata": stamps,
            "payload": channel.encode(value) if channel.encode else _codec.encode(value),
        }

    def _identity_stamps(self, channel: ChannelSpec, key: str | None) -> list[tuple[str, str]]:
        """The output identity a keyed plugin frame carries, off the tier.

        Applied after user metadata, which is what makes them unspoofable: the
        reserved namespace is refused on the way in, and these are written on
        the way out.
        """
        if not channel.keyed or self._spec.effectively_once:
            return []
        return [
            (ENTITY_STAMP, f"{self._operative}/{self._node.name}/{self.entity_key or ''}"),
            (SEQ_STAMP, f"{self._change_seq}.{self._emit_ordinal}"),
            (KEY_STAMP, key or ""),
        ]

    def _meta_list(
        self, metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None
    ) -> list[tuple[str, str]]:
        if not metadata:
            return []
        pairs = metadata.items() if isinstance(metadata, Mapping) else metadata
        out: list[tuple[str, str]] = []
        for key, value in pairs:
            if str(key).startswith(RESERVED_PREFIX):
                raise EmitError(
                    f"{self._node.name}: metadata key `{key}` is inside the reserved "
                    f"`{RESERVED_PREFIX}` namespace, which the runtime stamps itself (entity "
                    f"identity, sequence, key and offset); choose a key outside it"
                )
            out.append((str(key), str(value)))
        return out


class WorkflowCtx:
    """The context a run-sugar workflow body receives."""

    #: How long a workflow body's ``recv`` parks when the caller names no timeout.
    DEFAULT_RECV_TIMEOUT_MS = 1_000

    __slots__ = ("instance_id", "result", "_core", "_completed")

    def __init__(self, instance_id: str, core: Core | Any) -> None:
        self.instance_id = instance_id
        self.result: Any = None
        self._core = core
        self._completed = False

    def emit(self, value: Any) -> None:
        """Emit one output payload. Best-effort and at-least-once."""
        self._core.workflow_emit(self.instance_id, _codec.encode(value))

    def recv(self, timeout_ms: int | None = None) -> Any:
        """The next inbox item, or ``None`` on timeout."""
        payload = self._core.workflow_recv(
            self.instance_id, self.DEFAULT_RECV_TIMEOUT_MS if timeout_ms is None else timeout_ms
        )
        return _codec.decode(payload) if payload is not None else None

    def cancelled(self) -> bool:
        """Whether the body's cancellation token has tripped."""
        return self._core.workflow_cancelled(self.instance_id)

    def complete(self, result: Any = None) -> None:
        """Set the terminal result; the body is completed once this is called."""
        self._completed = True
        self.result = result

    @property
    def completed(self) -> bool:
        return self._completed

    def now(self) -> int:
        return self._core.monotonic_nanos()
