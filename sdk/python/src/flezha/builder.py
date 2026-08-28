"""The channel-graph DSL, and the specs it produces.

The graph is built by calling an operative's declaration function with a
:class:`Channels`; every fluent call answers a new :class:`Edge`, so a shared
prefix can be branched without the arms colliding.

Edges are generic in the value that flows on them, which is what makes a wrong
wiring a type error before it is a registration error: ``Edge[Invoice].into``
takes a ``Procedure[Invoice, O]`` and nothing else, so a type checker rejects
the mismatch in the editor and the registration-time refusal is the backstop
for what a type checker cannot see.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Iterable, TypeVar

from .errors import DeclarationError
from .procedure import Procedure

__all__ = [
    "DROPPED",
    "Channels",
    "Edge",
    "ChannelSpec",
    "ClassSpec",
    "ProcNode",
    "apply_transforms",
]

T = TypeVar("T")
U = TypeVar("U")
O = TypeVar("O")


class _Dropped:
    """What a ``filter`` yields for a value it rejects.

    A dropped value is not ``None`` or ``False`` — both are values a handler may
    legitimately carry — so the marker is an object nothing else can produce. It
    stops the transform chain: the item is consumed without reaching a handler,
    and at the prepare crossing it answers no key, which is how the core learns
    not to route it.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "flezha.DROPPED"

    def __bool__(self) -> bool:
        return False


DROPPED = _Dropped()


def apply_transforms(transforms: Iterable[Callable[[Any], Any]], value: Any) -> Any:
    """Run an edge's transform chain, stopping at the first drop.

    One implementation for the three places a chain runs — an entry payload
    before its handler, a payload at the prepare crossing, and an emit on its
    way out — so a filter cannot mean one thing on one path and another
    elsewhere.
    """
    for transform in transforms:
        value = transform(value)
        if value is DROPPED:
            return DROPPED
    return value


@dataclass(slots=True)
class ProcNode:
    """A procedure as the graph holds it, with its position in the declaration."""

    index: int
    name: str
    proc: Procedure[Any, Any]


@dataclass(slots=True)
class ChannelSpec:
    """A channel as the graph holds it. Its position in the list IS its id."""

    index: int
    name: str
    kind: str
    producer: int | None = None
    consumers: list[int] = field(default_factory=list)
    overflow: str = "drop_nothing"
    retry_max: int | None = None
    retry_backoff_ms: int | None = None
    keyed: bool = False
    transforms: list[Callable[[Any], Any]] = field(default_factory=list)
    key_by: Callable[[Any], Any] | None = None
    decode: Callable[[Any], Any] | None = None
    encode: Callable[[Any], bytes] | None = None


@dataclass(slots=True)
class ClassSpec:
    """Everything the graph knows about one declared class."""

    factory_id: str
    kind: str
    persistent: bool
    procedures: list[ProcNode]
    channels: list[ChannelSpec]
    effectively_once: bool = False
    ack_granularity: str = "per_input"
    checkpoint_every: int | None = None
    checkpoint_outputs: str = "as_produced"
    checkpoint_max_delay_ms: int | None = None
    correction_window_ms: int | None = None
    beyond_window_policy: str = "process_as_arrival"
    compact_after_bytes: int | None = None
    sync_durability: bool = False
    workflow: dict[str, Any] | None = None
    run: Callable[..., Any] | None = None


class Channels:
    """The fluent builder an operative's declaration function receives."""

    def __init__(self, factory_id: str) -> None:
        self.factory_id = factory_id
        self.procedures: list[ProcNode] = []
        self.channels: list[ChannelSpec] = []
        self.effectively_once_flag = False
        self.ack_granularity = "per_input"
        self.checkpoint_every_value: int | None = None
        self.checkpoint_outputs_value = "as_produced"
        self.checkpoint_max_delay_ms_value: int | None = None
        self.correction_window_ms_value: int | None = None
        self.beyond_window_policy_value = "process_as_arrival"
        self.compact_after_bytes_value: int | None = None
        self.sync_durability_flag = False

    # -- class-level declarations ----------------------------------------

    def effectively_once(self) -> Channels:
        self.effectively_once_flag = True
        return self

    def checkpoint_ack(self) -> Channels:
        """Acknowledge at the checkpoint rather than per input.

        The interval closes only on an explicit ``ctx.checkpoint()``;
        :meth:`checkpoint_every` is the same acknowledgement on a declared
        cadence. Both imply the tier, which is where the durable offset the
        checkpoint holds back actually lives.
        """
        self.effectively_once_flag = True
        self.ack_granularity = "checkpoint"
        return self

    def checkpoint_every(self, count: int) -> Channels:
        self.checkpoint_ack()
        self.checkpoint_every_value = count
        return self

    def outputs_at_checkpoint(self, max_delay_ms: int | None = None) -> Channels:
        self.checkpoint_outputs_value = "at_checkpoint"
        if max_delay_ms is not None:
            self.checkpoint_max_delay_ms_value = max_delay_ms
        return self

    def correction_window(self, milliseconds: int) -> Channels:
        self.correction_window_ms_value = milliseconds
        return self

    def beyond_window_policy(self, policy: str) -> Channels:
        self.beyond_window_policy_value = policy
        return self

    def compact_after_bytes(self, size: int) -> Channels:
        self.compact_after_bytes_value = size
        return self

    def sync_durability(self) -> Channels:
        self.sync_durability_flag = True
        return self

    # -- the graph --------------------------------------------------------

    def input(self, name: str, *, decode: Callable[[Any], Any] | None = None) -> Edge[Any]:
        """Declare a controller-fed entry channel.

        ``decode`` turns the wire bytes into the value a handler sees; its
        absence means the MessagePack default.
        """
        channel = ChannelSpec(index=len(self.channels), name=name, kind="entry", decode=decode)
        self.channels.append(channel)
        return Edge(self, ("entry", channel), {})

    def node_for(self, proc: Procedure[Any, Any], name: str | None = None) -> ProcNode:
        """The node for a procedure.

        Two unnamed mentions of one procedure are one node — the naming
        convention every SDK mirrors — so a diamond that fans out and back in
        has one instance, not two.
        """
        identity = name or proc.name
        for node in self.procedures:
            if node.name == identity and node.proc is proc:
                return node
        if any(node.name == identity for node in self.procedures):
            raise DeclarationError(
                self.factory_id,
                identity,
                "two different procedures claim this node identity; name one of them on the "
                "`into(...)` so each node has its own",
            )
        node = ProcNode(index=len(self.procedures), name=identity, proc=proc)
        self.procedures.append(node)
        return node

    def add_channel(self, **spec: Any) -> ChannelSpec:
        channel = ChannelSpec(index=len(self.channels), **spec)
        self.channels.append(channel)
        return channel


class Edge(Generic[T]):
    """One edge under construction, carrying the type that flows on it.

    Immutable: every fluent call answers a new edge, so a shared prefix can be
    branched without the arms colliding.
    """

    __slots__ = ("_builder", "_from", "_pending")

    def __init__(
        self,
        builder: Channels,
        source: tuple[str, Any],
        pending: dict[str, Any],
    ) -> None:
        self._builder = builder
        self._from = source
        self._pending = pending

    # -- edge options -----------------------------------------------------

    def named(self, name: str) -> Edge[T]:
        """Override the channel name this edge creates.

        The default is derived from the producing node's identity.
        """
        return self._with(channel_name=name)

    def map(self, transform: Callable[[T], U]) -> Edge[U]:
        """Transform every value on this edge. Evaluated producer-side."""
        return self._with(transforms=[*self._transforms, transform])

    def filter(self, predicate: Callable[[T], bool]) -> Edge[T]:
        """Drop the values a predicate rejects, before this edge's key is computed.

        A dropped value is consumed and reaches no handler.
        """

        def keep(value: Any) -> Any:
            return value if predicate(value) else DROPPED

        return self._with(transforms=[*self._transforms, keep])

    def key_by(self, key: Callable[[T], Any]) -> Edge[T]:
        """Route by a computed key: the consumer becomes a dynamic keyed procedure."""
        return self._with(key_by=key)

    def on_full(self, policy: str) -> Edge[T]:
        return self._with(overflow=policy)

    def retry_max(self, attempts: int) -> Edge[T]:
        return self._with(retry_max=attempts)

    def retry_backoff_ms(self, milliseconds: int) -> Edge[T]:
        return self._with(retry_backoff_ms=milliseconds)

    def keyed(self) -> Edge[T]:
        """Stamp identity and ``flz.key`` onto emitted plugin frames."""
        return self._with(keyed=True)

    # -- wiring ------------------------------------------------------------

    def branch(self, *arms: Callable[[Edge[T]], Any]) -> Edge[T]:
        """Fan out: each arm receives this same edge.

        ``branch`` builds exactly the topology the separate ``into`` statements
        would. At least two arms.
        """
        if len(arms) < 2:
            raise DeclarationError(
                self._builder.factory_id,
                self._pending.get("channel_name") or self._source_name,
                f"branch(...) requires at least two arms; {len(arms)} given — a single arm is "
                f"`into(...)` written the long way",
            )
        for arm in arms:
            arm(self)
        return self

    def into(self, proc: Procedure[T, O], name: str | None = None) -> Edge[O]:
        """Wire this edge into a procedure."""
        node = self._builder.node_for(proc, name)

        if self._from[0] == "entry":
            # One entry channel may feed several procedures; the transforms and
            # the key belong to the channel, which is what the prepare crossing
            # names.
            channel: ChannelSpec = self._from[1]
            channel.consumers.append(node.index)
            self._apply_pending(channel)
        else:
            source: ProcNode = self._from[1]
            self._builder.add_channel(
                name=self._pending.get("channel_name") or f"{source.name}__{node.name}",
                kind="inter_proc",
                producer=source.index,
                consumers=[node.index],
                overflow=self._pending.get("overflow", "drop_nothing"),
                retry_max=self._pending.get("retry_max"),
                retry_backoff_ms=self._pending.get("retry_backoff_ms"),
                keyed=self._pending.get("keyed", False),
                transforms=list(self._transforms),
                key_by=self._pending.get("key_by"),
            )

        return Edge(self._builder, ("procedure", node), {})

    def sink(self, name: str, *, encode: Callable[[Any], bytes] | None = None) -> None:
        """Terminate this edge on a plugin-bound channel."""
        if self._from[0] != "procedure":
            raise DeclarationError(
                self._builder.factory_id,
                name,
                "a plugin sink is fed by a procedure; wire the entry channel into one first",
            )
        source: ProcNode = self._from[1]
        self._builder.add_channel(
            name=name,
            kind="plugin",
            producer=source.index,
            consumers=[],
            overflow=self._pending.get("overflow", "drop_nothing"),
            retry_max=self._pending.get("retry_max"),
            retry_backoff_ms=self._pending.get("retry_backoff_ms"),
            keyed=self._pending.get("keyed", False),
            transforms=list(self._transforms),
            key_by=self._pending.get("key_by"),
            encode=encode,
        )

    # -- internals ---------------------------------------------------------

    @property
    def _transforms(self) -> list[Callable[[Any], Any]]:
        return self._pending.get("transforms", [])

    @property
    def _source_name(self) -> str:
        return self._from[1].name

    def _with(self, **pending: Any) -> Edge[Any]:
        return Edge(self._builder, self._from, {**self._pending, **pending})

    def _apply_pending(self, channel: ChannelSpec) -> None:
        if "overflow" in self._pending:
            channel.overflow = self._pending["overflow"]
        if self._pending.get("retry_max") is not None:
            channel.retry_max = self._pending["retry_max"]
        if self._pending.get("retry_backoff_ms") is not None:
            channel.retry_backoff_ms = self._pending["retry_backoff_ms"]
        if self._pending.get("keyed"):
            channel.keyed = True
        if self._pending.get("key_by") is not None:
            channel.key_by = self._pending["key_by"]
        channel.transforms.extend(self._transforms)
