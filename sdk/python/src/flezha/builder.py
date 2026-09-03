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
    "ConsumerEdge",
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
    output: "ChannelSpec | None" = None
    """The one channel this node produces on, created on first wiring."""


@dataclass(slots=True)
class ConsumerEdge:
    """One consumer of a channel, with the transforms and key of its own edge."""

    procedure: int
    transforms: list[Callable[[Any], Any]] = field(default_factory=list)
    key_by: Callable[[Any], Any] | None = None


@dataclass(slots=True)
class _Chain:
    """A transform/key chain declared on an edge but not yet closed by ``into``.

    Every ``map`` / ``filter`` / ``key_by`` opens a new one over the previous,
    which is what lets a shared prefix branch without the arms colliding.
    """

    source: tuple[str, Any]
    superseded: bool = False
    """Set when a later link built on this one, so only a tip can dangle."""
    consumed: bool = False
    """Set when ``into(...)`` closed the chain onto an edge."""
    keyed: bool = False
    """Set once a ``key_by`` link joined, so a later link knows it follows one."""


@dataclass(slots=True)
class ChannelSpec:
    """A channel as the graph holds it. Its position in the list IS its id."""

    index: int
    name: str
    kind: str
    producer: int | None = None
    consumers: list[ConsumerEdge] = field(default_factory=list)
    overflow: str = "drop_nothing"
    retry_max: int | None = None
    retry_backoff_ms: int | None = None
    keyed: bool = False
    sink: str | None = None
    """The plugin sink attached to this channel; ``None`` means none."""
    decode: Callable[[Any], Any] | None = None
    sink_encode: Callable[[Any], bytes] | None = None
    """The plugin sink's payload encoder; ``None`` means the MessagePack default.

    It belongs to the sink frame alone: an inter-procedure frame is decoded
    with the class codec, so encoding one with this hands the consuming
    procedure bytes it cannot read.
    """


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
    chain_issues: list[tuple[str, str]] = field(default_factory=list)
    """Every transform-chain defect, as ``(channel, reference error name)``."""
    workflow: dict[str, Any] | None = None
    run: Callable[..., Any] | None = None

    @property
    def structured_workflow(self) -> bool:
        """Whether this class's producers release their values untransformed.

        A structured workflow's stage releases ONE value per emit and the driver
        fans it out to every consumer itself, so each consumer edge — its
        transforms and its key — belongs to the consumer. The producer and the
        consumer read this one predicate, so they cannot disagree about which
        side applies the edge.
        """
        return self.kind == "workflow" and bool(self.channels)


class Channels:
    """The fluent builder an operative's declaration function receives."""

    def __init__(self, factory_id: str) -> None:
        self.factory_id = factory_id
        self.procedures: list[ProcNode] = []
        self.channels: list[ChannelSpec] = []
        self.chains: list[_Chain] = []
        # What each link refused as it was declared, in declaration order.
        self.link_issues: list[tuple[str, str]] = []
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
        cadence. All three require :meth:`effectively_once`, which is where the
        durable offset the checkpoint holds back actually lives.
        """
        return self._amend_checkpoint(lambda: None)

    def checkpoint_every(self, count: int) -> Channels:
        def amend() -> None:
            self.checkpoint_every_value = count

        return self._amend_checkpoint(amend)

    def outputs_at_checkpoint(self, max_delay_ms: int | None = None) -> Channels:
        def amend() -> None:
            self.checkpoint_outputs_value = "at_checkpoint"
            if max_delay_ms is not None:
                self.checkpoint_max_delay_ms_value = max_delay_ms

        return self._amend_checkpoint(amend)

    def _amend_checkpoint(self, amend: Callable[[], None]) -> Channels:
        """Turn the granularity on (idempotent) and amend its policy, so each
        option is one call that preserves the others."""
        self.ack_granularity = "checkpoint"
        amend()
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

    def output_channel_for(self, node: ProcNode) -> ChannelSpec:
        """The channel this node produces on, created on first use.

        Lazily, so a leaf node never carries a phantom channel. Named by the
        node's identity until a ``named(...)`` override, and reused by every
        later call on an edge whose source is this node — which is what makes a
        fan-out one channel with two consumers rather than two channels
        claiming one producer.
        """
        if node.output is not None:
            return node.output
        channel = ChannelSpec(
            index=len(self.channels),
            name=node.name,
            kind="inter_proc",
            producer=node.index,
        )
        self.channels.append(channel)
        node.output = channel
        return channel

    def extend_chain(self, source: tuple[str, Any], base: int | None, *, key: bool) -> int:
        """Extend the chain at ``base`` (or open one on ``source``) with one
        more link, answering the new chain's id.

        The three refusals the references raise from their own ``extend_pending``
        are raised here: a link declared behind a plugin sink, a transform
        declared after the key it would feed, and a second key — which without
        this would overwrite the author's first without a word.
        """
        channel = _channel_name(source)
        if _sink_attached(source):
            self.link_issues.append((channel, "TransformAfterTerminal"))
        prior_key = base is not None and self.chains[base].keyed
        if base is not None:
            self.chains[base].superseded = True
        # The key is evaluated on the final post-transform value, so a transform
        # declared after it would hand it a value of a type it was never written
        # against.
        if prior_key and not key:
            self.link_issues.append((channel, "TransformAfterKeyBy"))
        if prior_key and key:
            self.link_issues.append((channel, "DuplicateKeyBy"))
        self.chains.append(_Chain(source=source, keyed=key or prior_key))
        return len(self.chains) - 1

    def close_chain(self, chain: int | None) -> None:
        """Mark a chain closed by an ``into(...)``. A bare edge closes nothing."""
        if chain is not None:
            self.chains[chain].consumed = True

    def chain_issues(self) -> list[tuple[str, str]]:
        """Every transform-chain defect this declaration carries.

        What each link refused as it was declared, then the tips no
        ``into(...)`` closed — the order the references report them in, so the
        sharper defect wins over the dangling one it also produces.
        """
        return [
            *self.link_issues,
            *(
                (_channel_name(chain.source), "DanglingTransformChain")
                for chain in self.chains
                if not chain.superseded and not chain.consumed
            ),
        ]


def _channel_name(source: tuple[str, Any]) -> str:
    """The name of the channel an edge's values leave on.

    The entry's own, or the producing node's — its materialised channel when one
    exists, since a ``named(...)`` override lives there, and the name it would
    derive otherwise.
    """
    if source[0] == "entry":
        return str(source[1].name)
    node: ProcNode = source[1]
    return str(node.output.name if node.output is not None else node.name)


def _sink_attached(source: tuple[str, Any]) -> bool:
    """Whether the channel this edge's values leave on already has a plugin sink.

    Only a procedure's can: ``sink(...)`` refuses an entry edge, and a node's
    channel is created on first use, so one without a channel has no sink.
    """
    if source[0] == "entry":
        return False
    node: ProcNode = source[1]
    return node.output is not None and node.output.sink is not None


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
        return self._relink(transforms=[*self._transforms, transform])

    def filter(self, predicate: Callable[[T], bool]) -> Edge[T]:
        """Drop the values a predicate rejects, before this edge's key is computed.

        A dropped value is consumed and reaches no handler.
        """

        def keep(value: Any) -> Any:
            return value if predicate(value) else DROPPED

        return self._relink(transforms=[*self._transforms, keep])

    def key_by(self, key: Callable[[T], Any]) -> Edge[T]:
        """Route by a computed key: the consumer becomes a dynamic keyed procedure."""
        return self._relink(key_by=key)

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
        self._builder.close_chain(self._pending.get("chain"))
        # The transforms and the key belong to this edge, not to the channel:
        # one channel may feed several procedures, and a chain declared before
        # one `into` must not reach another.
        channel = self._channel()
        existing = next((e for e in channel.consumers if e.procedure == node.index), None)
        declares = bool(self._transforms) or self._pending.get("key_by") is not None
        if existing is None:
            channel.consumers.append(
                ConsumerEdge(
                    procedure=node.index,
                    transforms=list(self._transforms),
                    key_by=self._pending.get("key_by"),
                )
            )
        elif existing.transforms or existing.key_by is not None or declares:
            # Re-declaring one where either side carries a transform or a key is
            # refused rather than silently dropped — the edge would otherwise
            # keep whichever closure was declared first and deliver the wrong
            # value, to the wrong Entity, forever. Re-declaring a bare edge is
            # the fan-in / repeated ``into`` case and stays a no-op.
            raise DeclarationError(
                self._builder.factory_id,
                channel.name,
                f"EdgeAlreadyWired: already feeds procedure `{node.name}`; an edge carries "
                "exactly one transform and one key or neither, so a re-declaration where "
                "either side carries one is refused rather than silently keeping the first",
            )
        return Edge(self._builder, ("procedure", node), {})

    def sink(self, name: str, *, encode: Callable[[Any], bytes] | None = None) -> Edge[T]:
        """Attach this edge's channel to the named plugin sink, and answer the edge.

        ``sink(...).into(...)`` wires a consumer behind the sink, so one emit
        reaches both. A channel with no consumer behind it is the plugin-bound
        terminal — terminality is the absence of further wiring, not a
        different method.
        """
        if self._from[0] != "procedure":
            raise DeclarationError(
                self._builder.factory_id,
                name,
                "a plugin sink is fed by a procedure; wire the entry channel into one first",
            )
        channel = self._channel()
        if channel.sink is not None:
            raise DeclarationError(
                self._builder.factory_id,
                channel.name,
                "declares a plugin sink more than once",
            )
        channel.sink = name
        channel.sink_encode = encode
        return self

    # -- internals ---------------------------------------------------------

    @property
    def _transforms(self) -> list[Callable[[Any], Any]]:
        return self._pending.get("transforms", [])

    @property
    def _source_name(self) -> str:
        return _channel_name(self._from)

    def _with(self, **pending: Any) -> Edge[Any]:
        return Edge(self._builder, self._from, {**self._pending, **pending})

    def _relink(self, **pending: Any) -> Edge[Any]:
        """This edge with one more chain link — the one place a transform's or a
        key's result edge is built, so every link is registered and a chain no
        ``into(...)`` closes is reported rather than silently discarded."""
        return self._with(
            **pending,
            chain=self._builder.extend_chain(
                self._from, self._pending.get("chain"), key="key_by" in pending
            ),
        )

    def _channel(self) -> ChannelSpec:
        """The channel this edge's values leave on, with the pending options applied.

        An entry edge already has one; a procedure edge's is created on first
        use and reused after.
        """
        if self._from[0] == "entry":
            channel: ChannelSpec = self._from[1]
        else:
            channel = self._builder.output_channel_for(self._from[1])
            if self._pending.get("channel_name"):
                channel.name = self._pending["channel_name"]
        self._apply_pending(channel)
        return channel

    def _apply_pending(self, channel: ChannelSpec) -> None:
        if "overflow" in self._pending:
            channel.overflow = self._pending["overflow"]
        if self._pending.get("retry_max") is not None:
            channel.retry_max = self._pending["retry_max"]
        if self._pending.get("retry_backoff_ms") is not None:
            channel.retry_backoff_ms = self._pending["retry_backoff_ms"]
        if self._pending.get("keyed"):
            channel.keyed = True
