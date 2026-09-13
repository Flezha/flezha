"""The authoring surface, and what the builder still refuses itself.

Every topology rule now lives in ``flz_core::host::register::check``, and
``test/fixtures/reference_topologies.txt`` is where each one's case lives — one
row per refusal, read by every SDK's conformance run. What is left here is the
authoring surface: the shape a declaration builds, the names it derives, which
closure reaches which edge, and the defects a procedure's own declaration is
refused for when the decorator builds it. The ones a type checker can see are
proved by ``test_typing.py``.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass, field

import pytest

import flezha
from conftest import Bumped, Counter, count_up, declare, demo, echo
from flezha import graph
from flezha.errors import DeclarationError, UnrepresentableValueError


def _derived(operative: flezha.Operative) -> list[tuple[object, ...]]:
    """The channels the core derived, as ``(name, kind, producer, consumers)``."""
    return [
        (
            channel["name"],
            channel["kind"],
            channel["producer"],
            [edge["procedure"] for edge in channel["consumers"]],
        )
        for channel in declare(operative)["channels"]
    ]


# ---------------------------------------------------------------------------
# The surface itself
# ---------------------------------------------------------------------------


def test_a_decorated_handler_is_an_ordinary_callable() -> None:
    assert echo("hello") == "hello"
    assert echo.name == "echo"


def test_a_class_procedure_carries_its_hooks() -> None:
    assert count_up.initial_state is not None
    assert count_up.apply_change is not None
    assert count_up.on_timer is None
    assert count_up.state_type is Counter
    assert count_up.change_type is Bumped


def test_declaring_performs_no_io(monkeypatch: pytest.MonkeyPatch) -> None:
    """Importing a declaring module opens no socket and starts no thread."""
    opened: list[object] = []
    real_socket = socket.socket

    def watched(*args: object, **kwargs: object) -> object:
        opened.append(args)
        return real_socket(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(socket, "socket", watched)

    @flezha.procedure
    def later(value: str) -> str:
        return value

    @flezha.stream_operative("late")
    def late(c: flezha.Channels) -> None:
        c.input("in").into(later).sink()

    declare(late)
    assert opened == []


def test_the_decl_carries_the_python_language_and_codec_bytes() -> None:
    decl = graph.to_host_class_decl(demo.spec)
    assert decl["sdk_lang"] == graph.SDK_LANG_PYTHON == 7
    assert decl["change_format_id"] == graph.FORMAT_ID_MSGPACK == 5


def test_two_unnamed_mentions_of_one_procedure_are_one_node() -> None:
    @flezha.procedure
    def split(value: str) -> str:
        return value

    @flezha.procedure
    def join(value: str) -> str:
        return value

    @flezha.stream_operative("diamond")
    def diamond(c: flezha.Channels) -> None:
        entry = c.input("in")
        entry.into(split).named("a").into(join, "join_a").sink()
        entry.into(split).named("b").into(join, "join_b").sink()

    declare(diamond)
    assert [node.name for node in diamond.spec.procedures] == ["split", "join_a", "join_b"]
    # And one edge: the second bare mention re-declares the first rather than
    # wiring `split` a second time off the same channel.
    assert [e.procedure for e in diamond.spec.channels[0].consumers] == [0]


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


def test_an_unannotated_parameter_is_refused_naming_it() -> None:
    with pytest.raises(DeclarationError) as caught:

        @flezha.procedure
        def handle(order):  # type: ignore[no-untyped-def]
            return order

    assert "order" in str(caught.value)
    assert "annotation" in str(caught.value)


def test_a_node_identity_collision_is_refused() -> None:
    @flezha.procedure(name="shared")
    def first(value: str) -> str:
        return value

    @flezha.procedure(name="shared")
    def second(value: str) -> str:
        return value

    @flezha.stream_operative("collision")
    def collision(c: flezha.Channels) -> None:
        entry = c.input("in")
        entry.into(first).sink()
        entry.into(second).sink()

    with pytest.raises(DeclarationError, match="node identity") as caught:
        declare(collision)
    # The one refusal this builder raises under a core name reads the way the core's do.
    assert caught.value.refusals == ["NodeIdentityCollision"]
    assert str(caught.value).startswith(
        "collision: shared: topology rejected [NodeIdentityCollision]:\n  - two different"
    )


# KI-122: three options, one implication. Each writes its own raw flag and the
# core derives the acknowledgement granularity from any of them, so no order of
# calls can clear another's.
def test_the_checkpoint_options_compose_in_any_order_and_none_clears_another() -> None:
    @flezha.procedure
    def one(value: str) -> str:
        return value

    @flezha.stream_operative("compose")
    def compose(c: flezha.Channels) -> None:
        c.effectively_once()
        c.outputs_at_checkpoint(250)
        c.checkpoint_ack()
        c.checkpoint_every(4)
        c.input("in").into(one).keyed().sink()

    assert declare(compose)["ack_granularity"] == "checkpoint"
    decl = graph.to_host_class_decl(compose.spec)

    assert decl["checkpoint_ack"] is True
    assert decl["checkpoint_every"] == 4
    assert decl["checkpoint_outputs"] == "at_checkpoint"
    assert decl["checkpoint_max_delay_ms"] == 250


# The verdict itself: one exception carrying every name the core answered with.
def test_a_refusal_reaches_the_author_under_the_cores_own_names() -> None:
    @flezha.stream_operative("edge_already_wired")
    def already_wired(c: flezha.Channels) -> None:
        edge = c.input("in")
        edge.into(echo)
        edge.map(lambda value: f"{value}!").into(echo)

    with pytest.raises(DeclarationError) as caught:
        declare(already_wired)
    assert caught.value.refusals == ["EdgeAlreadyWired"]
    assert str(caught.value).startswith(
        "edge_already_wired: topology rejected [EdgeAlreadyWired]:\n  - "
    )
    assert "already feeds procedure `echo`" in str(caught.value)


# The record carries ``change_is_nochange`` per procedure: a handler that folds
# no change has the empty Change type, and a logical clock over it never
# advances. The refusal is the core's ``UnservableClock``; what is asserted here
# is that this builder writes the fact it reads.
def test_a_procedure_that_folds_no_change_declares_it() -> None:
    @flezha.stream_operative("nochange")
    def nochange(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink()

    declare(nochange)
    folds_nothing = graph.to_host_class_decl(nochange.spec)["procedures"][0]
    assert folds_nothing["change_is_nochange"] is True
    folds = graph.to_host_class_decl(demo.spec)["procedures"][1]
    assert folds["name"] == "count_up"
    assert folds["change_is_nochange"] is False


def test_an_event_time_clock_without_an_extractor_is_refused() -> None:
    with pytest.raises(DeclarationError, match="no event_time extractor"):

        @flezha.procedure(clock="event_time")
        def timed(value: str) -> str:
            return value


def test_an_extractor_without_an_event_time_clock_is_refused() -> None:
    """A procedure-local defect is refused when the decorator builds it.

    Earlier than the `define_stream` call site and with the traceback on the
    handler — which is why every procedure-local refusal lives on `Procedure`
    and `graph.validate` holds only the defects that need the graph.
    """

    class Timed:
        def process(self, value: dict[str, int]) -> dict[str, int]:
            return value

        def event_time(self, value: dict[str, int]) -> int:
            return value["at"]

    with pytest.raises(DeclarationError, match="only consulted by an event_time clock"):
        flezha.procedure(Timed)


def test_apply_change_without_initial_state_is_refused() -> None:
    class Folding:
        def process(self, value: str) -> str:
            return value

        def apply_change(self, state: dict[str, int], change: dict[str, int]) -> None:
            return None

    with pytest.raises(DeclarationError, match="nothing to fold into"):
        flezha.procedure(Folding)


def test_an_unrepresentable_state_field_is_refused_naming_it() -> None:
    @flezha.state
    @dataclass
    class WithSocket:
        conn: object = field(default_factory=lambda: socket.socket())

    class Holder:
        def process(self, value: str) -> str:
            return value

        def initial_state(self, key: str | None) -> WithSocket:
            return WithSocket()

    with pytest.raises(UnrepresentableValueError, match="conn"):
        flezha.procedure(Holder)


def test_a_workflow_that_is_neither_graph_nor_run_body_is_refused() -> None:
    with pytest.raises(DeclarationError, match="neither a channels graph nor a run body"):

        @flezha.workflow_operative("neither")
        def neither(first: int, second: int) -> None:
            return None


def test_a_run_body_workflow_declares_no_channels() -> None:
    @flezha.workflow_operative("saga")
    def saga(args: dict[str, str], ctx: flezha.WorkflowCtx) -> str:
        return "done"

    spec = saga.spec
    assert spec.run is not None
    assert spec.channels == []
    declare(saga)


def test_a_valid_graph_passes() -> None:
    declare(demo)
    assert [node.name for node in demo.spec.procedures] == ["echo", "count_up"]


# -- one output channel per producing procedure -------------------------------
#
# The model Rust and Java carry: a procedure node owns ONE output channel,
# created on first wiring and reused after. A held edge wired twice appends a
# second consumer to it — it does not declare a second channel claiming the same
# producer, which the core resolves by first match (`host/register.rs`
# `procedure_wiring`) and so silently truncates.


# -- one output channel per producing procedure -------------------------------
#
# The model Rust and Java carry: a procedure node owns ONE output channel,
# created on first wiring and reused after. A held edge wired twice appends a
# second consumer to it — it does not declare a second channel claiming the same
# producer, which the core resolves by first match (`host/register.rs`
# `procedure_wiring`) and so silently truncates.


def test_a_held_edge_wired_twice_is_one_channel_with_two_consumers() -> None:
    @flezha.stream_operative("fan_out")
    def fan_out(c: flezha.Channels) -> None:
        edge = c.input("in").into(echo)
        edge.into(count_up, "b")
        edge.into(count_up, "c")

    assert _derived(fan_out) == [
        ("in", "entry", None, [0]),
        ("echo", "inter_proc", 0, [1, 2]),
    ]


def test_branch_and_two_into_statements_derive_the_identical_topology() -> None:
    # The record differs by one link — the sugar's declared arity — and the
    # topology the core derives from the two does not.
    @flezha.stream_operative("separate")
    def separate(c: flezha.Channels) -> None:
        edge = c.input("in").into(echo)
        edge.into(count_up, "b")
        edge.into(count_up, "c")

    @flezha.stream_operative("branched")
    def branched(c: flezha.Channels) -> None:
        c.input("in").into(echo).branch(
            lambda arm: arm.into(count_up, "b"), lambda arm: arm.into(count_up, "c")
        )

    assert _derived(separate) == _derived(branched)


def test_sink_returns_the_edge_so_a_consumer_wires_behind_it() -> None:
    @flezha.stream_operative("hybrid")
    def hybrid(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink("hybrid_indicator").into(count_up)

    # A sink alongside a consumer stays inter-procedure: the value reaches both,
    # and the channel is journalled as the edge it still is.
    assert _derived(hybrid) == [
        ("in", "entry", None, [0]),
        ("hybrid_indicator", "inter_proc", 0, [1]),
    ]


def test_the_sink_names_the_channel_and_the_no_argument_form_does_not() -> None:
    @flezha.stream_operative("sink_named")
    def named(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink("items_output")

    @flezha.stream_operative("sink_derived")
    def derived(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink()

    assert [row[0] for row in _derived(named)] == ["in", "items_output"]
    assert [row[0] for row in _derived(derived)] == ["in", "echo"]


def test_a_channel_named_twice_is_refused_naming_both() -> None:
    @flezha.stream_operative("named_twice")
    def named_twice(c: flezha.Channels) -> None:
        c.input("in").into(echo).named("a").sink("b")

    with pytest.raises(DeclarationError, match="ChannelNamedTwice"):
        declare(named_twice)


def test_a_channel_option_with_nothing_wired_after_it_still_reaches_the_channel() -> None:
    @flezha.stream_operative("trailing_named")
    def renamed(c: flezha.Channels) -> None:
        c.input("in").into(echo).named("renamed")

    @flezha.stream_operative("trailing_keyed")
    def keyed(c: flezha.Channels) -> None:
        c.input("in").into(echo).keyed()

    assert [row[0] for row in _derived(renamed)] == ["in", "renamed"]
    with pytest.raises(DeclarationError, match="KeyedChannelWithoutPluginWire"):
        declare(keyed)


def test_two_channels_naming_one_sink_is_refused() -> None:
    @flezha.stream_operative("one_sink_two_channels")
    def one_sink(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink("shared").into(count_up).sink("shared")

    with pytest.raises(DeclarationError, match="DuplicateSinkName"):
        declare(one_sink)


def test_keyed_reaches_the_channel_of_an_edge_that_also_declares_a_sink() -> None:
    @flezha.stream_operative("keyed_hybrid")
    def keyed_hybrid(c: flezha.Channels) -> None:
        c.input("in").into(echo).keyed().sink("mid").into(count_up)

    declare(keyed_hybrid)
    assert graph.to_host_class_decl(keyed_hybrid.spec)["channels"][1]["keyed"] is True


def test_a_transform_before_one_into_reaches_that_consumer_only() -> None:
    @flezha.stream_operative("per_edge_transform")
    def per_edge_transform(c: flezha.Channels) -> None:
        edge = c.input("in").into(echo)
        edge.map(lambda value: f"{value}!").into(count_up, "b")
        edge.into(count_up, "c")

    declare(per_edge_transform)
    channel = per_edge_transform.spec.channels[1]
    assert [(e.procedure, len(e.transforms)) for e in channel.consumers] == [(1, 1), (2, 0)]


# The refusal both references carry by name — Rust's
# `TopologyIssue::DanglingTransformChain` and Java's `danglingTransformChain`.
# A transform lives on the consumer edge it terminates in, so one no `into(...)`
# closed has nowhere to run and would be discarded without a word.


def test_a_bare_re_declared_edge_is_a_no_op_not_a_second_edge() -> None:
    @flezha.stream_operative("edge_redeclared_bare")
    def bare(c: flezha.Channels) -> None:
        edge = c.input("in")
        edge.into(echo)
        edge.into(echo)

    declare(bare)
    channel = bare.spec.channels[0]
    assert [(e.procedure, len(e.transforms)) for e in channel.consumers] == [(0, 0)]


# The reference declares exactly this — `tier_commit_block_workflow` opens with
# `c.effectively_once()` on a workflow class in Rust and Java — so the flag is
# read where the workflow spec is assembled rather than defaulted to False. It
# stays off the wire: the core refuses `effectively_once` on a workflow class,
# and Java's `HostClassDeclaration.forWorkflow` never writes it either.


def test_a_workflow_class_carries_its_effectively_once_declaration() -> None:
    @flezha.workflow_operative("tier_workflow")
    def tier_workflow(c: flezha.Channels) -> None:
        c.effectively_once()
        c.input("in").into(echo).keyed().sink("out")

    declare(tier_workflow)
    assert tier_workflow.spec.effectively_once is True
    assert "effectively_once" not in graph.to_host_class_decl(tier_workflow.spec)
