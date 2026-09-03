"""The authoring surface, and the registration-time refusals behind it.

Every defect class the authoring contract names has a case here. Registration
refusal is what Python has in place of the Rust SDK's compile-time refusal for
the defects a type checker cannot see; the ones it *can* see are proved by
``test_typing.py``.
"""

from __future__ import annotations

import socket
from dataclasses import dataclass, field

import pytest

import flezha
from flezha import graph
from flezha.errors import DeclarationError, UnrepresentableValueError

from conftest import Bumped, Counter, count_up, demo, echo


def declare(operative: flezha.Operative) -> None:
    graph.validate(operative.spec)


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
        c.input("in").into(later).sink("out")

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
        entry.into(split).named("a").into(join, "join_a").sink("sink_a")
        entry.into(split).named("b").into(join, "join_b").sink("sink_b")

    declare(diamond)
    assert [node.name for node in diamond.spec.procedures] == ["split", "join_a", "join_b"]
    # And one edge: the second bare mention re-declares the first rather than
    # wiring `split` a second time off the same channel.
    assert [e.procedure for e in diamond.spec.channels[0].consumers] == [0]


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


def test_a_cycle_is_refused_naming_the_node() -> None:
    @flezha.procedure
    def a(value: str) -> str:
        return value

    @flezha.procedure
    def b(value: str) -> str:
        return value

    @flezha.stream_operative("cyclic")
    def cyclic(c: flezha.Channels) -> None:
        node_a = c.input("in").into(a)
        node_b = node_a.named("a_b").into(b)
        node_b.named("b_a").into(a)

    with pytest.raises(DeclarationError, match="cycle"):
        declare(cyclic)


def test_a_stream_with_no_entry_channel_is_refused() -> None:
    @flezha.stream_operative("no_entry")
    def no_entry(c: flezha.Channels) -> None:
        return None

    with pytest.raises(DeclarationError, match="no entry channel"):
        declare(no_entry)


def test_an_entry_channel_with_no_consumer_is_refused() -> None:
    @flezha.stream_operative("dangling")
    def dangling(c: flezha.Channels) -> None:
        c.input("in")

    with pytest.raises(DeclarationError, match="names no consumer"):
        declare(dangling)


def test_a_duplicate_plugin_sink_is_refused() -> None:
    @flezha.procedure
    def one(value: str) -> str:
        return value

    @flezha.stream_operative("twice")
    def twice(c: flezha.Channels) -> None:
        node = c.input("in").into(one)
        node.sink("same")
        node.sink("same")

    with pytest.raises(DeclarationError, match="more than once"):
        declare(twice)


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
        entry.into(first).sink("a")
        entry.into(second).sink("b")

    with pytest.raises(DeclarationError, match="node identity"):
        declare(collision)


def test_a_keyed_channel_without_a_plugin_wire_is_refused() -> None:
    @flezha.procedure
    def one(value: str) -> str:
        return value

    @flezha.procedure
    def two(value: str) -> str:
        return value

    @flezha.stream_operative("keyed_edge")
    def keyed_edge(c: flezha.Channels) -> None:
        c.input("in").into(one).keyed().named("mid").into(two).sink("out")

    with pytest.raises(DeclarationError, match="no plugin wire"):
        declare(keyed_edge)


def test_a_tier_channel_that_is_not_keyed_is_refused() -> None:
    @flezha.procedure
    def one(value: str) -> str:
        return value

    @flezha.stream_operative("tiered")
    def tiered(c: flezha.Channels) -> None:
        c.effectively_once()
        c.input("in").into(one).sink("out")

    with pytest.raises(DeclarationError, match="must opt in with keyed"):
        declare(tiered)


@pytest.mark.parametrize(
    "option",
    [
        lambda c: c.checkpoint_ack(),
        lambda c: c.checkpoint_every(4),
        lambda c: c.outputs_at_checkpoint(250),
    ],
    ids=["checkpoint_ack", "checkpoint_every", "outputs_at_checkpoint"],
)
def test_a_checkpoint_option_without_the_tier_is_refused(option) -> None:
    @flezha.procedure
    def one(value: str) -> str:
        return value

    @flezha.stream_operative("ack")
    def ack(c: flezha.Channels) -> None:
        option(c)
        c.input("in").into(one).keyed().sink("out")

    with pytest.raises(DeclarationError, match="checkpoint acknowledgement"):
        declare(ack)


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
        c.input("in").into(one).keyed().sink("out")

    declare(compose)
    decl = graph.to_host_class_decl(compose.spec)

    assert decl["ack_granularity"] == "checkpoint"
    assert decl["checkpoint_every"] == 4
    assert decl["checkpoint_outputs"] == "at_checkpoint"
    assert decl["checkpoint_max_delay_ms"] == 250


def test_a_correction_window_without_its_preconditions_is_refused() -> None:
    @flezha.procedure
    def one(value: str) -> str:
        return value

    @flezha.stream_operative("window")
    def window(c: flezha.Channels) -> None:
        c.correction_window(1000)
        c.input("in").into(one).sink("out")

    with pytest.raises(DeclarationError, match="without effectively_once"):
        declare(window)


def test_a_correction_window_without_an_event_time_clock_is_refused() -> None:
    @flezha.procedure
    def one(value: str) -> str:
        return value

    @flezha.stream_operative("window_no_clock")
    def window_no_clock(c: flezha.Channels) -> None:
        c.effectively_once().correction_window(1000)
        c.input("in").into(one).keyed().sink("out")

    with pytest.raises(DeclarationError, match="no procedure declares an event_time clock"):
        declare(window_no_clock)


def test_a_branch_with_one_arm_is_refused() -> None:
    @flezha.procedure
    def one(value: str) -> str:
        return value

    @flezha.stream_operative("thin_branch")
    def thin_branch(c: flezha.Channels) -> None:
        c.input("in").branch(lambda edge: edge.into(one).sink("out"))

    with pytest.raises(DeclarationError, match="at least two arms"):
        thin_branch.spec


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


def test_a_static_key_set_without_entities_for_is_refused() -> None:
    class Static:
        def process(self, value: dict[str, str]) -> dict[str, str]:
            return value

    with pytest.raises(DeclarationError, match="without an entities_for"):
        flezha.procedure(entity_keys=["a", "b"])(Static)


def test_a_dynamic_consumer_without_a_key_by_is_refused() -> None:
    @flezha.procedure(dynamic_entities=True)
    def keyed(value: dict[str, str]) -> dict[str, str]:
        return value

    @flezha.stream_operative("dyn")
    def dyn(c: flezha.Channels) -> None:
        c.input("in").into(keyed).sink("out")

    with pytest.raises(DeclarationError, match="without a key_by"):
        declare(dyn)


def test_a_key_by_into_a_static_procedure_is_refused() -> None:
    class Static:
        def process(self, value: dict[str, str]) -> dict[str, str]:
            return value

        def entities_for(self, value: dict[str, str]) -> list[str]:
            return ["a"]

    static = flezha.procedure(entity_keys=["a", "b"])(Static)

    @flezha.stream_operative("both_keyings")
    def both_keyings(c: flezha.Channels) -> None:
        c.input("in").key_by(lambda value: value["k"]).into(static).sink("out")

    with pytest.raises(DeclarationError, match="declare one or the other"):
        declare(both_keyings)


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


def test_sync_durability_on_a_stream_is_refused() -> None:
    @flezha.procedure
    def one(value: str) -> str:
        return value

    @flezha.stream_operative("sync_stream")
    def sync_stream(c: flezha.Channels) -> None:
        c.sync_durability()
        c.input("in").into(one).sink("out")

    with pytest.raises(DeclarationError, match="sync_durability on a stream class"):
        declare(sync_stream)


def test_retry_max_zero_is_refused() -> None:
    @flezha.procedure
    def one(value: str) -> str:
        return value

    @flezha.stream_operative("no_attempts")
    def no_attempts(c: flezha.Channels) -> None:
        c.input("in").retry_max(0).into(one).sink("out")

    with pytest.raises(DeclarationError, match="at least one attempt"):
        declare(no_attempts)


def test_an_unreachable_procedure_is_refused() -> None:
    @flezha.procedure
    def wired(value: str) -> str:
        return value

    @flezha.procedure
    def orphan(value: str) -> str:
        return value

    @flezha.stream_operative("orphaned")
    def orphaned(c: flezha.Channels) -> None:
        c.input("in").into(wired).sink("out")
        c.node_for(orphan)

    with pytest.raises(DeclarationError, match="not reachable"):
        declare(orphaned)


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


def _channels(operative: flezha.Operative) -> list[tuple[object, ...]]:
    decl = graph.to_host_class_decl(operative.spec)
    return [
        (c.get("name"), c.get("kind"), c.get("producer"), c.get("consumers"))
        for c in decl["channels"]
    ]


def test_a_held_edge_wired_twice_is_one_channel_with_two_consumers() -> None:
    @flezha.stream_operative("fan_out")
    def fan_out(c: flezha.Channels) -> None:
        edge = c.input("in").into(echo)
        edge.into(count_up, "b")
        edge.into(count_up, "c")

    declare(fan_out)
    assert _channels(fan_out) == [
        ("in", "entry", None, [0]),
        ("echo", "inter_proc", 0, [1, 2]),
    ]


def test_branch_and_two_into_statements_build_the_identical_declaration() -> None:
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

    declare(separate)
    declare(branched)
    assert _channels(separate) == _channels(branched)


def test_sink_returns_the_edge_so_a_consumer_wires_behind_it() -> None:
    @flezha.stream_operative("hybrid")
    def hybrid(c: flezha.Channels) -> None:
        c.input("in").into(echo).named("hybrid_indicator").sink("hybrid_indicators").into(count_up)

    declare(hybrid)
    assert _channels(hybrid) == [
        ("in", "entry", None, [0]),
        ("hybrid_indicator", "inter_proc", 0, [1]),
    ]


def test_a_second_sink_on_one_channel_is_refused() -> None:
    @flezha.stream_operative("twice")
    def twice(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink("out").sink("out_again")

    with pytest.raises(DeclarationError, match="declares a plugin sink more than once"):
        declare(twice)


def test_keyed_is_refused_without_a_plugin_sink_and_accepted_with_one() -> None:
    @flezha.stream_operative("keyed_no_sink")
    def keyed_no_sink(c: flezha.Channels) -> None:
        c.input("in").into(echo).named("mid").keyed().into(count_up)

    with pytest.raises(DeclarationError, match="is declared keyed but has no plugin wire"):
        declare(keyed_no_sink)

    @flezha.stream_operative("keyed_hybrid")
    def keyed_hybrid(c: flezha.Channels) -> None:
        c.input("in").into(echo).named("mid").keyed().sink("mid_sink").into(count_up)

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
def test_a_transform_chain_no_into_closes_is_refused() -> None:
    @flezha.stream_operative("dangling")
    def dangling(c: flezha.Channels) -> None:
        c.input("in").into(echo).map(lambda value: f"{value}!").sink("s")

    with pytest.raises(
        DeclarationError,
        match="echo: DanglingTransformChain: declares a transform or key_by",
    ):
        declare(dangling)

    # A key dangles the same way. Declared on a channel another edge consumes,
    # so no other refusal reaches the declaration first.
    @flezha.stream_operative("dangling_key")
    def dangling_key(c: flezha.Channels) -> None:
        edge = c.input("in")
        edge.into(echo)
        edge.key_by(str)

    with pytest.raises(DeclarationError, match="in: DanglingTransformChain"):
        declare(dangling_key)


# The three refusals both references raise from their own ``extend_pending`` —
# Rust's ``TopologyIssue::{DuplicateKeyBy, TransformAfterKeyBy,
# TransformAfterTerminal}`` and Java's factory methods of the same names. Each
# shape was ACCEPTED here before this: a second ``key_by`` overwrote the
# author's first without a word, and neither a prior key nor an attached sink
# was inspected at all.
def test_a_second_key_by_is_refused_rather_than_overwriting_the_first() -> None:
    @flezha.procedure(dynamic_entities=True)
    def keyed(value: str) -> str:
        return value

    @flezha.stream_operative("dup_key_by")
    def dup(c: flezha.Channels) -> None:
        c.input("in").key_by(lambda _: "a").key_by(lambda _: "b").into(keyed)

    with pytest.raises(
        DeclarationError,
        match=r"in: DuplicateKeyBy: declares key_by\(\.\.\.\) twice before one into",
    ):
        declare(dup)


# The key is evaluated on the final post-transform value, so it must be the last
# link before the ``into(...)`` that closes the chain.
def test_a_transform_after_key_by_is_refused() -> None:
    @flezha.procedure(dynamic_entities=True)
    def keyed(value: str) -> str:
        return value

    @flezha.stream_operative("transform_after_key_by")
    def after_key(c: flezha.Channels) -> None:
        c.input("in").key_by(lambda _: "a").map(lambda value: f"{value}!").into(keyed)

    with pytest.raises(
        DeclarationError,
        match=r"in: TransformAfterKeyBy: declares a transform after key_by",
    ):
        declare(after_key)


# The sink serializes the channel's own value, so nothing declared behind it can
# reshape what it already wrote.
def test_a_transform_after_a_plugin_sink_is_refused() -> None:
    @flezha.procedure
    def tail(value: str) -> str:
        return value

    @flezha.stream_operative("transform_after_terminal")
    def after_sink(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink("s").map(lambda value: f"{value}!").into(tail)

    with pytest.raises(
        DeclarationError,
        match=r"echo: TransformAfterTerminal: declares a transform or key_by\(\.\.\.\) after a plugin sink",
    ):
        declare(after_sink)


# The refusal both references raise from consumer wiring rather than chain
# extension — Rust's ``attach_consumer`` and Java's ``attachEdge``. Both shapes
# were ACCEPTED here before this, leaving two consumer edges naming one
# procedure, so the value reached it twice.
def test_a_re_declared_edge_carrying_a_transform_is_refused() -> None:
    @flezha.stream_operative("edge_already_wired")
    def already_wired(c: flezha.Channels) -> None:
        edge = c.input("in")
        edge.into(echo)
        edge.map(lambda value: f"{value}!").into(echo)

    with pytest.raises(
        DeclarationError,
        match=r"in: EdgeAlreadyWired: already feeds procedure `echo`; an edge carries exactly one",
    ):
        declare(already_wired)


# A re-declaration carrying neither transform nor key is the fan-in /
# repeated-``into`` case both references make a true no-op: one consumer edge,
# not two, and not a refusal.
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
        c.input("in").into(echo).named("out").keyed().sink("out")

    declare(tier_workflow)
    assert tier_workflow.spec.effectively_once is True
    assert "effectively_once" not in graph.to_host_class_decl(tier_workflow.spec)
