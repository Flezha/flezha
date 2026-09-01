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
