"""One activation in, one answer out — and never none.

``answer`` is a pure function of the activation and the registry, so every
mapping below asserts what it returns. The handout contract — an activation
that returns unanswered retires its entity for the life of the process — is
asserted against a real core, whose refusal of a token it never handed out is
what both the completion and the abandon meet.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

import flezha
from conftest import FakeCore, compensating, demo, deps_for, solo_deps, work_activation
from flezha import codec, native
from flezha.dispatch import DispatchDeps, answer, dispatch_now, run_now
from flezha.errors import NonRetryableError


def answered(activation: dict[str, Any], deps: DispatchDeps) -> dict[str, Any]:
    """The answer for one activation, driven with no event loop."""
    return run_now(answer(activation, deps))


@pytest.fixture
def deps(fake_core: FakeCore) -> DispatchDeps:
    return deps_for(demo, fake_core)


def test_an_input_is_answered_with_its_effects(deps: DispatchDeps) -> None:
    result = answered(work_activation(1, "abc"), deps)
    assert result["outcome"] == "work"
    (effect,) = result["effects"]
    assert effect["verdict"] == "ok"
    assert effect["consumed_offset"] == 1
    assert codec.decode(effect["outputs"][0]["payload"]) == "abc"


def test_an_unknown_factory_is_a_non_retryable_failure(deps: DispatchDeps) -> None:
    result = answered(work_activation(2, "x", factory="absent"), deps)
    assert result["outcome"] == "failed"
    assert result["non_retryable"] is True
    assert "absent" in result["error"]


def test_a_handler_error_becomes_a_retryable_verdict(fake_core: FakeCore) -> None:
    @flezha.procedure
    def boom(value: str) -> str:
        raise ValueError("nope")

    deps = solo_deps(boom, fake_core)
    (effect,) = answered(work_activation(3, "x", factory="solo"), deps)["effects"]
    assert effect["verdict"] == "retryable"
    assert effect["message"] == "ValueError: nope"


def test_a_non_retryable_error_says_so(fake_core: FakeCore) -> None:
    @flezha.procedure
    def refuse(value: str) -> str:
        raise NonRetryableError("bad input")

    deps = solo_deps(refuse, fake_core)
    (effect,) = answered(work_activation(4, "x", factory="solo"), deps)["effects"]
    assert effect["verdict"] == "non_retryable"


def test_a_batch_stops_at_the_first_failure(fake_core: FakeCore) -> None:
    seen: list[str] = []

    @flezha.procedure
    def picky(value: str) -> str:
        seen.append(value)
        if value == "bad":
            raise ValueError("nope")
        return value

    deps = solo_deps(picky, fake_core)
    effects = answered(work_activation(5, "ok", "bad", "never", factory="solo"), deps)["effects"]
    # Items after the failure are re-handed by the core, so the batch stops
    # rather than reporting effects the entity never saw.
    assert seen == ["ok", "bad"]
    assert [effect["verdict"] for effect in effects] == ["ok", "retryable"]


def test_a_failure_inside_complete_abandons_rather_than_retiring_the_entity(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The guard, against the collaborator that decides it.

    A real core created without ``start`` never handed this token out, so it
    refuses the completion — and refuses the abandon the guard falls back to.
    Neither refusal escapes, and both are reported.
    """
    core = native.Core.create()
    try:
        with caplog.at_level("DEBUG", logger="flezha"):
            dispatch_now(work_activation(6, "x"), core, deps_for(demo, core))
    finally:
        core.free()
    assert "unknown activation token 6" in caplog.text
    assert "abandoning activation 6 failed" in caplog.text


def test_an_async_handler_is_driven_off_an_event_loop(fake_core: FakeCore) -> None:
    @flezha.procedure
    async def slow(value: str) -> str:
        await asyncio.sleep(0)
        return value.upper()

    deps = solo_deps(slow, fake_core)
    (effect,) = answered(work_activation(7, "hi", factory="solo"), deps)["effects"]
    assert effect["outputs"][0]["payload"] == b"HI"


def test_a_filtered_input_is_consumed_and_reaches_no_handler(fake_core: FakeCore) -> None:
    seen: list[str] = []

    @flezha.procedure
    def only_long(value: str) -> str:
        seen.append(value)
        return value

    @flezha.stream_operative("filtered")
    def filtered(c: flezha.Channels) -> None:
        c.input("in").filter(lambda value: len(value) > 2).into(only_long).sink()

    deps = deps_for(filtered, fake_core)

    (effect,) = answered(work_activation(8, "ab", factory="filtered"), deps)["effects"]
    assert seen == []
    # The offset still advances: the item is consumed, not retried.
    assert effect["verdict"] == "ok"
    assert effect["consumed_offset"] == 8
    assert effect["outputs"] == []


def test_a_key_request_answers_one_entry_per_payload(fake_core: FakeCore) -> None:
    @flezha.procedure(dynamic_entities=True)
    def routed(value: dict[str, str]) -> dict[str, str]:
        return value

    @flezha.stream_operative("routed_op")
    def routed_op(c: flezha.Channels) -> None:
        c.input("in").key_by(lambda value: value["k"]).into(routed).sink()

    deps = deps_for(routed_op, fake_core)

    result = answered(
        {
            "token": 9,
            "operative": "op",
            "procedure": 0,
            "change_seq": 0,
            "factory": "routed_op",
            "kind": "key_request",
            "key_request": {
                "channel": 0,
                "payloads": [json.dumps({"k": "a"}).encode(), json.dumps({"k": "b"}).encode()],
                "want_keys": True,
                "want_event_time": False,
            },
        },
        deps,
    )
    assert result["outcome"] == "prepared"
    assert [answer["keys"] for answer in result["prepared"]] == [["a"], ["b"]]


def test_a_per_payload_refusal_does_not_fail_the_whole_crossing(fake_core: FakeCore) -> None:
    @flezha.procedure(dynamic_entities=True)
    def routed(value: dict[str, str]) -> dict[str, str]:
        return value

    def key_of(value: dict[str, str]) -> str:
        if value["k"] == "bad":
            raise ValueError("no key")
        return value["k"]

    @flezha.stream_operative("picky_keys")
    def picky_keys(c: flezha.Channels) -> None:
        c.input("in").key_by(key_of).into(routed).sink()

    deps = deps_for(picky_keys, fake_core)

    result = answered(
        {
            "token": 10,
            "operative": "op",
            "procedure": 0,
            "change_seq": 0,
            "factory": "picky_keys",
            "kind": "key_request",
            "key_request": {
                "channel": 0,
                "payloads": [json.dumps({"k": "bad"}).encode(), json.dumps({"k": "ok"}).encode()],
                "want_keys": True,
                "want_event_time": False,
            },
        },
        deps,
    )
    answers = result["prepared"]
    assert "no key" in answers[0]["error"]
    assert answers[1]["keys"] == ["ok"]


def test_a_live_state_read_answers_json(deps: DispatchDeps) -> None:
    # Straight to the stateful procedure: an answer routes no edges, so an
    # input to `echo` would never reach `count_up`.
    answered(work_activation(11, "abc", procedure=1), deps)
    result = answered(
        {
            "token": 12,
            "operative": "op",
            "procedure": 1,
            "change_seq": 0,
            "factory": "demo",
            "kind": "live_state_read",
        },
        deps,
    )
    assert result["outcome"] == "state_json"
    assert codec.from_state_json(result["json"]) == {"count": 3, "seen": ["3"]}


def test_a_snapshot_and_a_restore_fold_round_trip(deps: DispatchDeps) -> None:
    answered(work_activation(13, "abcd", procedure=1), deps)
    snapshot = answered(
        {
            "token": 14,
            "operative": "op",
            "procedure": 1,
            "change_seq": 0,
            "factory": "demo",
            "kind": "snapshot_request",
        },
        deps,
    )["state"]

    restored = answered(
        {
            "token": 15,
            "operative": "op",
            "procedure": 1,
            "entity_key": "other",
            "change_seq": 0,
            "factory": "demo",
            "kind": "restore_fold",
            "snapshot": {"bytes": snapshot},
            "changes": [],
        },
        deps,
    )
    assert restored["outcome"] == "restored"


def test_an_unknown_activation_kind_is_a_non_retryable_failure(deps: DispatchDeps) -> None:
    activation = work_activation(16, "x")
    activation["kind"] = "invented"
    result = answered(activation, deps)
    assert result["outcome"] == "failed"
    assert result["non_retryable"] is True


def test_reserved_metadata_is_refused_naming_the_key(fake_core: FakeCore) -> None:
    @flezha.procedure
    def stamped(value: str, ctx: flezha.Ctx[None]) -> None:
        ctx.emit(value, metadata={"flz.seq": "1"})

    deps = solo_deps(stamped, fake_core)
    (effect,) = answered(work_activation(17, "x", factory="solo"), deps)["effects"]
    assert effect["verdict"] == "retryable"
    assert "flz.seq" in effect["message"]


def test_a_keyed_channel_with_no_key_is_refused_naming_it(fake_core: FakeCore) -> None:
    @flezha.procedure
    def keyless(value: str, ctx: flezha.Ctx[None]) -> None:
        ctx.emit(value)

    @flezha.stream_operative("keyed_sink")
    def keyed_sink(c: flezha.Channels) -> None:
        c.input("in").into(keyless).keyed().sink("out")

    deps = deps_for(keyed_sink, fake_core)
    (effect,) = answered(work_activation(18, "x", factory="keyed_sink"), deps)["effects"]
    assert "out" in effect["message"]


def test_a_workflow_run_body_answers_workflow_done(fake_core: FakeCore) -> None:
    @flezha.workflow_operative("saga")
    def saga(args: dict[str, str], ctx: flezha.WorkflowCtx) -> str:
        return f"done:{args['id']}"

    deps = deps_for(saga, fake_core)
    result = answered(
        {
            "token": 19,
            "operative": "instance-1",
            "procedure": 0,
            "change_seq": 0,
            "factory": "saga",
            "kind": "workflow_run",
            "args": codec.encode({"id": "x"}),
        },
        deps,
    )
    assert result["outcome"] == "workflow_done"
    assert result["workflow_completed"] is True
    assert codec.decode(result["workflow_result"]) == "done:x"


def test_a_compensation_runs_over_the_recorded_outputs(fake_core: FakeCore) -> None:
    seen: list[Any] = []
    deps = solo_deps(compensating(seen), fake_core)
    result = answered(
        {
            "token": 20,
            "operative": "op",
            "procedure": 0,
            "change_seq": 0,
            "factory": "solo",
            "kind": "compensate_run",
            "outputs": [codec.encode("a"), codec.encode("b")],
        },
        deps,
    )
    assert seen == ["a", "b"]
    assert result["outcome"] == "work"


# -- the structured-workflow emit arm ------------------------------------------
#
# One released value is ONE edge output on a workflow class, untransformed and
# unkeyed, naming the producing channel and its first consumer: the driver fans
# it out from the journaled handoff. On a stream class the same topology
# releases one output per consumer, transformed and keyed. The plugin output
# rides alongside either way.


@flezha.procedure(name="wf_producer")
def _wf_producer(value: int, ctx: flezha.Ctx[None]) -> None:
    ctx.emit(value)


_DELIVERED: list[Any] = []
"""What a consumer of the ``handoff`` channel was handed, in delivery order."""


@flezha.procedure(name="wf_b")
def _wf_b(value: int) -> None:
    _DELIVERED.append(value)


@flezha.procedure(name="wf_c")
def _wf_c(value: int) -> None:
    _DELIVERED.append(value)


def _workflow_graph(c: flezha.Channels) -> None:
    edge = c.input("in").into(_wf_producer).named("handoff")
    edge.map(lambda value: value + 1).into(_wf_b)
    edge.into(_wf_c)
    edge.sink()


@flezha.workflow_operative("wf_emit")
def _wf_emit(c: flezha.Channels) -> None:
    _workflow_graph(c)


@flezha.stream_operative("wf_emit_stream")
def _wf_emit_stream(c: flezha.Channels) -> None:
    _workflow_graph(c)


def _handoff(factory: str, procedure: int, payload: bytes) -> dict[str, Any]:
    """One work activation handing `payload` to `procedure` on the handoff channel."""
    return {
        "token": 50,
        "operative": "op",
        "procedure": procedure,
        "change_seq": 0,
        "factory": factory,
        "kind": "work",
        "items": [
            {
                "kind": "input",
                "channel": 1,
                "payload": payload,
                "metadata": [],
                "consumed_offset": None,
                "replay": False,
                "now": 0,
            }
        ],
    }


def _delivered(factory: str, procedure: int, payload: bytes, deps: DispatchDeps) -> list[Any]:
    _DELIVERED.clear()
    (effect,) = answered(_handoff(factory, procedure, payload), deps)["effects"]
    assert effect["verdict"] == "ok"
    return list(_DELIVERED)


def _payload_of(output: dict[str, Any]) -> Any:
    """An edge payload is the class codec's MessagePack; the ``handoff`` sink
    names no codec, so its frame for a number is JSON."""
    if output["target"] == "plugin":
        return json.loads(output["payload"])
    return codec.decode(output["payload"])


def test_a_structured_workflow_releases_one_untransformed_edge_output(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_wf_emit, fake_core)
    (effect,) = answered(work_activation(40, 10, factory="wf_emit"), deps)["effects"]
    assert [
        (o["target"], o["consumer"], o["plugin_channel"], _payload_of(o)) for o in effect["outputs"]
    ] == [("edge", 1, None, 10), ("plugin", None, "handoff", 10)]
    assert effect["outputs"][0]["key"] is None
    assert effect["outputs"][0]["event_time"] is None


def test_an_edge_payload_is_the_encoders_own_bytes(
    fake_core: FakeCore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """MessagePack hands back finished bytes, so nothing is layered on them.

    The requirement this arms says a host whose encoder returns the bytes
    already satisfies it by doing nothing; a copy or a conversion introduced
    here would be exactly what it forbids.
    """
    produced: list[bytes] = []
    encode = codec.encode

    def recording(value: Any) -> bytes:
        made = encode(value)
        produced.append(made)
        return made

    monkeypatch.setattr(codec, "encode", recording)
    deps = deps_for(_wf_emit_stream, fake_core)
    (effect,) = answered(work_activation(43, 10, factory="wf_emit_stream"), deps)["effects"]

    assert len(effect["outputs"]) == 3
    for output in (o for o in effect["outputs"] if o["target"] == "edge"):
        assert any(made is output["payload"] for made in produced), (
            "the frame carries a copy of the bytes MessagePack returned, not the bytes"
        )


def test_the_same_graph_on_a_stream_class_releases_one_output_per_consumer(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_wf_emit_stream, fake_core)
    (effect,) = answered(work_activation(41, 10, factory="wf_emit_stream"), deps)["effects"]
    assert [(o["target"], o["consumer"], _payload_of(o)) for o in effect["outputs"]] == [
        ("edge", 1, 11),
        ("edge", 2, 10),
        ("plugin", None, 10),
    ]


# The other end of the same two arms: what the CONSUMER is handed. A stream
# producer already applied the edge, so delivery applies nothing; a workflow
# stage released the value raw, so delivery is the one application.


def test_a_stream_inter_proc_edge_applies_its_map_exactly_once(fake_core: FakeCore) -> None:
    deps = deps_for(_wf_emit_stream, fake_core)
    (effect,) = answered(work_activation(42, 10, factory="wf_emit_stream"), deps)["effects"]
    assert _delivered("wf_emit_stream", 1, effect["outputs"][0]["payload"], deps) == [11]


def test_a_workflow_consumer_applies_its_own_edge_exactly_once_at_delivery(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_wf_emit, fake_core)
    (effect,) = answered(work_activation(43, 10, factory="wf_emit"), deps)["effects"]
    released = effect["outputs"][0]["payload"]
    # One released value reaches BOTH children — the driver's fan-out — and
    # each evaluates its own edge here.
    assert _delivered("wf_emit", 1, released, deps) == [11]
    assert _delivered("wf_emit", 2, released, deps) == [10]


def test_plain_text_into_an_entry_naming_no_codec_never_reaches_the_handler(
    fake_core: FakeCore,
) -> None:
    """An entry naming no codec reads JSON, so plain text fails its decode: the
    item rides the retry ladder and the handler is never handed it."""
    seen: list[Any] = []

    @flezha.procedure
    def record(value: Any) -> None:
        seen.append(value)

    activation = work_activation(60, {"a": 1}, "placeholder", factory="solo")
    activation["items"][1]["payload"] = b"hello"
    effects = answered(activation, solo_deps(record, fake_core))["effects"]
    assert [effect["verdict"] for effect in effects] == ["ok", "retryable"]
    assert effects[1]["message"].startswith("JSONDecodeError")
    assert seen == [{"a": 1}]
