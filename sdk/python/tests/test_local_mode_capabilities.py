"""Local mode over the five capabilities the LANGUAGE LAYER owns, each driven
through the fixture class ``test/apps/python_runner/conformance.json`` names for
it: the same class the end-to-end scenario drives, so what passes here and what
passes there cannot be two different graphs.

The other ten capabilities in the vocabulary are the controller's: persistence,
effectively-once, correction, partitioning, workflows, liveness, restarts. Local
mode refuses them by name rather than approximating them.

What each test can assert is what a run with no controller can see. A durable
write reaches a null sink, a parked payload has nowhere to park, and a partition
member's key range arrives with ``AddOperative``, so the assertions here are the
output side of each capability, which is the half the language layer produces.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable

import pytest

import app
from flezha import CoreError, LocalOperative, Operative, SinkOutput

fixtures = app.module("fixtures")
fixtures_state = app.module("fixtures_state")

#: The app's own key convention, so a test keys a value the way the class did.
prefix = app.module("fixture_support").prefix

#: The app's own registration list, keyed by the id `conformance.json` names.
_REGISTERED: dict[str, Operative] = {
    operative.spec.factory_id: operative for operative in fixtures.ALL
}


def release(
    operative: Operative,
    channel: str,
    sink: str,
    inputs: Iterable[str],
    budget_ms: int = 20_000,
) -> list[SinkOutput]:
    """Run one class over ``inputs`` on ``channel``, and answer what ``sink`` released.

    Read after the run is closed, not at the moment it settles: a released value
    can still be queued for the drain when the graph has nothing left to do, and
    closing is what hands the last of them over.
    """
    released: list[SinkOutput] = []
    with LocalOperative.start(operative) as run:
        run.on_sink(sink, released.append)
        for value in inputs:
            run.feed(channel, value)
        run.run_until_idle(budget_ms)
    return released


def per_entity(
    released: Iterable[SinkOutput], key: Callable[[SinkOutput], str]
) -> dict[str, list[str]]:
    """What each key released, in that key's own order.

    Two keys are two Entities with their own mailboxes, and this host dispatches
    from a thread pool, so the order a run can assert is the one the runtime
    guarantees: within an Entity, never one sequence across them.
    """
    grouped: dict[str, list[str]] = {}
    for output in released:
        grouped.setdefault(key(output), []).append(output.payload.decode())
    return grouped


def declares(capability: str, factory_id: str) -> Operative:
    """The class under test, which is the one the manifest names for the capability."""
    listed = app.declared_fixtures()[capability]
    assert factory_id in listed, (
        f"conformance.json lists {listed} for {capability}, not {factory_id}; "
        "the local-mode run must drive a class the e2e drives"
    )
    return _REGISTERED[factory_id]


def test_graph_builder_a_declared_graph_is_built_and_run_with_no_controller() -> None:
    released = release(
        declares("graph_builder", "sugar_do_nothing"),
        "input",
        "sugar_do_nothing_echo",
        ["alpha", "beta"],
    )

    assert [output.payload.decode() for output in released] == ["alpha", "beta"], (
        "the builder produced a graph the core ran, in order"
    )


def test_keyed_channels_a_keyed_sink_stamps_the_key_the_class_resolved() -> None:
    # `partition_relay` is the manifest's only keyed-channel class. Partitioning
    # itself is the controller's (a member's key-group range arrives with
    # `AddOperative`), so what is asserted here is the half the language layer
    # owns: the key it resolves and the stamps it puts on the released value.
    released = release(
        declares("keyed_channels", "partition_relay"),
        "in",
        "input",
        ["a:1", "b:1", "a:2"],
    )

    assert per_entity(released, lambda output: output.metadata["flz.key"]) == {
        "a": ["a:1", "a:2"],
        "b": ["b:1"],
    }, "each release carries the key its own value resolved to, in that Entity's order"
    for stamp in ("flz.entity", "flz.seq"):
        assert stamp in released[0].metadata, f"a keyed sink carries {stamp}"


def test_dynamic_entity_the_declared_filter_runs_before_keying_and_nothing_else_is_lost() -> None:
    # The class's sink is not keyed, so a released value carries no entity stamp
    # and the per-key minting is not visible on this channel. It is asserted
    # above, on `partition_relay`, whose procedure is dynamic too and whose sink
    # IS keyed. What this class shows that that one does not is the declared
    # filter: a value it rejects answers no key, so the core is never asked to
    # mint an Entity for it.
    released = release(
        declares("dynamic_entity", "dynamic_entity"),
        "input",
        "kafka",
        ["a:1", "b:1", "drop", "a:2"],
    )

    # The sink is unkeyed, so a release carries no key stamp to group by; the
    # value's own prefix is what the declaration keyed it on.
    assert per_entity(released, lambda output: prefix(output.payload.decode())) == {
        "a": ["a:1", "a:2"],
        "b": ["b:1"],
    }, "the filter dropped `drop` before it was keyed, and every other value reached its Entity"
    assert [output.metadata for output in released] == [{"channel": "kafka"}] * 3, (
        "an unkeyed sink carries its channel and no identity stamps"
    )


def test_retry_ladder_a_bounded_mailbox_that_drops_nothing_loses_no_input() -> None:
    # `poison_probe` is the manifest's other class for this capability and its
    # poison path ends in the dead-letter queue, which is the controller's. The
    # ladder's other arm, a full mailbox under `drop_nothing` where the producer
    # is held rather than the value discarded, is entirely the language layer's,
    # and is what a run with no controller can see.
    inputs = [f"n{at}" for at in range(24)]
    released = release(
        declares("retry_ladder", "bounded_mailbox_drop_nothing"),
        "drop_nothing_input",
        "drop_nothing_echo",
        inputs,
    )

    assert [output.payload.decode() for output in released] == inputs, (
        "the mailbox holds 4 and 24 were fed: under `drop_nothing` the feed waits, in order"
    )


def test_entity_timers_a_declared_timer_fires_locally_with_its_scheduled_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_timeout = declares("entity_timers", "session_timeout")

    # The class reads its own deadline from the environment, the way the e2e
    # sets it; 200 ms keeps the run short without changing what is proved.
    monkeypatch.setenv("TIMER_TEST_TIMEOUT_MS", "200")
    released = release(session_timeout, "input", "kafka", ["open"])

    payloads = [output.payload.decode() for output in released]
    closed = [payload for payload in payloads if payload.startswith("closed@")]
    assert len(closed) == 1, f"the timer fired exactly once: {payloads}"
    assert re.fullmatch(r"closed@\d+", closed[0]), (
        "the firing carries the timestamp it was scheduled for"
    )


def test_a_controller_capability_is_refused_by_name_not_approximated() -> None:
    # The line `sdk-local-mode` draws: the ten the controller owns stay refused.
    # `tier_store` is effectively-once, which needs a durable store to release
    # from, so a local run says so instead of running the graph.
    with pytest.raises(CoreError) as caught:
        LocalOperative.start(fixtures_state.tier_store_operative)

    message = str(caught.value)
    assert "effectively_once" in message and "requires a controller" in message, message
