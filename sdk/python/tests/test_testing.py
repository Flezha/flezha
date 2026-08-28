"""The procedure test harness, at parity with the Rust and Java ones."""

from __future__ import annotations

import asyncio

import pytest

import flezha
from flezha.testing import ProcedureTestHarness, ReplayMismatch, replay_equals

from conftest import Bumped, Counter, count_up, echo


def test_a_handler_is_testable_with_no_runtime_and_no_controller() -> None:
    """The decorator returns something that forwards, not something to drive."""
    assert echo("plain") == "plain"


def test_the_harness_reports_state_changes_and_emits() -> None:
    harness: ProcedureTestHarness[Counter] = ProcedureTestHarness(count_up)
    outcome = harness.feed("abcd")
    assert harness.state == Counter(count=4, seen=["4"])
    assert outcome.changes == [Bumped(by=4)]
    assert outcome.outputs == [{"total": 4}]


def test_apply_inputs_runs_them_in_order() -> None:
    harness: ProcedureTestHarness[Counter] = ProcedureTestHarness(count_up)
    harness.apply_inputs(["a", "bb", "ccc"])
    assert harness.state == Counter(count=6, seen=["1", "2", "3"])


def test_construction_from_an_initial_state() -> None:
    harness: ProcedureTestHarness[Counter] = ProcedureTestHarness(
        count_up, initial_state=Counter(count=10, seen=["seeded"])
    )
    harness.feed("a")
    assert harness.state.count == 11


def test_construction_from_a_restore_payload() -> None:
    source: ProcedureTestHarness[Counter] = ProcedureTestHarness(count_up)
    source.apply_inputs(["a", "bb"])
    snapshot = source.snapshot()

    restored: ProcedureTestHarness[Counter] = ProcedureTestHarness(count_up)
    assert restored.restore(snapshot=snapshot) == source.state


def test_the_replay_gate_passes_on_a_pure_fold() -> None:
    assert replay_equals(count_up, ["a", "bb", "ccc"])


def test_the_replay_gate_reports_both_states_on_a_mismatch() -> None:
    """An impure fold is lost on restart and reported nowhere until then."""
    drift = {"n": 0}

    class Impure:
        def process(self, value: str, ctx: flezha.Ctx[dict[str, int]]) -> None:
            ctx.change({"by": 1})

        def initial_state(self, key: str | None) -> dict[str, int]:
            return {"total": 0}

        def apply_change(self, state: dict[str, int], change: dict[str, int]) -> None:
            # Not a pure fold: the answer depends on how many times it has run.
            drift["n"] += 1
            state["total"] += change["by"] * drift["n"]

    proc = flezha.procedure(Impure)
    harness: ProcedureTestHarness[dict[str, int]] = ProcedureTestHarness(proc)
    harness.apply_inputs(["a", "b"])
    with pytest.raises(ReplayMismatch) as caught:
        harness.replay_equals()
    assert caught.value.live != caught.value.replayed


def test_the_replay_gate_catches_a_mutation_that_recorded_no_change() -> None:
    """The gate catches what the state guard cannot: an unguarded state shape."""

    class Sneaky:
        def process(self, value: str, ctx: flezha.Ctx[dict[str, int]]) -> None:
            # A plain dict is not a declared state, so nothing refuses this —
            # the replay gate is what reports it.
            ctx.state["total"] = ctx.state["total"] + 1

        def initial_state(self, key: str | None) -> dict[str, int]:
            return {"total": 0}

        def apply_change(self, state: dict[str, int], change: dict[str, int]) -> None:
            state["total"] += change["by"]

    proc = flezha.procedure(Sneaky)
    harness: ProcedureTestHarness[dict[str, int]] = ProcedureTestHarness(proc)
    harness.feed("a")
    with pytest.raises(ReplayMismatch):
        harness.replay_equals()


def test_timers_are_recorded_and_fired() -> None:
    fired: list[int] = []

    class Timed:
        def process(self, value: str, ctx: flezha.Ctx[None]) -> None:
            ctx.timer_after(1000)

        def on_timer(self, timer: dict[str, object], ctx: flezha.Ctx[None]) -> None:
            fired.append(int(timer["scheduled_at"]))  # type: ignore[arg-type]

    proc = flezha.procedure(Timed)
    harness: ProcedureTestHarness[None] = ProcedureTestHarness(proc)
    outcome = harness.feed("x", now=500)
    assert outcome.timers == [{"kind": "processing_time", "scheduled_at": 1500, "register": True}]
    harness.fire_timer("processing_time", 1500)
    assert fired == [1500]


def test_event_time_is_advanced_through_the_registered_timers() -> None:
    fired: list[int] = []

    class EventTimed:
        def process(self, value: dict[str, int], ctx: flezha.Ctx[None]) -> None:
            ctx.timer_at_event_time(value["at"])

        def on_timer(self, timer: dict[str, object], ctx: flezha.Ctx[None]) -> None:
            fired.append(int(timer["scheduled_at"]))  # type: ignore[arg-type]

    proc = flezha.procedure(EventTimed)
    harness: ProcedureTestHarness[None] = ProcedureTestHarness(proc)
    harness.feed({"at": 300})
    harness.feed({"at": 100})
    harness.feed({"at": 900})
    harness.advance_event_time(500)
    assert fired == [100, 300]


def test_a_compensation_is_driven_over_recorded_outputs() -> None:
    seen: list[object] = []

    class Compensating:
        def process(self, value: str, ctx: flezha.Ctx[None]) -> None:
            ctx.emit(value)

        def compensate(self, outputs: list[object], ctx: flezha.Ctx[None]) -> None:
            seen.extend(outputs)

    proc = flezha.procedure(Compensating)
    harness: ProcedureTestHarness[None] = ProcedureTestHarness(proc)
    harness.compensate(["a", "b"])
    assert seen == ["a", "b"]


def test_an_async_handler_needs_no_event_loop_in_the_test() -> None:
    @flezha.procedure
    async def slow(value: str) -> str:
        await asyncio.sleep(0)
        return value.upper()

    harness: ProcedureTestHarness[None] = ProcedureTestHarness(slow)
    assert harness.feed("hi").outputs == ["HI"]


def test_the_context_clock_and_identity_are_stable_within_an_invocation() -> None:
    ids: list[str] = []

    @flezha.procedure
    def reads(value: str, ctx: flezha.Ctx[None]) -> None:
        assert ctx.now() == ctx.now() == ctx.now()
        ids.append(ctx.stable_id())
        ids.append(ctx.stable_id())

    harness: ProcedureTestHarness[None] = ProcedureTestHarness(reads, entity_key="e")
    harness.feed("x", now=99)
    assert ids[0] != ids[1]

    replayed: list[str] = []
    ids_backup = list(ids)
    ids.clear()
    again: ProcedureTestHarness[None] = ProcedureTestHarness(reads, entity_key="e")
    again.feed("x", now=99)
    replayed = list(ids)
    # The n-th call of a replayed invocation equals the n-th of the original.
    assert replayed == ids_backup


def test_a_missing_hook_is_refused_by_name() -> None:
    harness: ProcedureTestHarness[Counter] = ProcedureTestHarness(count_up)
    with pytest.raises(flezha.FlezhaError, match="on_timer"):
        harness.fire_timer("processing_time", 1)
    with pytest.raises(flezha.FlezhaError, match="compensate"):
        harness.compensate([])
