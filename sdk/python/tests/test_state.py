"""The state-mutation guard, and the runtime path it deliberately leaves open."""

from __future__ import annotations

import threading
from dataclasses import FrozenInstanceError, dataclass, field

import pytest

import flezha
from conftest import Bumped, Counter, count_up
from flezha import entity_state as state_module
from flezha.errors import FlezhaError
from flezha.testing import ProcedureTestHarness


def test_direct_assignment_is_refused_naming_the_field_and_the_path() -> None:
    counter = Counter()
    with pytest.raises(FlezhaError) as caught:
        counter.count = 5
    message = str(caught.value)
    assert "count" in message
    assert "ctx.change" in message


def test_construction_is_not_mutation() -> None:
    """A state object has to be built before it can be guarded."""
    assert Counter(count=3).count == 3
    assert Counter().seen == []


def test_the_change_path_succeeds_and_is_recorded() -> None:
    harness: ProcedureTestHarness[Counter] = ProcedureTestHarness(count_up)
    outcome = harness.feed("abc")
    assert harness.state.count == 3
    assert outcome.changes == [Bumped(by=3)]
    assert harness.replay_equals()


def test_recorded_changes_reproduce_the_state_on_replay() -> None:
    harness: ProcedureTestHarness[Counter] = ProcedureTestHarness(count_up)
    for value in ("a", "bb", "ccc"):
        harness.feed(value)
    live = harness.state
    rebuilt = harness.restore(changes=list(harness.log))
    assert rebuilt == live


def test_a_snapshot_plus_a_change_log_rebuilds_the_state() -> None:
    harness: ProcedureTestHarness[Counter] = ProcedureTestHarness(count_up)
    harness.feed("ab")
    snapshot = harness.snapshot()
    harness.feed("cde")
    tail = harness.log[1:]

    fresh: ProcedureTestHarness[Counter] = ProcedureTestHarness(count_up)
    assert fresh.restore(snapshot=snapshot, changes=tail) == harness.state


def test_the_runtime_permission_is_thread_local() -> None:
    """The runtime's own path opens the guard on one thread and not on others."""
    seen: list[bool] = []

    def observe() -> None:
        seen.append(state_module._mutation_permitted())

    with state_module.runtime_mutation():
        assert state_module._mutation_permitted()
        thread = threading.Thread(target=observe)
        thread.start()
        thread.join()
    assert seen == [False]
    assert not state_module._mutation_permitted()


@flezha.state
@dataclass(frozen=True)
class Frozen:
    """A class that refuses assignment on its own terms."""

    value: int = 0


def test_the_guard_refuses_before_the_class_it_wraps_can() -> None:
    """A frozen dataclass gains our refusal; its own never runs while sealed."""
    with pytest.raises(FlezhaError):
        Frozen().value = 1  # type: ignore[misc]


def test_the_guard_delegates_to_the_class_it_wraps_inside_the_runtime_window() -> None:
    """Inside the runtime window the class's own refusal is what answers."""
    frozen = Frozen()
    with state_module.runtime_mutation(), pytest.raises(FrozenInstanceError):
        frozen.value = 1  # type: ignore[misc]


def test_decorating_twice_is_idempotent() -> None:
    once = flezha.state(Counter)
    assert once is Counter


def test_a_declared_state_survives_a_nested_fold() -> None:
    """The permission nests, so a restore that folds changes stays open."""

    @flezha.state
    @dataclass
    class Nested:
        items: list[str] = field(default_factory=list)
        depth: int = 0

    class Folding:
        def process(self, value: str, ctx: flezha.Ctx[Nested]) -> None:
            ctx.change({"item": value})

        def initial_state(self, key: str | None) -> Nested:
            return Nested()

        def apply_change(self, state: Nested, change: dict[str, str]) -> None:
            state.items = [*state.items, change["item"]]
            state.depth += 1

    proc = flezha.procedure(Folding)
    harness: ProcedureTestHarness[Nested] = ProcedureTestHarness(proc)
    harness.feed("a")
    harness.feed("b")
    assert harness.state == Nested(items=["a", "b"], depth=2)
    assert harness.replay_equals()
