"""Ambient clock and identity reads warn, name their site, and do not abort."""

from __future__ import annotations

import datetime
import time
import uuid
import warnings

import pytest

import flezha
from flezha import determinism
from flezha.errors import DeterminismWarning
from flezha.testing import ProcedureTestHarness


@pytest.fixture(autouse=True)
def _installed() -> None:
    determinism.install()


@flezha.procedure
def reads_the_clock(value: str, ctx: flezha.Ctx[None]) -> str:
    ambient = time.time()
    return f"{value}:{ambient > 0}"


@flezha.procedure
def reads_a_uuid(value: str, ctx: flezha.Ctx[None]) -> str:
    return f"{value}:{uuid.uuid4() is not None}"


@flezha.procedure
def reads_datetime_now(value: str, ctx: flezha.Ctx[None]) -> str:
    return f"{value}:{datetime.datetime.now().year > 2000}"


@flezha.procedure
def uses_the_context(value: str, ctx: flezha.Ctx[None]) -> str:
    return f"{value}:{ctx.now()}"


def _warnings_from(proc: flezha.Procedure[str, str]) -> list[warnings.WarningMessage]:
    harness: ProcedureTestHarness[None] = ProcedureTestHarness(proc)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        outcome = harness.feed("v")
    assert outcome.returned is not None, "the invocation completes normally"
    return [w for w in caught if issubclass(w.category, DeterminismWarning)]


def test_an_ambient_clock_read_warns_and_the_invocation_completes() -> None:
    caught = _warnings_from(reads_the_clock)
    assert len(caught) == 1
    assert "ctx.now()" in str(caught[0].message)
    # The warning names the handler's own file and line, not the SDK's.
    assert caught[0].filename.endswith("test_determinism.py")


def test_an_ambient_uuid_warns_and_points_at_stable_id() -> None:
    caught = _warnings_from(reads_a_uuid)
    assert len(caught) == 1
    assert "ctx.stable_id()" in str(caught[0].message)


def test_datetime_now_warns() -> None:
    caught = _warnings_from(reads_datetime_now)
    assert len(caught) == 1
    assert "datetime.now()" in str(caught[0].message)


def test_the_context_clock_does_not_warn() -> None:
    assert _warnings_from(uses_the_context) == []


def test_nothing_warns_outside_an_invocation() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        time.time()
        uuid.uuid4()
        datetime.datetime.now()
    assert [w for w in caught if issubclass(w.category, DeterminismWarning)] == []


def test_isinstance_still_answers_about_the_real_datetime() -> None:
    """The subclass that observes ``now`` must not break every type check.

    ``datetime.now`` is a C-level classmethod, so the only way to observe it is
    to put a subclass in the module's place — and a subclass that made
    ``isinstance(a_real_datetime, datetime)`` answer ``False`` would be a far
    worse defect than the one being diagnosed.
    """
    real = determinism._REAL_DATETIME(2026, 1, 1)
    assert isinstance(real, datetime.datetime)
    assert isinstance(datetime.datetime.now(), datetime.datetime)
    assert issubclass(determinism._REAL_DATETIME, datetime.datetime)
    assert isinstance(datetime.datetime(2026, 1, 1), determinism._REAL_DATETIME)


def test_the_wrapped_originals_stay_reachable() -> None:
    assert time.time.__wrapped__() > 0  # type: ignore[attr-defined]


def test_installing_twice_does_not_stack_wrappers() -> None:
    determinism.install()
    determinism.install()
    caught = _warnings_from(reads_the_clock)
    assert len(caught) == 1
