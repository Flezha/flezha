"""The durable codec: what it carries, what it refuses, and what it never is.

``pickle`` is the one thing this codec must never become, so its absence is
asserted over the loaded source rather than trusted — a later change that adds
an import fails here.
"""

from __future__ import annotations

import ast
import datetime
import pathlib
from dataclasses import dataclass, field

import pytest

from flezha import codec
from flezha.errors import UnrepresentableValueError

# Every module of the package: the persistence and wire paths are all of it,
# because there is no part of this SDK a durable body does not pass through.
_PACKAGE = pathlib.Path(codec.__file__).parent
_PICKLE_MODULES = ("pickle", "cPickle", "dill", "shelve")


def test_pickle_is_absent_from_the_persistence_and_wire_paths() -> None:
    """A later change cannot reintroduce ``pickle`` without failing here.

    Loading a pickle executes code, and its payloads embed class paths so a
    refactor breaks the restore of every record written before it. Both reasons
    are independent of taste, which is why this is a test and not a review note.
    """
    offenders: list[str] = []
    for path in sorted(_PACKAGE.glob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                offenders += [
                    f"{path.name}:{node.lineno}"
                    for alias in node.names
                    if alias.name.split(".")[0] in _PICKLE_MODULES
                ]
            elif (
                isinstance(node, ast.ImportFrom)
                and (node.module or "").split(".")[0] in _PICKLE_MODULES
            ):
                offenders.append(f"{path.name}:{node.lineno}")
    assert offenders == [], f"pickle reached the durable path at {offenders}"


# ---------------------------------------------------------------------------
# Round trips
# ---------------------------------------------------------------------------


def test_plain_values_round_trip() -> None:
    for value in (None, True, 7, -7, 1.5, "text", b"\x00\xff", [1, 2], {"a": 1}):
        assert codec.decode(codec.encode(value)) == value


def test_integers_round_trip_without_narrowing() -> None:
    """u64 offsets, seqs and timestamps must not narrow.

    Python's integers are arbitrary-precision, so the hazard other SDKs carry —
    a double losing precision above 2^53, a signed ceiling at 2^63 — has no
    analogue here. Above 2^64 MessagePack itself has no integer, and raises
    rather than truncating, which is the outcome that matters.
    """
    for value in (2**53 + 1, 2**63, 2**63 + 12345, 2**64 - 1):
        assert codec.decode(codec.encode(value)) == value
    with pytest.raises(OverflowError):
        codec.encode(2**64)


def test_a_datetime_keeps_its_type() -> None:
    when = datetime.datetime(2026, 8, 28, 12, 30, tzinfo=datetime.UTC)
    assert codec.decode(codec.encode(when)) == when


def test_a_view_is_decoded_without_a_copy() -> None:
    """The codec reads any buffer, which is what makes the veneer's payload view
    usable with no intermediate ``bytes``."""
    body = codec.encode({"a": 1})
    assert codec.decode(memoryview(body)) == {"a": 1}


# ---------------------------------------------------------------------------
# Declared types
# ---------------------------------------------------------------------------


@dataclass
class Item:
    name: str
    tags: list[str] = field(default_factory=list)


@dataclass
class Basket:
    items: list[Item] = field(default_factory=list)
    total: int = 0


def test_a_dataclass_round_trips_through_its_fields() -> None:
    basket = Basket(items=[Item("a", ["x"]), Item("b")], total=2)
    codec_ = codec.codec_for(Basket, "state")
    assert codec_ is not None
    assert codec_.decode(codec_.encode(basket)) == basket


def test_an_added_field_with_a_default_stays_loadable() -> None:
    """State written by an older declaration loads after a field is added."""
    old_body = codec.encode({"name": "a", "tags": []})

    @dataclass
    class ItemV2:
        name: str
        tags: list[str] = field(default_factory=list)
        priority: int = 3

    codec_ = codec.codec_for(ItemV2, "state")
    assert codec_ is not None
    assert codec_.decode(old_body) == ItemV2(name="a", tags=[], priority=3)


def test_a_removed_field_does_not_fail_the_restore() -> None:
    body = codec.encode({"name": "a", "tags": [], "retired": 1})
    codec_ = codec.codec_for(Item, "state")
    assert codec_ is not None
    assert codec_.decode(body) == Item(name="a", tags=[])


def test_a_tuple_field_comes_back_a_tuple() -> None:
    """MessagePack has no tuple, so the annotation is what restores one."""

    @dataclass
    class Pairs:
        values: tuple[int, ...] = ()

    codec_ = codec.codec_for(Pairs, "state")
    assert codec_ is not None
    assert codec_.decode(codec_.encode(Pairs(values=(1, 2)))) == Pairs(values=(1, 2))


def test_the_hand_written_escape_is_used_when_present() -> None:
    class Custom:
        def __init__(self, raw: str) -> None:
            self.raw = raw

        def to_dict(self) -> dict[str, str]:
            return {"raw": self.raw}

        @classmethod
        def from_dict(cls, body: dict[str, str]) -> Custom:
            return cls(body["raw"])

        def __eq__(self, other: object) -> bool:
            return isinstance(other, Custom) and other.raw == self.raw

    codec_ = codec.codec_for(Custom, "state")
    assert codec_ is not None
    assert codec_.decode(codec_.encode(Custom("x"))) == Custom("x")


def test_a_type_with_no_adapter_is_refused_naming_it() -> None:
    class Bare:
        pass

    with pytest.raises(UnrepresentableValueError, match="Bare"):
        codec.codec_for(Bare, "state")


def test_a_set_is_refused_with_its_reason() -> None:
    with pytest.raises(UnrepresentableValueError, match="set"):
        codec.assert_representable("s", {"tags": {"a"}})


def test_a_cycle_is_refused() -> None:
    looped: dict[str, object] = {}
    looped["self"] = looped
    with pytest.raises(UnrepresentableValueError, match="refers back to itself"):
        codec.assert_representable("s", looped)


def test_a_callable_field_is_refused() -> None:
    with pytest.raises(UnrepresentableValueError, match="callable"):
        codec.assert_representable("s", {"fn": lambda: None})


# ---------------------------------------------------------------------------
# The JSON form
# ---------------------------------------------------------------------------


def test_the_json_form_marks_what_json_cannot_carry() -> None:
    when = datetime.datetime(2026, 8, 28, tzinfo=datetime.UTC)
    value = {"raw": b"\x00\xff", "at": when, "n": 1}
    assert codec.from_state_json(codec.to_state_json(value)) == value


def test_the_json_form_of_a_declared_state_is_its_fields() -> None:
    body = codec.to_state_json(Item("a", ["x"]))
    assert codec.from_state_json(body) == {"name": "a", "tags": ["x"]}


def test_an_empty_json_body_is_none() -> None:
    assert codec.from_state_json(b"") is None
    assert codec.decode(b"") is None
