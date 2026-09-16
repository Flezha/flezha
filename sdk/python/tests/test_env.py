"""This runtime's reading of the shared parse fixture."""

from __future__ import annotations

import contract
from flezha import env

FIXTURE = contract.member("test/fixtures/knob_parse_cases.txt")

RULE = {
    "positive_int": env.parse_positive,
    "ms": env.parse_ms,
    "int": env.parse_int,
    "bool": env.parse_bool,
}


def cases():
    for line in FIXTURE.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        kind, raw, expected = line.split("|")
        yield kind, "" if raw == "<empty>" else raw, expected


def render(value):
    if value is None:
        return "error"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def test_every_case_parses_the_way_the_fixture_says():
    seen = set()
    for kind, raw, expected in cases():
        seen.add(kind)
        assert kind in RULE, f"the fixture names a type this runtime has no rule for: {kind}"
        assert render(RULE[kind](raw)) == expected, f"{kind}|{raw}"
    assert sorted(seen) == ["bool", "int", "ms", "positive_int"]


def test_an_unset_variable_leaves_the_default_standing():
    absent = "FLZ_TEST_ABSENT_VARIABLE"
    assert env.positive(absent, 7) == 7
    assert env.flag(absent, True) is True
