"""Every ``HostClassDecl`` key the core learned, spelled by the Python builder.

The fixture at ``test/fixtures/host_class_decl_keys.txt`` is the table of
record; this is the sixth reader of it. A key the core learns that this builder
cannot spell is a red build here, and a key this builder invents that the core
never learned already fails at the core's ``deny_unknown_fields``.
"""

from __future__ import annotations

import pathlib

import pytest

import flezha
from flezha import graph

_FIXTURE = (
    pathlib.Path(__file__).resolve().parents[3] / "test" / "fixtures" / "host_class_decl_keys.txt"
)


def _expected() -> dict[str, set[str]]:
    sections: dict[str, set[str]] = {}
    for line in _FIXTURE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        section, key = line.split(".", 1)
        sections.setdefault(section, set()).add(key)
    return sections


@flezha.state
class Everything:
    """A state class with a hand-written codec, so the walk has something."""

    def __init__(self) -> None:
        self.value = 0

    def to_dict(self) -> dict[str, int]:
        return {"value": self.value}

    @classmethod
    def from_dict(cls, body: dict[str, int]) -> Everything:
        made = cls()
        made.value = body["value"]
        return made


class Keyed:
    """A dynamic, event-time-clocked, compensating, migrating procedure.

    Every optional declaration at once, so the emitted keys are the full set
    rather than the subset a simpler fixture would produce.
    """

    def process(self, value: dict[str, int], ctx: flezha.Ctx[Everything]) -> None:
        ctx.change({"by": 1})

    def initial_state(self, key: str | None) -> Everything:
        return Everything()

    def apply_change(self, state: Everything, change: dict[str, int]) -> None:
        state.value += change["by"]

    def event_time(self, value: dict[str, int]) -> int:
        return value["at"]

    def compensate(self, outputs: list[object], ctx: flezha.Ctx[Everything]) -> None:
        return None

    def migrate_state(self, from_version: int, body: bytes) -> Everything:
        return Everything()

    def migrate_change(self, from_version: int, body: bytes) -> dict[str, int]:
        return {"by": 0}


keyed = flezha.procedure(
    Keyed,
    clock="event_time",
    dynamic_entities=True,
    idle_ttl_ms=60_000,
    max_entities=1024,
    mailbox_capacity=2048,
    schema_version=3,
    min_readable_schema_version=1,
)


class Statics:
    def process(self, value: dict[str, int], ctx: flezha.Ctx[None]) -> None:
        return None

    def entities_for(self, value: dict[str, int]) -> list[str]:
        return ["a"]


statics = flezha.procedure(Statics, entity_keys=["a", "b"], name="statics")


@flezha.stream_operative("every_key")
def every_key(c: flezha.Channels) -> None:
    c.effectively_once().checkpoint_every(10).outputs_at_checkpoint(500)
    c.correction_window(60_000).beyond_window_policy("drop").compact_after_bytes(1 << 20)
    c.input("in").key_by(lambda value: str(value["k"])).into(keyed).retry_max(5).retry_backoff_ms(
        100
    ).keyed().sink("out")


@flezha.stream_operative("static_key")
def static_key(c: flezha.Channels) -> None:
    c.input("in").into(statics).sink("out")


@flezha.workflow_operative(
    "every_workflow_key",
    loop_mode="chained",
    rollback_mode="best_effort",
    retry_attempts=3,
    retry_backoff_ms=250,
    barrier_retry_attempts=2,
    max_duration_ms=600_000,
    wake_after_ms=30_000,
    sync_durability=True,
)
def every_workflow_key(args: dict[str, str], ctx: flezha.WorkflowCtx) -> str:
    return "done"


def _emitted() -> dict[str, set[str]]:
    """Every key the Python builder can produce, over the fixtures above."""
    emitted: dict[str, set[str]] = {"class": set(), "channel": set(), "procedure": set(), "workflow": set()}
    for operative in (every_key, static_key, every_workflow_key):
        decl = graph.to_host_class_decl(operative.spec)
        emitted["class"] |= set(decl)
        for channel in decl.get("channels", []):
            emitted["channel"] |= set(channel)
        for procedure in decl.get("procedures", []):
            emitted["procedure"] |= set(procedure)
        emitted["workflow"] |= set(decl.get("workflow") or {})
    return emitted


def test_the_builder_spells_every_key_the_core_learned() -> None:
    expected = _expected()
    emitted = _emitted()
    for section, keys in expected.items():
        missing = keys - emitted[section]
        assert not missing, (
            f"the Python builder cannot spell {sorted(f'{section}.{key}' for key in missing)}; "
            f"teach it, or the core has a key no Python declaration can carry"
        )


def test_the_builder_invents_no_key_the_core_never_learned() -> None:
    expected = _expected()
    emitted = _emitted()
    for section, keys in emitted.items():
        extra = keys - expected[section]
        assert not extra, (
            f"the Python builder emits {sorted(f'{section}.{key}' for key in extra)}, which the "
            f"fixture does not list; the core refuses unknown fields, so this would fail at "
            f"declare time"
        )


def test_the_fixture_names_the_python_builder_as_a_reader() -> None:
    """A detector nobody knows about is a detector that lapses."""
    text = _FIXTURE.read_text()
    assert "Python" in text or "python" in text


@pytest.mark.parametrize(
    "operative,factory",
    [(every_key, "every_key"), (static_key, "static_key"), (every_workflow_key, "every_workflow_key")],
)
def test_each_declaration_carries_the_python_language_byte(
    operative: flezha.Operative, factory: str
) -> None:
    decl = graph.to_host_class_decl(operative.spec)
    assert decl["factory_id"] == factory
    assert decl["sdk_lang"] == 7
    assert decl["change_format_id"] == 5
