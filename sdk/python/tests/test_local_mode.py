"""The Python SDK's local mode: a user's own operative, its declared
two-procedure graph, and no controller anywhere.

The graph and its expected sink output are read from
``test/fixtures/local_mode_word_count.txt``, which the Rust, Java, Node and
Elixir local-mode suites read too — so every core is compared against one
recorded expectation instead of by inspection.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from flezha import Channels, CoreError, Ctx, LocalOperative, procedure, state, stream_operative

FIXTURE = Path(__file__).resolve().parents[3] / "test" / "fixtures" / "local_mode_word_count.txt"


@procedure(name="split")
def split(line: str, ctx: Ctx[None]) -> None:
    """One line into its words: the hop to `tally` is the runtime's job."""
    for word in line.split():
        ctx.emit(word.lower())


@state
@dataclass
class Count:
    seen: int = 0


@dataclass
class Seen:
    seen: int


@procedure(name="tally", state=Count, change=Seen, dynamic_entities=True, idle_ttl_ms=300_000)
class Tally:
    """One Entity per word, minted on first sight of it."""

    def process(self, word: str, ctx: Ctx[Count]) -> None:
        seen = ctx.state.seen + 1
        ctx.change(Seen(seen=seen))
        ctx.emit(f"{word} {seen}")

    def initial_state(self, entity_key: str | None) -> Count:
        return Count()

    def apply_change(self, state: Count, change: Seen) -> None:
        state.seen = change.seen


@stream_operative("local_word_count_python")
def word_count(c: Channels) -> None:
    c.input("lines", decode=lambda raw: bytes(raw).decode()).into(split).key_by(
        lambda word: word
    ).into(Tally).keyed().sink("totals", encode=lambda text: text.encode())


def scenario() -> tuple[list[str], Counter[str]]:
    """The scenario every core runs: the lines to feed, and the sink values."""
    lines: list[str] = []
    totals: Counter[str] = Counter()
    for row in FIXTURE.read_text().splitlines():
        if row.startswith("lines: "):
            lines.append(row.removeprefix("lines: "))
        elif row.startswith("totals: "):
            totals[row.removeprefix("totals: ")] += 1
    return lines, totals


def test_a_declared_graph_runs_locally_and_releases_the_recorded_output() -> None:
    lines, expected = scenario()
    released: list[Any] = []
    with LocalOperative.start(word_count) as run:
        run.on_sink("totals", released.append)
        for line in lines:
            run.feed("lines", line)
        run.run_until_idle(20_000)

    counted = Counter(output.payload.decode() for output in released)
    assert counted == expected, (
        "both procedures ran, the runtime performed the hop, and one Entity per word counted"
    )

    first = released[0]
    assert first.channel == "totals"
    for stamp in ("flz.entity", "flz.seq", "flz.key"):
        assert stamp in first.metadata, f"a keyed sink carries {stamp}: {first.metadata}"


def test_a_channel_the_class_does_not_declare_is_refused_by_name() -> None:
    with LocalOperative.start(word_count) as run, pytest.raises(CoreError) as caught:
        run.feed("nope", "1")
    message = str(caught.value)
    assert "nope" in message and "lines" in message, (
        f"the refusal names the channel and what is declared: {message}"
    )
