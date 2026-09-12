"""The two-procedure word count, run in this process with no controller.

    scripts/build-python-sdk.sh
    sdk/python/.venv/bin/python examples/python/local_word_count.py

`split` and `Tally` are ordinary procedures with nothing local-mode about
them: the same declaration deploys against a controller unchanged. What local
mode supplies is the sink — the values `Tally` releases arrive here instead of
on a socket.

State is held in memory and nothing is durable; the run says so on stderr.
"""

from __future__ import annotations

from dataclasses import dataclass

from flezha import Channels, Ctx, LocalOperative, procedure, state, stream_operative


@procedure(name="split")
def split(line: str, ctx: Ctx[None]) -> None:
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
    def process(self, word: str, ctx: Ctx[Count]) -> None:
        seen = ctx.state.seen + 1
        ctx.change(Seen(seen=seen))
        ctx.emit(f"{word} {seen}")

    def initial_state(self, entity_key: str | None) -> Count:
        return Count()

    def apply_change(self, state: Count, change: Seen) -> None:
        state.seen = change.seen


@stream_operative("local_word_count_example")
def word_count(c: Channels) -> None:
    # One Entity per word: the hop from `split` to `Tally` is the runtime's,
    # not this file's.
    c.input("lines", decode=lambda raw: bytes(raw).decode()).into(split).key_by(
        lambda word: word
    ).into(Tally).keyed().sink("totals", encode=lambda text: text.encode())


if __name__ == "__main__":
    with LocalOperative.start(word_count) as run:
        run.on_sink("totals", lambda output: print(output.payload.decode()))
        run.feed("lines", "the quick brown fox")
        run.feed("lines", "the lazy dog")
        run.run_until_idle(10_000)
