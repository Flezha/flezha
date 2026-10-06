"""Counts the visits of one page and answers the new count.

The relay keys the channel by page, so each page has a cell of its own, and
`Seen` is that cell's state. The state changes only through the `Counted`
change, which is recorded as it happens, so a runner that restarts finds the
count it had.
"""

from dataclasses import dataclass
from typing import TypedDict

import flezha


class Hit(TypedDict):
    page: str


class Count(TypedDict):
    page: str
    total: int


@flezha.state
@dataclass
class Seen:  # what each page's cell remembers
    total: int = 0


class Counted(TypedDict):  # how a visit changes it
    pass


@flezha.tasklet(name="count")  # state is saved under this name, so choose it once
class CountHits:
    def run(self, hit: Hit, ctx: flezha.Ctx[Seen, Count]) -> None:
        ctx.change(Counted())
        ctx.emit(Count(page=hit["page"], total=ctx.state.total))

    def apply_change(self, seen: Seen, _counted: Counted) -> None:
        seen.total += 1
