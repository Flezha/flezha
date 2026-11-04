"""Keeps the tokens a user's answers used today, and admits a message while they
are under the day's budget. One cell per user.

What it hands on is a verdict: the message, and the tokens the user had spent
when it arrived.
"""

import time
from dataclasses import dataclass
from typing import TypedDict

from demo_model import Answer
from request import Request

import flezha
from agent import Message

DAILY_TOKENS = 40


class Used(TypedDict):  # the tokens an answer used
    user: str
    tokens: int


Event = Message | Used  # what reaches a user's cell


class Verdict(TypedDict):
    message: Message
    spent: int


@flezha.state
@dataclass
class Spent:  # what each user's cell remembers
    day: str = ""
    tokens: int = 0

    def on(self, today: str) -> int:
        return self.tokens if today == self.day else 0


class Counted(TypedDict):  # how an answer's tokens are written down
    day: str
    tokens: int


def admitted(verdict: Verdict) -> bool:
    return verdict["spent"] < DAILY_TOKENS


def sent(request: Request) -> Event:  # a message to admit
    return request.message()


def used(response: flezha.Response[Message, Answer]) -> Event:  # a failed turn used none
    tokens = 0 if response.value is None else response.value["tokens"]
    return Used(user=response.request["user"], tokens=tokens)


@flezha.tasklet(name="budget")
class Budget:
    def run(self, event: Event, ctx: flezha.Ctx[Spent, Verdict]) -> None:
        today = time.strftime("%Y-%m-%d", time.gmtime(ctx.now() // 1000))
        if "tokens" in event:  # the tokens an answer used: count them
            ctx.change(Counted(day=today, tokens=event["tokens"]))
        else:  # a message to admit: hand on its verdict
            ctx.emit(Verdict(message=event, spent=ctx.state.on(today)))

    def apply_change(self, spent: Spent, counted: Counted) -> None:
        spent.tokens = spent.on(counted["day"]) + counted["tokens"]
        spent.day = counted["day"]
