"""The demo model: a canned answer, one token at a time, and the number of
tokens it used. It stands in for a real model so the example runs anywhere; a
real model's client would be called here instead, handing each token it streams
to `part` the same way.
"""

import re
import time
from collections.abc import Callable
from typing import TypedDict

import flezha

MAX_MESSAGE = 200
PER_TOKEN_S = 0.05


class Answer(TypedDict):  # an answer, or a part of one
    text: str
    tokens: int


def answer(earlier: list[str], message: str, part: Callable[[Answer], None]) -> Answer:
    if len(message) > MAX_MESSAGE:
        raise flezha.NonRetryableError(
            f"the demo model reads at most {MAX_MESSAGE} characters, "
            f"and this message has {len(message)}"
        )
    text = f'Message {len(earlier) + 1}: you said "{message}". I am a demo model.'
    tokens = re.split(r"(?<= )", text)
    for i, token in enumerate(tokens, start=1):
        part(Answer(text=token, tokens=i))
        time.sleep(PER_TOKEN_S)
    return Answer(text=text, tokens=len(tokens))
