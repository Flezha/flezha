"""Answers a message with the demo model, sending the answer as it grows, and
keeps the conversation so the next answer knows what came before.
"""

from dataclasses import dataclass, field
from typing import Literal, NotRequired, TypedDict

import demo_model
from demo_model import Answer

import flezha


@flezha.state
@dataclass
class Conversation:  # what the session remembers
    messages: list[str] = field(default_factory=list)


class Edit(TypedDict):  # a message said, or the last one taken back
    kind: Literal["said", "unsaid"]
    message: NotRequired[str]


@flezha.tasklet(name="reply")
class Reply:
    def run(self, message: str, ctx: flezha.Ctx[Conversation, Answer]) -> None:
        # Each piece streams out as it is made; then the whole answer.
        ctx.emit(demo_model.answer(ctx.state.messages, message, ctx.part))
        ctx.change(Edit(kind="said", message=message))

    def apply_change(self, conversation: Conversation, edit: Edit) -> None:
        match edit:
            case {"kind": "said", "message": message}:
                conversation.messages.append(message)
            case _:
                conversation.messages.pop()

    # The undo, when a turn is taken back: the conversation forgets its message.
    def compensate(self, outputs: list[Answer], ctx: flezha.Ctx[Conversation, Answer]) -> None:
        print(f'[undo] forgot "{ctx.state.messages[-1]}"', flush=True)
        ctx.change(Edit(kind="unsaid"))
