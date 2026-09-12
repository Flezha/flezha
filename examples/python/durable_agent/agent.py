"""A durable agent loop: model call, retry ladder, compensation, suspension.

The shape this SDK is aimed at. An agent turn is a sequence of side effects —
call a model, act on what it said, and undo the action if a later step fails —
and every one of those is a place a process can die. The engine makes the
sequence survivable; this file is only the part that is about the agent.

What each piece is doing, and why it is not the obvious thing:

* **The state is declared, and mutated only through changes.** The transcript
  IS the durable record: on a restart the entity is rebuilt by folding the
  change log, so an append that recorded no change would be live now and gone
  after the restart. ``@flezha.state`` refuses the direct assignment rather
  than letting that happen.
* **The retry ladder is the channel's, not the handler's.** A model call that
  fails transiently raises and is retried on the edge's declared ladder, then
  parked to the dead-letter queue. A refusal the model will never accept
  differently raises ``NonRetryableError`` and parks after one attempt instead
  of burning the budget.
* **Identity and time come from ``ctx``.** A tool call stamped with
  ``uuid.uuid4()`` would get a different id on replay, and the SDK warns when a
  handler reads one; ``ctx.stable_id()`` reproduces.
* **Compensation is declared, not remembered.** ``compensate`` receives the
  forward outputs the engine recorded, so an undo does not depend on the agent
  having written down what it did.
* **Suspension is waiting without holding anything.** The workflow body parks
  on its inbox; a suspended workflow holds no runner resources, and a delivery
  wakes it wherever it is rehydrated.

Run it against a live controller with::

    scripts/build-python-sdk.sh
    sdk/python/.venv/bin/python examples/python/durable_agent/agent.py
"""

from __future__ import annotations

import os
import random
from dataclasses import dataclass, field
from typing import Any

import flezha

# ---------------------------------------------------------------------------
# The durable state of one conversation
# ---------------------------------------------------------------------------


@dataclass
class Turn:
    """One exchange, as the transcript records it."""

    call_id: str
    prompt: str
    answer: str


@flezha.state
@dataclass
class Conversation:
    """Everything a restarted process needs to continue this conversation."""

    turns: list[Turn] = field(default_factory=list)
    spent_tokens: int = 0
    booked: list[str] = field(default_factory=list)


@dataclass
class TurnRecorded:
    """The declared change: everything one turn adds to the conversation.

    One class rather than a union of variants, because a durable body carries
    no tag saying which arm wrote it — every SDK answers this the same way, and
    the SDK refuses a union change annotation naming this choice. Rust spells
    it as an enum; here the variants are fields, and an absent one is ``None``.
    """

    turn: Turn
    tokens: int
    booked: str | None = None


# ---------------------------------------------------------------------------
# The agent turn
# ---------------------------------------------------------------------------


class ModelUnavailable(Exception):
    """A transient failure. The channel's retry ladder handles it."""


class Agent:
    """One agent turn: call the model, act, record, emit."""

    def process(self, prompt: str, ctx: flezha.Ctx[Conversation]) -> None:
        # Deterministic and replay-stable: the n-th call of a replayed
        # invocation returns the n-th id of the original, so a retry does not
        # mint a second tool call for the same turn.
        call_id = ctx.stable_id()

        answer = self._ask_model(prompt, call_id)

        booked = answer.removeprefix("book ").strip() if answer.startswith("book ") else None

        # Recorded before the emit: if the process dies between them the engine
        # re-hands the activation and the fold is idempotent, whereas an
        # unrecorded booking would be invisible to the undo.
        ctx.change(
            TurnRecorded(
                turn=Turn(call_id=call_id, prompt=prompt, answer=answer),
                tokens=len(prompt) + len(answer),
                booked=booked,
            )
        )
        ctx.emit(answer, key=call_id)

    def initial_state(self, entity_key: str | None) -> Conversation:
        return Conversation()

    def apply_change(self, state: Conversation, change: TurnRecorded) -> None:
        """The fold. Pure, in place, and the only way the state moves.

        It runs live and again on every restore, so it must produce the same
        state from the same log — which is what ``replay_equals()`` in
        ``test_agent.py`` gates.
        """
        state.turns.append(change.turn)
        state.spent_tokens += change.tokens
        if change.booked is not None:
            state.booked.append(change.booked)

    def compensate(self, outputs: list[Any], ctx: flezha.Ctx[Conversation]) -> None:
        """Undo what this turn did, from what the engine recorded it doing.

        The engine hands back the forward outputs; the agent does not have to
        have remembered them, which is the difference between a saga that works
        after a crash and one that works only when nothing crashed.
        """
        for reservation in ctx.state.booked:
            print(f"[agent] cancelling reservation {reservation}", flush=True)
        del outputs

    # -- the side effect ---------------------------------------------------

    def _ask_model(self, prompt: str, call_id: str) -> str:
        """Stand-in for a real model call.

        A real one raises on a 5xx or a timeout and raises
        :class:`flezha.NonRetryableError` on a refusal or a malformed prompt.
        The distinction is the whole point: one is worth the ladder and the
        other is not.
        """
        if not prompt.strip():
            raise flezha.NonRetryableError("an empty prompt will not become valid on a retry")
        if os.environ.get("AGENT_FLAKY") == "1" and random.random() < 0.4:
            raise ModelUnavailable("the model endpoint answered 503")
        if prompt.startswith("book "):
            return f"book {prompt.removeprefix('book ').strip()}"
        return f"answered({call_id[:8]}): {prompt}"


# One entity per conversation, appearing on first sight of its key: an agent
# deployment does not know its users in advance, and `idle_ttl_ms` is what stops
# a finished conversation holding memory forever.
agent = flezha.procedure(
    Agent, name="agent_turn", dynamic_entities=True, idle_ttl_ms=15 * 60 * 1000
)


@flezha.stream_operative("durable_agent")
def durable_agent(c: flezha.Channels) -> None:
    """One conversation per key, with a retry ladder on the model call.

    The key is the text before the first ``|``, so ``user-1|hello`` reaches
    user-1's conversation. Every edge into a dynamic procedure must name the
    entity its inputs reach; the SDK refuses one that does not.
    """
    c.input("prompts", decode=lambda body: bytes(body).decode()).retry_max(4).retry_backoff_ms(
        250
    ).key_by(lambda prompt: prompt.split("|", 1)[0]).into(agent).keyed().sink(
        "answers", encode=lambda answer: str(answer).encode()
    )


# ---------------------------------------------------------------------------
# The long-running side: a workflow that suspends while it waits
# ---------------------------------------------------------------------------


@flezha.workflow_operative("agent_session", max_duration_ms=3_600_000)
def agent_session(args: dict[str, Any], ctx: flezha.WorkflowCtx) -> str | None:
    """A session that waits for the human between turns.

    ``recv`` parks with the GIL released, so waiting costs one thread and
    nothing else; a suspended workflow holds no runner resources at all, and a
    delivery wakes it wherever it is rehydrated.
    """
    goal = (args or {}).get("goal", "help the user")
    ctx.emit({"event": "session_started", "goal": goal})

    turns = 0
    while not ctx.cancelled():
        message = ctx.recv(timeout_ms=30_000)
        if message is None:
            continue
        if message == "done":
            break
        turns += 1
        ctx.emit({"event": "turn", "n": turns, "message": message})

    ctx.complete({"goal": goal, "turns": turns})
    return None


if __name__ == "__main__":
    flezha.configure_logging("info")
    (
        flezha.create_runner(os.environ.get("FLEZHA_NAMESPACE", "default"))
        .define_stream(durable_agent)
        .define_workflow(agent_session)
        .run()
    )
