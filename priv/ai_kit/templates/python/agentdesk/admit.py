"""Answers a message 202 when it goes on to the session, and 429 when its user is
over the budget.
"""

from budget import DAILY_TOKENS, Verdict, admitted
from request import Http

import flezha


@flezha.tasklet(name="admit")
class Admit:
    def run(self, verdict: Verdict, ctx: flezha.Ctx[None, Http]) -> None:
        user = verdict["message"]["user"]
        ctx.emit(
            Http(status=202, body=f"sent to {user}")
            if admitted(verdict)
            else Http(
                status=429,
                body=f"{user} has used {verdict['spent']} of today's {DAILY_TOKENS} tokens",
            ),
            key=user,
        )
