"""The HTTP side of the desk: a flow relay that carries the HTTP plugin,
answers every request, and calls the session a message is for.
"""

from operator import itemgetter

from admit import Admit
from budget import Budget, admitted, sent, used
from request import Request
from watch import Watch

import flezha

DEADLINE_MS = 60_000


# each request and answer on disk before it runs
@flezha.relay("gateway", run={"flow": {"exactly": "effectively_once"}})
def gateway(c: flezha.ChannelBuilder) -> None:
    user = itemgetter("user")
    # every request arrives on "http"; kept as a handle to split it
    http = c.input("http").map(Request.of)
    # route 1: GET .../events gets the session's stream
    http.filter(lambda request: not request.sends()).into(
        Watch
    ).keyed().output()  # a stream, or 404
    # route 2: POST .../messages is checked against the user's budget
    verdicts = (
        http.filter(Request.sends)
        .map(sent)
        .key_by(user)  # one cell per user
        .into(Budget)
    )
    # a verdict: the message plus its user's tokens today; used twice
    # use 1: answer the POST
    verdicts.into(Admit).keyed().output()  # 202, or 429 over the budget
    # use 2: pass an admitted message on to the user's session
    (
        verdicts.filter(admitted)
        .map(itemgetter("message"))
        .key_by(user)  # the user's session
        # call the "agent" relay; its answer comes back here
        .call(factory="agent", channel="message", deadline_ms=DEADLINE_MS)
        .map(used)  # the session's answer, as tokens used
        .key_by(user)
        .into(Budget)  # back into the same budget
    )
