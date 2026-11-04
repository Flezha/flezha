"""Answers a request for a session's events with its stream, and any other
request with 404.
"""

from request import Http, Request

import flezha


@flezha.tasklet(name="watch")
class Watch:
    def run(self, request: Request, ctx: flezha.Ctx[None, Http]) -> None:
        # keyed by the user: an output of an effectively-once relay needs a key
        ctx.emit(
            Http(stream=request.user())
            if request.listens()
            else Http(status=404, body="no such route"),
            key=request.user(),
        )
