"""One HTTP request, as the plugin hands it to the gateway, and the gateway's
answer to one. The gateway knows two routes: `POST /sessions/<user>/messages`,
whose body is the message, and `GET /sessions/<user>/events`, the session's
answers as they are written.
"""

import re
from dataclasses import dataclass
from typing import Any, TypedDict

from agent import Message

ROUTE = re.compile(r"/sessions/([a-z][a-z0-9_-]{0,63})/(messages|events)")


class Http(TypedDict, total=False):  # a status and a body, or the session to stream
    status: int
    body: str
    stream: str


@dataclass(frozen=True)
class Request:
    method: str
    path: str
    body: str | None

    # The envelope carries more than the gateway reads.
    @classmethod
    def of(cls, envelope: dict[str, Any]) -> "Request":
        return cls(envelope["method"], envelope["path"], envelope.get("body"))

    def sends(self) -> bool:
        return self._is("POST", "messages")

    def listens(self) -> bool:
        return self._is("GET", "events")

    def user(self) -> str:
        return self._route(1)

    def message(self) -> Message:
        return Message(user=self.user(), text=self.body or "")

    def _is(self, verb: str, what: str) -> bool:
        return self.method == verb and self._route(2) == what

    def _route(self, part: int) -> str:  # "" when the path is not a route
        route = ROUTE.fullmatch(self.path)
        return route[part] if route else ""
