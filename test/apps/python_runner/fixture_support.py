"""Helpers every fixture module shares: the wire codecs the reference's
`String` payloads need, the environment readers the tunable fixtures use, and
the pass-through procedure six operatives declare identically.
"""

from __future__ import annotations

import os
from typing import Any

import flezha


def text(body: Any) -> str:
    """The wire bytes as the reference's `String` entry payload."""
    return bytes(body).decode()


def raw(value: Any) -> bytes:
    """A plugin frame as the reference's bytes."""
    return str(value).encode()


def prefix(value: str) -> str:
    """The key convention every keyed fixture shares: text before the first ':'."""
    return value.split(":", 1)[0]


def _as_int(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        return 0


def _bytes_through(value: Any) -> bytes:
    """A handler that already produced the wire bytes hands them through."""
    return value if isinstance(value, bytes) else raw(value)


def _env_int(key: str, default: int | None) -> int | None:
    """A positive integer from the environment, or the default.

    The e2e sets three of these — a timer timeout, a dynamic-entity TTL and the
    bench's checkpoint cadence — and each was reading it its own way.
    """
    declared = os.environ.get(key, "")
    return int(declared) if declared.isdigit() and int(declared) > 0 else default


def passthrough(name: str) -> flezha.Procedure[str, str]:
    """A procedure that answers its input, under the reference's node name.

    Six fixtures declare exactly this and differ only in the name the topology
    knows them by, which is what the Ruby app spells as a `PassThrough` base
    with six subclasses.
    """

    @flezha.procedure(name=name)
    def handler(value: str) -> str:
        return value

    return handler
