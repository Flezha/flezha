"""Helpers every fixture module shares: the key convention, the environment
readers the tunable fixtures use, and the pass-through procedure six operatives
declare identically.
"""

from __future__ import annotations

import os

import flezha


def prefix(value: str) -> str:
    """The key convention every keyed fixture shares: text before the first ':'."""
    return value.split(":", 1)[0]


def _as_int(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        return 0


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
