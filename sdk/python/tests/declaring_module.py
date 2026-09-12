"""A module a runner names, for the explicit-registration tests.

Importing it must open no socket and start no thread — which is what makes
`define_modules` safe to call before `start`.
"""

from __future__ import annotations

import flezha


@flezha.procedure
def handle(value: str) -> str:
    return value


@flezha.stream_operative("from_module")
def from_module(c: flezha.Channels) -> None:
    c.input("in").into(handle).sink()
