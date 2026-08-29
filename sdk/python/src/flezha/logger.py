"""The SDK's diagnostics, through the standard library's ``logging``.

A quiet library by default: the loggers exist, nothing is written, and no
handler is installed — a library that configures the root logger takes a
decision that belongs to the application. :func:`configure` is the one opt-in
that turns diagnostics on, and it is what a test app or a small script calls.

The same posture the Rust SDK's ``diag`` defaults and the Elixir SDK's
``Logger`` use provide. No OpenTelemetry: no SDK ships spans, and adding them to
one binding is the exact asymmetry the lockstep rule exists to prevent.
"""

from __future__ import annotations

import logging
import os
import sys

__all__ = ["log", "logger", "configure", "ROOT_LOGGER_NAME"]

#: Every logger this SDK writes to lives under this name, so an application
#: silences or routes the whole SDK with one call.
ROOT_LOGGER_NAME = "flezha"

_root = logging.getLogger(ROOT_LOGGER_NAME)
# A library must not write to `stderr` on its own initiative when the
# application has configured nothing. `NullHandler` is how the standard library
# says so, and it is removed by nothing but `configure`.
_root.addHandler(logging.NullHandler())


def log() -> logging.Logger:
    """The SDK's root logger."""
    return _root


def logger(name: str) -> logging.Logger:
    """A child logger, for one part of the SDK."""
    return _root.getChild(name)


def configure(level: str | int | None = None, *, stream: object | None = None) -> logging.Logger:
    """Turn the SDK's diagnostics on, at a documented default.

    The default level is ``FLEZHA_LOG_LEVEL`` or ``warning``: a runner that says
    nothing about a wedge is the failure mode diagnostics exist to close, and
    one that narrates every activation is noise. The default destination is
    standard error, so a runner's output stays on the stream the harness reads.
    """
    if level is None:
        level = os.environ.get("FLEZHA_LOG_LEVEL", "warning")
    if isinstance(level, str):
        level = logging.getLevelNamesMapping().get(level.upper(), logging.WARNING)

    handler = logging.StreamHandler(stream or sys.stderr)  # type: ignore[arg-type]
    handler.setFormatter(logging.Formatter("[flezha] %(message)s"))
    for existing in list(_root.handlers):
        if not isinstance(existing, logging.NullHandler):
            _root.removeHandler(existing)
    _root.addHandler(handler)
    _root.setLevel(level)
    return _root
