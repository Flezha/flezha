"""The one parse rule per knob type, shared with every other Flezha runtime.

A knob's name, type and default are declared once in ``Flezha.Config.Keys``;
what this module owns is the rule that declaration promises, so a Python runner
answers ``"0"``, ``"on"`` or a typo exactly as the controller does.
``test/fixtures/knob_parse_cases.txt`` is that rule written down.
"""

from __future__ import annotations

import os

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def parse_positive(raw: str | None) -> int | None:
    """A count of one or more, else ``None`` -- ``"0"`` already means unset."""
    value = _whole(raw)
    return value if value is not None and value > 0 else None


def parse_ms(raw: str | None) -> int | None:
    """A whole number of milliseconds, zero included."""
    value = _whole(raw)
    return value if value is not None and value >= 0 else None


def parse_int(raw: str | None) -> int | None:
    """A whole number, negative included."""
    return _whole(raw)


def parse_bool(raw: str | None) -> bool | None:
    """``1/true/yes/on`` and ``0/false/no/off``, case-insensitively."""
    if raw is None:
        return None
    text = raw.strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    return None


def positive(name: str, fallback: int) -> int:
    """A count from the environment, with ``fallback`` for anything that is not one."""
    parsed = parse_positive(os.environ.get(name))
    return fallback if parsed is None else parsed


def flag(name: str, fallback: bool) -> bool:
    """A flag from the environment, with ``fallback`` for anything that is not one."""
    parsed = parse_bool(os.environ.get(name))
    return fallback if parsed is None else parsed


def _whole(raw: str | None) -> int | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None
