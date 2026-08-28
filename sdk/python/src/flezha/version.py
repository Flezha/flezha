"""The package's version, and the SDK version the durable header stamps."""

from __future__ import annotations

__all__ = ["__version__", "SDK_VERSION"]

__version__ = "0.1.0"

#: ``(major << 16) | (minor << 8) | patch``, as the 16-byte durable header
#: carries it. Derived from :data:`__version__` so the two cannot drift.
_major, _minor, _patch = (int(part) for part in __version__.split(".")[:3])
SDK_VERSION = (_major << 16) | (_minor << 8) | _patch
