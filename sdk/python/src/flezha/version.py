"""The package's version, and the SDK version the durable header stamps."""

from __future__ import annotations

__all__ = ["__version__"]

#: This SDK's own released version, stamped into every durable record it
#: writes; the core packs it. Pinned to ``pyproject.toml`` by
#: ``test/sdk_conformance/gate_test.exs``.
__version__ = "0.1.0"
