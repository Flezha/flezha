#!/usr/bin/env python3
"""The Python SDK's e2e test app.

Registers the reference fixture twins and runs until shutdown. The namespace is
``argv[1]`` (default ``default``), matching rust_runner / java_runner /
node_runner / ruby_runner / php_runner / elixir_runner. The executor is an axis
of the run rather than a property of this file: ``FLEZHA_PYTHON_EXECUTOR``
selects the thread pool (the default) or the asyncio loop, and the fixtures are
identical under both.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# The SDK from this checkout, so the app runs against what was just built
# rather than against whatever is installed.
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "sdk" / "python" / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import flezha  # noqa: E402
import fixtures  # noqa: E402


def main() -> None:
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"
    flezha.configure_logging(os.environ.get("FLEZHA_LOG_LEVEL", "info"))

    runner = flezha.create_runner(namespace)
    for operative in fixtures.ALL:
        runner.define_stream(operative)

    # The configuration this run is under, before it starts: readiness is the
    # SDK's own startup line, which reports the wheel tier, the executor and the
    # pool sizing.
    print(
        f"[python_runner] namespace={namespace} "
        f"transport={os.environ.get('FLEZHA_TRANSPORT', 'uds')} "
        f"executor={os.environ.get('FLEZHA_PYTHON_EXECUTOR', 'thread')}",
        file=sys.stderr,
        flush=True,
    )

    runner.run()


if __name__ == "__main__":
    main()
