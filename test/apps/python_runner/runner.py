#!/usr/bin/env python3
"""The Python SDK's e2e test app.

Registers the reference fixture twins and runs until shutdown. The namespace is
``argv[1]`` (default ``default``), matching rust_runner / java_runner /
node_runner / ruby_runner / php_runner / elixir_runner. The executor is an axis
of the run rather than a property of this file: ``FLEZHA_PYTHON_EXECUTOR``
selects the thread pool (the default) or the asyncio loop, and the fixtures are
identical under both.

Two environment variables select a conformance run instead of the fixtures:
``FLEZHA_CONFORMANCE_REPORT`` writes the refusal table's verdicts and exits
without connecting, and ``FLEZHA_REFUSAL_FIXTURE`` registers ONE row of that
table in place of every fixture. Both are ``conformance.py``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# The SDK from this checkout, so the app runs against what was just built
# rather than against whatever is installed.
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "sdk" / "python" / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import conformance
import fixtures
import fixtures_workflow
import flezha


def main() -> None:
    report_at = os.environ.get("FLEZHA_CONFORMANCE_REPORT")
    if report_at:
        conformance.write_report(report_at)
        return

    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"
    flezha.configure_logging(os.environ.get("FLEZHA_LOG_LEVEL", "info"))

    runner = flezha.create_runner(namespace)
    refusal_fixture = os.environ.get("FLEZHA_REFUSAL_FIXTURE")
    if refusal_fixture:
        # ONE row of the refusal table, in place of every fixture: the drill
        # reads this process's exit status and output, because a runner whose
        # class the core refuses never reaches a controller.
        conformance.register_only(runner, refusal_fixture)
    else:
        for operative in fixtures.ALL:
            runner.define_stream(operative)
        for operative in fixtures_workflow.ALL:
            runner.define_workflow(operative)

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

    try:
        runner.run()
    except flezha.DeclarationError as refusal:
        # A refused declaration names itself: the verdict's own names ride the
        # exception as data, and the drill reads them off this process's output.
        print(
            f"[python_runner] declaration refused: {', '.join(refusal.refusals)}\n{refusal}",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1) from refusal


if __name__ == "__main__":
    main()
