"""A wrong edge is a type error before it is a runtime error.

Rust pins 25 compile-fail cases in ``tests/ui``. Python cannot have those, and
demoting every one of them to a registration-time refusal gives up too much: a
fully annotated, ``py.typed`` package with generic decorators lets a standard
type checker reject a wrong edge *in the editor*. This is the pin for that —
the fixture that must fail, and the one that must not.
"""

from __future__ import annotations

import json
import pathlib
import shutil
import subprocess
import sys

import pytest

_HERE = pathlib.Path(__file__).resolve().parent
_FIXTURES = _HERE / "typing_fixtures"
_SRC = _HERE.parent / "src"

pytestmark = pytest.mark.skipif(
    shutil.which("pyright") is None,
    reason="pyright is not installed; the dev group of pyproject.toml pins it "
    "(`pip install --group dev`, pip 25.1+) and the CI type-check job supplies it",
)


def _check(path: pathlib.Path) -> list[dict[str, object]]:
    """Run pyright over one path and return its diagnostics."""
    result = subprocess.run(
        [
            "pyright",
            "--outputjson",
            "--pythonpath",
            sys.executable,
            str(path),
        ],
        capture_output=True,
        text=True,
        cwd=_HERE.parent,
        env={**_env(), "PYTHONPATH": str(_SRC)},
        # pyright exits non-zero whenever it reports a diagnostic; the report
        # below is what this reads, so a non-zero status is not a failure here.
        check=False,
    )
    if not result.stdout:
        pytest.fail(f"pyright produced no report: {result.stderr}")
    return json.loads(result.stdout)["generalDiagnostics"]


def _env() -> dict[str, str]:
    import os

    return dict(os.environ)


def test_a_well_typed_graph_reports_nothing() -> None:
    errors = [d for d in _check(_FIXTURES / "well_typed.py") if d["severity"] == "error"]
    assert errors == [], errors


def test_a_wrong_edge_type_is_reported_naming_both_handlers() -> None:
    errors = [d for d in _check(_FIXTURES / "wrong_edge.py") if d["severity"] == "error"]
    assert len(errors) == 1, errors
    message = str(errors[0]["message"])
    # The mismatch names the producing edge's type and the consumer's, which is
    # what makes it actionable without running anything.
    assert "Invoice" in message
    assert "Shipment" in message


def test_the_package_ships_py_typed() -> None:
    """Without the marker a checker treats the package as untyped and says nothing."""
    assert (_SRC / "flezha" / "py.typed").exists()


def test_the_public_surface_type_checks_clean() -> None:
    """The annotations are load-bearing, so they are checked rather than hoped for."""
    errors = [d for d in _check(_SRC / "flezha") if d["severity"] == "error"]
    assert errors == [], errors
