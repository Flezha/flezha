"""The runner app's declarations, where a node name is carried and nowhere else.

`test/apps/python_runner` is the porting template later Python ports are read
against, and a procedure's node name reaches the controller inside the
declaration only — no golden, fixture or e2e in the suite reads one, so a name
copied from the wrong class is invisible everywhere. This module is what makes
one red.
"""

from __future__ import annotations

from typing import Any

import app
from flezha import graph

fixtures_state = app.module("fixtures_state")


def _procedure_names(decl: dict[str, Any]) -> list[str]:
    return [node["name"] for node in decl["procedures"]]


def test_persistence_off_counter_declares_its_own_nodes() -> None:
    """The reference's names (`test/apps/rust_runner/src/state.rs`), which the
    node and ruby ports carry too, and no node identity shared with the
    persistent twin."""
    decl = graph.to_host_class_decl(fixtures_state.persistence_off_operative.spec)

    assert decl["factory_id"] == "persistence_off_counter"
    assert decl["persistent"] is False
    assert _procedure_names(decl) == [
        "persistence_off_entry_procedure",
        "persistence_off_counter_procedure",
    ]

    twin = graph.to_host_class_decl(fixtures_state.stateful_counter_operative.spec)
    assert set(_procedure_names(twin)).isdisjoint(_procedure_names(decl))
