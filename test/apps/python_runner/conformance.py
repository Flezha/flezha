"""The conformance run: every row of ``test/fixtures/reference_topologies.txt``
driven through THIS SDK's builder.

``flz_core/src/host/check_tests.rs`` is the same table driven against the record
directly, because the core has no builder. This one maps the same step
vocabulary onto the Python graph builder, so what is measured here is what an
author of a Python operative actually writes.

Two entry points, both off ``runner.py``:

``FLEZHA_CONFORMANCE_REPORT=<path>``
    drive every row and write the report the drill reads
    (``test/system/topology_refusal_e2e_test.exs``), then exit without
    connecting to a controller.

``FLEZHA_REFUSAL_FIXTURE=<topology id>``
    register ONLY that row's topology at startup, so a refused one kills the
    process with the refusal's own name in its output.

The palette procedures live here rather than in ``fixtures.py``: they exist only
for this run, and nothing registers them but a row that names them.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import flezha
from fixtures import hybrid_channel_operative
from fixtures_workflow import tier_commit_block_workflow
from flezha import graph
from flezha.native import Core

FIXTURE = "test/fixtures/reference_topologies.txt"

# The authoring surfaces this builder carries — the same list the app's
# `conformance.json` declares. A row needing anything else is skipped BY NAME,
# which is what makes the skip checkable. There is no merge terminator in this
# SDK; the class flag a typed builder cannot spell is a plain builder call here.
SURFACES = ("branch", "workflow", "sync_durability")

# The two topologies the fixture cites rather than re-spells: this app's own
# fixtures, the twins of its seven-column table.
CITED = {
    "hybrid_channel": hybrid_channel_operative,
    "commit_block_chain": tier_commit_block_workflow,
}

# -- the palette -------------------------------------------------------------
#
# One fixture procedure per role the fixture header names. The identity each
# declares is the role's own name, except `twin`, which claims `plain`'s: that
# IS the collision the `dup_node` row names.


class Plain:
    def process(self, value: str) -> str:
        return value


class Routed:
    """`entity_keys` WITH the router that says which entity an input belongs to."""

    def process(self, value: str) -> str:
        return value

    def entities_for(self, value: str) -> list[str]:
        return ["a"]


class NoChangeLogical:
    """A logical clock over a procedure that journals nothing.

    State without a fold IS journaling nothing: the change type stays empty, so
    the durable change sequence a logical clock reads never advances.
    """

    def process(self, value: str) -> str:
        return value

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {}


class EventTime:
    def process(self, value: str) -> str:
        return value

    def event_time(self, value: str) -> int:
        return len(value)


#: role -> the keyword declarations its procedure carries beyond a node identity.
PALETTE: dict[str, tuple[type, str, dict[str, Any]]] = {
    "plain": (Plain, "plain", {}),
    "second": (Plain, "second", {}),
    "twin": (Plain, "plain", {}),
    "static": (Plain, "static", {"entity_keys": ["a", "b"]}),
    "routed": (Routed, "routed", {"entity_keys": ["a", "b"]}),
    "dynamic": (Plain, "dynamic", {"dynamic_entities": True}),
    "nochange": (NoChangeLogical, "nochange", {"clock": "logical"}),
    "eventtime": (EventTime, "eventtime", {"clock": "event_time"}),
}


def split_call(text: str) -> tuple[str, str]:
    """``verb(arg)`` -> ``("verb", "arg")``; a bare word -> ``("word", "")``."""
    verb, _, arg = text.partition("(")
    return verb.strip(), arg.rstrip(")").strip()


class Interpreter:
    """The record a row's steps describe, written the way an author writes it."""

    def __init__(self, steps: str) -> None:
        self.steps = [step.strip() for step in steps.split(";") if step.strip()]
        # A handle is a channel id from an `entry(...)` step, or a procedure
        # role naming that procedure's output channel.
        self._handles: dict[str, Any] = {}
        self._procedures: dict[str, flezha.Procedure[Any, Any]] = {}
        self._mailbox = self._mailbox_capacities()

    def declare(self, builder: flezha.Channels) -> None:
        for step in self.steps:
            self._run_step(builder, step)

    # -- the steps ---------------------------------------------------------

    def _run_step(self, builder: flezha.Channels, step: str) -> None:
        verb, arg = split_call(step)
        if verb == "entry":
            self._handles[arg] = builder.input(arg)
        elif verb == "branch":
            self._branch(arg)
        elif verb == "merge":
            raise AssertionError("this SDK carries no merge terminator; the row needs `merge`")
        else:
            self._dotted(builder, step)

    def _dotted(self, builder: flezha.Channels, step: str) -> None:
        """``<handle>.<link>…`` — a chain, a channel knob or a class flag."""
        head, _, rest = step.partition(".")
        if head == "class":
            self._class_flag(builder, rest)
            return
        edge = self._handle(head)
        for link in rest.split("."):
            edge = self._apply_link(edge, link)

    def _apply_link(self, edge: Any, link: str) -> Any:
        verb, arg = split_call(link)
        if verb == "map":
            return edge.map(lambda value: value)
        if verb == "key":
            return edge.key_by(str)
        if verb == "named":
            return edge.named(arg)
        if verb == "keyed":
            return edge.keyed()
        if verb == "retry_max":
            return edge.retry_max(int(arg))
        if verb == "on_full":
            return edge.on_full(arg)
        if verb == "sink":
            return edge.sink(arg) if arg else edge.sink()
        if verb == "into":
            # The result is that procedure's output channel, addressed by role.
            self._handles[arg] = edge.into(self._procedure(arg))
            return self._handles[arg]
        if verb == "mailbox":
            # Declared on the procedure in this SDK, and read as that procedure
            # was built — one step earlier than the step that names it.
            return edge
        raise AssertionError(f"the fixture spells a link this interpreter does not map: {verb}")

    def _branch(self, arg: str) -> None:
        """``branch(<x>, <n>)`` — the arms are empty: the row states how many."""
        target, arms = (part.strip() for part in arg.split(","))
        self._handle(target).branch(*[lambda _arm: None] * int(arms))

    def _class_flag(self, builder: flezha.Channels, rest: str) -> None:
        """``class.<flag>[(<value>)]`` — the facts the class declares.

        ``persistent`` and ``workflow`` are operative-level declarations, read
        as the row's operative was minted rather than called on the builder.
        """
        flag, value = split_call(rest)
        if flag in ("persistent", "workflow"):
            return
        if flag == "effectively_once":
            builder.effectively_once()
        elif flag == "checkpoint_ack":
            builder.checkpoint_ack()
        elif flag == "sync_durability":
            builder.sync_durability()
        elif flag == "checkpoint_every":
            builder.checkpoint_every(int(value))
        elif flag == "correction_window":
            builder.correction_window(int(value))
        elif flag == "outputs_at_checkpoint":
            builder.outputs_at_checkpoint(int(value))
        else:
            raise AssertionError(f"the fixture spells a class flag this run does not map: {flag}")

    # -- what the steps address ---------------------------------------------

    def _handle(self, name: str) -> Any:
        if name not in self._handles:
            raise AssertionError(f"the row addresses `{name}`, which nothing has declared")
        return self._handles[name]

    def _procedure(self, role: str) -> flezha.Procedure[Any, Any]:
        """The row's own procedure for a palette role, built once."""
        if role not in self._procedures:
            if role not in PALETTE:
                raise AssertionError(f"the fixture names a palette role that is not one: {role}")
            target, identity, declarations = PALETTE[role]
            capacity = self._mailbox.get(role)
            if capacity is not None:
                declarations = {**declarations, "mailbox_capacity": capacity}
            self._procedures[role] = flezha.procedure(target, name=identity, **declarations)
        return self._procedures[role]

    def _mailbox_capacities(self) -> dict[str, int]:
        """The capacities a row declares, read BEFORE the steps run.

        A procedure is built by the `into` that first names it, which is one
        step earlier than the `mailbox` step that declares its capacity.
        """
        capacities = {}
        for step in self.steps:
            role, _, rest = step.partition(".")
            if rest.startswith("mailbox("):
                capacities[role] = int(split_call(rest)[1])
        return capacities


def fixture_path() -> Path:
    """The fixture, from wherever the app was launched.

    The drill runs it from the repository root and a developer from the app's
    own directory, so the app's own location settles it.
    """
    return Path(__file__).resolve().parents[3] / FIXTURE


def rows() -> list[dict[str, str]]:
    """The four-column refusal table, in file order."""
    table = []
    for raw_line in fixture_path().read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        columns = [cell.strip() for cell in line.split("|")]
        if len(columns) == 4:
            table.append(dict(zip(("refusal", "topology", "steps", "needs"), columns, strict=True)))
    return table


def operative_for(row: dict[str, str]) -> flezha.Operative:
    """A row's operative. The cited twins are this app's own; the rest are built.

    Fresh per row: an operative memoises its own spec, and `persistent` and the
    class kind are declarations rather than builder calls.
    """
    if row["topology"] in CITED:
        return CITED[row["topology"]]
    interpreter = Interpreter(row["steps"])
    workflow = "class.workflow" in row["steps"]
    declare = flezha.workflow_operative if workflow else flezha.stream_operative

    def channels(c: flezha.Channels) -> None:
        interpreter.declare(c)

    channels.__name__ = row["topology"]
    return declare(row["topology"], persistent="class.persistent" in row["steps"])(channels)


def channels_of(derived: dict[str, Any]) -> list[dict[str, Any]]:
    """The derived declaration in the fixture's seven-column spelling.

    The core spells a consumer ``{"procedure":N,"chain":M}``; the report carries
    the flat list of procedure indices, which is what that table pins.
    """
    return [
        {
            "index": channel["index"],
            "name": channel["name"],
            "kind": channel["kind"],
            "producer": channel.get("producer"),
            "consumers": [edge["procedure"] for edge in channel["consumers"]],
            "sink": channel.get("sink"),
        }
        for channel in derived["channels"]
    ]


def judge(row: dict[str, str], core: Core) -> dict[str, Any]:
    """One row's verdict, in the report's shape.

    The refusal NAMES are the core's own, carried on the refusal this SDK
    raises; nothing here re-derives one.
    """
    if row["needs"] != "-" and row["needs"] not in SURFACES:
        return {"topology": row["topology"], "outcome": "skipped", "needs": row["needs"]}
    try:
        derived = graph.validate(operative_for(row).spec, core)
    except flezha.DeclarationError as refusal:
        return {
            "topology": row["topology"],
            "outcome": "refused",
            "names": list(refusal.refusals),
            "defect": str(refusal),
        }
    return {"topology": row["topology"], "outcome": "accepted", "derived": channels_of(derived)}


def write_report(path: str) -> None:
    """Drive the whole table and write the report. Nothing here connects."""
    core = Core.create()
    try:
        table = rows()
        judged = [judge(row, core) for row in table]
        Path(path).write_text(json.dumps({"runtime": "python", "rows": judged}, indent=2) + "\n")
        skipped = sum(1 for entry in judged if entry["outcome"] == "skipped")
        print(
            f"[python_runner] conformance: {len(table)} rows, "
            f"{len(table) - skipped} ran, {skipped} skipped",
            file=sys.stderr,
            flush=True,
        )
    finally:
        core.free()


def register_only(runner: flezha.Runner, topology: str) -> None:
    """Register ONLY the named row's topology, the way the app registers any.

    A refused row never reaches a connected runner: the declaration is judged
    before the core is started, and the process dies naming the refusal.
    """
    row = next((candidate for candidate in rows() if candidate["topology"] == topology), None)
    if row is None:
        raise AssertionError(f"{FIXTURE} has no row `{topology}`")
    runner.define_stream(operative_for(row))
