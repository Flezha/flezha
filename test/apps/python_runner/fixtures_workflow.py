"""Twins of the reference's durable structured workflow fixtures
(``test/apps/rust_runner/src/workflow.rs`` and ``workflow_loop.rs``) under the
same factory ids, procedure names, channel names and output bytes.

Entry payloads and plugin frames are UTF-8 text on the wire, as in the
reference: its bare ``sink(name)`` serialises a String as UTF-8, while this
SDK's plugin default is MessagePack — so every sink here declares an encoder. An
inter-procedure hop between two Python procedures carries MessagePack, and the
core derives that channel's journal codec from the channel's kind, so no fixture
declares one.

A procedure node owns exactly one output channel, not one per outgoing edge, so
the whole topology vocabulary is edge reuse: a fan-out is one held ``Edge``
wired twice, and a fan-in is two arms naming one procedure with no node name.
"""

from __future__ import annotations

import asyncio
import bisect
import itertools
import os
from collections.abc import Callable
from typing import Any

import flezha
from fixture_support import raw, text

# -- the release gate --------------------------------------------------------

RELEASE_ENV = "FLEZHA_WORKFLOW_E2E_RELEASE"


async def block_until_released() -> None:
    """Hold this activation until a runner boots with the release variable set.

    The harness kills the first runner and launches its replacement with the
    variable present, so the gate is a boot-time decision: never settling on the
    first runner, returning at once on the second.

    A future nobody resolves, rather than a wait with a deadline, because this
    host has two executors and one spelling has to serve both. On the thread
    pool ``dispatch.resolve`` drives it to completion on the worker's own loop,
    which parks that pool worker and no other — the core's driver keeps
    answering control pings and the starvation supervisor grows the pool, so a
    sibling procedure stays dispatchable. Under ``FLEZHA_PYTHON_EXECUTOR=asyncio``
    the same line suspends the one activation and yields the loop.
    """
    if RELEASE_ENV in os.environ:
        return
    await asyncio.get_running_loop().create_future()


def armed(name: str) -> bool:
    """Read at handler time: each scenario arms it on the runner it launches."""
    return name in os.environ


# -- the three factories -----------------------------------------------------


def insert_sorted(items: list[str], value: str) -> None:
    """Insert into a sorted, de-duplicated list.

    The reference's ``SeenState`` is a ``BTreeSet<String>``, chosen because a
    set keeps snapshot bytes deterministic. MessagePack cannot represent a
    ``set`` and the declared starting state is walked at import, so the twin is
    the list it would decode to — kept sorted for the same determinism and
    idempotent for the same re-run after a resume.
    """
    at = bisect.bisect_left(items, value)
    if at == len(items) or items[at] != value:
        items.insert(at, value)


def durable_step(
    name: str,
    *,
    change: Callable[[str], str] | None = None,
    emit: Callable[[str, flezha.Ctx[Any]], None] | None = None,
    compensate: Callable[..., Any] | None = None,
    gate: Callable[[str], bool] | None = None,
) -> flezha.Procedure[str, str]:
    """The reference's ``durable_step!`` macro: a ``str -> str`` step committing
    one idempotent Change keyed by its own name, so a re-run after a resume
    folds to the state it already had.

    ``emit`` and ``compensate`` are the two axes that macro parameterises;
    ``change`` (which value is recorded) and ``gate`` (whether this step waits
    for the release gate BEFORE recording it) are the two the reference varies
    by hand-writing the procedure instead. Carrying all four is what lets every
    durable procedure in this module be a single call.

    The handler is a coroutine only when a gate is declared. ``resolve`` answers
    a plain value with no event loop at all, and the bench chains run this
    factory sixteen deep at rate.
    """

    def record(value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        ctx.change({"insert": change(value) if change else name})
        if emit:
            emit(value, ctx)
        else:
            ctx.emit(value)

    class Step:
        def initial_state(self, entity_key: str | None) -> dict[str, Any]:
            return {"items": []}

        def apply_change(self, state: dict[str, Any], recorded: dict[str, Any]) -> None:
            insert_sorted(state["items"], recorded["insert"])

        if gate is None:

            def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
                record(value, ctx)

        else:

            async def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
                # A gated step waits BEFORE its change and its emit, so the kill
                # window always finds it as the uncommitted producer while its
                # sibling arm has committed.
                if gate(value):
                    await block_until_released()
                record(value, ctx)

    if compensate is not None:
        # A crash-resumed compensation reconstructs completed stages with EMPTY
        # outputs, so a hook that records an undo does so unconditionally.
        Step.compensate = staticmethod(compensate)
    return flezha.procedure(Step, name=name)


def stateless_step(name: str, handle: Callable[..., Any]) -> flezha.Procedure[str, str]:
    """The reference's ``stateless_step!``: no state, no change, the whole
    behaviour is the handler."""
    return flezha.procedure(handle, name=name)


def failing_step(name: str) -> flezha.Procedure[str, str]:
    """A terminal step that always fails, which is how a saga is made to unwind."""

    def always_fails(value: str, ctx: flezha.Ctx[None]) -> None:
        raise RuntimeError(f"{name} always fails")

    return stateless_step(name, always_fails)


def undoing(record: str) -> Callable[[list[Any], flezha.Ctx[Any]], None]:
    """A compensation hook whose whole behaviour is emitting one undo record."""

    def compensate(outputs: list[Any], ctx: flezha.Ctx[Any]) -> None:
        ctx.emit(record)

    return compensate


# -- echo_workflow -----------------------------------------------------------
#
# The run-sugar shape: emits its args back once and returns, so the SDK ships
# the completion and the controller deregisters it.


@flezha.workflow_operative("echo_workflow")
def echo_workflow(args: Any, ctx: flezha.WorkflowCtx) -> None:
    ctx.emit(f"echo:{args}")


# -- commit_block_workflow / sync_durable_workflow ---------------------------
#
# Proc A commits durable state, proc B blocks until the resumed runner sets the
# release variable — so a SIGKILL lands with A's state already on the WAL and B
# never finished. `sync_durable_workflow` is the same chain declaring
# SYNCHRONOUS remote durability, which is what lets the kill-the-disk drill
# destroy the state directory and still resume with A committed.

commit_a = durable_step("commit_a")


async def _block(value: str, ctx: flezha.Ctx[None]) -> None:
    await block_until_released()


block_b = stateless_step("block_b", _block)


def commit_block_chain(c: flezha.Channels) -> None:
    c.input("in", decode=text).into(commit_a).named("mid").into(block_b)


commit_block_workflow = flezha.workflow_operative("commit_block_workflow")(commit_block_chain)

sync_durable_workflow = flezha.workflow_operative("sync_durable_workflow", sync_durability=True)(
    commit_block_chain
)


# -- optout_commit_block -----------------------------------------------------
#
# The opt-out twin with a live mid-chain signal: proc A's output also reaches a
# plugin sink, so the crash-resume drill can observe the mid-run point without
# durable state. A crash re-runs the chain from the top, so the sink sees A's
# marker once per run and no journal file exists.


@flezha.workflow_operative("optout_commit_block", persistent=False)
def optout_commit_block(c: flezha.Channels) -> None:
    c.input("in", decode=text).into(commit_a).sink(
        "mid", encode=lambda value: raw(f"a-ran:{value}")
    ).into(block_b)


# -- tier_commit_block_workflow ----------------------------------------------
#
# The effectively-once fixture. Proc A commits durable state AND emits
# `committed:<input>` under a stable key on a keyed hybrid channel, captured
# into the atomic effect record; proc B blocks. A SIGKILL lands with A's effect
# record durable and B unfinished, and the resume re-runs A — the change is
# idempotent and the re-emit deterministic.

tier_commit_a = durable_step(
    "tier_commit_a",
    emit=lambda value, ctx: ctx.emit(f"committed:{value}", key="job"),
)


@flezha.workflow_operative("tier_commit_block_workflow")
def tier_commit_block_workflow(c: flezha.Channels) -> None:
    c.effectively_once()
    c.input("in", decode=text).into(tier_commit_a).keyed().sink("mid", encode=raw).into(block_b)


# -- saga_block_workflow -----------------------------------------------------
#
# SA and SB commit durable state and compensate; SC fails, triggering
# reverse-order rollback (SB then SA). SA's compensation BLOCKS, so a SIGKILL
# lands mid-rollback with SB's done-marker already durable — the respawn
# finishes SA and SKIPS the already-done SB.


async def _block_compensation(outputs: list[Any], ctx: flezha.Ctx[Any]) -> None:
    await block_until_released()


saga_a = durable_step("saga_a", compensate=_block_compensation)
saga_b = durable_step("saga_b", compensate=lambda outputs, ctx: None)
saga_c = failing_step("saga_c")


@flezha.workflow_operative("saga_block_workflow")
def saga_block_workflow(c: flezha.Channels) -> None:
    c.input("in", decode=text).into(saga_a).named("s1").into(saga_b).named("s2").into(saga_c)


# -- outbox_block_workflow ---------------------------------------------------
#
# The transactional outbox. Proc A commits durable state AND emits plugin-bound
# output on a hybrid channel — STAGED, so it reaches the sink only after the
# controller has journaled and fsynced A's committed entry with the output
# inside it and flushed the outbox. Proc B blocks, so a SIGKILL lands with A
# committed and flushed and B unfinished.


@flezha.workflow_operative("outbox_block_workflow")
def outbox_block_workflow(c: flezha.Channels) -> None:
    c.input("in", decode=text).into(commit_a).sink(
        "mid", encode=lambda value: raw(f"a-out:{value}")
    ).into(block_b)


# -- undo_outbox_workflow ----------------------------------------------------
#
# The compensation half of the transactional outbox. Proc UA commits, emits its
# forward output on a hybrid plugin channel, and compensates by EMITTING an undo
# record — plugin-bound, so it is staged and rides a compensated journal entry
# rather than the wire. Proc C fails immediately, so the chain always rolls
# back; the hook BLOCKS, so on the first runner the undo is
# emitted-but-never-journaled when the SIGKILL lands, proving staged undo output
# cannot leak. The respawn re-runs the outstanding hook, whose done marker never
# landed, and the undo is delivered exactly once.


async def _undo_after_release(outputs: list[Any], ctx: flezha.Ctx[Any]) -> None:
    await block_until_released()
    ctx.emit("undone")


undo_outbox_a = durable_step("undo_outbox_a", compensate=_undo_after_release)
undo_outbox_c = failing_step("undo_outbox_c")


@flezha.workflow_operative("undo_outbox_workflow")
def undo_outbox_workflow(c: flezha.Channels) -> None:
    c.input("in", decode=text).into(undo_outbox_a).sink(
        "mid", encode=lambda value: raw(f"out:{value}")
    ).into(undo_outbox_c)


# -- branch_workflow ---------------------------------------------------------
#
#     BranchA ─┬─▶ BranchB ─▶ BranchD
#              └─▶ BranchC
#
# The procedure TREE. Every procedure commits durable state and compensates by
# emitting an undo record, so both tree behaviours are observable at one sink:
#
#   * crash-resume — BranchD BLOCKS, so the SIGKILL lands with A, B and C
#     committed and only D's subtree outstanding. The respawn re-runs D alone,
#     seeded from B's journaled edge, so the sibling branch's `c:` marker
#     appears exactly ONCE across both runs.
#   * compensation — with FLEZHA_BRANCH_FAIL armed, BranchD fails instead of
#     blocking. A, B and C have all completed, so the rollback unwinds the two
#     branches before the root they share: `undo-c`, `undo-b`, `undo-a`.
#
# The inter-proc channels are hybrid, which is what makes a non-leaf
# procedure's forward and undo output visible at the sink.

BRANCH_FAIL_ENV = "FLEZHA_BRANCH_FAIL"

branch_a = durable_step("branch_a", compensate=undoing("undo-a"))
branch_b = durable_step("branch_b", compensate=undoing("undo-b"))

# The sibling leaf: emits straight to the sink, and its undo does too.
branch_c = durable_step(
    "branch_c",
    emit=lambda value, ctx: ctx.emit(f"c:{value}"),
    compensate=undoing("undo-c"),
)


# The deep leaf under BranchB: blocks (crash-resume), or fails (compensation).
async def _branch_leaf(value: str, ctx: flezha.Ctx[None]) -> None:
    if armed(BRANCH_FAIL_ENV):
        raise RuntimeError("branch_d failed")
    await block_until_released()
    ctx.emit(f"d:{value}")


branch_d = stateless_step("branch_d", _branch_leaf)


# Fail fast: the compensation scenario is about the rollback order, not the
# ladder that precedes it.
@flezha.workflow_operative("branch_workflow", retry_attempts=0)
def branch_workflow(c: flezha.Channels) -> None:
    # One channel, two consumers: THE fan-out. Both branches read exactly what
    # BranchA released on it (held edge, wired twice).
    mid = (
        c.input("in", decode=text)
        .into(branch_a)
        .sink("mid", encode=lambda value: raw(f"a:{value}"))
    )
    mid.into(branch_b).sink("bmid", encode=lambda value: raw(f"b:{value}")).into(branch_d).sink(
        "d_out", encode=raw
    )
    mid.into(branch_c).sink("c_out", encode=raw)


# -- diamond_workflow --------------------------------------------------------
#
#     DiamA ─┬─▶ DiamB1 ─▶ DiamB2(blocks) ─┬─▶ DiamJoin
#            └─▶ DiamC ────────────────────┘
#
# The fan-in DAG. The join accumulates BOTH arms' outputs and is dispatched
# exactly once, after both commit. DiamB2 blocks one level BELOW its sibling arm
# — siblings commit at one level barrier — so a SIGKILL lands with A, B1 and C
# committed and B2 mid-run: the respawn re-runs B2 alone, and the join runs once
# with C's JOURNALED handoff banked and B2's fresh output appended, so `c:`
# appears exactly once across both runs.

diam_a = durable_step("diam_a")
diam_b1 = durable_step("diam_b1")

diam_b2 = durable_step(
    "diam_b2",
    change=lambda value: "diam_b",
    gate=lambda value: True,
    emit=lambda value, ctx: ctx.emit(f"b:{value}"),
)

diam_c = durable_step("diam_c", emit=lambda value, ctx: ctx.emit(f"c:{value}"))


# The join: one `j:`-tagged record per accumulated contribution.
def _join(value: str, ctx: flezha.Ctx[None]) -> None:
    ctx.emit(f"j:{value}")


diam_join = stateless_step("diam_join", _join)


@flezha.workflow_operative("diamond_workflow")
def diamond_workflow(c: flezha.Channels) -> None:
    # Held edge wired twice = the fan-out; two arms naming diam_join with no
    # node name = the fan-in, since two unnamed mentions are one node.
    fan = (
        c.input("in", decode=text).into(diam_a).sink("dfan", encode=lambda value: raw(f"a:{value}"))
    )
    fan.into(diam_b1).named("db1").into(diam_b2).sink("db2", encode=raw).into(diam_join)
    fan.into(diam_c).sink("dc", encode=raw).into(diam_join).sink("j_out", encode=raw)


# -- retry_workflow ----------------------------------------------------------
#
# The per-entity retry ladder. Proc RA commits durable state, forwards on a
# hybrid channel, and compensates by emitting an undo record; `retry_flaky`
# fails its first FLEZHA_WORKFLOW_RETRY_FAILS attempts (none by default — each
# scenario arms it) and then emits `attempt:<n>:<input>`. One armed failure is
# absorbed by the ladder, so the run completes and the sink sees attempt 2;
# arming more failures than the ladder has attempts exhausts it, RA rolls back
# and the run reports an error.

RETRY_FAILS_ENV = "FLEZHA_WORKFLOW_RETRY_FAILS"

# Attempts made in this process. A retry re-runs the procedure of the same
# instance in place and dispatch is entity-affine, so one counter is the whole
# ladder's view; `next` on a count is atomic, which a `+= 1` on a module global
# under a worker pool is not.
_attempts = itertools.count(1)

retry_a = durable_step("retry_a", compensate=undoing("undone"))


def _armed_failures() -> int:
    declared = os.environ.get(RETRY_FAILS_ENV, "")
    return int(declared) if declared.isdigit() else 0


def _flaky(value: str, ctx: flezha.Ctx[None]) -> None:
    attempt = next(_attempts)
    if attempt <= _armed_failures():
        raise RuntimeError(f"flaky attempt {attempt}")
    ctx.emit(f"attempt:{attempt}:{value}")


retry_flaky = stateless_step("retry_flaky", _flaky)


@flezha.workflow_operative("retry_workflow")
def retry_workflow(c: flezha.Channels) -> None:
    c.input("in", decode=text).into(retry_a).sink(
        "mid", encode=lambda value: raw(f"res:{value}")
    ).into(retry_flaky).sink("out", encode=raw)


# -- the durable loop family -------------------------------------------------
#
# chat_loop_workflow is the durable-loop + suspension shape. One iteration per
# inbox item: TURN commits the turn to Entity state, so a resumed iteration is
# visible in the WAL, and REPLY emits the answer to the sink. Between turns the
# loop finds an empty inbox at a committed boundary and PARKS — the instance is
# evicted and rehydrated by the next message. The turn whose payload is the
# crash turn BLOCKS while the release variable is unset, so a SIGKILL lands with
# the earlier iterations durable and that one in flight. Keying the block on the
# payload rather than on an invocation count keeps it deterministic across the
# park/rehydrate cycles, which re-run the driver but not the turns.
#
# stateless_loop_workflow is the same loop MINUS its stateful first procedure:
# chat_reply alone holds no per-Entity state, so nothing writes an Entity WAL
# and the instance's proc-journal reaches the driver only if it rides the bind
# itself.
#
# chat_rewind_workflow is the loop whose replying procedure can be undone. The
# undo handle lives in Entity state, because a rewind's compensation is rebuilt
# from the journal and is handed NO retained outputs, so the hook reads the last
# thing it said, retracts it and pops it.

CRASH_TURN = "mid-crash"

# Keyed by the turn, so the record count is the number of DISTINCT turns that
# ran — a re-run of a committed iteration would show up.
chat_turn = durable_step(
    "chat_turn",
    change=lambda value: value,
    gate=lambda value: value == CRASH_TURN,
)


def _reply(value: str, ctx: flezha.Ctx[None]) -> None:
    ctx.emit(f"reply:{value}")


chat_reply = stateless_step("chat_reply", _reply)


class ChatReplyUndoable:
    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"said": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        if "said" in change:
            state["said"].append(change["said"])
        else:
            state["said"].pop()

    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        ctx.change({"said": value})
        ctx.emit(f"reply:{value}")

    def compensate(self, outputs: list[Any], ctx: flezha.Ctx[dict[str, Any]]) -> None:
        said = ctx.state["said"]
        if not said:
            return
        ctx.change({"retracted": True})
        ctx.emit(f"retract:{said[-1]}")


chat_reply_undoable = flezha.procedure(ChatReplyUndoable, name="chat_reply_undoable")

# A chat session ends on an external stop, not on its own; the duration backstop
# keeps a stray instance from outliving the test run.
LOOP_KNOBS: dict[str, Any] = {"loop_mode": "per_input", "max_duration_ms": 600_000}


def chat_chain(c: flezha.Channels) -> None:
    c.input("in", decode=text).into(chat_turn).named("mid").into(chat_reply).sink("out", encode=raw)


chat_loop_workflow = flezha.workflow_operative("chat_loop_workflow", **LOOP_KNOBS)(chat_chain)


@flezha.workflow_operative("stateless_loop_workflow", **LOOP_KNOBS)
def stateless_loop_workflow(c: flezha.Channels) -> None:
    c.input("in", decode=text).into(chat_reply).sink("out", encode=raw)


@flezha.workflow_operative("chat_rewind_workflow", **LOOP_KNOBS)
def chat_rewind_workflow(c: flezha.Channels) -> None:
    c.input("in", decode=text).into(chat_turn).named("mid").into(chat_reply_undoable).sink(
        "out", encode=raw
    )


# -- the bench chains --------------------------------------------------------
#
# What `mix bench.workflow --scenario chain` drives: K durable pass-through
# steps in a line, the payload unchanged, so the sink receives the seed input
# and the bench reads its own seq and send timestamp back. Each chain position
# needs its own procedure, so the sixteen are generated rather than written out;
# the edge names follow the reference, which spells them from the step TYPE
# rather than from the procedure name.

BENCH_STEPS = [durable_step(f"bench_s{at}") for at in range(16)]


def bench_chain(factory_id: str, steps: int, persistent: bool) -> flezha.Operative:
    @flezha.workflow_operative(factory_id, persistent=persistent)
    def declare(c: flezha.Channels) -> None:
        edge = c.input("in", decode=text).into(BENCH_STEPS[0])
        for at in range(1, steps):
            edge = edge.named(f"edge_BenchS{at}").into(BENCH_STEPS[at])
        edge.sink("out", encode=raw)

    return declare


bench_chain_1 = bench_chain("bench_chain_1", 1, True)
bench_chain_4 = bench_chain("bench_chain_4", 4, True)
bench_chain_16 = bench_chain("bench_chain_16", 16, True)
bench_chain_optout_4 = bench_chain("bench_chain_optout_4", 4, False)


# -- bench_loop_workflow -----------------------------------------------------
#
# The long-lived loop the `turns` scenario drives: the chat loop's shape with a
# BOUNDED per-turn state, so a million-turn soak snapshots the same few bytes at
# every iteration, and a wake deadline for the timer-wake measurements.

WAKE_AFTER_ENV = "FLEZHA_BENCH_WAKE_AFTER_MS"


class BenchTurn:
    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"turns": 0, "last": ""}

    def apply_change(self, state: dict[str, Any], seen: dict[str, Any]) -> None:
        state["turns"] += 1
        state["last"] = seen["turn"]

    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        ctx.change({"turn": value})
        ctx.emit(value)


bench_turn = flezha.procedure(BenchTurn, name="bench_turn")


def _wake_after_ms() -> int | None:
    declared = os.environ.get(WAKE_AFTER_ENV, "")
    return int(declared) if declared.isdigit() and int(declared) > 0 else None


# A day, not the chat loop's ten minutes: a soak outlives 600 s.
@flezha.workflow_operative(
    "bench_loop_workflow",
    loop_mode="per_input",
    max_duration_ms=24 * 3_600_000,
    wake_after_ms=_wake_after_ms(),
)
def bench_loop_workflow(c: flezha.Channels) -> None:
    c.input("in", decode=text).into(bench_turn).named("mid").into(chat_reply).sink(
        "out", encode=raw
    )


#: Every workflow operative this app registers, in the reference's order.
ALL: list[flezha.Operative] = [
    echo_workflow,
    commit_block_workflow,
    sync_durable_workflow,
    optout_commit_block,
    tier_commit_block_workflow,
    saga_block_workflow,
    outbox_block_workflow,
    undo_outbox_workflow,
    chat_loop_workflow,
    stateless_loop_workflow,
    chat_rewind_workflow,
    retry_workflow,
    branch_workflow,
    diamond_workflow,
    bench_chain_1,
    bench_chain_4,
    bench_chain_16,
    bench_chain_optout_4,
    bench_loop_workflow,
]
