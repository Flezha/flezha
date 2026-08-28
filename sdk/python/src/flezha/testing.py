"""Driving one procedure without a controller, for the author's own tests.

The harness is not a second implementation of the dispatcher: it drives
:func:`flezha.dispatch.invoke` — the same function every activation kind that
reaches a handler goes through — over a one-procedure
:class:`~flezha.dispatch.RegisteredClass`, and :meth:`ProcedureTestHarness.replay_equals`
folds through :func:`flezha.state.fold_restore`, the function a real restore
calls. So what it proves is what the runner does, rather than something that
agrees with it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, Iterable, TypeVar

from . import codec as _codec
from .builder import Channels, ClassSpec
from .dispatch import DispatchDeps, RegisteredClass, invoke, run_now
from .errors import FlezhaError
from .procedure import Procedure
from .entity_state import fold_restore

__all__ = ["ProcedureTestHarness", "ReplayMismatch", "Outcome", "replay_equals"]

S = TypeVar("S")


class ReplayMismatch(FlezhaError):
    """The recorded change log and the live state disagree."""

    def __init__(self, procedure: str, live: Any, replayed: Any) -> None:
        self.live = live
        self.replayed = replayed
        super().__init__(
            f"{procedure}: replaying the recorded change log against a fresh initial state "
            f"produced {replayed!r}, but the live state is {live!r}. Either a handler mutated "
            f"the state without recording a change, or apply_change is not a pure fold — both "
            f"are lost on restart and reported nowhere until then."
        )


@dataclass(slots=True)
class Outcome:
    """What one invocation produced."""

    outputs: list[Any] = field(default_factory=list)
    changes: list[Any] = field(default_factory=list)
    timers: list[dict[str, Any]] = field(default_factory=list)
    returned: Any = None


class ProcedureTestHarness(Generic[S]):
    """Drive one procedure the way a runner does, with no controller running.

    ``async def`` handlers are driven exactly as the thread executor drives
    them, so a test needs no event loop of its own.
    """

    def __init__(
        self,
        proc: Procedure[Any, Any],
        *,
        entity_key: str | None = None,
        operative: str = "harness",
        initial_state: S | None = None,
    ) -> None:
        self._proc = proc
        self._entity_key = entity_key
        self._operative = operative
        # The real channel builder, not a hand-written spec: one entry channel
        # into the procedure and one plugin sink so `ctx.emit` has somewhere to
        # go — a procedure under test emits to be observed, not to be routed.
        # Driving the builder means the harness proves it too.
        channels = Channels("<harness>")
        channels.input("in").into(proc).sink("out")
        self._spec = ClassSpec(
            factory_id="<harness>",
            kind="stream",
            persistent=True,
            procedures=channels.procedures,
            channels=channels.channels,
        )
        self._node = channels.procedures[0]
        # The dispatcher's own entity store, so the state this harness reports
        # is the state a runner would hold.
        self._registered = RegisteredClass(self._spec)
        self._entity = self._registered.entity(self._node, entity_key)
        self._deps = DispatchDeps(registry=None, bridge=self, workflow_ctx=None)
        self.log: list[bytes] = []
        self._timers: list[dict[str, Any]] = []
        self._change_seq = 0
        if initial_state is not None:
            self._entity.state = initial_state

    @property
    def state(self) -> S:
        """The entity's live state, as the dispatcher left it."""
        return self._entity.state

    def feed(self, value: Any, *, now: int = 0, replay: bool = False) -> Outcome:
        """Run the procedure against one input."""
        return self._run(now, replay, lambda ctx: self._proc(value, ctx))

    def apply_inputs(self, values: Iterable[Any], *, now: int = 0) -> list[Outcome]:
        """Run the procedure against several inputs, in order."""
        return [self.feed(value, now=now) for value in values]

    def fire_timer(self, kind: str, scheduled_at: int) -> Outcome:
        """Fire a per-entity timer. ``now`` IS the scheduled time, as it is live."""
        if self._proc.on_timer is None:
            raise FlezhaError(f"{self._proc.name} declares no on_timer")
        return self._run(
            scheduled_at,
            False,
            lambda ctx: self._proc.on_timer(  # type: ignore[misc]
                {"kind": kind, "scheduled_at": scheduled_at}, ctx
            ),
        )

    def advance_event_time(self, to: int) -> list[Outcome]:
        """Fire every event-time timer registered at or before ``to``, in order."""
        due = sorted(
            (
                timer
                for timer in self._timers
                if timer["kind"] == "event_time" and timer["scheduled_at"] <= to
            ),
            key=lambda timer: timer["scheduled_at"],
        )
        return [self.fire_timer("event_time", timer["scheduled_at"]) for timer in due]

    def compensate(self, outputs: list[Any]) -> Outcome:
        """Run the declared compensation over recorded forward outputs."""
        if self._proc.compensate is None:
            raise FlezhaError(f"{self._proc.name} declares no compensate hook")
        return self._run(0, False, lambda ctx: self._proc.compensate(outputs, ctx))  # type: ignore[misc]

    def restore(self, *, snapshot: bytes | None = None, changes: Iterable[bytes] = ()) -> S:
        """Rebuild the state the way a restore does, from a snapshot and a log.

        The same call the dispatcher answers ``restore_fold`` with.
        """
        state = fold_restore(
            self._proc,
            {
                "snapshot": {"bytes": snapshot} if snapshot is not None else None,
                "changes": [{"bytes": body} for body in changes],
            },
            self._entity_key,
            self._proc.name,
        )
        self._entity.state = state
        return state

    def snapshot(self) -> bytes:
        """The state as a snapshot body, encoded the way the dispatcher does."""
        return self._proc.encode_state(self.state)

    def replay_equals(self) -> bool:
        """Fold every recorded change into a fresh initial state and require it
        to equal the live one.

        In this SDK that catches both failures at once: a handler that mutated
        the state without recording a change — which the state guard refuses
        for a declared class but not for a plain dict — and an ``apply_change``
        that is not a pure fold.
        """
        replayed = fold_restore(
            self._proc,
            {"snapshot": None, "changes": [{"bytes": body} for body in self.log]},
            self._entity_key,
            self._proc.name,
        )
        live = self.state
        if replayed != live:
            raise ReplayMismatch(self._proc.name, live, replayed)
        return True

    # -- the spawn bridge a `Ctx` asks ---------------------------------------

    def spawn_workflow(self, *args: Any, **kwargs: Any) -> str:
        """A harness has no controller, so a spawn answers what one would have."""
        del args, kwargs
        return "spawned"

    # -- internals -----------------------------------------------------------

    def _run(self, now: int, replay: bool, body: Any) -> Outcome:
        activation = {
            "operative": self._operative,
            "entity_key": self._entity_key,
            "change_seq": self._change_seq,
        }
        ctx, returned = run_now(invoke(self._entity, activation, self._deps, now, replay, body))
        # A returned value is emitted, exactly as the dispatcher's work arm does.
        if returned is not None:
            ctx.emit(returned)
        self.log.extend(ctx.changes)
        self._change_seq += len(ctx.changes)
        self._timers.extend(timer for timer in ctx.timers if timer["register"])
        return Outcome(
            outputs=[_codec.decode(output["payload"]) for output in ctx.outputs],
            changes=[self._proc.decode_change(body) for body in ctx.changes],
            timers=list(ctx.timers),
            returned=returned,
        )


def replay_equals(
    proc: Procedure[Any, Any], inputs: Iterable[Any], *, entity_key: str | None = None
) -> bool:
    """Drive ``proc`` over ``inputs`` and assert the replay agrees."""
    harness: ProcedureTestHarness[Any] = ProcedureTestHarness(proc, entity_key=entity_key)
    harness.apply_inputs(inputs)
    return harness.replay_equals()
