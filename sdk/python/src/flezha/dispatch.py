"""Turning one activation into one answer, exactly once.

Per-entity exclusivity IS the handout: the core holds an entity's next
activation until this one is answered, so an activation that returns unanswered
retires that entity for the life of the process. Every path out of
:func:`dispatch` therefore ends in a completion or an abandon, through a
``finally`` rather than a rule applied at each return site — a raised handler,
an encoding failure and a killed thread all reach the same guard.
"""

from __future__ import annotations

import asyncio
import inspect
import threading
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from . import codec as _codec
from . import determinism
from .builder import DROPPED, ChannelSpec, ClassSpec, ConsumerEdge, ProcNode, apply_transforms
from .ctx import Ctx
from .logger import log
from .errors import FlezhaError
from .procedure import Procedure
from .entity_state import fold_restore

__all__ = [
    "Registry",
    "RegisteredClass",
    "Entity",
    "DispatchDeps",
    "answer",
    "dispatch",
    "dispatch_now",
    "invoke",
    "run_now",
    "resolve",
]


@dataclass(slots=True)
class Entity:
    """One Entity of one declared class: what every activation kind operates on.

    The `(registered, node, key)` triple every activation kind names, and where
    `state` is read and written — so the lock stays inside
    :class:`RegisteredClass` and nothing else touches the entity table.
    """

    registered: RegisteredClass
    node: ProcNode
    key: str | None

    @property
    def proc(self) -> Procedure[Any, Any]:
        return self.node.proc

    @property
    def name(self) -> str:
        return self.node.name

    @property
    def state(self) -> Any:
        """The live state, created from the declaration on first touch."""
        return self.registered.state_of(self.node, self.key)

    @state.setter
    def state(self, value: Any) -> None:
        self.registered.set_state(self.node, self.key, value)


class RegisteredClass:
    """A declared class, with the lookups dispatch needs, and its live entities."""

    __slots__ = ("spec", "outgoing", "_entities", "_lock")

    def __init__(self, spec: ClassSpec) -> None:
        self.spec = spec
        self.outgoing: list[list[ChannelSpec]] = [
            [channel for channel in spec.channels if channel.producer == node.index]
            for node in spec.procedures
        ]
        self._entities: dict[tuple[int, str | None], Any] = {}
        # Entities of one class are touched from every worker thread, and the
        # core's per-entity exclusivity does not extend to this dict: two
        # different entities of the same class run concurrently by design.
        self._lock = threading.Lock()

    def state_of(self, node: ProcNode, entity_key: str | None) -> Any:
        """The entity's live state, created from the declaration on first touch."""
        key = (node.index, entity_key)
        with self._lock:
            if key not in self._entities:
                self._entities[key] = node.proc.build_initial_state(entity_key)
            return self._entities[key]

    def set_state(self, node: ProcNode, entity_key: str | None, state: Any) -> None:
        with self._lock:
            self._entities[(node.index, entity_key)] = state

    def has_entities(self) -> bool:
        """Whether any entity of this class has been materialised."""
        with self._lock:
            return bool(self._entities)

    def entity(self, node: ProcNode, key: str | None) -> Entity:
        """The handle every activation kind operates through."""
        return Entity(self, node, key)

    def channel_at(self, index: int) -> ChannelSpec | None:
        """The channel an activation names, or ``None`` when it names none.

        An out-of-range index is a frame this runner's declaration does not
        describe; the caller decodes with the default rather than failing, so a
        declaration drift degrades to plain MessagePack instead of a crash.
        """
        channels = self.spec.channels
        return channels[index] if 0 <= index < len(channels) else None


class Registry:
    """The declared classes this runner serves."""

    __slots__ = ("_classes", "_lock")

    def __init__(self) -> None:
        self._classes: dict[str, RegisteredClass] = {}
        self._lock = threading.Lock()

    def add(self, spec: ClassSpec) -> None:
        with self._lock:
            if spec.factory_id in self._classes:
                raise FlezhaError(
                    f"duplicate factory id `{spec.factory_id}`: a runner declares each factory once"
                )
            self._classes[spec.factory_id] = RegisteredClass(spec)

    def remove(self, factory_id: str) -> RegisteredClass:
        with self._lock:
            if factory_id not in self._classes:
                raise FlezhaError(f"no operative type `{factory_id}` is declared on this runner")
            return self._classes.pop(factory_id)

    def get(self, factory_id: str) -> RegisteredClass | None:
        return self._classes.get(factory_id)

    def ids(self) -> list[str]:
        with self._lock:
            return list(self._classes)

    def all(self) -> list[RegisteredClass]:
        with self._lock:
            return list(self._classes.values())

    def __len__(self) -> int:
        return len(self._classes)


@dataclass(slots=True)
class DispatchDeps:
    """What a dispatch loop needs to answer an activation, over one extension."""

    registry: Registry | None
    bridge: Any
    workflow_ctx: Callable[[str], Any] | None


def failed(token: int, message: str, *, non_retryable: bool = False) -> dict[str, Any]:
    return {
        "token": token,
        "outcome": "failed",
        "error": message,
        "non_retryable": non_retryable,
    }


def error_message(error: BaseException) -> str:
    """What a failure says on the wire.

    ``class: message``, in family with what the other SDKs send — this string is
    stored in a dead-letter entry and read by an operator, and a multi-line
    traceback there is noise the retry ladder multiplies. The traceback is
    logged host-side instead, where it is a diagnostic rather than a record.
    """
    return f"{type(error).__name__}: {error}"


def is_non_retryable(error: BaseException) -> bool:
    """A handler declares an error non-retryable by carrying ``non_retryable``."""
    return bool(getattr(error, "non_retryable", False))


_thread_state = threading.local()


def _thread_loop() -> asyncio.AbstractEventLoop:
    """This thread's own event loop, created on first need and reused.

    Only a thread that actually runs an async handler ever makes one, and it
    makes exactly one for its lifetime — a loop per activation would cost more
    than the handler on a fast one.
    """
    loop = getattr(_thread_state, "loop", None)
    if loop is None:
        loop = asyncio.new_event_loop()
        _thread_state.loop = loop
    return loop


def close_thread_loop() -> None:
    """Release this thread's event loop, if it made one. Called as a worker ends."""
    loop = getattr(_thread_state, "loop", None)
    if loop is not None:
        loop.close()
        _thread_state.loop = None


async def resolve(value: Any) -> Any:
    """Settle what a handler returned, whichever style it was written in.

    Sync and async handlers are both first class and the user never declares
    which they wrote, so this is the one place the difference is resolved — and
    it resolves it by asking where it is running rather than by being told:

    * on an event loop (the asyncio executor), the awaitable is awaited, so the
      loop keeps serving everything else while the handler waits;
    * off one (the thread executor), it is driven to completion on this
      thread's own loop, which is a blocking call on a thread whose whole job
      is this activation.

    The second branch is what lets the thread executor drive the entire
    dispatch without an event loop when nothing is async at all: it never
    suspends, so :func:`run_now` finishes it in one step.
    """
    if not inspect.isawaitable(value):
        return value
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _thread_loop().run_until_complete(value)
    return await value


def run_now(coroutine: Any) -> Any:
    """Drive a coroutine that has nothing to suspend on, with no event loop.

    The dispatch path's only await point is :func:`resolve`, which off a loop
    blocks rather than suspends — so a dispatch driven this way completes in one
    step, and the thread executor pays no event loop for a synchronous handler.
    """
    try:
        coroutine.send(None)
    except StopIteration as stop:
        return stop.value
    coroutine.close()
    raise FlezhaError(
        "the dispatch suspended off an event loop; this is a defect in the SDK's coroutine "
        "seam rather than in the handler"
    )


def value_of(channel: ChannelSpec | None, edge: ConsumerEdge | None, payload: Any) -> Any:
    """A payload as the handler sees it.

    The channel's declared decoder (or the MessagePack default), then the
    consuming edge's transforms in declaration order. :data:`~flezha.builder.DROPPED` when a
    filter rejected it. One function for the two places a payload crosses in —
    an input item, and a payload at the prepare crossing — so a filter cannot
    mean one thing on one path and another on the other.
    """
    if channel is not None and channel.decode is not None:
        decoded = channel.decode(payload)
    else:
        decoded = _codec.decode(payload)
    return apply_transforms(edge.transforms if edge else (), decoded)


async def invoke(
    entity: Entity,
    activation: dict[str, Any],
    deps: DispatchDeps,
    now: int,
    replay: bool,
    body: Callable[[Ctx[Any]], Any],
) -> tuple[Ctx[Any], Any]:
    """Run user code against one entity, and keep the state it produced.

    Every activation kind that reaches a handler goes through here — an input, a
    timer firing, a compensation — so the context they receive and the state
    they leave behind cannot differ between them. The state is written back
    whether or not the body raised: a change recorded before the raise is
    already journaled, and dropping it here would make the live entity disagree
    with its own log.
    """
    ctx: Ctx[Any] = Ctx(
        spec=entity.registered.spec,
        node=entity.node,
        outgoing=entity.registered.outgoing[entity.node.index],
        operative=activation.get("operative", ""),
        entity_key=entity.key,
        change_seq=activation.get("change_seq", 0),
        now=now,
        replay=replay,
        state=entity.state,
        bridge=deps.bridge,
    )
    try:
        with determinism.invocation():
            return ctx, await resolve(body(ctx))
    finally:
        entity.state = ctx.state


async def answer(activation: dict[str, Any], deps: DispatchDeps) -> dict[str, Any]:
    """Produce the answer for one activation.

    Raising is not an option here: every failure becomes a result the core can
    act on, because the alternative is an entity that never receives work again.
    """
    token = activation["token"]
    try:
        assert deps.registry is not None
        registered = deps.registry.get(activation["factory"])
        if registered is None:
            return failed(
                token,
                f"no class registered for factory `{activation['factory']}`",
                non_retryable=True,
            )

        # The workflow body is the one kind addressed to a class rather than to
        # a procedure, so it is answered before the procedure is resolved —
        # which is what lets every arm below hold a procedure rather than
        # assert one.
        if activation["kind"] == "workflow_run":
            return await _run_workflow(activation, registered, deps)

        procedures = registered.spec.procedures
        index = activation["procedure"]
        if index >= len(procedures):
            return failed(
                token,
                f"class `{activation['factory']}` has no procedure at index {index}",
                non_retryable=True,
            )
        entity = registered.entity(procedures[index], activation.get("entity_key"))
        return await _answer_kind(entity, activation, registered, deps)
    except Exception as error:  # noqa: BLE001 - every failure becomes a result
        return failed(token, error_message(error), non_retryable=is_non_retryable(error))


async def _answer_kind(
    entity: Entity, activation: dict[str, Any], registered: RegisteredClass, deps: DispatchDeps
) -> dict[str, Any]:
    """One arm per activation kind the boundary defines."""
    token = activation["token"]
    kind = activation["kind"]

    if kind == "work":
        return await _run_work(entity, activation, deps)
    if kind == "restore_fold":
        return _restore_fold(entity, activation)
    if kind == "snapshot_request":
        return {
            "token": token,
            "outcome": "snapshot",
            "state": entity.proc.encode_state(entity.state),
        }
    # Two names for one read: a live query and a migration handover both want
    # the state as JSON, and only the outcome key differs.
    if kind in ("live_state_read", "migration_serialize"):
        outcome = "state_json" if kind == "live_state_read" else "migration_state"
        return {"token": token, "outcome": outcome, "json": _codec.to_state_json(entity.state)}
    if kind == "apply_state":
        entity.state = _codec.from_state_json(activation.get("state_json"))
        return {"token": token, "outcome": "state_applied"}
    if kind == "key_request":
        return _prepare(activation, registered)
    if kind == "compensate_run":
        return await _compensate(entity, activation, deps)
    return failed(token, f"unknown activation kind `{kind}`", non_retryable=True)


async def _run_work(
    entity: Entity, activation: dict[str, Any], deps: DispatchDeps
) -> dict[str, Any]:
    effects: list[dict[str, Any]] = []

    for item in activation.get("items") or ():
        now = item.get("now") or 0
        try:
            ctx, returned = await invoke(
                entity,
                activation,
                deps,
                now,
                bool(item.get("replay")),
                lambda c, item=item: _run_item(entity, item, c),
            )
            # A returned value is emitted; `None` emits nothing, which is the
            # warmup gate. A handler that emits through the context returns
            # nothing, so the two spellings compose rather than conflict.
            if returned is not None and returned is not DROPPED:
                ctx.emit(returned)
            effects.append(ctx.effect(item.get("consumed_offset")))
        except Exception as error:  # noqa: BLE001 - the verdict is the report
            # The traceback is a host diagnostic, not a record: the verdict
            # below travels to the dead-letter entry and the controller's log.
            log().debug("%s failed an item", entity.name, exc_info=error)
            # Items after this one are re-handed by the core, so the batch stops
            # here rather than reporting effects the entity never saw.
            effects.append(
                {
                    "verdict": "non_retryable" if is_non_retryable(error) else "retryable",
                    "message": error_message(error),
                    "event_time": now,
                    "consumed_offset": item.get("consumed_offset"),
                }
            )
            break

    return {"token": activation["token"], "outcome": "work", "effects": effects}


async def _run_item(entity: Entity, item: dict[str, Any], ctx: Ctx[Any]) -> Any:
    proc = entity.proc
    if item["kind"] == "timer":
        if proc.on_timer is None:
            raise FlezhaError(
                f"{proc.name} registered a timer but declares no on_timer; registration is a "
                f"runtime call, so this cannot be refused at declaration time"
            )
        return await resolve(
            proc.on_timer(
                {
                    "kind": item.get("timer_kind"),
                    "scheduled_at": item.get("scheduled_at") or item.get("now") or 0,
                },
                ctx,
            )
        )

    channel = entity.registered.channel_at(item.get("channel") or 0)
    # This node's own edge off the channel: a transform chain declared before
    # one `into` belongs to that consumer and to no other. It is evaluated here
    # only for a payload that reached us untransformed — an entry payload,
    # whose producer is the wire, or a workflow stage's released value. A
    # stream producer evaluated it as its activation completed, and evaluating
    # it again would compose the chain with itself.
    untransformed = (
        channel is None or channel.kind == "entry" or entity.registered.spec.structured_workflow
    )
    edge = _edge_into(channel, entity.node.index) if untransformed else None
    value = value_of(channel, edge, item.get("payload") or b"")
    # A filtered value is consumed and reaches no handler. It still produces an
    # effect, so its offset advances and nothing is retried.
    if value is DROPPED:
        return None
    return await resolve(proc(value, ctx))


def _restore_fold(entity: Entity, activation: dict[str, Any]) -> dict[str, Any]:
    entity.state = fold_restore(
        entity.proc,
        {"snapshot": activation.get("snapshot"), "changes": activation.get("changes")},
        entity.key,
        entity.name,
    )
    return {"token": activation["token"], "outcome": "restored"}


def _prepare(activation: dict[str, Any], registered: RegisteredClass) -> dict[str, Any]:
    request = activation.get("key_request")
    if request is None:
        return failed(activation["token"], "a key request carried no payloads", non_retryable=True)

    channel = registered.channel_at(request["channel"])
    # The crossing answers one key set for the whole payload, so it reads the
    # first consumer edge — the one the core routes the answer to.
    edge = channel.consumers[0] if channel and channel.consumers else None
    proc = registered.spec.procedures[edge.procedure].proc if edge is not None else None

    answers: list[dict[str, Any]] = []
    for payload in request["payloads"]:
        try:
            value = value_of(channel, edge, payload)
            # A filtered payload answers no key. For a dynamic consumer that is
            # what tells the core not to route it at all — the drop happens
            # before an entity is minted rather than inside one.
            if value is DROPPED:
                answers.append({"keys": []})
                continue
            keys = _keys_for(request, edge, proc, value)
            event_time = (
                proc.event_time_of(value)
                if request["want_event_time"] and proc is not None and proc.event_time is not None
                else None
            )
            answers.append({"keys": keys, "event_time": event_time})
        except Exception as error:  # noqa: BLE001 - a per-payload refusal
            # A per-payload refusal enters the input channel's retry ladder; it
            # is not a failure of the crossing, whose other payloads still have
            # answers.
            answers.append(
                {"error": error_message(error), "non_retryable": is_non_retryable(error)}
            )

    return {"token": activation["token"], "outcome": "prepared", "prepared": answers}


def _edge_into(channel: ChannelSpec | None, node: int) -> ConsumerEdge | None:
    """The channel's edge that feeds `node`, or ``None`` when it feeds none."""
    if channel is None:
        return None
    return next((edge for edge in channel.consumers if edge.procedure == node), None)


def _keys_for(
    request: dict[str, Any],
    edge: ConsumerEdge | None,
    proc: Procedure[Any, Any] | None,
    value: Any,
) -> list[str]:
    if not request["want_keys"]:
        return []
    if edge is not None and edge.key_by is not None:
        return [str(edge.key_by(value))]
    if proc is not None and proc.entities_for is not None:
        return proc.entities_of(value)
    return []


async def _run_workflow(
    activation: dict[str, Any], registered: RegisteredClass, deps: DispatchDeps
) -> dict[str, Any]:
    run = registered.spec.run
    if run is None:
        return failed(
            activation["token"],
            f"class `{activation['factory']}` was handed a workflow body but declares none",
            non_retryable=True,
        )
    assert deps.workflow_ctx is not None

    ctx = deps.workflow_ctx(activation["operative"])
    args = _codec.decode(activation["args"]) if activation.get("args") is not None else None
    with determinism.invocation():
        returned = await resolve(run(args, ctx))
    completed = ctx.completed or returned is not None
    return {
        "token": activation["token"],
        "outcome": "workflow_done",
        "workflow_completed": completed,
        "workflow_result": _codec.encode(ctx.result if ctx.completed else returned),
    }


async def _compensate(
    entity: Entity, activation: dict[str, Any], deps: DispatchDeps
) -> dict[str, Any]:
    proc = entity.proc
    if proc.compensate is None:
        return failed(
            activation["token"], f"{entity.name} declares no compensate hook", non_retryable=True
        )

    outputs = [_codec.decode(payload) for payload in activation.get("outputs") or ()]
    ctx, _ = await invoke(
        entity,
        activation,
        deps,
        0,
        False,
        lambda c: proc.compensate(outputs, c),  # type: ignore[misc]
    )
    return {"token": activation["token"], "outcome": "work", "effects": [ctx.effect(None)]}


def _release_views(activation: dict[str, Any]) -> None:
    """End the validity of every payload view this activation handed out.

    The packaging contract says the borrowed view's lifetime ends with the
    invocation; this is where it ends. A view a handler stashed in state then
    names the rule on its next read instead of serving bytes whose owner is
    gone.
    """
    for value in _views_in(activation):
        release = getattr(value, "release", None)
        if release is not None:
            release()


def _views_in(value: Any) -> Iterable[Any]:
    if isinstance(value, dict):
        for item in value.values():
            yield from _views_in(item)
    elif isinstance(value, list):
        for item in value:
            yield from _views_in(item)
    elif hasattr(value, "release") and hasattr(value, "tobytes"):
        yield value


async def dispatch(activation: dict[str, Any], core: Any, deps: DispatchDeps) -> None:
    """Dispatch one activation and answer it exactly once.

    The guard is the contract, not politeness. An unanswered activation retires
    its entity for the life of the process, so a raise from the dispatcher, from
    encoding the result, or from ``complete`` itself abandons rather than
    logging and returning.
    """
    answered = False
    try:
        core.complete(await answer(activation, deps))
        answered = True
    except Exception as error:  # noqa: BLE001 - the abandon below is the answer
        log().error("activation processing failed: %s", error_message(error), exc_info=error)
    finally:
        if not answered:
            try:
                core.abandon(activation["token"])
            except Exception as error:  # noqa: BLE001 - nothing is wedged
                # The core refuses an unknown token: `complete` had already
                # landed and only the bookkeeping after it raised.
                log().warning(
                    "abandoning activation %s failed: %s",
                    activation["token"],
                    error_message(error),
                )
        _release_views(activation)


def dispatch_now(activation: dict[str, Any], core: Any, deps: DispatchDeps) -> None:
    """Dispatch on this thread, with no event loop unless a handler needs one."""
    run_now(dispatch(activation, core, deps))
