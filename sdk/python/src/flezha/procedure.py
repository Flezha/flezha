"""What a declared procedure resolves to, and the decorator that declares one.

A procedure is a handler plus the optional hooks that make it stateful. Two
spellings populate one type, because the two shapes it takes in practice are
genuinely different sizes:

* a function, when the procedure is a transform — it stays an ordinary callable
  a plain unit test invokes with no runtime present;
* a class, when it has state, timers, a clock extractor or a compensation —
  the hooks are its methods, so a declaration and its implementation cannot
  disagree about which ones exist.

Both produce a :class:`Procedure`, and everything downstream — the graph, the
dispatcher, the harness — sees only that.
"""

from __future__ import annotations

import inspect
import re
import types
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Iterable,
    Protocol,
    TypeVar,
    Union,
    cast,
    get_origin,
    overload,
)

from . import codec as _codec
from .errors import DeclarationError
from .entity_state import runtime_mutation

if TYPE_CHECKING:
    from .ctx import Ctx

__all__ = ["Procedure", "procedure", "positional_parameters"]

I = TypeVar("I")
O = TypeVar("O")
# The class-spelling protocol is matched, never constructed, so its parameters
# are declared with the variance a consumer/producer position implies.
I_contra = TypeVar("I_contra", contravariant=True)
O_co = TypeVar("O_co", covariant=True)

Clock = str
EntityMode = str

#: The largest mailbox a procedure may declare. The core refuses more; this is
#: where an author hears about it, with the range in the message.
MAX_MAILBOX_CAPACITY = 1_048_576


class _ClassHandler(Protocol[I_contra, O_co]):
    """The shape the class spelling of a procedure has."""

    def process(self, value: I_contra, ctx: Ctx[Any], /) -> O_co: ...


def _snake_case(name: str) -> str:
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name).lower()


def positional_parameters(fn: Callable[..., Any], *, skip_self: bool = True) -> list[str]:
    """The names of a callable's positional parameters.

    Every declaration this SDK reads is read from a signature — how many
    positional parameters a handler takes, which one carries the input, which
    one carries the context — so the walk lives here and the four readers ask
    it rather than each spelling `POSITIONAL_OR_KEYWORD` again.
    """
    return [
        p.name
        for p in inspect.signature(fn).parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        and not (skip_self and p.name == "self")
    ]


class Procedure(Generic[I, O]):
    """One declared procedure: its handler, its hooks and its declarations.

    Holds no per-entity state — an entity's state lives in the dispatcher's
    registry and is handed to each invocation — so one instance per declaration
    is enough and the dispatcher allocates nothing per activation.
    """

    __slots__ = (
        "name",
        "handler",
        "clock",
        "entity_keys",
        "dynamic",
        "idle_ttl_ms",
        "max_entities",
        "mailbox_capacity",
        "schema_version",
        "min_readable_schema_version",
        "state_type",
        "change_type",
        "state_codec",
        "change_codec",
        "initial_state",
        "apply_change",
        "entities_for",
        "event_time",
        "on_timer",
        "compensate",
        "migrate_state",
        "migrate_change",
        "_target",
        "_handler_takes_ctx",
        "__name__",
        "__wrapped__",
        "doc",
    )

    def __init__(
        self,
        target: Any,
        *,
        name: str | None = None,
        clock: Clock = "wall",
        entity_keys: Iterable[str] | None = None,
        dynamic_entities: bool = False,
        idle_ttl_ms: int | None = None,
        max_entities: int | None = None,
        mailbox_capacity: int | None = None,
        schema_version: int | None = None,
        min_readable_schema_version: int | None = None,
        state: type | None = None,
        change: type | None = None,
    ) -> None:
        self._target = target
        instance: Any = target() if inspect.isclass(target) else None
        handler: Any = getattr(instance, "process", None) if instance is not None else target
        if not callable(handler):
            raise DeclarationError(
                "<procedure>",
                getattr(target, "__name__", str(target)),
                "declares no `process`; a procedure is what runs against an input",
            )
        self.handler: Callable[..., Any] = handler

        self.name = name or _snake_case(getattr(target, "__name__", "procedure"))
        self.__name__ = self.name
        #: The handler's own docstring. Not `__doc__`, which `__slots__` may
        #: not name on a class that has one of its own.
        self.doc = target.__doc__
        self.__wrapped__ = self.handler
        # A handler may be written `(value)` or `(value, ctx)`: the first is
        # what a transform needs and the second what an emit, a timer or a
        # change needs. Which one it is is read from the signature.
        self._handler_takes_ctx = len(positional_parameters(self.handler)) >= 2

        self.clock = clock
        self.entity_keys = list(entity_keys) if entity_keys is not None else None
        self.dynamic = dynamic_entities
        self.idle_ttl_ms = idle_ttl_ms
        self.max_entities = max_entities
        self.mailbox_capacity = mailbox_capacity
        self.schema_version = schema_version
        self.min_readable_schema_version = min_readable_schema_version

        hook = _hooks_of(instance)
        self.initial_state: Callable[[str | None], Any] | None = hook("initial_state")
        self.apply_change: Callable[[Any, Any], Any] | None = hook("apply_change")
        self.entities_for: Callable[[Any], Iterable[str]] | None = hook("entities_for")
        self.event_time: Callable[[Any], int] | None = hook("event_time")
        self.on_timer: Callable[..., Any] | None = hook("on_timer")
        self.compensate: Callable[..., Any] | None = hook("compensate")
        self.migrate_state: Callable[[int, Any], Any] | None = hook("migrate_state")
        self.migrate_change: Callable[[int, Any], Any] | None = hook("migrate_change")

        # Type hints drive the codec exactly as they drive the graph: the state
        # type is what `initial_state` returns and the change type is what
        # `apply_change` folds, so a declaration cannot disagree with the code.
        self.state_type = state or _returned_type(self.initial_state)
        self.change_type = change or _parameter_type(self.apply_change, 1)
        _refuse_union_change(self, change)
        self.state_codec = _codec.codec_for(self.state_type, "state")
        self.change_codec = _codec.codec_for(self.change_type, "change")

        self._validate()

    # -- the ordinary callable -------------------------------------------

    def __call__(self, value: I, ctx: Ctx[Any] | None = None) -> O:
        """Invoke the handler directly, as an ordinary Python callable.

        This is what makes a decorated handler unit-testable with no runtime
        present: the decorator returns something that forwards, not something
        that has to be driven.
        """
        if self._handler_takes_ctx:
            if ctx is None:
                raise TypeError(
                    f"{self.name} takes a context; pass one, or use "
                    f"flezha.testing.ProcedureTestHarness to supply it"
                )
            return cast("O", self.handler(value, ctx))
        return cast("O", self.handler(value))

    def __repr__(self) -> str:
        return f"<flezha procedure {self.name}>"

    # -- what the dispatcher and the graph ask ---------------------------

    @property
    def entity_mode(self) -> EntityMode:
        if self.entity_keys is not None:
            return "static"
        return "dynamic" if self.dynamic else "single"

    def build_initial_state(self, entity_key: str | None) -> Any:
        return self.initial_state(entity_key) if self.initial_state else None

    def fold_change(self, state_value: Any, change: Any) -> None:
        """Fold one change into the live state, through the runtime's own path.

        ``apply_change`` mutates in place and its answer is ignored — the same
        contract Rust's ``&mut S`` and the other SDKs carry — so a scalar state
        is declared as a class or a dict rather than reassigned here.
        """
        assert self.apply_change is not None
        with runtime_mutation():
            self.apply_change(state_value, change)

    def encode_state(self, value: Any) -> bytes:
        """One durable state body, through the declared codec or the default.

        The four encode/decode pairs live here because this is where the codecs
        are resolved; the dispatcher, the restore fold, the context and the test
        harness all ask rather than each repeating the choice.
        """
        return self.state_codec.encode(value) if self.state_codec else _codec.encode(value)

    def decode_state(self, body: Any) -> Any:
        return self.state_codec.decode(body) if self.state_codec else _codec.decode(body)

    def encode_change(self, value: Any) -> bytes:
        return self.change_codec.encode(value) if self.change_codec else _codec.encode(value)

    def decode_change(self, body: Any) -> Any:
        return self.change_codec.decode(body) if self.change_codec else _codec.decode(body)

    def event_time_of(self, value: Any) -> int:
        assert self.event_time is not None
        return int(self.event_time(value))

    def entities_of(self, value: Any) -> list[str]:
        assert self.entities_for is not None
        answer = self.entities_for(value)
        if isinstance(answer, (str, bytes)):
            return [answer if isinstance(answer, str) else answer.decode()]
        return [str(key) for key in answer]

    # -- registration-time refusals owned by the procedure ---------------

    def _refuse(self, defect: str) -> None:
        raise DeclarationError("<procedure>", self.name, defect)

    def _validate(self) -> None:
        if self.entity_keys is not None and self.dynamic:
            self._refuse(
                "declares both a static key set and dynamic entities; the two modes are exclusive"
            )
        if self.entity_keys is not None and not self.entity_keys:
            self._refuse("declares an empty static key set; a static procedure names its entities")
        # State without a fold is legitimate: a value seeded at start and read
        # but never mutated has nothing to journal, and `ctx.change` already
        # refuses at the point of use. A fold with no state to fold into is not.
        if self.initial_state is None and self.apply_change is not None:
            self._refuse(
                "declares apply_change but no initial_state; there is nothing to fold into"
            )
        if self.clock not in ("wall", "logical", "event_time"):
            self._refuse(
                f"declares clock {self.clock!r}; the declared clocks are wall, logical and event_time"
            )
        _refuse_unannotated(self)
        self._validate_clock()
        self._validate_entities()
        self._validate_schema()
        # A state that cannot be encoded cannot be restored, and the first
        # anyone would learn of it is a restart. Walk the declared starting
        # state now.
        if self.initial_state is not None and self.state_codec is None:
            _codec.assert_representable(f"{self.name}.state", self.build_initial_state(None))

    def _validate_clock(self) -> None:
        """The declared clock against what it needs to evaluate.

        The core evaluates the declared clock per input, so a clock with
        nothing to evaluate is refused here rather than at the first item.
        """
        if self.clock == "event_time" and self.event_time is None:
            self._refuse(
                "declares an event_time clock but no event_time extractor; the core evaluates "
                "the declared clock per input and has nothing to evaluate"
            )
        if self.clock != "event_time" and self.event_time is not None:
            self._refuse(
                f"declares an event_time extractor with clock `{self.clock}`; the extractor is "
                f"only consulted by an event_time clock, so one of the two is not what was meant"
            )
        if self.clock == "logical" and self.initial_state is None:
            self._refuse(
                "declares a logical clock but no state; logical time IS the durable change "
                "sequence, which never advances for a procedure that records no changes"
            )

    def _validate_entities(self) -> None:
        """The entity mode against the declarations it needs."""
        if self.entity_mode == "static" and self.entities_for is None:
            self._refuse(
                "declares a static key set without an entities_for; static keying needs the "
                "method that says which declared entities an input belongs to"
            )
        capacity = self.mailbox_capacity
        if capacity is not None and not (1 <= capacity <= MAX_MAILBOX_CAPACITY):
            self._refuse(
                f"declares mailbox_capacity {capacity}; capacity must be in "
                f"1..{MAX_MAILBOX_CAPACITY}"
            )
        if self.max_entities == 0:
            self._refuse(
                "declares max_entities 0; a dynamic procedure that may hold no entity can "
                "process nothing"
            )

    def _validate_schema(self) -> None:
        """The readable floor against the running version and its migration."""
        floor = self.min_readable_schema_version
        if floor is None:
            return
        running = self.schema_version or 1
        if floor > running:
            self._refuse(
                f"declares min_readable_schema_version {floor} above its schema_version {running}"
            )
        if floor < running and self.migrate_state is None and self.migrate_change is None:
            self._refuse(
                f"declares it reads back to schema version {floor} but supplies no migration; "
                f"a floor without one fails at the first old record instead of at startup"
            )


def _hooks_of(instance: Any) -> Callable[[str], Any]:
    def hook(name: str) -> Any:
        return getattr(instance, name, None) if instance is not None else None

    return hook


def _annotations(fn: Callable[..., Any] | None) -> dict[str, Any]:
    if fn is None:
        return {}
    try:
        import typing

        return typing.get_type_hints(fn)
    except Exception:
        return getattr(fn, "__annotations__", {}) or {}


def _declared_type(annotation: Any) -> type | None:
    """The class an annotation names, or ``None`` for "undeclared".

    ``Any`` and ``object`` are *not* declarations: they say the author did not
    name a type, and the answer to that is the MessagePack default rather than
    a refusal. (`Any` is a class from 3.11 on, so the plain `isinstance` check
    accepts it and the codec then refuses a type nobody declared.) A union is
    not a declaration either, and is refused where it is used, because a decoded
    body has nothing in it that says which arm wrote it.
    """
    if annotation is None or annotation is Any or annotation is object:
        return None
    return annotation if isinstance(annotation, type) else None


def _returned_type(fn: Callable[..., Any] | None) -> type | None:
    return _declared_type(_annotations(fn).get("return"))


def _parameter_annotation(fn: Callable[..., Any] | None, index: int) -> Any:
    if fn is None:
        return None
    names = positional_parameters(fn)
    if index >= len(names):
        return None
    return _annotations(fn).get(names[index])


def _parameter_type(fn: Callable[..., Any] | None, index: int) -> type | None:
    return _declared_type(_parameter_annotation(fn, index))


def input_type(proc: Procedure[Any, Any]) -> Any:
    """The type the handler accepts, as its annotation names it."""
    return _annotations(proc.handler).get(_first_parameter(proc.handler))


def output_type(proc: Procedure[Any, Any]) -> Any:
    """The type the handler returns, as its annotation names it."""
    return _annotations(proc.handler).get("return")


def _first_parameter(fn: Callable[..., Any]) -> str:
    names = positional_parameters(fn)
    return names[0] if names else ""


def _refuse_union_change(proc: Procedure[Any, Any], declared: type | None) -> None:
    """A union change type is refused, naming the two ways to spell a sum.

    A durable body carries the codec's bytes and nothing that says which arm of
    a union wrote them, so a union would decode to whichever arm happened to be
    tried first — a defect that surfaces only on a restore. Every SDK answers
    this the same way: one Change type per procedure. Rust's is an enum, and
    Python's is either one class with the variants as fields, or a plain dict.
    """
    if declared is not None:
        return
    annotation = _parameter_annotation(proc.apply_change, 1)
    if annotation is None or get_origin(annotation) not in (Union, types.UnionType):
        return
    raise DeclarationError(
        "<procedure>",
        proc.name,
        f"declares its change as the union `{annotation}`; a durable body carries no tag "
        f"saying which arm wrote it, so the restore would guess. Declare one change class "
        f"whose fields cover the variants, or annotate it `Any` and record plain dicts",
    )


def _refuse_unannotated(proc: Procedure[Any, Any]) -> None:
    """A handler parameter with no annotation cannot be wired.

    The graph is built from the hints, so an unannotated parameter is not a
    style question — it is a wiring the SDK cannot check and a type checker
    cannot see. Refused here, naming the handler and the parameter, rather than
    silently wiring anything to anything.
    """
    name = _first_parameter(proc.handler)
    if not name:
        raise DeclarationError(
            "<procedure>",
            proc.name,
            "takes no input parameter; a procedure is what runs against an input",
        )
    if name not in _annotations(proc.handler):
        raise DeclarationError(
            "<procedure>",
            proc.name,
            f"parameter `{name}` has no type annotation; the graph is wired from the hints, "
            f"so an unannotated input cannot be checked against the edge that feeds it",
        )


# ---------------------------------------------------------------------------
# The decorator
# ---------------------------------------------------------------------------


@overload
def procedure(target: type[_ClassHandler[I, O]], /) -> Procedure[I, O]: ...


@overload
def procedure(target: Callable[[I, Ctx[Any]], O], /) -> Procedure[I, O]: ...


@overload
def procedure(target: Callable[[I], O], /) -> Procedure[I, O]: ...


@overload
def procedure(
    *,
    name: str | None = ...,
    clock: Clock = ...,
    entity_keys: Iterable[str] | None = ...,
    dynamic_entities: bool = ...,
    idle_ttl_ms: int | None = ...,
    max_entities: int | None = ...,
    mailbox_capacity: int | None = ...,
    schema_version: int | None = ...,
    min_readable_schema_version: int | None = ...,
    state: type | None = ...,
    change: type | None = ...,
) -> Callable[[Any], Procedure[Any, Any]]: ...


def procedure(target: Any = None, /, **declarations: Any) -> Any:
    """Declare a procedure.

    Decorates a handler function, or a class whose ``process`` is the handler
    and whose other methods are the hooks. Declaring performs no I/O, opens no
    socket and starts no thread: it builds one :class:`Procedure` and returns
    it.
    """
    if target is None:
        return lambda inner: Procedure(inner, **declarations)
    return Procedure(target, **declarations)
