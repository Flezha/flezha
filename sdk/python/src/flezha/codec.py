"""The MessagePack codec for durable ``State`` and ``Change`` bodies.

The mapping is specified here rather than inherited from a library default,
because the defaults are wrong for a format that has to survive a restart:

===============  =========================  ==================================
Python           MessagePack                Why not the default
===============  =========================  ==================================
``bytes``        ``bin``                    so a byte string survives as bytes
``datetime``     timestamp extension        a string or a float loses the type
``int``          int, any width             u64 offsets and seqs must not
                                            narrow, and Python's integers are
                                            arbitrary-precision so they do not
declared class   the dict of its fields     the class name is not durable — a
                                            rename must not break restore
===============  =========================  ==================================

``pickle`` appears nowhere on this path, or on the wire path, for two
independent reasons: loading a pickle executes code, and its payloads embed
class paths so a refactor breaks the restore of every record written before it.
:mod:`flezha.tests` asserts the absence rather than trusting it.

Field evolution is additive: a body is decoded into the fields the class
declares *now*, so a field added with a default takes its default when an older
record does not carry it, and a field since removed is ignored.
"""

from __future__ import annotations

import base64
import dataclasses
import datetime as _datetime
import json
import types
import typing
from typing import Any, Protocol, get_args, get_origin, runtime_checkable

import msgpack

from .errors import UnrepresentableValueError

__all__ = [
    "TypeCodec",
    "ValueCodec",
    "encode",
    "decode",
    "codec_for",
    "assert_representable",
    "to_state_json",
    "from_state_json",
]

# What a plain (undeclared) value may be. Anything else is refused at
# registration, naming the class and the path to the offending field.
_SCALARS = (type(None), bool, int, float, str, bytes, _datetime.datetime)


@runtime_checkable
class ValueCodec(Protocol):
    """How one declared type crosses to durable bytes and back.

    Three built-in adapters implement it — dataclasses, Pydantic models and
    attrs classes — and a class with ``to_dict`` / ``from_dict`` is the escape
    for anything else. A procedure may also be handed one directly.
    """

    def encode(self, value: Any) -> bytes: ...

    def decode(self, body: Any) -> Any: ...


# ---------------------------------------------------------------------------
# The MessagePack default
# ---------------------------------------------------------------------------


def encode(value: Any) -> bytes:
    """Encode a plain value: scalars, lists, dicts and datetimes."""
    return msgpack.packb(value, use_bin_type=True, datetime=True)  # type: ignore[return-value]


def decode(body: Any) -> Any:
    """Decode a plain value from anything that exposes bytes."""
    if body is None:
        return None
    if len(body) == 0:
        return None
    return msgpack.unpackb(body, raw=False, strict_map_key=False, timestamp=3)


# ---------------------------------------------------------------------------
# Declared types
# ---------------------------------------------------------------------------


def _is_attrs(cls: type) -> bool:
    return hasattr(cls, "__attrs_attrs__")


def _is_pydantic(cls: type) -> bool:
    # Duck-typed rather than imported: pydantic is an optional dependency and
    # this module must import cleanly without it.
    return hasattr(cls, "model_validate") and hasattr(cls, "model_fields")


def _is_escape(cls: type) -> bool:
    return hasattr(cls, "to_dict") and hasattr(cls, "from_dict")


def _hints(cls: type) -> dict[str, Any]:
    try:
        return typing.get_type_hints(cls)
    except Exception:
        # An unresolvable forward reference is not fatal: the field is then
        # restored as whatever MessagePack produced, which is what an
        # unannotated field gets anyway.
        return {}


def _field_names(cls: type) -> list[str]:
    if _is_pydantic(cls):
        return list(cls.model_fields)  # type: ignore[attr-defined]
    if dataclasses.is_dataclass(cls):
        return [f.name for f in dataclasses.fields(cls)]
    if _is_attrs(cls):
        return [f.name for f in cls.__attrs_attrs__]  # type: ignore[attr-defined]
    return []


def _to_plain(value: Any) -> Any:
    """One declared value as the plain structure MessagePack carries.

    Recursive, because a State holding a list of declared Changes is ordinary.
    """
    if isinstance(value, _SCALARS):
        return value
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_plain(v) for v in value]
    cls = type(value)
    if _is_pydantic(cls):
        return {k: _to_plain(v) for k, v in value.model_dump(mode="python").items()}
    if dataclasses.is_dataclass(cls):
        return {f.name: _to_plain(getattr(value, f.name)) for f in dataclasses.fields(cls)}
    if _is_attrs(cls):
        return {f.name: _to_plain(getattr(value, f.name)) for f in cls.__attrs_attrs__}
    if _is_escape(cls):
        return _to_plain(value.to_dict())
    raise UnrepresentableValueError(
        cls.__name__,
        "<value>",
        "no codec adapter fits this type; declare it as a dataclass, a Pydantic "
        "model or an attrs class, or give it to_dict()/from_dict()",
    )


def _from_plain(annotation: Any, value: Any) -> Any:
    """Rebuild one declared value from what MessagePack produced.

    Driven by the annotation, which is the same source the graph is wired from:
    a field declared ``list[Item]`` comes back as a list of ``Item``, and a
    field declared ``tuple[int, ...]`` comes back as a tuple rather than as the
    list MessagePack has no other form for.
    """
    if annotation is None or annotation is Any or value is None:
        return value

    origin = get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        arms = [a for a in get_args(annotation) if a is not type(None)]
        # An optional of one type is the common declaration; a wider union is
        # ambiguous on the way back and is left as MessagePack produced it.
        return _from_plain(arms[0], value) if len(arms) == 1 else value
    if origin in (list, set, frozenset):
        (item,) = get_args(annotation) or (Any,)
        return origin(_from_plain(item, v) for v in value)
    if origin is tuple:
        args = get_args(annotation)
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_from_plain(args[0], v) for v in value)
        return tuple(_from_plain(a, v) for a, v in zip(args, value))
    if origin is dict:
        key, item = get_args(annotation) or (Any, Any)
        return {_from_plain(key, k): _from_plain(item, v) for k, v in value.items()}

    if isinstance(annotation, type) and isinstance(value, dict):
        if (
            _is_pydantic(annotation)
            or dataclasses.is_dataclass(annotation)
            or _is_attrs(annotation)
        ):
            return _build(annotation, value)
        if _is_escape(annotation):
            return annotation.from_dict(value)  # type: ignore[attr-defined]
    return value


def _build(cls: type, body: dict[str, Any]) -> Any:
    """Construct one declared class from a decoded dict.

    Only the fields the class declares now are passed, so a record written
    before a field was added takes that field's default and one written before
    a field was removed does not fail on the leftover key.
    """
    hints = _hints(cls)
    kwargs = {
        name: _from_plain(hints.get(name), body[name]) for name in _field_names(cls) if name in body
    }
    return cls(**kwargs)


class TypeCodec:
    """The codec for one author-declared ``State`` or ``Change`` class.

    Built on the same seam a hand-written codec plugs into rather than beside
    it: the dispatcher and the restore fold already ask that seam, and a second
    one would be a second answer to one question.
    """

    __slots__ = ("type", "_which")

    def __init__(self, type_: type, which: str) -> None:
        self.type = type_
        self._which = which

    def encode(self, value: Any) -> bytes:
        return encode(_to_plain(value))

    def decode(self, body: Any) -> Any:
        plain = decode(body)
        if plain is None:
            return None
        if not isinstance(plain, dict):
            # A declared class whose durable form is not a mapping came from a
            # `to_dict` that answered something else; say so here rather than
            # in the constructor's argument list.
            raise UnrepresentableValueError(
                self.type.__name__,
                "<root>",
                f"a durable {self._which} decodes as {type(plain).__name__}, not a mapping",
            )
        if _is_escape(self.type) and not (
            dataclasses.is_dataclass(self.type) or _is_pydantic(self.type) or _is_attrs(self.type)
        ):
            return self.type.from_dict(plain)  # type: ignore[attr-defined]
        return _build(self.type, plain)

    def __repr__(self) -> str:
        return f"<TypeCodec {self._which} of {self.type.__name__}>"


def codec_for(type_: type | None, which: str) -> ValueCodec | None:
    """The codec a declared type resolves to, or ``None`` for the default.

    ``None`` means "encode the value as MessagePack carries it", which is what
    a procedure whose state is a plain dict wants.
    """
    if type_ is None:
        return None
    if (
        dataclasses.is_dataclass(type_)
        or _is_pydantic(type_)
        or _is_attrs(type_)
        or _is_escape(type_)
    ):
        return TypeCodec(type_, which)
    raise UnrepresentableValueError(
        type_.__name__,
        "<root>",
        f"declared as the {which} type but is not a dataclass, a Pydantic model, an "
        "attrs class, or a class with to_dict()/from_dict()",
    )


# ---------------------------------------------------------------------------
# Registration-time representability
# ---------------------------------------------------------------------------


def assert_representable(type_name: str, sample: Any) -> None:
    """Walk what ``initial_state`` produces and refuse what cannot be stored.

    Walked once at registration, so an author learns at startup rather than at
    the first restore that their state does not survive one.
    """
    _walk(type_name, sample, "", set())


def _walk(type_name: str, value: Any, path: str, seen: set[int]) -> None:
    where = path or "<root>"
    reason = _unrepresentable_reason(value)
    if reason:
        raise UnrepresentableValueError(type_name, where, reason)

    children = _children(value)
    if children is None:
        return
    if id(value) in seen:
        raise UnrepresentableValueError(
            type_name,
            where,
            "the value refers back to itself; MessagePack has no representation for a cycle",
        )
    seen.add(id(value))
    for key, item in children:
        _walk(type_name, item, f"{path}.{key}" if path else str(key), seen)
    seen.discard(id(value))


def _children(value: Any) -> list[tuple[Any, Any]] | None:
    """The named parts of a composite value, or ``None`` for a leaf."""
    if isinstance(value, dict):
        return list(value.items())
    if isinstance(value, (list, tuple)):
        return [(f"[{i}]", item) for i, item in enumerate(value)]
    cls = type(value)
    if _is_pydantic(cls) or dataclasses.is_dataclass(cls) or _is_attrs(cls):
        return [(name, getattr(value, name)) for name in _field_names(cls)]
    if _is_escape(cls):
        return [("to_dict()", value.to_dict())]
    return None


def _unrepresentable_reason(value: Any) -> str | None:
    if isinstance(value, _SCALARS) or isinstance(value, (dict, list, tuple)):
        if isinstance(value, dict):
            for key in value:
                if not isinstance(key, (str, int)):
                    return (
                        f"a dict key of {type(key).__name__} has no MessagePack form; declare "
                        "string or integer keys"
                    )
        return None
    cls = type(value)
    if _is_pydantic(cls) or dataclasses.is_dataclass(cls) or _is_attrs(cls) or _is_escape(cls):
        return None
    if isinstance(value, (set, frozenset)):
        return (
            "a set has no MessagePack form and its order is not stable across a restart; "
            "declare a list, or supply a codec"
        )
    if callable(value):
        return "a callable has no durable form; hold the data it closes over instead"
    return (
        f"an instance of {cls.__name__} has no codec adapter; declare it as a dataclass, a "
        "Pydantic model or an attrs class, or give it to_dict()/from_dict()"
    )


# ---------------------------------------------------------------------------
# The JSON form, for a live read and a migration handover
# ---------------------------------------------------------------------------

# JSON has no form for two things this codec carries, so each is marked rather
# than lost: bytes would be mangled by UTF-8 decoding, and a datetime would
# come back a string.
_BYTES_MARKER = "$bytes"
_TIME_MARKER = "$time"


def to_state_json(value: Any) -> bytes:
    return json.dumps(_json_safe(_to_plain(value))).encode()


def from_state_json(body: Any) -> Any:
    if body is None or len(body) == 0:
        return None
    return _json_revive(json.loads(bytes(body).decode()))


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {_BYTES_MARKER: base64.b64encode(value).decode()}
    if isinstance(value, _datetime.datetime):
        return {_TIME_MARKER: [int(value.timestamp()), value.microsecond * 1000]}
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _json_revive(value: Any) -> Any:
    if isinstance(value, dict):
        if len(value) == 1:
            marker = next(iter(value))
            if marker == _BYTES_MARKER:
                return base64.b64decode(value[marker])
            if marker == _TIME_MARKER:
                seconds, nanos = value[marker]
                return _datetime.datetime.fromtimestamp(
                    seconds + nanos / 1_000_000_000, tz=_datetime.timezone.utc
                )
        return {k: _json_revive(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_revive(v) for v in value]
    return value
