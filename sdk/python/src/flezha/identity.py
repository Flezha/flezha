"""Entity identity bytes, and the replay-stable identifier derived from them.

Both algorithms are fixed by the SDK contract rather than by a library, so
every SDK derives the same value from the same entity: the identity is
length-prefixed ``instance | procedure | key``, and the identifier is FNV-1a
over ``identity | change seq | per-invocation ordinal``.
"""

from __future__ import annotations

import struct

from .errors import FlezhaError

__all__ = ["entity_identity", "stable_id"]

_FNV128_OFFSET_BASIS = 0x6C62272E07BB014262B821756295C58D
_FNV128_PRIME = 0x0000000001000000000000000000013B
_MASK128 = (1 << 128) - 1


def _fnv1a128(seed: int, data: bytes) -> int:
    value = seed
    for byte in data:
        value = (value ^ byte) & _MASK128
        value = (value * _FNV128_PRIME) & _MASK128
    return value


def entity_identity(instance_id: str, procedure_name: str, entity_key: str | None) -> bytes:
    """``[len instance][instance][len procedure][procedure][u16 len key][key]``.

    Big-endian lengths, the same layout every SDK writes, so an identifier
    derived here matches one derived there for the same entity.
    """
    instance = instance_id.encode()
    procedure = procedure_name.encode()
    key = (entity_key or "").encode()
    if len(instance) > 255 or len(procedure) > 255:
        raise FlezhaError("operative class / procedure id must be at most 255 bytes")
    if len(key) > 0xFFFF:
        raise FlezhaError("entity key must be at most 65535 bytes")
    return (
        struct.pack("B", len(instance))
        + instance
        + struct.pack("B", len(procedure))
        + procedure
        + struct.pack(">H", len(key))
        + key
    )


def stable_id(identity: bytes, change_seq: int, ordinal: int) -> str:
    """Thirty-two lowercase hex characters.

    Repeated calls in one invocation differ; the n-th call of a replayed
    invocation equals the n-th call of the original, which is what makes it the
    replacement for a random UUID.
    """
    value = _fnv1a128(_FNV128_OFFSET_BASIS, identity)
    value = _fnv1a128(value, struct.pack(">Q", change_seq & 0xFFFFFFFFFFFFFFFF))
    value = _fnv1a128(value, struct.pack(">I", ordinal & 0xFFFFFFFF))
    return f"{value:032x}"
