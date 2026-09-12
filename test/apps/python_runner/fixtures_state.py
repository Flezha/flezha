"""Fixtures whose subject is persisted Entity state: the keyed upsert the
bench and tier classes share, the counter and resume twins, schema evolution,
and the effectively-once tier store with its checkpoint variants.
"""

from __future__ import annotations

import os
from typing import Any

import flezha
from fixture_support import _as_int, _bytes_through, _env_int, raw, text
from flezha import codec as flezha_codec

# -- bench_keyed_nopersist ---------------------------------------------------


class KeyedUpsert:
    """The reference's `TierStoreState` and its upsert fold.

    `tier_store`, `checkpoint_store` and `bench_checkpoint` are the same keyed
    upsert under three acknowledgement postures; only `process` differs.
    """

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"values": {}}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["values"][change["key"]] = change["value"]

    def upsert(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        """Record and emit one `key:value` input under its natural key."""
        key, _, item = value.partition(":")
        ctx.change({"key": key, "value": item})
        ctx.emit(f"{key}={item}", key=key)


class TierStore(KeyedUpsert):
    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        if value.partition(":")[2] == "poison":
            raise ValueError("poison value")
        self.upsert(value, ctx)


tier_store = flezha.procedure(TierStore, name="tier_store_procedure")


@flezha.stream_operative("bench_keyed_nopersist", persistent=False)
def bench_keyed_nopersist_operative(c: flezha.Channels) -> None:
    """Per-entity persistence off: the heap-only posture the bench isolates
    against the durable one."""
    c.input("input", decode=text).on_full("drop_nothing").into(tier_store).keyed().sink(
        "sink", encode=raw
    )


# -- store_input / stateful_counter / coalescing_counter / persistence_off ---
# -- resume_probe / bench_keyed ----------------------------------------------


class StoreInput:
    """The durable-state reference: every input is appended through a change."""

    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> str:
        ctx.change({"push_text": value})
        return value

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"binary_inputs": [], "text_inputs": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["text_inputs"].append(change["push_text"])


store_input = flezha.procedure(StoreInput, name="store_input_procedure")


@flezha.stream_operative("store_input")
def store_input_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(store_input).sink("sink", encode=raw)


@flezha.procedure(name="counter_entry_procedure")
def counter_entry(value: str) -> int:
    return _as_int(value)


class Counter:
    def process(self, value: int, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        ctx.change({"delta": value})

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"value": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["value"] += change["delta"]


counter = flezha.procedure(Counter, name="counter_procedure")


@flezha.stream_operative("stateful_counter")
def stateful_counter_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(counter_entry).named("ticks").into(counter)


@flezha.stream_operative("persistence_off_counter", persistent=False)
def persistence_off_operative(c: flezha.Channels) -> None:
    """The same topology with per-Entity persistence off.

    Its own node names, as the reference declares them: a node identity is per
    class, so reusing `stateful_counter`'s would leave the two classes
    indistinguishable to anything that reads a declaration.
    """
    c.input("input", decode=text).into(counter_entry, "persistence_off_entry_procedure").named(
        "ticks"
    ).into(counter, "persistence_off_counter_procedure")


@flezha.procedure(name="tick_entry_procedure")
def tick_entry(value: str) -> dict[str, int]:
    """Wire format: ``<interval>:<ts>:<delta>``."""
    parts = value.split(":", 2)

    def number(index: int) -> int:
        return _as_int(parts[index]) if index < len(parts) else 0

    return {"interval": number(0), "ts": number(1), "delta": number(2)}


class Tick:
    """A change whose coalesce key is `(interval, ts)`.

    The SDK carries no coalesce-key seam — the Rust host's is a `Change` trait
    method — so the change ships uncoalesced, which is a throughput property
    and not a semantic one: the fold is idempotent per `(interval, ts)` either
    way, so replay produces the same state.
    """

    def process(self, value: dict[str, int], ctx: flezha.Ctx[dict[str, Any]]) -> None:
        ctx.change(dict(value))

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"last": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["last"] = change["delta"]


tick = flezha.procedure(Tick, name="tick_procedure")


@flezha.stream_operative("coalescing_counter")
def coalescing_counter_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(tick_entry).named("ticks").into(tick)


class ResumeProbe:
    """Emits the running total, so a resume is observable on the wire."""

    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> bytes:
        delta = _as_int(value)
        total = ctx.state["total"] + delta
        ctx.change({"add": delta})
        return str(total).encode()

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        # Default + change replay rebuilds the total on restore (the same
        # pattern as stateful_counter); no snapshot fast path needed.
        return {"total": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["total"] += change["add"]


resume_probe = flezha.procedure(ResumeProbe, name="resume_probe_procedure")


@flezha.stream_operative("resume_probe")
def resume_probe_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(resume_probe).sink("total", encode=_bytes_through)


@flezha.stream_operative("bench_keyed")
def bench_keyed_operative(c: flezha.Channels) -> None:
    """`bench_keyed_nopersist`'s durable twin: the same keyed upsert with
    per-Entity persistence on, so the bench can isolate that cost."""
    c.input("input", decode=text).on_full("drop_nothing").into(
        tier_store, "tier_store_procedure"
    ).keyed().sink("sink", encode=raw)


# -- schema_evolution_v1 / schema_evolution_v2 -------------------------------
#
# The same class either side of a deploy that edits its own state type. Both
# halves declare the SAME procedure name, so an operative started under v1 and
# restarted under v2 restores from the very WAL v1 wrote — which is what a
# deploy does, with the two binaries collapsed into one fixture file.
#
# v1: `{total}`. v2: `{total, currency}` at schema version 2, reading down to 1
# through a declared migration. Output is `total:currency` so the e2e can read
# the migrated field off the wire.


class EvolvingCartV1:
    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> bytes:
        delta = _as_int(value)
        total = ctx.state["total"] + delta
        ctx.change({"add": delta})
        return f"{total}:".encode()

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"total": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["total"] += change["add"]


class EvolvingCartV2:
    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> bytes:
        delta = _as_int(value)
        total = ctx.state["total"] + delta
        currency = ctx.state.get("currency") or "EUR"
        ctx.change({"add": delta, "note": "live"})
        return f"{total}:{currency}".encode()

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"total": 0, "currency": ""}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["total"] += change["add"]

    def migrate_state(self, from_version: int, body: Any) -> dict[str, Any]:
        if from_version != 1:
            raise ValueError(f"no migration from {from_version}")
        old = flezha_codec.decode(body) or {}
        return {"total": old.get("total", 0), "currency": "EUR"}

    def migrate_change(self, from_version: int, body: Any) -> dict[str, Any]:
        if from_version != 1:
            raise ValueError(f"no migration from {from_version}")
        old = flezha_codec.decode(body) or {}
        return {"add": old.get("add", 0), "note": "migrated"}


evolving_cart_v1 = flezha.procedure(EvolvingCartV1, name="evolving_cart")
evolving_cart_v2 = flezha.procedure(
    EvolvingCartV2,
    name="evolving_cart",
    schema_version=2,
    min_readable_schema_version=1,
)


@flezha.stream_operative("schema_evolution_v1")
def schema_evolution_v1_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(evolving_cart_v1).sink("total", encode=_bytes_through)


@flezha.stream_operative("schema_evolution_v2")
def schema_evolution_v2_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(evolving_cart_v2).sink("total", encode=_bytes_through)


# -- tier_store / checkpoint_store / checkpoint_batched_store / bench_checkpoint


@flezha.stream_operative("tier_store")
def tier_store_operative(c: flezha.Channels) -> None:
    """The effectively-once reference: a keyed upsert under the tier.

    A 300 ms backoff base keeps the min-watermark freeze observable in the DLQ
    e2e. Declared DropNothing: a durable-input-log operative that shed at its
    mailbox would hold the min-watermark on the shed offset forever while the
    log behind it kept growing.
    """
    c.effectively_once()
    c.input("input", decode=text).on_full("drop_nothing").retry_backoff_ms(300).into(
        tier_store, "tier_store_procedure"
    ).keyed().sink("sink", encode=raw)


class CheckpointStore(KeyedUpsert):
    """``tier_store``'s twin, whose interval closes on a `commit:` input."""

    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        if value.partition(":")[0] == "commit":
            ctx.checkpoint()
            return
        self.upsert(value, ctx)


checkpoint_store = flezha.procedure(CheckpointStore, name="checkpoint_store_procedure")


@flezha.stream_operative("checkpoint_store")
def checkpoint_store_operative(c: flezha.Channels) -> None:
    c.effectively_once().checkpoint_ack()
    c.input("input", decode=text).into(checkpoint_store).keyed().sink("sink", encode=raw)


@flezha.stream_operative("checkpoint_batched_store")
def checkpoint_batched_operative(c: flezha.Channels) -> None:
    """The same procedure declared ``outputs_at_checkpoint``.

    The interval's outputs are held with its changes and ship inside its
    checkpoint record. ``checkpoint_every`` is deliberately absent so the test
    drives the close itself — with ``commit:`` for the batch case, and with the
    delay alone for the quiet-Entity case.
    """
    c.effectively_once().checkpoint_ack().outputs_at_checkpoint(1_500)
    c.input("input", decode=text).into(checkpoint_store, "checkpoint_store_procedure").keyed().sink(
        "sink", encode=raw
    )


class BenchCheckpoint(KeyedUpsert):
    """``checkpoint_store`` driven by a DECLARED cadence rather than an input."""

    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        self.upsert(value, ctx)


bench_checkpoint = flezha.procedure(BenchCheckpoint, name="bench_checkpoint_procedure")


@flezha.stream_operative("bench_checkpoint")
def bench_checkpoint_operative(c: flezha.Channels) -> None:
    c.effectively_once().checkpoint_every(_env_int("FLEZHA_BENCH_CHECKPOINT_EVERY", 500) or 500)
    if os.environ.get("FLEZHA_BENCH_CHECKPOINT_OUTPUTS") == "at_checkpoint":
        c.outputs_at_checkpoint(_env_int("FLEZHA_BENCH_CHECKPOINT_DELAY_MS", 50) or 50)
    c.input("input", decode=text).on_full("drop_nothing").into(bench_checkpoint).keyed().sink(
        "sink", encode=raw
    )
