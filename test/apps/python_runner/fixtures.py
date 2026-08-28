"""Twins of the rust_runner e2e fixtures (``test/apps/rust_runner/src/main.rs``)
under the same factory ids, procedure names, channel names and output bytes.

Entry payloads and plugin frames are UTF-8 text on the wire, the same as the
reference: the SDK's default codec is MessagePack, so every fixture that
exchanges bytes with another SDK or with a plugin declares its own decoder and
encoder. An inter-procedure hop between two Python procedures carries
MessagePack, which no one else reads.
"""

from __future__ import annotations

import os
import struct
import sys
import time
from typing import Any

import flezha
from flezha import codec as flezha_codec


def text(body: Any) -> str:
    """The wire bytes as the reference's `String` entry payload."""
    return bytes(body).decode()


def raw(value: Any) -> bytes:
    """A plugin frame as the reference's bytes."""
    return str(value).encode()


def prefix(value: str) -> str:
    """The key convention every keyed fixture shares: text before the first ':'."""
    return value.split(":", 1)[0]


def _as_int(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        return 0


def _bytes_through(value: Any) -> bytes:
    """A handler that already produced the wire bytes hands them through."""
    return value if isinstance(value, bytes) else raw(value)


def _env_int(key: str, default: int | None) -> int | None:
    """A positive integer from the environment, or the default.

    The e2e sets three of these — a timer timeout, a dynamic-entity TTL and the
    bench's checkpoint cadence — and each was reading it its own way.
    """
    declared = os.environ.get(key, "")
    return int(declared) if declared.isdigit() and int(declared) > 0 else default


def passthrough(name: str) -> flezha.Procedure[str, str]:
    """A procedure that answers its input, under the reference's node name.

    Six fixtures declare exactly this and differ only in the name the topology
    knows them by, which is what the Ruby app spells as a `PassThrough` base
    with six subclasses.
    """

    @flezha.procedure(name=name)
    def handler(value: str) -> str:
        return value

    return handler


# -- do_nothing / bench_echo -------------------------------------------------

do_nothing = passthrough("do_nothing_procedure")


@flezha.stream_operative("do_nothing")
def do_nothing_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(do_nothing).named("echo").sink("do_nothing_echo", encode=raw)


@flezha.stream_operative("bench_echo")
def bench_echo_operative(c: flezha.Channels) -> None:
    """Echo with a declared DropNothing on its entry mailbox.

    ``do_nothing`` is the merge-blocking bench factory and must not change; this
    keeps the round-trip bench's back-pressure posture independent of it.
    """
    c.input("input", decode=text).on_full("drop_nothing").into(do_nothing).named("echo").sink(
        "bench_echo_sink", encode=raw
    )


# -- compare_filter ----------------------------------------------------------


class CompareFilter:
    """State seeded by the controller's init values and then only read.

    There is nothing to journal, so the procedure declares no change fold.
    """

    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> str | None:
        return value if ctx.state["comparable"] == value else None

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"comparable": "default", "inverted": False}


compare_filter = flezha.procedure(CompareFilter, name="compare_filter_procedure")


@flezha.stream_operative("compare_filter")
def compare_filter_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(compare_filter).named("sink").sink(
        "compare_filter_sink", encode=raw
    )


# -- static_entity / sugar_static_entity / mixed_operative -------------------

STATIC_KEYS = ["a", "b", "c", "d"]


class SeenLog:
    """The reference's `seen: Vec<String>` state and its append-only fold.

    Three fixtures declare exactly this — the static entry, the partition relay
    and the dynamic entity — and differ only in what they do after recording.
    Sharing it is how the Ruby app spells the same thing.
    """

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"seen": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["seen"].append(change["push"])


class StaticEntry(SeenLog):
    """The static fan-out every static fixture shares."""

    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> str:
        ctx.change({"push": value})
        return value

    def entities_for(self, value: str) -> list[str]:
        first = value[0] if value else ""
        return [first if first in STATIC_KEYS else "d"]


static_entry = flezha.procedure(StaticEntry, name="static_entry_procedure", entity_keys=STATIC_KEYS)
macro_static_entry = flezha.procedure(
    StaticEntry, name="macro_static_entry_procedure", entity_keys=STATIC_KEYS
)


static_downstream = passthrough("static_downstream_procedure")


macro_static_downstream = passthrough("macro_static_downstream_procedure")


@flezha.stream_operative("static_entity")
def static_entity_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(static_entry).named("hop").into(static_downstream).named(
        "kafka"
    ).sink("test_kafka", encode=raw)


@flezha.stream_operative("sugar_static_entity")
def sugar_static_entity_operative(c: flezha.Channels) -> None:
    """The reference pipeline sugar leaves the hop unnamed and names the plugin
    channel after the plugin."""
    c.input("input", decode=text).into(macro_static_entry).into(macro_static_downstream).named(
        "test_kafka"
    ).sink("test_kafka", encode=raw)


mixed_macro_entry = passthrough("mixed_macro_entry_procedure")


mixed_hand_downstream = passthrough("mixed_hand_downstream")


@flezha.stream_operative("mixed_operative")
def mixed_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(mixed_macro_entry).named("hop").into(
        mixed_hand_downstream
    ).named("kafka").sink("test_kafka", encode=raw)


# -- sugar_do_nothing / sugar_store_input ------------------------------------


sugar_do_nothing = passthrough("sugar_do_nothing")


@flezha.stream_operative("sugar_do_nothing")
def sugar_do_nothing_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(sugar_do_nothing).named("sugar_do_nothing_echo").sink(
        "sugar_do_nothing_echo", encode=raw
    )


class SugarStoreInput:
    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> str:
        ctx.change({"push": value})
        return value

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"text_inputs": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["text_inputs"].append(change["push"])


sugar_store_input = flezha.procedure(SugarStoreInput, name="sugar_store_input")


@flezha.stream_operative("sugar_store_input")
def sugar_store_input_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(sugar_store_input).named("sugar_store_sink").sink(
        "sugar_store_sink", encode=raw
    )


# -- hybrid_channel ----------------------------------------------------------
#
# One emit reaching two destinations: an inter-procedure consumer and a plugin
# wire with its own serializer. The serializer refuses a sentinel value, so an
# operator can observe end to end that a refusal is isolated to the emit rather
# than failing the item.

HYBRID_SENTINEL = 0xDEADBEEF


@flezha.procedure(name="hybrid_producer_procedure")
def hybrid_producer(value: str, ctx: flezha.Ctx[None]) -> None:
    try:
        parsed = int(value)
    except ValueError:
        parsed = 0
    number = parsed if 0 <= parsed <= 0xFFFFFFFF else 0
    try:
        ctx.emit(number)
    except Exception as error:  # noqa: BLE001 - the isolation is the fixture
        print(
            f"hybrid_producer_procedure: serializer refused an emit: {error}",
            file=sys.stderr,
            flush=True,
        )


@flezha.procedure(name="hybrid_consumer_procedure")
def hybrid_consumer(value: int) -> None:
    print(f"hybrid_consumer_procedure: received {value}", flush=True)


def hybrid_encode(value: int) -> bytes:
    if value == HYBRID_SENTINEL:
        raise ValueError("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF")
    return struct.pack(">I", value)


@flezha.stream_operative("hybrid_channel")
def hybrid_channel_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(hybrid_producer).branch(
        # The consumer edge first, so a refused plugin serialization leaves the
        # in-process fan-out already recorded — the reference's isolation.
        lambda edge: edge.named("hybrid_indicator").into(hybrid_consumer),
        lambda edge: edge.sink("hybrid_indicators", encode=hybrid_encode),
    )


# -- entity_stress -----------------------------------------------------------

STRESS_KEYS = ["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"]


class StressFanOut:
    """Answers its input; the fan-out is which entities it names."""

    #: The entities every input reaches. `stress_a` fans out to all of them and
    #: the rest funnel back to one, which is the topology the stress exercises.
    targets = STRESS_KEYS

    def process(self, value: str) -> str:
        return value

    def entities_for(self, value: str) -> list[str]:
        return self.targets


class StressToP0(StressFanOut):
    targets = ["p0"]


stress_a = flezha.procedure(StressFanOut, name="stress_a", entity_keys=STRESS_KEYS)
stress_b = flezha.procedure(StressToP0, name="stress_b", entity_keys=STRESS_KEYS)
stress_c = flezha.procedure(StressToP0, name="stress_c", entity_keys=STRESS_KEYS)
stress_d = flezha.procedure(StressToP0, name="stress_d", entity_keys=STRESS_KEYS)


@flezha.stream_operative("entity_stress")
def entity_stress_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(stress_a).named("a_to_b").into(stress_b).named(
        "b_to_c"
    ).into(stress_c).named("c_to_d").into(stress_d).named("echo").sink(
        "entity_stress_echo", encode=raw
    )


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
    c.input("input", decode=text).on_full("drop_nothing").into(tier_store).named(
        "sink"
    ).keyed().sink("tier_sink", encode=raw)


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
    c.input("input", decode=text).into(store_input).named("sink").sink(
        "store_input_sink", encode=raw
    )


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

    Same procedure names as `stateful_counter`, because the toggle is a class
    property and not a different computation.
    """
    c.input("input", decode=text).into(counter_entry, "counter_entry_procedure").named(
        "ticks"
    ).into(counter, "counter_procedure")


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
    c.input("input", decode=text).into(resume_probe).named("total").sink(
        "resume_probe_out", encode=_bytes_through
    )


@flezha.stream_operative("bench_keyed")
def bench_keyed_operative(c: flezha.Channels) -> None:
    """`bench_keyed_nopersist`'s durable twin: the same keyed upsert with
    per-Entity persistence on, so the bench can isolate that cost."""
    c.input("input", decode=text).on_full("drop_nothing").into(
        tier_store, "tier_store_procedure"
    ).named("sink").keyed().sink("tier_sink", encode=raw)


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
    c.input("input", decode=text).into(evolving_cart_v1).named("total").sink(
        "schema_evolution_out", encode=_bytes_through
    )


@flezha.stream_operative("schema_evolution_v2")
def schema_evolution_v2_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(evolving_cart_v2).named("total").sink(
        "schema_evolution_out", encode=_bytes_through
    )


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
    ).named("sink").keyed().sink("tier_sink", encode=raw)


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
    c.input("input", decode=text).into(checkpoint_store).named("sink").keyed().sink(
        "tier_sink", encode=raw
    )


@flezha.stream_operative("checkpoint_batched_store")
def checkpoint_batched_operative(c: flezha.Channels) -> None:
    """The same procedure declared ``outputs_at_checkpoint``.

    The interval's outputs are held with its changes and ship inside its
    checkpoint record. ``checkpoint_every`` is deliberately absent so the test
    drives the close itself — with ``commit:`` for the batch case, and with the
    delay alone for the quiet-Entity case.
    """
    c.effectively_once().checkpoint_ack().outputs_at_checkpoint(1_500)
    c.input("input", decode=text).into(checkpoint_store, "checkpoint_store_procedure").named(
        "sink"
    ).keyed().sink("tier_sink", encode=raw)


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
    c.input("input", decode=text).on_full("drop_nothing").into(bench_checkpoint).named(
        "sink"
    ).keyed().sink("tier_sink", encode=raw)


# -- candle ------------------------------------------------------------------
#
# Late-input correction is about fold ORDER, and the only way to prove that is
# to look at the order. Send 100, 300, then the late 200. Without correction the
# fold order is [100, 300, 200] and the candle is permanently wrong. With it,
# the engine rewinds behind 200, replays the retained input log sorted by event
# time, and the fold order becomes [100, 200, 300].


class Candle:
    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        # `ctx.now()` IS the extracted event time under an event-time clock.
        event_time = ctx.now()
        ctx.change({"fold": event_time})
        # One row per candle bucket, so a corrected replay overwrites by key
        # rather than appending a second row.
        bucket = event_time // 1_000
        ctx.emit(f"{bucket}={value}", key=f"candle/{bucket}")

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        #: Event times in fold order. The assertion surface.
        return {"folded": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["folded"].append(change["fold"])

    def event_time(self, value: str) -> int:
        return _as_int(value.split(":", 1)[0])


candle = flezha.procedure(Candle, name="candle_procedure", clock="event_time")


@flezha.stream_operative("candle")
def candle_operative(c: flezha.Channels) -> None:
    c.effectively_once().correction_window(600_000)
    c.input("input", decode=text).into(candle).named("sink").keyed().sink("tier_sink", encode=raw)


# -- poison_probe / wedge_probe ----------------------------------------------

_ANTIDOTE = {"taken": False}


@flezha.procedure(name="poison_probe_procedure")
def poison_probe(value: str) -> str:
    if value == "antidote":
        _ANTIDOTE["taken"] = True
    if value == "poison" and not _ANTIDOTE["taken"]:
        raise ValueError("poison payload")
    return f"ok:{value}"


@flezha.stream_operative("poison_probe")
def poison_probe_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).retry_backoff_ms(25).into(poison_probe).named("out").sink(
        "probe_sink", encode=raw
    )


_ABORTED = {"done": False}


@flezha.procedure(name="wedge_probe_procedure")
def wedge_probe(value: str) -> str:
    """The ``pool_wedge`` fixture: the two ways a runner stops doing work.

    ``wedge`` blocks the dispatch inside user code forever, so the SDK's
    dispatch counter stops advancing while the heartbeat keeps reporting the
    runner alive: connected, heartbeating, processing nothing. ``abort`` ends
    its activation as violently as this host allows, WITHOUT emitting, once per
    process — CPython has no way for user code to kill a worker outright (the
    dispatcher catches per item and the raise rides the retry ladder), so what
    the scenario asserts is that the Entity is served again and the aborting
    item does not produce twice.
    """
    if value == "wedge":
        while True:
            time.sleep(3600)
    if value == "abort" and not _ABORTED["done"]:
        _ABORTED["done"] = True
        raise RuntimeError("wedge_probe: killing the worker holding this activation")
    return f"ok:{value}"


@flezha.stream_operative("wedge_probe")
def wedge_probe_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(wedge_probe).named("out").sink("wedge_sink", encode=raw)


# -- session_timeout ---------------------------------------------------------
#
# The worked session-timeout shape (`entity_timers`): each input pushes the idle
# deadline out, and the firing closes the session. Single-entity so the e2e's
# crash-resume half exercises a durable Entity. The timeout is read from
# `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).


class SessionTimeout:
    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        ctx.change({"touched": value})
        # A processing-time timer, because the answer to "nothing has arrived"
        # cannot be an event-time one: nothing would advance the clock.
        ctx.timer_after(_env_int("TIMER_TEST_TIMEOUT_MS", 1_000) or 1_000)

    def on_timer(self, timer: dict[str, Any], ctx: flezha.Ctx[dict[str, Any]]) -> bytes:
        fired_at = int(timer["scheduled_at"])
        ctx.change({"closed": fired_at})
        return f"closed@{fired_at}".encode()

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"touched": [], "closed_at": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        if "touched" in change:
            state["touched"].append(change["touched"])
        else:
            state["closed_at"].append(change["closed"])


session_timeout = flezha.procedure(SessionTimeout, name="session_timeout_procedure")


@flezha.stream_operative("session_timeout")
def session_timeout_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(session_timeout).named("kafka").sink(
        "test_kafka", encode=_bytes_through
    )


# -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} ------------------


class BoundedMailbox:
    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> str:
        ctx.change({"processed": True})
        return value

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"processed": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["processed"] += 1


def _bounded_mailbox(policy: str) -> flezha.Operative:
    """One operative per overflow policy, over the same bounded procedure."""
    procedure = flezha.procedure(
        BoundedMailbox, name=f"{policy}_test_procedure", mailbox_capacity=4
    )

    @flezha.stream_operative(f"bounded_mailbox_{policy}")
    def operative(c: flezha.Channels) -> None:
        c.input(f"{policy}_input", decode=text).on_full(policy).into(procedure).named(
            f"{policy}_echo"
        ).sink("bounded_mailbox_echo", encode=raw)

    return operative


BOUNDED_MAILBOX = [
    _bounded_mailbox(policy) for policy in ("drop_nothing", "drop_newest", "drop_oldest")
]


# -- partition_relay ---------------------------------------------------------
#
# The partitioned-operatives upstream: keyed on the wiring so it can be
# partitioned at all, and emitting keyed so a partitioned forward target can
# resolve the owning member. The plugin channel is named `input` because a
# forwarded frame keeps its producing channel's name, and that is what selects
# the receiving operative's entry channel — so the entry here is `in`.


class PartitionRelay(SeenLog):
    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        ctx.change({"push": value})
        ctx.emit(value, key=prefix(value))


partition_relay = flezha.procedure(
    PartitionRelay, name="partition_relay_procedure", dynamic_entities=True
)


@flezha.stream_operative("partition_relay")
def partition_relay_operative(c: flezha.Channels) -> None:
    # The plugin channel is named `input`, not `relay_out`: a forwarded frame
    # keeps its producing channel's name, and that is what selects the
    # receiving operative's entry channel — the downstream fixture's `input`.
    # The Rust builder gets this by attaching the sink to a channel `named`
    # separately; this builder's `sink` names the channel, so the name goes
    # there. The entry here is `in` so the two cannot collide inside the class.
    c.input("in", decode=text).key_by(prefix).into(partition_relay).keyed().sink(
        "input", encode=raw
    )


# -- dynamic_entity ----------------------------------------------------------


class DynamicEntity(SeenLog):
    def process(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> str:
        ctx.change({"push": value})
        return value


dynamic_entity = flezha.procedure(
    DynamicEntity,
    name="dynamic_entity_procedure",
    dynamic_entities=True,
    idle_ttl_ms=_env_int("DYN_TEST_TTL_MS", None),
)


@flezha.stream_operative("dynamic_entity")
def dynamic_entity_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).on_full("drop_nothing").filter(
        # Filtered before keying: a dropped value answers no key, so the core
        # never mints an entity for it.
        lambda value: value != "drop"
    ).key_by(prefix).into(dynamic_entity).named("kafka").sink("test_kafka", encode=raw)


#: Every operative this app registers, in declaration order.
ALL: list[flezha.Operative] = [
    do_nothing_operative,
    bench_echo_operative,
    compare_filter_operative,
    static_entity_operative,
    sugar_static_entity_operative,
    mixed_operative,
    sugar_do_nothing_operative,
    sugar_store_input_operative,
    hybrid_channel_operative,
    entity_stress_operative,
    bench_keyed_nopersist_operative,
    store_input_operative,
    stateful_counter_operative,
    persistence_off_operative,
    coalescing_counter_operative,
    resume_probe_operative,
    bench_keyed_operative,
    schema_evolution_v1_operative,
    schema_evolution_v2_operative,
    tier_store_operative,
    checkpoint_store_operative,
    checkpoint_batched_operative,
    bench_checkpoint_operative,
    candle_operative,
    poison_probe_operative,
    wedge_probe_operative,
    session_timeout_operative,
    *BOUNDED_MAILBOX,
    partition_relay_operative,
    dynamic_entity_operative,
]
