"""Twins of the rust_runner e2e fixtures (``test/apps/rust_runner/src/main.rs``)
under the same factory ids, procedure names, channel names and output bytes.

Entry payloads and plugin frames are UTF-8 text on the wire, the same as the
reference: the SDK's default codec is MessagePack, so every fixture that
exchanges bytes with another SDK or with a plugin declares its own decoder and
encoder. An inter-procedure hop between two Python procedures carries
MessagePack, which no one else reads.
"""

from __future__ import annotations

import struct
import sys
import time
from typing import Any, ClassVar

import flezha
from fixture_support import _as_int, _bytes_through, _env_int, passthrough, prefix, raw, text
from fixtures_state import (
    bench_checkpoint_operative,
    bench_keyed_nopersist_operative,
    bench_keyed_operative,
    checkpoint_batched_operative,
    checkpoint_store_operative,
    coalescing_counter_operative,
    persistence_off_operative,
    resume_probe_operative,
    schema_evolution_v1_operative,
    schema_evolution_v2_operative,
    stateful_counter_operative,
    store_input_operative,
    tier_store_operative,
)

# -- do_nothing / bench_echo -------------------------------------------------

do_nothing = passthrough("do_nothing_procedure")


@flezha.stream_operative("do_nothing")
def do_nothing_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(do_nothing).sink("echo", encode=raw)


@flezha.stream_operative("bench_echo")
def bench_echo_operative(c: flezha.Channels) -> None:
    """Echo with a declared DropNothing on its entry mailbox.

    ``do_nothing`` is the merge-blocking bench factory and must not change; this
    keeps the round-trip bench's back-pressure posture independent of it.
    """
    c.input("input", decode=text).on_full("drop_nothing").into(do_nothing).sink("echo", encode=raw)


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
    c.input("input", decode=text).into(compare_filter).sink("sink", encode=raw)


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
    c.input("input", decode=text).into(static_entry).named("hop").into(static_downstream).sink(
        "kafka", encode=raw
    )


@flezha.stream_operative("sugar_static_entity")
def sugar_static_entity_operative(c: flezha.Channels) -> None:
    """The reference pipeline sugar leaves the hop unnamed and lets the sink
    name the producing channel."""
    c.input("input", decode=text).into(macro_static_entry).into(macro_static_downstream).sink(
        "test_kafka", encode=raw
    )


mixed_macro_entry = passthrough("mixed_macro_entry_procedure")


mixed_hand_downstream = passthrough("mixed_hand_downstream")


@flezha.stream_operative("mixed_operative")
def mixed_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(mixed_macro_entry).named("hop").into(
        mixed_hand_downstream
    ).sink("kafka", encode=raw)


# -- sugar_do_nothing / sugar_store_input ------------------------------------


sugar_do_nothing = passthrough("sugar_do_nothing")


@flezha.stream_operative("sugar_do_nothing")
def sugar_do_nothing_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(sugar_do_nothing).sink("sugar_do_nothing_echo", encode=raw)


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
    c.input("input", decode=text).into(sugar_store_input).sink("sugar_store_sink", encode=raw)


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
        lambda edge: edge.sink(encode=hybrid_encode),
    )


# -- entity_stress -----------------------------------------------------------

STRESS_KEYS = ["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"]


class StressFanOut:
    """Answers its input; the fan-out is which entities it names."""

    #: The entities every input reaches. `stress_a` fans out to all of them and
    #: the rest funnel back to one, which is the topology the stress exercises.
    targets: ClassVar[list[str]] = STRESS_KEYS

    def process(self, value: str) -> str:
        return value

    def entities_for(self, value: str) -> list[str]:
        return self.targets


class StressToP0(StressFanOut):
    targets: ClassVar[list[str]] = ["p0"]


stress_a = flezha.procedure(StressFanOut, name="stress_a", entity_keys=STRESS_KEYS)
stress_b = flezha.procedure(StressToP0, name="stress_b", entity_keys=STRESS_KEYS)
stress_c = flezha.procedure(StressToP0, name="stress_c", entity_keys=STRESS_KEYS)
stress_d = flezha.procedure(StressToP0, name="stress_d", entity_keys=STRESS_KEYS)


@flezha.stream_operative("entity_stress")
def entity_stress_operative(c: flezha.Channels) -> None:
    c.input("input", decode=text).into(stress_a).named("a_to_b").into(stress_b).named(
        "b_to_c"
    ).into(stress_c).named("c_to_d").into(stress_d).sink("echo", encode=raw)


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
    c.input("input", decode=text).into(candle).keyed().sink("sink", encode=raw)


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
    c.input("input", decode=text).retry_backoff_ms(25).into(poison_probe).sink("out", encode=raw)


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
    c.input("input", decode=text).into(wedge_probe).sink("out", encode=raw)


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
    c.input("input", decode=text).into(session_timeout).sink("kafka", encode=_bytes_through)


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
    """One operative per overflow policy, over the same bounded-mailbox procedure."""
    procedure = flezha.procedure(
        BoundedMailbox, name=f"{policy}_test_procedure", mailbox_capacity=4
    )

    @flezha.stream_operative(f"bounded_mailbox_{policy}")
    def operative(c: flezha.Channels) -> None:
        c.input(f"{policy}_input", decode=text).on_full(policy).into(procedure).sink(
            f"{policy}_echo", encode=raw
        )

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
    ).key_by(prefix).into(dynamic_entity).sink("kafka", encode=raw)


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
