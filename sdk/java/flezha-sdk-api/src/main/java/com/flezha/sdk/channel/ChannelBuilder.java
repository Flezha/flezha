package com.flezha.sdk.channel;

import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Producer-first fluent graph builder consumed by
 * {@link com.flezha.sdk.operative.StreamOperative#channels(ChannelBuilder)}
 * (and by workflow {@code procedures(...)} authoring).
 *
 * <p>{@code c.input(T.class, "name").into(A.class).into(B.class).sink("plugin")}
 * — {@code .into} wires a consumer and returns that procedure's <b>output
 * stream handle</b> ({@link Stream}), so a linear pipeline is one chained
 * expression. Branching is handle reuse (hold the handle in a variable and
 * wire it twice) or the equivalent {@link Stream#branch} sugar. Fan-in is two
 * streams naming the same consumer class.
 *
 * <p>Entry channel names stay explicit and mandatory — they are the
 * operative's external input address. Inter-procedure channel names
 * auto-derive from the producer node's identity, with {@link Stream#named}
 * as the override. Declarations land in a per-operative topology the runtime
 * freezes at registration.
 */
public interface ChannelBuilder {

    /**
     * Declare the entry channel fed by the controller. Any payload type is
     * accepted: {@code byte[]} is delivered unchanged and {@code String}
     * UTF-8-decoded, byte-identical to the pre-typed-entry contract; any
     * other type is deserialized at the entry edge from the wire bytes with
     * the SDK's default JSON codec (Jackson). A failing parse is classified
     * like a failing invocation and rides this channel's retry→DLQ ladder.
     * For a non-JSON wire format, use
     * {@link #input(Class, String, Function)}.
     */
    @Nonnull
    <T> EntryChannel<T> input(@Nonnull Class<T> type, @Nonnull String name);

    /**
     * Declare the entry channel with a custom {@code bytes → T} parser,
     * replacing the default JSON codec of {@link #input(Class, String)}.
     *
     * <p>The parser MUST be deterministic — the same bytes always produce the
     * same value, with no ambient reads (time, randomness, I/O) — the same
     * contract output serializers carry. A thrown exception is classified
     * like a failing invocation and rides this channel's retry→DLQ ladder.
     */
    @Nonnull
    <T> EntryChannel<T> input(@Nonnull Class<T> type, @Nonnull String name, @Nonnull Function<byte[], T> parser);

    /**
     * Open a <b>merged</b> subscription: one procedure whose {@code Input} is
     * the sum type {@code type}, fed by two or more source channels that each
     * supply a mapping into it. See {@link Merge}.
     */
    @Nonnull
    <I> Merge<I> merge(@Nonnull Class<I> type);

    /**
     * Opt this Operative into the <b>effectively-once output tier</b>: each
     * invocation's state changes, plugin outputs and consumed input offset ship
     * as ONE {@code entity_effect_record}, which the controller persists as one
     * WAL record under one fsync. A crash can then never advance the consumed
     * offset while losing the state or the output it was advanced for.
     *
     * <p>Every plugin-bound channel of a tier-enabled Operative MUST be declared
     * {@code .keyed()} — the tier's replay re-emits outputs, and only a keyed
     * sink can absorb that as an idempotent upsert. A non-keyed plugin channel
     * fails registration.
     */
    void effectivelyOnce();

    /**
     * Acknowledge the consumed input offset only at an explicit
     * {@code ctx.checkpoint()}, rather than after every invocation.
     *
     * <p>The interval between checkpoints is the work a crash replays and the
     * work the controller's input log must retain. In exchange, an Entity whose
     * state is only coherent at a boundary (a closed window, a completed batch)
     * never persists a half-built view of it.
     *
     * <p>Requires {@link #effectivelyOnce()} — there is no offset to withhold
     * without the tier — and declaring it alone fails registration with
     * {@code CheckpointAckWithoutTier}.
     */
    void checkpointAck();

    /**
     * Declare that this Operative corrects itself when an input arrives late:
     * an input whose event time falls behind the Entity's event-time high-water
     * mark, but by less than {@code windowMs}, triggers the controller to rewind
     * the Entity to a baseline before that event time and replay the affected
     * slice in event-time order.
     *
     * <p>The window is a promise the controller must be able to keep: it sizes
     * the input log's hold window and the snapshot age floor, because the past
     * being repaired has to still exist. That is why the window is declared to
     * the controller rather than handled purely SDK-side.
     *
     * <p>Requires {@link #effectivelyOnce()} (a correction rewinds a consumed
     * offset), an {@code EventTimeFrom} clock (there is no event time to compare
     * without one), and every channel keyed (a corrected re-emit must overwrite,
     * not duplicate). Each is checked at registration.
     */
    void correctionWindow(long windowMs);

    /**
     * How to treat an input that arrives beyond the declared
     * {@link #correctionWindow(long)}. Defaults to
     * {@link BeyondWindowPolicy#PROCESS_AS_ARRIVAL}.
     */
    void beyondWindowPolicy(@Nonnull BeyondWindowPolicy policy);
}
