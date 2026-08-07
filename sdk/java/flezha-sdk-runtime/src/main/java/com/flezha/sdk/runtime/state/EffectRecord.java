package com.flezha.sdk.runtime.state;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nonnull;

/**
 * Wire body of a {@code entity_effect_record} frame (header
 * {@code format_id = 2}): one invocation's state changes, plugin emits,
 * consumed input offset, and — for a parked poison payload — its dead-letter
 * entry, all persisted by the controller as ONE WAL record under ONE fsync.
 *
 * <p>That atomicity is the whole point of the effectively-once tier: a crash
 * can never advance the consumed offset while losing the state or the output it
 * was advanced for.
 *
 * <p><b>Two codecs, deliberately.</b> The body is postcard, because the
 * controller decodes it. The {@code changes} elements are the SDK's own change
 * bytes (Kryo, exactly what a plain {@code entity_state_change} record
 * carries) — the controller stores them opaquely and re-ships them on restore,
 * where this SDK folds them back through {@code applyChange}. Rust puts its own
 * postcard change bytes in the same slot for the same reason.
 *
 * <p>{@code timers} carries the invocation's mutations of the Entity's
 * pending-timer set. A firing's removal rides here beside the Changes and
 * outputs it produced, which is what makes the firing exactly-once under the
 * tier: a crash between "fired" and "durable" replays the whole thing, because
 * neither the effects nor the removal landed.
 *
 * <p>Field order is the wire contract; it must match the Rust
 * {@code EffectRecordBody} exactly, and is pinned by
 * {@code EffectRecordTest}.
 */
public record EffectRecord(
        @Nonnull List<byte[]> changes,
        @Nonnull List<OutputRecord> outputs,
        @Nonnull OptionalLong consumedOffset,
        @Nonnull Optional<DlqPark.DlqEntry> dlq,
        long eventTime,
        @Nonnull List<TimerOp> timers) {

    /** An effect record with no timer ops — the shape every non-timer invocation produces. */
    public EffectRecord(
            @Nonnull List<byte[]> changes,
            @Nonnull List<OutputRecord> outputs,
            @Nonnull OptionalLong consumedOffset,
            @Nonnull Optional<DlqPark.DlqEntry> dlq,
            long eventTime) {
        this(changes, outputs, consumedOffset, dlq, eventTime, List.of());
    }

    /**
     * One plugin output captured inside an effect record. {@code metadata}
     * carries the caller's user metadata only — the controller derives the
     * identity stamps at flush time, so none ride here.
     */
    public record OutputRecord(
            @Nonnull String channel,
            @Nonnull String key,
            @Nonnull List<Map.Entry<String, String>> metadata,
            @Nonnull byte[] payload) {}

    /** Postcard encoding, byte-identical to Rust's {@code postcard::to_allocvec(EffectRecordBody)}. */
    @Nonnull
    public byte[] encode() {
        Postcard.Writer w = new Postcard.Writer();
        w.vec(changes, Postcard.Writer::bytes);
        w.vec(outputs, (out, o) -> {
            out.string(o.channel()).string(o.key());
            out.vec(o.metadata(), (m, e) -> m.string(e.getKey()).string(e.getValue()));
            out.bytes(o.payload());
        });
        w.optionalVarint(consumedOffset);
        w.optional(dlq, DlqPark::encodeDlqEntry);
        w.varint(eventTime);
        w.vec(timers, TimerOp::encode);
        return w.toBytes();
    }

    /**
     * Inverse of {@link #encode}. A restore folds an effect record's change
     * bytes and its timer ops exactly as it folds the standalone records of each
     * kind, so the change log stays readable however the tier was configured
     * over the Entity's life.
     */
    @Nonnull
    public static EffectRecord decode(@Nonnull byte[] body) {
        Postcard.Reader r = new Postcard.Reader(body);
        List<byte[]> changes = r.vec(Postcard.Reader::bytes);
        List<OutputRecord> outputs = r.vec(reader -> new OutputRecord(
                reader.string(), reader.string(), reader.vec(m -> Map.entry(m.string(), m.string())), reader.bytes()));
        OptionalLong consumedOffset = r.optionalVarint();
        Optional<DlqPark.DlqEntry> dlq = r.some() ? Optional.of(DlqPark.decodeDlqEntry(r)) : Optional.empty();
        long eventTime = r.varint();
        List<TimerOp> timers = r.vec(TimerOp::decode);
        return new EffectRecord(changes, outputs, consumedOffset, dlq, eventTime, timers);
    }
}
