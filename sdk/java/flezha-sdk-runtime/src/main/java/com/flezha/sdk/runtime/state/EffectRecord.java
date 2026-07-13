package com.flezha.sdk.runtime.state;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nonnull;

/**
 * Wire body of a {@code segment_effect_record} frame (header
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
 * bytes (Kryo, exactly what a plain {@code segment_state_change} record
 * carries) — the controller stores them opaquely and re-ships them on restore,
 * where this SDK folds them back through {@code applyChange}. Rust puts its own
 * postcard change bytes in the same slot for the same reason.
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
        long eventTime) {

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
        return w.toBytes();
    }
}
