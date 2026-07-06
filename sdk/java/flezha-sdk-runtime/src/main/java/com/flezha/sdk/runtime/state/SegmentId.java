package com.flezha.sdk.runtime.state;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Wire identity of a Segment. Combines the operative-instance id (= controller's
 * {@code operative_id}), the Operation's name within the UnboundedOperative, and the segment
 * key (empty string for single-segment mode).
 *
 * <p>The controller keys each per-Segment WAL by this identity and echoes it
 * back verbatim as {@code RestoreInitState.segment_id}, so {@link #encodedBytes()}
 * MUST be byte-identical to the controller's {@code Flezha.State.Wire.encode_segment_id/3}.
 */
public record SegmentId(String instanceId, String operationName, String segmentKey) {

    public static SegmentId of(
            @Nonnull String instanceId, @Nonnull String operationName, @Nonnull Optional<String> segmentKey) {
        return new SegmentId(instanceId, operationName, segmentKey.orElse(""));
    }

    /** Slash-joined form used only as an in-process map key / log label. */
    @Nonnull
    public String encoded() {
        return instanceId + "/" + operationName + "/" + segmentKey;
    }

    /**
     * Structured binary identity consumed by the controller's
     * {@code decode_segment_id}:
     * {@code [rc_len:u8][rc][sh_len:u8][sh][pk_len:u16 BE][pk]}, where
     * {@code rc} = operative-instance id, {@code sh} = operation name, {@code pk} =
     * segment key (all UTF-8). Mirrors the Rust SDK's
     * {@code encode_segment_identity}.
     */
    @Nonnull
    public byte[] encodedBytes() {
        byte[] rc = instanceId.getBytes(StandardCharsets.UTF_8);
        byte[] sh = operationName.getBytes(StandardCharsets.UTF_8);
        byte[] pk = segmentKey.getBytes(StandardCharsets.UTF_8);
        if (rc.length > 255 || sh.length > 255) {
            throw new IllegalArgumentException("operative_class / operation_id must be <= 255 bytes");
        }
        if (pk.length > 0xFFFF) {
            throw new IllegalArgumentException("segment_key must be <= 65535 bytes");
        }
        ByteBuffer buf = ByteBuffer.allocate(1 + rc.length + 1 + sh.length + 2 + pk.length)
                .order(ByteOrder.BIG_ENDIAN);
        buf.put((byte) rc.length);
        buf.put(rc);
        buf.put((byte) sh.length);
        buf.put(sh);
        buf.putShort((short) pk.length);
        buf.put(pk);
        return buf.array();
    }
}
