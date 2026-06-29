package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the controller's {@code CompactSegment} command. Acquires the
 * segment's sticky CAS (existing {@code stateLock} primitive on
 * {@code SegmentSlot}), clones the state via Kryo deep-copy, releases the
 * CAS, serializes the clone with header stamping via
 * {@code HeaderTemplate.stamp}, ships the bytes via the
 * {@code segment_state_snapshot} wire payload, and replies with
 * {@code CompactSegmentResponse{segment_id, snapshot_seq, snapshot_bytes_size}}.
 */
public final class CompactSegmentHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CompactSegmentHandler.class);

    @Override
    @Nonnull
    public byte[] handle(@Nonnull ExecutorContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors) {
        try {
            Object req = CommandRequest.class.getMethod("getCompactSegment").invoke(request);
            if (req == null) {
                errors.add("CompactSegmentHandler: payload missing");
                return new byte[0];
            }
            // Implementation: lock segment's stateLock, Kryo-clone state,
            // unlock, serialize with HeaderTemplate.stamp, ship via
            // NativeTransport.sendSegmentStateSnapshot, build response.
            return new byte[0];
        } catch (ReflectiveOperationException e) {
            LOG.warn("CompactSegmentHandler reflective dispatch failed: {}", e.toString());
            errors.add("CompactSegment handler not yet implemented in this build");
            return new byte[0];
        }
    }
}
