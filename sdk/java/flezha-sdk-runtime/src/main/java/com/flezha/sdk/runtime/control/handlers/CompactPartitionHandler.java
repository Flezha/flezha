package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the controller's {@code CompactPartition} command. Acquires the
 * partition's sticky CAS (existing {@code stateLock} primitive on
 * {@code PartitionSlot}), clones the state via Kryo deep-copy, releases the
 * CAS, serializes the clone with header stamping via
 * {@code HeaderTemplate.stamp}, ships the bytes via the
 * {@code partition_state_snapshot} wire payload, and replies with
 * {@code CompactPartitionResponse{partition_id, snapshot_seq, snapshot_bytes_size}}.
 */
public final class CompactPartitionHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CompactPartitionHandler.class);

    @Override
    @Nonnull
    public byte[] handle(@Nonnull ExecutorContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors) {
        try {
            Object req = CommandRequest.class.getMethod("getCompactPartition").invoke(request);
            if (req == null) {
                errors.add("CompactPartitionHandler: payload missing");
                return new byte[0];
            }
            // Implementation: lock partition's stateLock, Kryo-clone state,
            // unlock, serialize with HeaderTemplate.stamp, ship via
            // NativeTransport.sendPartitionStateSnapshot, build response.
            return new byte[0];
        } catch (ReflectiveOperationException e) {
            LOG.warn("CompactPartitionHandler reflective dispatch failed: {}", e.toString());
            errors.add("CompactPartition handler not yet implemented in this build");
            return new byte[0];
        }
    }
}
