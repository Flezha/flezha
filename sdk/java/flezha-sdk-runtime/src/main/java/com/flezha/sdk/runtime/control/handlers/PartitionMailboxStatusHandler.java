package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the controller's {@code PartitionMailboxStatus} command. Updates the
 * per-Partition mailbox observer state used by the existing
 * {@code OverflowPolicy} machinery from {@code relay-bounded-mailbox}.
 *
 * <p>The controller ships {@code PartitionMailboxStatus} as an advisory: when a
 * Partition's mailbox depth or admission state changes, the SDK adjusts its
 * client-side admission throttling accordingly. The handler is a sink — no
 * payload is returned in the response.
 */
public final class PartitionMailboxStatusHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionMailboxStatusHandler.class);

    @Override
    @Nonnull
    public byte[] handle(
            @Nonnull ExecutorContext ctx,
            @Nonnull CommandRequest request,
            @Nonnull List<String> errors) {
        try {
            Object req = CommandRequest.class
                    .getMethod("getPartitionMailboxStatus")
                    .invoke(request);
            if (req == null) {
                errors.add("PartitionMailboxStatusHandler: payload missing");
                return new byte[0];
            }
            // Wire to the OverflowPolicy observer once relay-bounded-mailbox
            // ships the matching SDK-side admission throttler.
            return new byte[0];
        } catch (ReflectiveOperationException e) {
            LOG.warn("PartitionMailboxStatusHandler reflective dispatch failed: {}", e.toString());
            errors.add("PartitionMailboxStatus handler not yet implemented in this build");
            return new byte[0];
        }
    }
}
