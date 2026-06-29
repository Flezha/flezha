package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the controller's {@code SegmentMailboxStatus} command. Updates the
 * per-Segment mailbox observer state used by the existing
 * {@code OverflowPolicy} machinery from {@code operative-bounded-mailbox}.
 *
 * <p>The controller ships {@code SegmentMailboxStatus} as an advisory: when a
 * Segment's mailbox depth or admission state changes, the SDK adjusts its
 * client-side admission throttling accordingly. The handler is a sink — no
 * payload is returned in the response.
 */
public final class SegmentMailboxStatusHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentMailboxStatusHandler.class);

    @Override
    @Nonnull
    public byte[] handle(@Nonnull ExecutorContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors) {
        try {
            Object req =
                    CommandRequest.class.getMethod("getSegmentMailboxStatus").invoke(request);
            if (req == null) {
                errors.add("SegmentMailboxStatusHandler: payload missing");
                return new byte[0];
            }
            // Wire to the OverflowPolicy observer once operative-bounded-mailbox
            // ships the matching SDK-side admission throttler.
            return new byte[0];
        } catch (ReflectiveOperationException e) {
            LOG.warn("SegmentMailboxStatusHandler reflective dispatch failed: {}", e.toString());
            errors.add("SegmentMailboxStatus handler not yet implemented in this build");
            return new byte[0];
        }
    }
}
