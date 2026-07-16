package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Legacy controller → SDK back-pressure signal, kept only for compatibility
 * with pre-credit controllers. A credit-speaking controller never sends this
 * (back-pressure rides the credit-grant protocol — see
 * {@code com.flezha.sdk.runtime.state.CreditWindow}); a pre-credit
 * controller's {@code "full"}/{@code "open"} flags are tolerated and
 * dropped, since this SDK's send path is already bounded by its local
 * mailbox policies. One-way — no payload is returned in the response.
 */
public final class SegmentMailboxStatusHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentMailboxStatusHandler.class);

    @Override
    @Nonnull
    public byte[] handle(@Nonnull RunnerContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors) {
        LOG.debug("legacy segment mailbox status from a pre-credit controller (dropped)");
        return new byte[0];
    }
}
