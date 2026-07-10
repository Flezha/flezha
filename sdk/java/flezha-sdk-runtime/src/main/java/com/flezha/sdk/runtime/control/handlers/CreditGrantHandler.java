package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.state.CreditWindow;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the controller's {@code CreditGrant}: replenishes the sender
 * window. Runs on the control router and never blocks, so grants keep
 * flowing while data-path threads are parked on exhausted credit. One-way
 * — the generic {@code CommandResponse} carries no payload.
 */
public final class CreditGrantHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CreditGrantHandler.class);

    @Override
    @Nonnull
    public byte[] handle(@Nonnull ExecutorContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors) {
        try {
            Object grant = CommandRequest.class.getMethod("getCreditGrant").invoke(request);
            if (grant == null) {
                errors.add("CreditGrantHandler: payload missing");
                return new byte[0];
            }

            String streamId = (String) grant.getClass().getMethod("getStreamId").invoke(grant);
            long bytes = (Long) grant.getClass().getMethod("getBytes").invoke(grant);

            CreditWindow.grant(streamId, bytes);
            return new byte[0];
        } catch (ReflectiveOperationException e) {
            LOG.warn("CreditGrantHandler reflective dispatch failed: {}", e.toString());
            errors.add("CreditGrant handler not available in this build");
            return new byte[0];
        }
    }
}
