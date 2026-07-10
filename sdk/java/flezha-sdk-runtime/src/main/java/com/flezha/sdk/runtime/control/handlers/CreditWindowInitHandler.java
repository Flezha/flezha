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
 * Handles the controller's {@code CreditWindowInit} offer: arms the
 * process-global {@link CreditWindow} and returns the encoded
 * {@code CreditWindowAck} as this command's {@code CommandResponse}
 * payload — the controller marks the session credit-speaking on that ack.
 * Payload access is reflective so a stale protobuf codegen jar degrades to
 * "not implemented" instead of failing class-load.
 */
public final class CreditWindowInitHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CreditWindowInitHandler.class);

    @Override
    @Nonnull
    public byte[] handle(@Nonnull ExecutorContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors) {
        try {
            Object init = CommandRequest.class.getMethod("getCreditWindowInit").invoke(request);
            if (init == null) {
                errors.add("CreditWindowInitHandler: payload missing");
                return new byte[0];
            }

            String streamId = (String) init.getClass().getMethod("getStreamId").invoke(init);
            long windowBytes =
                    (Long) init.getClass().getMethod("getWindowBytes").invoke(init);

            CreditWindow.arm(streamId, windowBytes);

            Class<?> ackClass = Class.forName("com.flezha.sdk.proto.CreditWindowAck");
            Object builder = ackClass.getMethod("newBuilder").invoke(null);
            builder.getClass().getMethod("setStreamId", String.class).invoke(builder, streamId);
            Object ack = builder.getClass().getMethod("build").invoke(builder);
            return (byte[]) ack.getClass().getMethod("toByteArray").invoke(ack);
        } catch (ReflectiveOperationException e) {
            LOG.warn("CreditWindowInitHandler reflective dispatch failed: {}", e.toString());
            errors.add("CreditWindowInit handler not available in this build");
            return new byte[0];
        }
    }
}
