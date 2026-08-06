package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.state.DlqPark;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the controller's {@code ParkPayloadAck}: the dead-letter entry
 * identified by {@code park_id} is durable in the DLQ store. Wakes the
 * dispatch thread blocked in {@link DlqPark#parkBlocking} so the parked
 * entity drops its poison payload and advances. Runs on the control
 * router and never blocks. One-way — the generic {@code CommandResponse}
 * carries no payload.
 */
public final class ParkPayloadAckHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ParkPayloadAckHandler.class);

    @Override
    @Nonnull
    public byte[] handle(@Nonnull RunnerContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors) {
        try {
            Object ack = CommandRequest.class.getMethod("getParkPayloadAck").invoke(request);
            if (ack == null) {
                errors.add("ParkPayloadAckHandler: payload missing");
                return new byte[0];
            }
            String parkId = (String) ack.getClass().getMethod("getParkId").invoke(ack);
            DlqPark.completeAck(parkId);
            return new byte[0];
        } catch (ReflectiveOperationException e) {
            LOG.warn("ParkPayloadAckHandler reflective dispatch failed: {}", e.toString());
            errors.add("ParkPayloadAck handler not available in this build");
            return new byte[0];
        }
    }
}
