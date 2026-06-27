package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.PartitionApplyState;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.relay.RelayInstance;
import com.flezha.sdk.runtime.relay.RelayStateSerializer;
import java.util.List;

/**
 * Applies an opaque state blob captured by a previous snapshot to an
 * already-registered {@link RelayInstance} before any input is delivered.
 * Used by the controller's cluster-snapshot restore path.
 */
public final class PartitionApplyStateHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        PartitionApplyState cmd = request.getPartitionApplyState();
        String relayId = cmd.getRelayId();
        byte[] stateBytes = cmd.getStateBinary().toByteArray();

        RelayInstance instance = ctx.relayInstanceRegistry().getRelay(relayId);
        if (instance == null) {
            errors.add("Relay with id " + relayId + " not found");
            return new byte[0];
        }

        if (stateBytes.length == 0) {
            return new byte[0];
        }

        try {
            RelayStateSerializer.deserialize(instance, stateBytes);
        } catch (Exception e) {
            errors.add("apply_state_not_supported");
        }

        return new byte[0];
    }
}
