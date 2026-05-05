package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.ProcessorApplyState;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.relay.RelayInstance;
import com.flezha.sdk.runtime.relay.RelayStateSerializer;
import java.util.List;

/**
 * Applies an opaque state blob captured by a previous snapshot to an
 * already-registered {@link RelayInstance} before any input is delivered.
 * Used by the agent's cluster-snapshot restore path.
 */
public final class ProcessorApplyStateHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) {
        ProcessorApplyState cmd = request.getProcessorApplyState();
        String processorId = cmd.getProcessorId();
        byte[] stateBytes = cmd.getStateBinary().toByteArray();

        RelayInstance instance = ctx.relayInstanceRegistry().get(processorId);
        if (instance == null) {
            errors.add("Processor with id " + processorId + " not found");
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
