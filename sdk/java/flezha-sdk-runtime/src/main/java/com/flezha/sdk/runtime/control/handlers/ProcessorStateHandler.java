package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.relay.RelayInstance;
import com.flezha.sdk.runtime.relay.RelayStateSerializer;
import java.util.List;

public final class ProcessorStateHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) throws Exception {
        String processorId = request.getProcessorState().getProcessorId();
        RelayInstance instance = ctx.relayInstanceRegistry().get(processorId);
        if (instance == null) {
            errors.add("Processor with id " + processorId + " not found");
            return new byte[0];
        }
        return RelayStateSerializer.serialize(instance);
    }
}
