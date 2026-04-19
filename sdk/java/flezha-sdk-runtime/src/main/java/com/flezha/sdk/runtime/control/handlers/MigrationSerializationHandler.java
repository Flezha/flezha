package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.processor.ProcessorInstance;
import java.util.List;

public final class MigrationSerializationHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) throws Exception {
        String processorId = request.getMigrationSerialization().getProcessorId();
        ProcessorInstance<?, ?, ?> instance = ctx.processorRegistry().remove(processorId);
        if (instance == null) {
            errors.add("Processor with id " + processorId + " not found");
            return new byte[0];
        }
        return instance.serializeState(ctx.objectMapper());
    }
}
