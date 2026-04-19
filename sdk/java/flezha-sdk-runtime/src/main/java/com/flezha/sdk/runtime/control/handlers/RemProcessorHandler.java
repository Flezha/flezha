package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.processor.ProcessorInstance;
import java.util.List;

public final class RemProcessorHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) {
        String processorId = request.getRemProcessor().getProcessorId();
        ProcessorInstance<?, ?, ?> removed = ctx.processorRegistry().remove(processorId);
        if (removed == null) {
            errors.add("Processor not found");
        }
        return new byte[0];
    }
}
