package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.AddProcessor;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.processor.ProcessorInstance;
import com.flezha.sdk.runtime.processor.ProcessorRegistry;
import java.util.List;

public final class AddProcessorHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) {
        AddProcessor cmd = request.getAddProcessor();
        String factoryId = cmd.getFactoryId();
        String processorId = cmd.getProcessorId();
        String initValues = cmd.getInitValues();

        ProcessorRegistry registry = ctx.processorRegistry();
        ProcessorInstance<?, ?, ?> instance;
        try {
            instance = ProcessorInstance.create(factoryId, processorId);
        } catch (IllegalArgumentException e) {
            errors.add("Factory with id " + factoryId + " does not exist");
            return new byte[0];
        }

        if (initValues != null && !initValues.isEmpty()) {
            try {
                instance.deserializeState(initValues.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        ctx.objectMapper());
            } catch (Exception e) {
                errors.add("Failed to parse init_values as JSON: " + e.getMessage());
                return new byte[0];
            }
        }

        instance.postCreate();
        registry.add(instance);
        return new byte[0];
    }
}
