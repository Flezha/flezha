package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.MigrationDeserialization;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.processor.ProcessorInstance;
import java.util.List;

public final class MigrationDeserializationHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) throws Exception {
        MigrationDeserialization cmd = request.getMigrationDeserialization();
        String factoryId = cmd.getFactoryId();
        String processorId = cmd.getProcessorId();
        byte[] serializedState = cmd.getSerializedState().toByteArray();

        ProcessorInstance<?, ?, ?> instance;
        try {
            instance = ProcessorInstance.create(factoryId, processorId);
        } catch (IllegalArgumentException e) {
            errors.add("Factory with id " + factoryId + " does not exist");
            return new byte[0];
        }

        if (serializedState.length > 0) {
            instance.deserializeState(serializedState, ctx.objectMapper());
        }
        instance.postCreate();
        ctx.processorRegistry().add(instance);
        return new byte[0];
    }
}
