package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.MigrationDeserialization;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.ContinuousOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.ContinuousOperativeInstance;
import com.flezha.sdk.runtime.operative.ContinuousOperativeStateSerializer;
import com.flezha.sdk.runtime.operative.ContinuousOperativeTopology;
import com.flezha.sdk.runtime.operative.InitValuesContext;
import java.util.List;

public final class MigrationDeserializationHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) throws Exception {
        MigrationDeserialization cmd = request.getMigrationDeserialization();
        String factoryId = cmd.getFactoryId();
        String operativeId = cmd.getOperativeId();
        byte[] serializedState = cmd.getSerializedState().toByteArray();

        ContinuousOperativeTopology topology = ContinuousOperativeClassRegistry.getTopology(factoryId);
        if (topology == null) {
            errors.add("Factory with id " + factoryId + " does not exist");
            return new byte[0];
        }

        ContinuousOperativeInstance instance;
        InitValuesContext.set(null);
        try {
            instance = ContinuousOperativeInstance.create(topology, operativeId, ctx, ctx.workerPool());
        } catch (RuntimeException e) {
            errors.add("Failed to spawn operative instance: " + e.getMessage());
            return new byte[0];
        } finally {
            InitValuesContext.clear();
        }

        if (serializedState.length > 0) {
            try {
                ContinuousOperativeStateSerializer.deserialize(instance, serializedState);
            } catch (Exception e) {
                errors.add("Failed to deserialize operative state: " + e.getMessage());
                return new byte[0];
            }
        }

        ctx.operativeInstanceRegistry().add(instance);
        return new byte[0];
    }
}
