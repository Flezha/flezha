package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.MigrationDeserialization;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.InitValuesContext;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeStateSerializer;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import java.util.List;

public final class MigrationDeserializationHandler implements CommandHandler {

    @Override
    public byte[] handle(RunnerContext ctx, CommandRequest request, List<String> errors) throws Exception {
        MigrationDeserialization cmd = request.getMigrationDeserialization();
        String factoryId = cmd.getFactoryId();
        String operativeId = cmd.getOperativeId();
        byte[] serializedState = cmd.getSerializedState().toByteArray();

        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology(factoryId);
        if (topology == null) {
            errors.add("Factory with id " + factoryId + " does not exist");
            return new byte[0];
        }

        UnboundedOperativeInstance instance;
        InitValuesContext.set(null);
        try {
            instance = UnboundedOperativeInstance.create(topology, operativeId, ctx, ctx.workerPool());
        } catch (RuntimeException e) {
            errors.add("Failed to spawn operative instance: " + e.getMessage());
            return new byte[0];
        } finally {
            InitValuesContext.clear();
        }

        if (serializedState.length > 0) {
            try {
                UnboundedOperativeStateSerializer.deserialize(instance, serializedState);
            } catch (Exception e) {
                errors.add("Failed to deserialize operative state: " + e.getMessage());
                return new byte[0];
            }
        }

        ctx.operativeInstanceRegistry().add(instance);
        return new byte[0];
    }
}
