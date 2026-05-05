package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.MigrationDeserialization;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.relay.InitValuesContext;
import com.flezha.sdk.runtime.relay.RelayClassRegistry;
import com.flezha.sdk.runtime.relay.RelayInstance;
import com.flezha.sdk.runtime.relay.RelayInstanceRegistry;
import com.flezha.sdk.runtime.relay.RelayStateSerializer;
import com.flezha.sdk.runtime.relay.RelayTopology;
import java.util.List;

public final class MigrationDeserializationHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) throws Exception {
        MigrationDeserialization cmd = request.getMigrationDeserialization();
        String factoryId = cmd.getFactoryId();
        String processorId = cmd.getProcessorId();
        byte[] serializedState = cmd.getSerializedState().toByteArray();

        RelayTopology topology = RelayClassRegistry.getTopology(factoryId);
        if (topology == null) {
            errors.add("Factory with id " + factoryId + " does not exist");
            return new byte[0];
        }

        RelayInstance instance;
        InitValuesContext.set(null);
        try {
            instance = RelayInstance.create(topology, processorId, ctx, ctx.workerPool());
        } catch (RuntimeException e) {
            errors.add("Failed to spawn relay instance: " + e.getMessage());
            return new byte[0];
        } finally {
            InitValuesContext.clear();
        }

        if (serializedState.length > 0) {
            try {
                RelayStateSerializer.deserialize(instance, serializedState);
            } catch (Exception e) {
                errors.add("Failed to deserialize relay state: " + e.getMessage());
                return new byte[0];
            }
        }

        ctx.relayInstanceRegistry().add(instance);
        return new byte[0];
    }
}
