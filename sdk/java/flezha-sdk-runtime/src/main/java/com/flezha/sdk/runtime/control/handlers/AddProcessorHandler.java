package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.AddProcessor;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.relay.InitValuesContext;
import com.flezha.sdk.runtime.relay.RelayClassRegistry;
import com.flezha.sdk.runtime.relay.RelayInstance;
import com.flezha.sdk.runtime.relay.RelayInstanceRegistry;
import com.flezha.sdk.runtime.relay.RelayTopology;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for {@code AddProcessor}. Spawns a {@link RelayInstance} keyed by
 * the wire-supplied {@code processor_id}.
 *
 * <p>{@code init_values} (the JSON string from {@code AddProcessor.init_values})
 * is staged on a thread-local read by
 * {@link com.flezha.sdk.shard.Shard#initState} for any Shard×Partition that
 * needs to seed initial state.
 *
 * <p>Idempotent on reconnect: the agent re-issues {@code AddProcessor} for
 * every processor still tagged to this operator's profile after a restart.
 */
public final class AddProcessorHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AddProcessorHandler.class);

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) {
        AddProcessor cmd = request.getAddProcessor();
        String factoryId = cmd.getFactoryId();
        String processorId = cmd.getProcessorId();
        String initValues = cmd.getInitValues();

        RelayInstanceRegistry registry = ctx.relayInstanceRegistry();

        if (registry.contains(processorId)) {
            LOG.info("AddProcessor: relay instance {} already registered, skipping", processorId);
            return new byte[0];
        }

        RelayTopology topology = RelayClassRegistry.getTopology(factoryId);
        if (topology == null) {
            errors.add("Factory with id " + factoryId + " does not exist");
            return new byte[0];
        }

        InitValuesContext.set(initValues);
        try {
            RelayInstance instance = RelayInstance.create(
                    topology, processorId, ctx, ctx.workerPool());
            registry.add(instance);
        } catch (RuntimeException e) {
            errors.add("Failed to spawn relay instance: " + e.getMessage());
            return new byte[0];
        } finally {
            InitValuesContext.clear();
        }
        return new byte[0];
    }
}
