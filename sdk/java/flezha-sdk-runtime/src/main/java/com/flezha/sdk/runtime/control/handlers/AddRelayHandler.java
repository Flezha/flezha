package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.AddRelay;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.relay.InitValuesContext;
import com.flezha.sdk.runtime.relay.RelayClassRegistry;
import com.flezha.sdk.runtime.relay.RelayInstance;
import com.flezha.sdk.runtime.relay.RelayInstanceRegistry;
import com.flezha.sdk.runtime.relay.RelayTopology;
import com.flezha.sdk.runtime.relay.TaskInstance;
import com.flezha.sdk.runtime.task.TaskClassRegistry;
import com.flezha.sdk.runtime.task.TaskRunner;
import com.flezha.sdk.task.Task;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for {@code AddRelay}. Spawns a {@link RelayInstance} keyed by
 * the wire-supplied {@code relay_id}.
 *
 * <p>{@code init_values} (the JSON string from {@code AddRelay.init_values})
 * is staged on a thread-local read by
 * {@link com.flezha.sdk.shard.Shard#initState} for any Shard×Partition that
 * needs to seed initial state.
 *
 * <p>Idempotent on reconnect: the controller re-issues {@code AddRelay} for
 * every relay still tagged to this executor's profile after a restart.
 */
public final class AddRelayHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AddRelayHandler.class);

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        AddRelay cmd = request.getAddRelay();
        String factoryId = cmd.getFactoryId();
        String relayId = cmd.getRelayId();
        String initValues = cmd.getInitValues();

        RelayInstanceRegistry registry = ctx.relayInstanceRegistry();

        if (registry.contains(relayId)) {
            LOG.info("AddRelay: unit instance {} already registered, skipping", relayId);
            return new byte[0];
        }

        // Empty/"relay" → relay (backward-compatible default); "task" → task.
        if ("task".equals(cmd.getKind())) {
            Class<? extends Task> taskClass = TaskClassRegistry.getTaskClass(factoryId);
            if (taskClass == null) {
                errors.add("Task class with id " + factoryId + " does not exist");
                return new byte[0];
            }
            // Register before scheduling so a duplicate AddRelay (while run is
            // in-flight) is idempotent and never re-runs.
            registry.add(new TaskInstance(relayId, factoryId));
            TaskRunner.start(ctx, relayId, taskClass, initValues);
            return new byte[0];
        }

        RelayTopology topology = RelayClassRegistry.getTopology(factoryId);
        if (topology == null) {
            errors.add("Factory with id " + factoryId + " does not exist");
            return new byte[0];
        }

        InitValuesContext.set(initValues);
        try {
            RelayInstance instance = RelayInstance.create(topology, relayId, ctx, ctx.workerPool());
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
