package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.AddOperative;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.InitValuesContext;
import com.flezha.sdk.runtime.operative.ContinuousOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.ContinuousOperativeInstance;
import com.flezha.sdk.runtime.operative.ContinuousOperativeInstanceRegistry;
import com.flezha.sdk.runtime.operative.ContinuousOperativeTopology;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry;
import com.flezha.sdk.runtime.bounded.BoundedOperativeRunner;
import com.flezha.sdk.bounded.BoundedOperative;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for {@code AddOperative}. Spawns a {@link ContinuousOperativeInstance} keyed by
 * the wire-supplied {@code operative_id}.
 *
 * <p>{@code init_values} (the JSON string from {@code AddOperative.init_values})
 * is staged on a thread-local read by
 * {@link com.flezha.sdk.operation.Operation#initState} for any Operation×Segment that
 * needs to seed initial state.
 *
 * <p>Idempotent on reconnect: the controller re-issues {@code AddOperative} for
 * every operative still tagged to this executor's profile after a restart.
 */
public final class AddOperativeHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AddOperativeHandler.class);

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        AddOperative cmd = request.getAddOperative();
        String factoryId = cmd.getFactoryId();
        String operativeId = cmd.getOperativeId();
        String initValues = cmd.getInitValues();

        ContinuousOperativeInstanceRegistry registry = ctx.operativeInstanceRegistry();

        if (registry.contains(operativeId)) {
            LOG.info("AddOperative: operative instance {} already registered, skipping", operativeId);
            return new byte[0];
        }

        // Empty/"continuous" → continuous (default kind); "bounded" → bounded.
        if ("bounded".equals(cmd.getKind())) {
            Class<? extends BoundedOperative> boundedClass = BoundedOperativeClassRegistry.getBoundedOperativeClass(factoryId);
            if (boundedClass == null) {
                errors.add("BoundedOperative class with id " + factoryId + " does not exist");
                return new byte[0];
            }
            // Register before scheduling so a duplicate AddOperative (while run is
            // in-flight) is idempotent and never re-runs.
            registry.add(new BoundedOperativeInstance(operativeId, factoryId));
            BoundedOperativeRunner.start(ctx, operativeId, boundedClass, initValues);
            return new byte[0];
        }

        ContinuousOperativeTopology topology = ContinuousOperativeClassRegistry.getTopology(factoryId);
        if (topology == null) {
            errors.add("Factory with id " + factoryId + " does not exist");
            return new byte[0];
        }

        InitValuesContext.set(initValues);
        try {
            ContinuousOperativeInstance instance = ContinuousOperativeInstance.create(topology, operativeId, ctx, ctx.workerPool());
            registry.add(instance);
        } catch (RuntimeException e) {
            errors.add("Failed to spawn operative instance: " + e.getMessage());
            return new byte[0];
        } finally {
            InitValuesContext.clear();
        }
        return new byte[0];
    }
}
