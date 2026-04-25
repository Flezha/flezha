package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.ProcessorApplyState;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.processor.ProcessorInstance;
import java.util.List;

/**
 * Inverse of {@link ProcessorStateHandler}. Receives an opaque state binary
 * captured by a previous snapshot and feeds it to the registered processor
 * via {@link ProcessorInstance#deserializeState} before any input is
 * delivered. Used by the agent's {@code Flezha.Cluster.Snapshot} restore
 * path to prime a freshly-spawned processor.
 *
 * <p>If the processor is not found or its factory has no {@code TypeReference<State>}
 * registered, the response carries an explicit error and the agent surfaces
 * the canonical {@code apply_state_not_supported} token in its log so
 * operators can detect SDK builds that need upgrading.
 */
public final class ProcessorApplyStateHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) throws Exception {
        ProcessorApplyState cmd = request.getProcessorApplyState();
        String processorId = cmd.getProcessorId();
        byte[] stateBytes = cmd.getStateBinary().toByteArray();

        ProcessorInstance<?, ?, ?> instance = ctx.processorRegistry().get(processorId);
        if (instance == null) {
            errors.add("Processor with id " + processorId + " not found");
            return new byte[0];
        }

        if (stateBytes.length == 0) {
            return new byte[0];
        }

        try {
            instance.deserializeState(stateBytes, ctx.objectMapper());
        } catch (IllegalStateException e) {
            // Factory has no TypeReference<State> registered — surface as
            // the canonical not-supported token so the agent throttles to
            // a single WARN per factory rather than spamming the log on
            // every restore.
            errors.add("apply_state_not_supported");
        }

        return new byte[0];
    }
}
