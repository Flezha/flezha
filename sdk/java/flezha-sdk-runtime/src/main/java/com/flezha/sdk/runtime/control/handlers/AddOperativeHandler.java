package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.AddOperative;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry;
import com.flezha.sdk.runtime.bounded.BoundedOperativeRunner;
import com.flezha.sdk.runtime.bounded.BoundedSpec;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.Declarations;
import com.flezha.sdk.runtime.operative.InitValuesContext;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstanceRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for {@code AddOperative}. Spawns a {@link UnboundedOperativeInstance} keyed by
 * the wire-supplied {@code operative_id}.
 *
 * <p>{@code init_values} (the JSON string from {@code AddOperative.init_values})
 * is staged on a thread-local read by
 * {@link com.flezha.sdk.operation.Operation#initState} for any Operation×Segment that
 * needs to seed initial state.
 *
 * <p>Idempotent on reconnect: the controller re-issues {@code AddOperative} for
 * every operative still tagged to this runner's profile after a restart.
 */
public final class AddOperativeHandler implements CommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AddOperativeHandler.class);

    @Override
    public byte[] handle(RunnerContext ctx, CommandRequest request, List<String> errors) {
        AddOperative cmd = request.getAddOperative();
        String factoryId = cmd.getFactoryId();
        String operativeId = cmd.getOperativeId();
        String initValues = cmd.getInitValues();

        UnboundedOperativeInstanceRegistry registry = ctx.operativeInstanceRegistry();

        if (registry.contains(operativeId)) {
            LOG.info("AddOperative: operative instance {} already registered, skipping", operativeId);
            return new byte[0];
        }

        // Empty/"unbounded" → unbounded (default kind); "bounded" → bounded;
        // any other literal is rejected.
        String kind = cmd.getKind();
        if (!kind.isEmpty() && !"unbounded".equals(kind) && !"bounded".equals(kind)) {
            errors.add("unknown compute-unit kind '" + kind + "' for unit " + operativeId);
            return new byte[0];
        }
        if ("bounded".equals(kind)) {
            BoundedSpec spec = BoundedOperativeClassRegistry.getSpec(factoryId);
            if (spec == null) {
                errors.add("BoundedOperative class with id " + factoryId + " does not exist");
                return new byte[0];
            }
            BoundedOperativeInstance instance = new BoundedOperativeInstance(operativeId, factoryId);
            // The op-journal is instance-scoped, so it rides the bind rather
            // than a per-Segment restore — a durable class with no per-Segment
            // state ships no restore at all. Stashed before the spawn below: the
            // driver reads it to resume at the right iteration and to skip
            // already-committed operations.
            if (cmd.hasOpJournal()) {
                instance.setOpJournal(cmd.getOpJournal());
            }
            // Build + wire the per-Segment table (structured only) BEFORE
            // registering and spawning, so the Controller's RestoreInitState
            // frames (which follow this ACK) reach live Segments and the driver
            // holds its first Operation on the resume gate until
            // resume_segment_count restores have landed. Registering before spawn
            // also keeps a duplicate AddOperative idempotent and never loses an
            // immediate inbox / BoundedStop. Run-sugar has no Segments.
            if (spec.isRunSugar()) {
                instance.wireStageSegments(List.of(), 0);
            } else {
                UnboundedOperativeTopology boundedTopology = spec.topology().orElseThrow();
                instance.wireStageSegments(
                        BoundedOperativeRunner.buildAllStageSegments(
                                boundedTopology, operativeId, spec.chain(), spec.persistenceEnabled()),
                        cmd.getResumeSegmentCount());
            }
            registry.add(instance);
            BoundedOperativeRunner.spawn(ctx, instance, spec, initValues);
            return Declarations.forBounded(spec);
        }

        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology(factoryId);
        if (topology == null) {
            errors.add("Factory with id " + factoryId + " does not exist");
            return new byte[0];
        }

        InitValuesContext.set(initValues);
        try {
            UnboundedOperativeInstance instance =
                    UnboundedOperativeInstance.create(topology, operativeId, ctx, ctx.workerPool());
            registry.add(instance);
        } catch (RuntimeException e) {
            errors.add("Failed to spawn operative instance: " + e.getMessage());
            return new byte[0];
        } finally {
            InitValuesContext.clear();
        }
        return Declarations.forUnbounded(topology);
    }
}
