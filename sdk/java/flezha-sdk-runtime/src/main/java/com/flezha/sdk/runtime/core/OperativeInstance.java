package com.flezha.sdk.runtime.core;

import com.flezha.sdk.internal.RuntimeBridge;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.operative.ProcedureDescriptor;
import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.runtime.state.EntityId;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.PendingTimers;
import com.flezha.sdk.runtime.state.TimerOp;
import com.flezha.sdk.runtime.state.TimerPolicy;
import com.flezha.sdk.runtime.state.TimerSession;
import com.flezha.sdk.runtime.workflow.WorkflowOperativeClassRegistry;
import com.flezha.sdk.runtime.workflow.WorkflowSpec;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.StateHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * One live operative instance: the registered class resolved from the
 * activation's factory id, plus its per-Entity cells. Materialized on the
 * first activation naming an unknown operative id.
 *
 * <p>This is the mutable half of the host — the cell map and the user state
 * inside it. Its three collaborators read the topology and mutate nothing that
 * outlives a call: {@link EntityRouting} answers where a payload goes,
 * {@link StateExchange} moves state across the boundary, {@link WorkflowDispatch}
 * owns the workflow questions. Each is built once here, never per activation.
 */
public final class OperativeInstance {

    private final String operativeId;
    private final String factoryId;

    @Nullable
    final StreamOperativeTopology topology;

    /** Per-Procedure routing instances — {@code entitiesFor} / {@code eventTime} only. */
    @SuppressWarnings("rawtypes")
    final List<Procedure> routers = new ArrayList<>();

    /** Per-Procedure change/state codec (stable declared type ids). */
    private final List<KryoCodec> codecs = new ArrayList<>();

    private final List<TimerPolicy> timerPolicies = new ArrayList<>();

    /**
     * Codec for transient edge payloads crossing the core. Class-name
     * tagged (no declared id table) because a post-transform value can be
     * any user type; producer and consumer share this one instance.
     */
    final KryoCodec edgeCodec = new KryoCodec(List.of(), false);

    final WorkflowDispatch workflow;

    @Nullable
    private final EntityRouting routing;

    private final ConcurrentHashMap<CellKey, EntityCell> cells = new ConcurrentHashMap<>();

    private record CellKey(int procedure, String entityKey) {}

    private OperativeInstance(
            String operativeId,
            String factoryId,
            @Nullable StreamOperativeTopology topology,
            @Nullable WorkflowSpec workflowSpec) {
        this.operativeId = operativeId;
        this.factoryId = factoryId;
        this.topology = topology;
        this.workflow = new WorkflowDispatch(operativeId, factoryId, workflowSpec);
        if (topology != null) {
            for (ProcedureDescriptor desc : topology.procedures()) {
                Procedure<?, ?, ?, ?, ?, ?> instance = instantiate(desc);
                routers.add(instance);
                // The descriptor's list, not the instance's: it starts as a
                // copy of `kryoTypes()` and is where the wiring adds a type
                // only the wiring knows (a `routeLatest` document class).
                codecs.add(new KryoCodec(desc.kryoTypes()));
                timerPolicies.add(TimerPolicy.of(instance.maxTimersPerEntity(), desc.name(), desc.clockType()));
            }
        }
        this.routing = topology == null
                ? null
                : new EntityRouting(topology, routers, edgeCodec, workflow.isStructuredWorkflow());
    }

    static OperativeInstance materialize(String operativeId, String factoryId) {
        StreamOperativeTopology topology = StreamOperativeClassRegistry.getTopology(factoryId);
        if (topology != null) {
            return new OperativeInstance(operativeId, factoryId, topology, null);
        }
        WorkflowSpec spec = WorkflowOperativeClassRegistry.getSpec(factoryId);
        if (spec != null) {
            return new OperativeInstance(operativeId, factoryId, spec.topology().orElse(null), spec);
        }
        throw new IllegalStateException("unknown factory `" + factoryId + "` for operative `" + operativeId + "`");
    }

    @Nonnull
    public String factoryId() {
        return factoryId;
    }

    private static Procedure<?, ?, ?, ?, ?, ?> instantiate(ProcedureDescriptor desc) {
        try {
            return RuntimeBridge.construct(desc.procedureClass());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "procedure `" + desc.name() + "` has no public no-arg constructor: " + e, e);
        }
    }

    ActivationOutcome dispatch(Activation activation, Function<String, WorkflowSession> sessions) {
        return switch (activation.kind()) {
            case ActivationKind.Work work -> dispatchWork(activation, work.items());
            case ActivationKind.RestoreFold fold -> restoreFold(activation, fold);
            case ActivationKind.SnapshotRequest ignored -> {
                EntityCell cell = cellFor(activation.target());
                yield new ActivationOutcome.Snapshot(cell.codec.serialize(cell.state));
            }
            case ActivationKind.LiveStateRead ignored -> new ActivationOutcome.StateJson(
                    StateExchange.stateJson(cellFor(activation.target())));
            case ActivationKind.MigrationSerialize ignored -> new ActivationOutcome.MigrationState(
                    StateExchange.stateJson(cellFor(activation.target())));
            case ActivationKind.ApplyState apply -> {
                StateExchange.applyState(cellFor(activation.target()), apply.stateJson());
                yield new ActivationOutcome.StateApplied();
            }
            case ActivationKind.KeyRequest request -> {
                requireTopology("KeyRequest");
                yield new ActivationOutcome.Prepared(routing.prepare(request));
            }
            case ActivationKind.WorkflowRun run -> workflow.run(run.args(), sessions);
            case ActivationKind.CompensateRun comp -> compensateRun(activation, comp.outputs());
        };
    }

    // ------------------------------------------------------------------
    // Work
    // ------------------------------------------------------------------

    private ActivationOutcome dispatchWork(Activation activation, List<WorkItem> items) {
        requireTopology("Work");
        EntityCell cell = cellFor(activation.target());
        List<ItemEffect> effects = new ArrayList<>(items.size());
        for (WorkItem item : items) {
            ItemEffect effect = runItem(activation, cell, item);
            effects.add(effect);
            // Stop at the first failed item; the core re-hands the tail.
            if (!(effect.verdict() instanceof Verdict.Ok)) {
                break;
            }
        }
        return new ActivationOutcome.Work(effects);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ItemEffect runItem(Activation activation, EntityCell cell, WorkItem item) {
        long now = item instanceof WorkItem.Input in ? in.now() : ((WorkItem.Timer) item).scheduledAt();
        OptionalLong offset = item instanceof WorkItem.Input in ? in.consumedOffset() : OptionalLong.empty();
        boolean replay = item instanceof WorkItem.Input in && in.replay();

        EmitContext ctx = new EmitContext(this, cell, activation.changeSeq(), now, replay);
        TimerSession session = item instanceof WorkItem.Timer timer
                ? cell.timerPolicy.firingSession(new PendingTimers(), timer.scheduledAt(), timer.kind())
                : cell.timerPolicy.session(new PendingTimers(), now);
        ctx.timers = session;
        List<Change> buffer = new ArrayList<>();
        Procedure procedure = cell.procedure;
        StateHandle<Object, Change> handle = handleOver(cell, buffer);

        AtomicReference<List<TimerOp>> timerOps = new AtomicReference<>(List.of());
        Verdict verdict = runUserCode(() -> {
            if (item instanceof WorkItem.Timer timer) {
                procedure.onTimer(timer.scheduledAt(), timer.kind(), handle, ctx);
            } else {
                WorkItem.Input in = (WorkItem.Input) item;
                for (Object value : routing.valuesForEntity(cell, in.channel(), in.payload())) {
                    procedure.run(value, handle, ctx);
                }
            }
            timerOps.set(session.commit(new PendingTimers()));
        });

        if (!(verdict instanceof Verdict.Ok)) {
            return new ItemEffect(verdict, List.of(), List.of(), offset, now, List.of(), false, Optional.empty());
        }
        return new ItemEffect(
                verdict,
                StateExchange.serializeChanges(cell, buffer),
                ctx.outputs,
                offset,
                now,
                timerOps.get(),
                ctx.checkpointed,
                Optional.empty());
    }

    /** The state handle every user-code invocation writes through. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static StateHandle<Object, Change> handleOver(EntityCell cell, List<Change> buffer) {
        Procedure procedure = cell.procedure;
        return StateHandle.create(cell.state, (List) buffer, (s, c) -> procedure.applyChange(s, c));
    }

    /**
     * The ONE verdict authority: run user code and classify its outcome.
     * {@code NonRetryable} is the author's word; everything else —
     * including an unclassified throwable, panic parity with the Rust
     * host — stays retryable.
     */
    private static Verdict runUserCode(UserCode body) {
        try {
            body.invoke();
            return new Verdict.Ok();
        } catch (ProcedureException e) {
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            return e instanceof ProcedureException.NonRetryable
                    ? new Verdict.NonRetryableError(message)
                    : new Verdict.RetryableError(message);
        } catch (Throwable t) {
            return new Verdict.RetryableError(t.toString());
        }
    }

    /** User code as the verdict authority runs it — may throw anything. */
    @FunctionalInterface
    private interface UserCode {
        void invoke() throws Exception;
    }

    /**
     * Compensation runs the same invocation machinery as an input item, over
     * the outputs a rolled-back stage produced.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private ActivationOutcome compensateRun(Activation activation, List<byte[]> outputBytes) {
        requireTopology("CompensateRun");
        EntityCell cell = cellFor(activation.target());
        List<Object> outputs = new ArrayList<>(outputBytes.size());
        for (byte[] bytes : outputBytes) {
            outputs.add(edgeCodec.deserialize(bytes, Object.class));
        }
        // Compensation is wall-anchored (at-least-once, idempotent by
        // author contract) and carries no timer session — structural, the
        // same refusal the pre-core adapter enforced.
        EmitContext ctx = new EmitContext(this, cell, activation.changeSeq(), System.currentTimeMillis(), false);
        List<Change> buffer = new ArrayList<>();
        Procedure procedure = cell.procedure;
        StateHandle<Object, Change> handle = handleOver(cell, buffer);
        Verdict verdict = runUserCode(() -> procedure.compensate(outputs, handle, ctx));
        return new ActivationOutcome.Work(List.of(new ItemEffect(
                verdict,
                StateExchange.serializeChanges(cell, buffer),
                ctx.outputs,
                OptionalLong.empty(),
                0,
                List.of(),
                false,
                Optional.empty())));
    }

    // ------------------------------------------------------------------
    // Cells
    // ------------------------------------------------------------------

    private ActivationOutcome restoreFold(Activation activation, ActivationKind.RestoreFold fold) {
        requireTopology("RestoreFold");
        EntityRef target = activation.target();
        EntityCell cell = freshCell(target.procedure(), target.entityKey());
        StateExchange.foldInto(cell, fold);
        cells.put(new CellKey(target.procedure(), target.entityKey().orElse("")), cell);
        return new ActivationOutcome.Restored();
    }

    private void requireTopology(String kind) {
        if (topology == null) {
            throw new IllegalStateException(
                    kind + " activation for run-sugar workflow class `" + factoryId + "`, which has no procedures");
        }
    }

    private EntityCell cellFor(EntityRef target) {
        return cellFor(target.procedure(), target.entityKey());
    }

    private EntityCell cellFor(int procedureId, Optional<String> entityKey) {
        requireTopology("entity");
        return cells.computeIfAbsent(
                new CellKey(procedureId, entityKey.orElse("")), k -> freshCell(procedureId, entityKey));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private EntityCell freshCell(int procedureId, Optional<String> entityKey) {
        ProcedureDescriptor desc = topology.procedure(procedureId);
        if (desc == null) {
            throw new IllegalStateException("activation names unknown procedure id " + procedureId);
        }
        // One Procedure instance per Entity: a mutable field on a user's
        // Procedure is per-Entity scratch, never shared across keys.
        Procedure procedure = instantiate(desc);
        Object state = procedure.initState(entityKey, Optional.empty());
        return new EntityCell(
                procedureId,
                entityKey.orElse(""),
                procedure,
                state,
                codecs.get(procedureId),
                timerPolicies.get(procedureId),
                EntityId.of(operativeId, desc.name(), entityKey));
    }
}
