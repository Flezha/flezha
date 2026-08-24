package com.flezha.sdk.runtime.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.procedure.EmitKeys;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.procedure.StableId;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.EdgeYield;
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
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.Stateless;
import com.flezha.sdk.state.TimerKind;
import com.flezha.sdk.workflow.WorkflowOperative;
import com.flezha.sdk.workflow.WorkflowOperativeException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The pure host layer over the poll core: {@link #dispatch} turns one
 * {@link Activation} into one {@link ActivationResult} by running user code —
 * no native calls, which is what makes the whole typed layer unit-testable
 * without the core. The only impurities are injected: a per-instance
 * {@link WorkflowSession} for run-sugar bodies (and the api-side
 * {@code SpawnRequests} static, reached through {@code ProcedureCtx}'s
 * default {@code spawnWorkflow}).
 *
 * <p>The core guarantees per-Entity exclusivity — it never hands out a second
 * activation for the same Entity until the first completes — so there is no
 * Java-side Entity locking; the maps are merely concurrent-safe across
 * different Entities.
 */
public final class ActivationDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(ActivationDispatcher.class);

    /** Sorted-key, non-pretty JSON — the Decision-10 per-Entity state shape. */
    private static final ObjectMapper JSON = JsonMapper.builder()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .build();

    private final ConcurrentHashMap<String, JavaInstance> instances = new ConcurrentHashMap<>();
    private final Function<String, WorkflowSession> workflowSessions;

    public ActivationDispatcher(@Nonnull Function<String, WorkflowSession> workflowSessions) {
        this.workflowSessions = workflowSessions;
    }

    /** Live operative instances keyed by operative id — the {@code operatives()} view. */
    @Nonnull
    public Map<String, JavaInstance> instances() {
        return instances;
    }

    /** Whether any live instance belongs to {@code factoryId}. */
    public boolean hasInstanceOf(@Nonnull String factoryId) {
        return instances.values().stream().anyMatch(i -> factoryId.equals(i.factoryId()));
    }

    @Nonnull
    public ActivationResult dispatch(@Nonnull Activation activation) {
        try {
            // Keyed by operative id, but the FACTORY the activation names is what
            // decides which class runs: an operative id rebound to a different
            // factory — a deploy putting the next version of a class behind the
            // same id — must not keep running the class we materialized first.
            JavaInstance instance = instances.compute(
                    activation.target().operative(),
                    (id, existing) -> existing != null && existing.factoryId().equals(activation.factory())
                            ? existing
                            : JavaInstance.materialize(id, activation.factory()));
            return new ActivationResult(activation.token(), instance.dispatch(activation, workflowSessions));
        } catch (Throwable t) {
            String message = t.getMessage() == null ? t.toString() : t.getMessage();
            LOG.error(
                    "activation dispatch failed for `{}`: {}",
                    activation.target().operative(),
                    message);
            return new ActivationResult(activation.token(), new ActivationOutcome.Failed(new WorkError(message, true)));
        }
    }

    /**
     * One live operative instance: the registered class resolved from the
     * activation's factory id, plus its per-Entity cells. Materialized on the
     * first activation naming an unknown operative id.
     */
    public static final class JavaInstance {

        private final String operativeId;
        private final String factoryId;

        @Nullable
        private final StreamOperativeTopology topology;

        @Nullable
        private final WorkflowSpec workflowSpec;

        /** Per-Procedure routing instances — {@code entitiesFor} / {@code eventTime} only. */
        @SuppressWarnings("rawtypes")
        private final List<Procedure> routing = new ArrayList<>();

        /** Per-Procedure change/state codec (stable declared type ids). */
        private final List<KryoCodec> codecs = new ArrayList<>();

        private final List<TimerPolicy> timerPolicies = new ArrayList<>();

        /**
         * Codec for transient edge payloads crossing the core. Class-name
         * tagged (no declared id table) because a post-transform value can be
         * any user type; producer and consumer share this one instance.
         */
        private final KryoCodec edgeCodec = new KryoCodec(List.of(), false);

        private final ConcurrentHashMap<CellKey, EntityCell> cells = new ConcurrentHashMap<>();

        private record CellKey(int procedure, String entityKey) {}

        private JavaInstance(
                String operativeId,
                String factoryId,
                @Nullable StreamOperativeTopology topology,
                @Nullable WorkflowSpec workflowSpec) {
            this.operativeId = operativeId;
            this.factoryId = factoryId;
            this.topology = topology;
            this.workflowSpec = workflowSpec;
            if (topology != null) {
                for (ProcedureDescriptor desc : topology.procedures()) {
                    Procedure<?, ?, ?, ?, ?, ?> instance = instantiate(desc);
                    routing.add(instance);
                    // The descriptor's list, not the instance's: it starts as a
                    // copy of `kryoTypes()` and is where the wiring adds a type
                    // only the wiring knows (a `routeLatest` document class).
                    codecs.add(new KryoCodec(desc.kryoTypes()));
                    timerPolicies.add(TimerPolicy.of(instance.maxTimersPerEntity(), desc.name(), desc.clockType()));
                }
            }
        }

        static JavaInstance materialize(String operativeId, String factoryId) {
            StreamOperativeTopology topology = StreamOperativeClassRegistry.getTopology(factoryId);
            if (topology != null) {
                return new JavaInstance(operativeId, factoryId, topology, null);
            }
            WorkflowSpec spec = WorkflowOperativeClassRegistry.getSpec(factoryId);
            if (spec != null) {
                return new JavaInstance(operativeId, factoryId, spec.topology().orElse(null), spec);
            }
            throw new IllegalStateException("unknown factory `" + factoryId + "` for operative `" + operativeId + "`");
        }

        @Nonnull
        public String factoryId() {
            return factoryId;
        }

        @Nonnull
        public String operativeId() {
            return operativeId;
        }

        private static Procedure<?, ?, ?, ?, ?, ?> instantiate(ProcedureDescriptor desc) {
            try {
                return desc.procedureClass().getDeclaredConstructor().newInstance();
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
                        stateJson(activation.target()));
                case ActivationKind.MigrationSerialize ignored -> new ActivationOutcome.MigrationState(
                        stateJson(activation.target()));
                case ActivationKind.ApplyState apply -> applyState(activation.target(), apply.stateJson());
                case ActivationKind.KeyRequest request -> prepare(request);
                case ActivationKind.WorkflowRun run -> workflowRun(run.args(), sessions);
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
                    for (Object value : valuesForEntity(cell, in.channel(), in.payload())) {
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
                    serializeChanges(cell, buffer),
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

        private static List<byte[]> serializeChanges(EntityCell cell, List<Change> buffer) {
            List<byte[]> changeBytes = new ArrayList<>(buffer.size());
            for (Change change : buffer) {
                changeBytes.add(cell.codec.serialize(change));
            }
            return changeBytes;
        }

        /**
         * Decode an input payload and apply the consumer edge's transform
         * chain, keeping only the yielded values that route to this Entity —
         * the producer-side evaluation the pre-core {@code routeToConsumers}
         * did, re-run symmetrically at delivery because only bytes cross the
         * core.
         */
        @SuppressWarnings("unchecked")
        private List<Object> valuesForEntity(EntityCell cell, int channelId, byte[] payload) {
            ChannelDescriptor channel = topology.channel(channelId);
            if (channel == null) {
                throw new IllegalStateException("input names unknown channel id " + channelId);
            }
            Object base = decodePayload(channel, payload);
            ConsumerEdge edge = edgeInto(channel, cell.procedureId);
            if (edge == null) {
                return List.of(base);
            }
            EdgeYield yielded = edge.transformValue(base);
            List<Object> values = yielded == null ? List.of(base) : yielded.values();
            if (values.isEmpty()) {
                return values;
            }
            ProcedureDescriptor desc = topology.procedure(cell.procedureId);
            if (edge.key().isPresent()) {
                return values.stream()
                        .filter(v -> cell.entityKey.equals(edge.keyOf(v)))
                        .toList();
            }
            if (!desc.entityKeys().isEmpty()) {
                @SuppressWarnings("rawtypes")
                Procedure router = routing.get(cell.procedureId);
                return values.stream()
                        .filter(v -> ((List<String>) router.entitiesFor(v)).contains(cell.entityKey))
                        .toList();
            }
            return values;
        }

        private Object decodePayload(ChannelDescriptor channel, byte[] payload) {
            if (channel.kind() == ChannelKind.ENTRY) {
                return channel.entryParser()
                        .map(parser -> parser.apply(payload))
                        .orElse(payload);
            }
            return edgeCodec.deserialize(payload, Object.class);
        }

        @Nullable
        private static ConsumerEdge edgeInto(ChannelDescriptor channel, int procedureId) {
            for (ConsumerEdge edge : channel.consumers()) {
                if (edge.procedure() == procedureId) {
                    return edge;
                }
            }
            return null;
        }

        // ------------------------------------------------------------------
        // Commands
        // ------------------------------------------------------------------

        @SuppressWarnings({"unchecked", "rawtypes"})
        private ActivationOutcome restoreFold(Activation activation, ActivationKind.RestoreFold fold) {
            requireTopology("RestoreFold");
            EntityRef target = activation.target();
            EntityCell cell = freshCell(target.procedure(), target.entityKey());
            if (fold.snapshot().isPresent()) {
                ActivationKind.RestoreBody body = fold.snapshot().get();
                cell.state = read(body, cell, Object.class, cell.procedure::migrateState);
            }
            for (ActivationKind.RestoreBody body : fold.changes()) {
                Change change = read(body, cell, Change.class, cell.procedure::migrateChange);
                cell.procedure.applyChange(cell.state, change);
            }
            cells.put(new CellKey(target.procedure(), target.entityKey().orElse("")), cell);
            return new ActivationOutcome.Restored();
        }

        /**
         * One restore body: decoded with this version's codec, or handed to
         * the migration the core tagged it for. The core has already refused
         * everything unreadable, so there is no third case.
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        private static <T> T read(
                ActivationKind.RestoreBody body,
                EntityCell cell,
                Class<T> type,
                java.util.function.BiFunction<Integer, byte[], ?> migrate) {
            return body.fromVersion().isPresent()
                    ? (T) migrate.apply(body.fromVersion().getAsInt(), body.bytes())
                    : cell.codec.deserialize(body.bytes(), type);
        }

        private byte[] stateJson(EntityRef target) {
            EntityCell cell = cellFor(target);
            // A stateless procedure has nothing to ship. Jackson refuses an
            // empty bean, and that refusal is RIGHT for a user state class with
            // no readable properties — an author mistake worth surfacing — but
            // `Stateless` is the SDK's own marker for "there is no state", so it
            // ships `{}` and reads back through `applyState`'s no-arg
            // constructor. Without this, every stateless Java operative failed
            // its migration serialize and its shutdown state ship.
            if (cell.state instanceof Stateless) {
                return "{}".getBytes(StandardCharsets.UTF_8);
            }
            try {
                return JSON.writeValueAsBytes(cell.state);
            } catch (Exception e) {
                throw new IllegalStateException("state serialization failed: " + e.getMessage(), e);
            }
        }

        private ActivationOutcome applyState(EntityRef target, byte[] json) {
            EntityCell cell = cellFor(target);
            try {
                Class<?> targetClass = cell.state == null ? Object.class : cell.state.getClass();
                cell.state = JSON.readValue(json, targetClass);
            } catch (Exception e) {
                throw new IllegalStateException("state apply failed: " + e.getMessage(), e);
            }
            return new ActivationOutcome.StateApplied();
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private ActivationOutcome prepare(ActivationKind.KeyRequest request) {
            requireTopology("KeyRequest");
            ChannelDescriptor channel = topology.channel(request.channel());
            if (channel == null) {
                throw new IllegalStateException("key request names unknown channel id " + request.channel());
            }
            List<PrepareResult> results = new ArrayList<>(request.payloads().size());
            for (byte[] payload : request.payloads()) {
                try {
                    Object base = decodePayload(channel, payload);
                    List<String> keys = List.of();
                    OptionalLong eventTime = OptionalLong.empty();
                    if (request.wantKeys()) {
                        keys = prepareKeys(channel, base);
                    }
                    if (request.wantEventTime()) {
                        eventTime = prepareEventTime(channel, base);
                    }
                    results.add(new PrepareResult.Ok(new PrepareAnswer(keys, eventTime)));
                } catch (Throwable t) {
                    results.add(new PrepareResult.Err(
                            new WorkError(t.toString(), t instanceof ProcedureException.NonRetryable)));
                }
            }
            return new ActivationOutcome.Prepared(results);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private List<String> prepareKeys(ChannelDescriptor channel, Object base) {
            for (ConsumerEdge edge : channel.consumers()) {
                ProcedureDescriptor desc = topology.procedure(edge.procedure());
                if (desc == null) {
                    continue;
                }
                if (desc.dynamic() && edge.key().isPresent()) {
                    LinkedHashSet<String> keys = new LinkedHashSet<>();
                    for (Object value : transformed(edge, base)) {
                        keys.add(edge.keyOf(value));
                    }
                    return List.copyOf(keys);
                }
                if (!desc.dynamic() && desc.entitiesForOverridden()) {
                    LinkedHashSet<String> keys = new LinkedHashSet<>();
                    Procedure router = routing.get(edge.procedure());
                    for (Object value : transformed(edge, base)) {
                        keys.addAll(router.entitiesFor(value));
                    }
                    return List.copyOf(keys);
                }
            }
            return List.of();
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private OptionalLong prepareEventTime(ChannelDescriptor channel, Object base) {
            for (ConsumerEdge edge : channel.consumers()) {
                ProcedureDescriptor desc = topology.procedure(edge.procedure());
                if (desc == null || desc.clockType() != EventTimeFrom.class) {
                    continue;
                }
                List<Object> values = transformed(edge, base);
                if (!values.isEmpty()) {
                    Procedure router = routing.get(edge.procedure());
                    return OptionalLong.of(router.eventTime(values.get(0)));
                }
            }
            return OptionalLong.empty();
        }

        private static List<Object> transformed(ConsumerEdge edge, Object base) {
            EdgeYield yielded = edge.transformValue(base);
            return yielded == null ? List.of(base) : yielded.values();
        }

        // ------------------------------------------------------------------
        // Workflow
        // ------------------------------------------------------------------

        private ActivationOutcome workflowRun(byte[] args, Function<String, WorkflowSession> sessions) {
            if (workflowSpec == null || !workflowSpec.isRunSugar()) {
                throw new IllegalStateException(
                        "WorkflowRun activation for `" + factoryId + "`, which is not a run-sugar workflow class");
            }
            WorkflowOperative body;
            try {
                body = workflowSpec.operativeClass().getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(
                        "workflow class `" + factoryId + "` has no public no-arg constructor: " + e, e);
            }
            SessionWorkflowCtx ctx = new SessionWorkflowCtx(sessions.apply(operativeId));
            try {
                body.run(ctx, new String(args, StandardCharsets.UTF_8));
                return new ActivationOutcome.WorkflowDone(ctx.completed, ctx.result);
            } catch (WorkflowOperativeException e) {
                String message = e.getMessage() == null ? e.toString() : e.getMessage();
                return new ActivationOutcome.Failed(new WorkError(message, e.isNonRetryable()));
            } catch (Throwable t) {
                LOG.warn("workflow run-sugar body `{}` failed: {}", operativeId, t.toString());
                return new ActivationOutcome.Failed(new WorkError(t.toString(), false));
            }
        }

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
                    serializeChanges(cell, buffer),
                    ctx.outputs,
                    OptionalLong.empty(),
                    0,
                    List.of(),
                    false,
                    Optional.empty())));
        }

        /** User code as the verdict authority runs it — may throw anything. */
        @FunctionalInterface
        private interface UserCode {
            void invoke() throws Exception;
        }

        // ------------------------------------------------------------------
        // Cells
        // ------------------------------------------------------------------

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

    /** One live Entity: its own Procedure instance, state, and codec. */
    private static final class EntityCell {
        final int procedureId;
        final String entityKey;

        @SuppressWarnings("rawtypes")
        final Procedure procedure;

        Object state;
        final KryoCodec codec;
        final TimerPolicy timerPolicy;
        final EntityId id;

        @SuppressWarnings("rawtypes")
        EntityCell(
                int procedureId,
                String entityKey,
                Procedure procedure,
                Object state,
                KryoCodec codec,
                TimerPolicy timerPolicy,
                EntityId id) {
            this.procedureId = procedureId;
            this.entityKey = entityKey;
            this.procedure = procedure;
            this.state = state;
            this.codec = codec;
            this.timerPolicy = timerPolicy;
            this.id = id;
        }
    }

    /**
     * Per-invocation {@link ProcedureCtx}: serves the item's core-evaluated
     * clock, the replay-stable id, and evaluates emits producer-side into
     * schema {@link Output}s — edge transforms, keys and the consumer's
     * event-time extractor run here, so live delivery and replay cannot drift.
     */
    private static final class EmitContext implements ProcedureCtx {

        private final JavaInstance instance;
        private final EntityCell cell;
        private final long invocationSeq;
        private final long now;
        private final boolean replay;

        final List<Output> outputs = new ArrayList<>();
        boolean checkpointed;

        @Nullable
        TimerSession timers;

        private byte[] identityBytes;
        private int stableIdOrdinal;
        private int emitOrdinal;

        EmitContext(JavaInstance instance, EntityCell cell, long invocationSeq, long now, boolean replay) {
            this.instance = instance;
            this.cell = cell;
            this.invocationSeq = invocationSeq;
            this.now = now;
            this.replay = replay;
        }

        @Override
        public long now() {
            return now;
        }

        @Override
        @Nonnull
        public StableId stableId() {
            if (identityBytes == null) {
                identityBytes = cell.id.encodedBytes();
            }
            return StableId.derive(identityBytes, invocationSeq, stableIdOrdinal++);
        }

        @Override
        public void checkpoint() {
            checkpointed = true;
        }

        @Override
        public boolean replaying() {
            return replay;
        }

        @Override
        @Nonnull
        public Optional<String> entityKey() {
            // The cell stores a single-entity Procedure's absent key as "";
            // that is not a key to emit under.
            return cell.entityKey.isEmpty() ? Optional.empty() : Optional.of(cell.entityKey);
        }

        private TimerSession session() throws ProcedureException {
            if (timers == null) {
                throw new ProcedureException.TimersUnavailable();
            }
            return timers;
        }

        @Override
        public long timerAtEventTime(long scheduledAt) throws ProcedureException {
            return session().registerAtEventTime(scheduledAt);
        }

        @Override
        public long timerAfter(@Nonnull Duration delay) throws ProcedureException {
            return session().registerAfter(delay);
        }

        @Override
        public boolean cancelEventTimeTimer(long scheduledAt) throws ProcedureException {
            return session().cancel(TimerKind.EVENT_TIME, scheduledAt);
        }

        @Override
        public boolean cancelProcessingTimeTimer(long scheduledAt) throws ProcedureException {
            return session().cancel(TimerKind.PROCESSING_TIME, scheduledAt);
        }

        @Override
        public <C> void emit(@Nonnull C value) throws ProcedureException {
            doEmit(defaultOutputChannel(), value, null, null);
        }

        @Override
        public <C> void emit(@Nonnull String channelName, @Nonnull C value) throws ProcedureException {
            doEmit(resolveChannel(channelName), value, null, null);
        }

        @Override
        public <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) throws ProcedureException {
            doEmit(defaultOutputChannel(), value, meta, null);
        }

        @Override
        public <C> void emitWithMeta(@Nonnull String channelName, @Nonnull C value, @Nonnull Map<String, String> meta)
                throws ProcedureException {
            doEmit(resolveChannel(channelName), value, meta, null);
        }

        @Override
        public <C> void emitKeyed(@Nonnull String key, @Nonnull C value) throws ProcedureException {
            doEmit(defaultOutputChannel(), value, null, key);
        }

        @Override
        public <C> void emitKeyedWithMeta(@Nonnull String key, @Nonnull C value, @Nonnull Map<String, String> meta)
                throws ProcedureException {
            doEmit(defaultOutputChannel(), value, meta, key);
        }

        private int defaultOutputChannel() throws ProcedureException {
            ProcedureDescriptor desc = instance.topology.procedure(cell.procedureId);
            if (desc == null || desc.outputChannelId().isEmpty()) {
                throw new ProcedureException.UnknownChannel("<no output channel declared on this procedure>");
            }
            return desc.outputChannelId().getAsInt();
        }

        private int resolveChannel(String name) throws ProcedureException {
            Integer id = instance.topology.lookupChannelId(name);
            if (id == null) {
                throw new ProcedureException.UnknownChannel(name);
            }
            return id;
        }

        /**
         * The single emit funnel: keyed-channel contract, reserved-metadata
         * refusal, then producer-side routing into schema outputs. Supplying no
         * key on a keyed channel resolves to this Entity's own key here, so the
         * defaulted emit and an explicit {@code emitKeyed} are one path from
         * this point on and their stamping cannot diverge.
         */
        private void doEmit(int channelId, Object value, @Nullable Map<String, String> meta, @Nullable String key)
                throws ProcedureException {
            ChannelDescriptor descriptor = instance.topology.channel(channelId);
            if (descriptor == null) {
                throw new ProcedureException.UnknownChannel("<channel " + channelId + ">");
            }
            if (descriptor.kind() == ChannelKind.ENTRY) {
                throw new ProcedureException(
                        "cannot emit on entry channel `" + descriptor.name() + "` from a procedure");
            }
            key = EmitKeys.resolve(descriptor.name(), descriptor.keyed(), key, entityKey());
            if (meta != null) {
                for (String k : meta.keySet()) {
                    if (k.startsWith(ProcedureCtx.RESERVED_METADATA_PREFIX)) {
                        throw new ProcedureException.ReservedMetadataKey(k);
                    }
                }
                if (descriptor.pluginSink().isEmpty()) {
                    throw new ProcedureException.MetadataNotAllowedOnInterProcedureChannel(descriptor.name());
                }
            }
            // Fan out to the consumer edges, then to the plugin sink if the
            // channel has one. The plugin-bound terminal is the same path with
            // no consumer edges, which is why there is no second arm for it.
            collectEdgeOutputs(channelId, descriptor, value);
            if (descriptor.pluginSink().isPresent()) {
                byte[] bytes = serializeOrThrow(descriptor.pluginSink().get().serializer(), value, descriptor.name());
                collectPluginOutput(descriptor, bytes, meta, key);
            }
        }

        private static byte[] serializeOrThrow(Function<Object, byte[]> serializer, Object value, String channel)
                throws ProcedureException {
            try {
                return serializer.apply(value);
            } catch (RuntimeException | Error e) {
                throw new ProcedureException.PluginSerializerFailure(channel, e.toString(), e);
            }
        }

        /** Producer-side edge evaluation: transform chain, edge key, consumer event time. */
        @SuppressWarnings({"unchecked", "rawtypes"})
        private void collectEdgeOutputs(int channelId, ChannelDescriptor descriptor, Object value) {
            for (ConsumerEdge edge : descriptor.consumers()) {
                EdgeYield yielded = edge.transformValue(value);
                List<Object> values = yielded == null ? List.of(value) : yielded.values();
                ProcedureDescriptor consumer = instance.topology.procedure(edge.procedure());
                boolean eventTimeClocked = consumer != null && consumer.clockType() == EventTimeFrom.class;
                for (Object v : values) {
                    OptionalLong eventTime = OptionalLong.empty();
                    if (eventTimeClocked) {
                        Procedure router = instance.routing.get(edge.procedure());
                        eventTime = OptionalLong.of(router.eventTime(v));
                    }
                    outputs.add(new Output(
                            new OutputTarget.Edge(channelId, edge.procedure()),
                            Optional.ofNullable(edge.keyOf(v)),
                            eventTime,
                            List.of(),
                            instance.edgeCodec.serialize(v)));
                }
            }
        }

        /**
         * A plugin output. Non-tier keyed channels carry the identity stamps
         * the pre-core frame writer minted; under the tier the controller
         * derives them at flush time, so none ride here — both byte-shapes
         * preserved.
         */
        private void collectPluginOutput(
                ChannelDescriptor descriptor, byte[] bytes, @Nullable Map<String, String> meta, @Nullable String key) {
            List<Map.Entry<String, String>> metadata = new ArrayList<>();
            if (meta != null) {
                for (Map.Entry<String, String> e : meta.entrySet()) {
                    if (!"channel".equals(e.getKey())) {
                        metadata.add(Map.entry(e.getKey(), e.getValue()));
                    }
                }
            }
            if (descriptor.keyed() && !instance.topology.effectivelyOnce()) {
                metadata.add(Map.entry("flz.entity", cell.id.encoded()));
                metadata.add(Map.entry("flz.seq", invocationSeq + "." + emitOrdinal));
                metadata.add(Map.entry("flz.key", key));
            }
            emitOrdinal++;
            outputs.add(new Output(
                    new OutputTarget.Plugin(descriptor.name()),
                    Optional.ofNullable(key),
                    OptionalLong.empty(),
                    metadata,
                    bytes));
        }
    }

    /** {@link com.flezha.sdk.workflow.WorkflowOperativeCtx} over an injected {@link WorkflowSession}. */
    private static final class SessionWorkflowCtx implements com.flezha.sdk.workflow.WorkflowOperativeCtx {

        private final WorkflowSession session;
        byte[] result = new byte[0];
        boolean completed;

        SessionWorkflowCtx(WorkflowSession session) {
            this.session = session;
        }

        @Override
        public void emit(@Nonnull byte[] value) {
            session.emit(value);
        }

        @Override
        public void complete(@Nonnull byte[] result) {
            this.result = result;
            this.completed = true;
        }

        @Override
        public boolean isCancelled() {
            return session.cancelled();
        }

        @Override
        @Nonnull
        public Optional<byte[]> recv() {
            while (true) {
                if (isCancelled() || completed) {
                    return Optional.empty();
                }
                byte[] item;
                try {
                    // ≤200 ms wake granularity so cancellation is honoured.
                    item = session.recv(200);
                } catch (RuntimeException e) {
                    // Body gone / inbox permanently closed.
                    return Optional.empty();
                }
                if (item != null) {
                    return Optional.of(item);
                }
            }
        }

        @Override
        public void loopUntil(
                @Nonnull java.util.function.BooleanSupplier predicate,
                @Nonnull com.flezha.sdk.workflow.WorkflowLoopBody body)
                throws WorkflowOperativeException {
            while (!isCancelled() && !completed && !predicate.getAsBoolean()) {
                body.run(this);
            }
        }
    }
}
