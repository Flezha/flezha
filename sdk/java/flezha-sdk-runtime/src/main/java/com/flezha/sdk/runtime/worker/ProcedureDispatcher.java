package com.flezha.sdk.runtime.worker;

import com.flezha.sdk.channel.BeyondWindowPolicy;
import com.flezha.sdk.channel.RetryPolicy;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.procedure.StableId;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.entity.EntitySlot;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.PluginSink;
import com.flezha.sdk.runtime.operative.ProcedureDescriptor;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.state.DlqPark;
import com.flezha.sdk.runtime.state.EffectRecord;
import com.flezha.sdk.runtime.state.EntityId;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.StateHandle;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-Procedure dispatcher. Caches the producer-edge
 * {@code outputChannelId} at build time so {@code ctx.emit(...)} pays no
 * channel-name lookup on the hot path.
 *
 * <p>One {@link ProcedureDispatcher} per Procedure, shared across every Entity
 * that Procedure owns (each {@link EntitySlot} references it via
 * {@link EntitySlot#dispatcher()}). The Procedure instance held here serves
 * only the pre-Entity routing calls — {@code entitiesFor}, {@code entityKey},
 * {@code eventTime}. Each Entity runs {@code run} / {@code applyChange} on its
 * OWN instance ({@link EntitySlot#procedureInstance()}), so mutable Procedure
 * fields are per-Entity scratch, matching the Rust SDK.
 */
public final class ProcedureDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureDispatcher.class);

    /**
     * Indirection over {@link com.flezha.sdk.native_.NativeTransport#streamWrite(long, long, byte[])}
     * so dispatcher integration tests can observe plugin-wire emissions
     * without mocking the {@code native} static (mockito-inline cannot
     * intercept native methods).
     */
    @FunctionalInterface
    public interface WireWriter {
        int write(long handle, long streamId, byte[] frame);
    }

    private static volatile WireWriter WIRE_WRITER = com.flezha.sdk.native_.NativeTransport::streamWrite;

    /** Replace the wire-writer hook. Tests only; production code does not call this. */
    public static void setWireWriter(@Nonnull WireWriter writer) {
        WIRE_WRITER = writer;
    }

    /** Restore the default {@link com.flezha.sdk.native_.NativeTransport#streamWrite} writer. */
    public static void resetWireWriter() {
        WIRE_WRITER = com.flezha.sdk.native_.NativeTransport::streamWrite;
    }

    private final RunnerContext ctx;
    private final ExecutorService pool;
    private final WeakReference<UnboundedOperativeInstance> instance;
    private final UnboundedOperativeTopology topology;
    private final int procedureId;
    private final ProcedureDescriptor procedureDescriptor;
    private final Optional<ChannelDescriptor> outputChannelDescriptor;
    private final OptionalInt outputChannelId;
    /** Retry-then-park policy of the input channel (defaults when unwired). */
    private final RetryPolicy retryPolicy;
    /** Input-channel name stamped on dead-letter entries. */
    private final String inputChannelName;

    /** Whether this Procedure creates its Entities lazily, one per key seen. */
    private final boolean dynamic;

    /** Whether this Operative opted into the effectively-once output tier. */
    private final boolean tier;

    /** Whether the consumed offset is withheld until an explicit ctx.checkpoint(). */
    private final boolean checkpointAck;

    /** Declared late-input correction window; 0 = corrections are off. */
    private final long correctionWindowMs;

    private final BeyondWindowPolicy beyondWindowPolicy;

    /** Serializes Changes into an effect record's opaque change bytes (tier only). */
    private final KryoCodec codec;

    /**
     * The dispatcher-level instance, shared across every Entity this Procedure
     * owns. It serves only the pre-Entity routing / declaration calls
     * ({@code entitiesFor}, {@code entityKey}, {@code eventTime},
     * {@code kryoTypes}); {@code run} / {@code applyChange} run on the Entity's
     * own instance ({@link EntitySlot#procedureInstance()}).
     */
    @SuppressWarnings("rawtypes")
    private final Procedure routingInstance;

    @SuppressWarnings("unchecked")
    public ProcedureDispatcher(
            @Nonnull RunnerContext ctx,
            @Nonnull ExecutorService pool,
            @Nonnull UnboundedOperativeInstance instance,
            @Nonnull UnboundedOperativeTopology topology,
            int procedureId,
            @Nonnull Procedure<?, ?, ?, ?, ?, ?> routingInstance) {
        this.ctx = ctx;
        this.pool = pool;
        this.instance = new WeakReference<>(instance);
        this.topology = topology;
        this.procedureId = procedureId;
        this.procedureDescriptor = topology.procedure(procedureId);
        this.outputChannelId = procedureDescriptor.outputChannelId();
        this.outputChannelDescriptor = outputChannelId.isPresent()
                ? Optional.ofNullable(topology.channel(outputChannelId.getAsInt()))
                : Optional.empty();
        OptionalInt inputChannelId = procedureDescriptor.inputChannelId();
        ChannelDescriptor inputChannel =
                inputChannelId.isPresent() ? topology.channel(inputChannelId.getAsInt()) : null;
        this.retryPolicy = inputChannel == null ? RetryPolicy.DEFAULT : inputChannel.retry();
        this.inputChannelName = inputChannel == null ? "<unwired>" : inputChannel.name();
        this.routingInstance = routingInstance;
        this.dynamic = procedureDescriptor.entityKeyOverridden()
                && procedureDescriptor.entityKeys().isEmpty();
        this.tier = topology.effectivelyOnce();
        this.checkpointAck = topology.checkpointAck();
        this.correctionWindowMs = topology.correctionWindowMs();
        this.beyondWindowPolicy = topology.beyondWindowPolicy();
        this.codec = new KryoCodec(routingInstance.kryoTypes());
    }

    public int procedureId() {
        return procedureId;
    }

    /**
     * Whether this Operative opted into the effectively-once output tier — the
     * fact that also decides whether the Controller journals its inputs, and so
     * whether a restore may discard the ones still queued.
     */
    public boolean tier() {
        return tier;
    }

    public ProcedureDescriptor descriptor() {
        return procedureDescriptor;
    }

    /**
     * Enqueue an input on the entities targeted by this Procedure. Single-mode
     * procedures skip the {@link Procedure#entitiesFor(Object) entitiesFor} call
     * and route to entity index 0; static-mode procedures call
     * {@code entitiesFor} and fan out across the matching entities.
     */
    public void fanoutEntities(@Nonnull Object value, long primaryStreamId) {
        fanoutEntities(value, primaryStreamId, OptionalLong.empty());
    }

    /**
     * {@code offset} is the input's position in the controller's durable input
     * log, present only on an entry-channel input under the effectively-once
     * tier. A value derived downstream carries none — it was never journaled,
     * so there is nothing for it to acknowledge.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void fanoutEntities(@Nonnull Object value, long primaryStreamId, @Nonnull OptionalLong offset) {
        fanoutEntities(value, primaryStreamId, offset, false);
    }

    /** {@code replay} marks an input that belongs to a correction replay slice. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void fanoutEntities(
            @Nonnull Object value, long primaryStreamId, @Nonnull OptionalLong offset, boolean replay) {
        UnboundedOperativeInstance ri = instance.get();
        if (ri == null) {
            return;
        }
        List<EntitySlot> entities = ri.entities().get(procedureId);
        // Dynamic mode: one Entity per key the input stream actually carries,
        // created on first sight rather than declared up front.
        if (dynamic) {
            Optional<String> key = routingInstance.entityKey(value);
            if (key.isEmpty()) {
                LOG.warn(
                        "procedure `{}`: dynamic entityKey() returned empty; dropping input",
                        procedureDescriptor.name());
                return;
            }
            int idx = ri.getOrCreateDynamicEntity(procedureId, key.get());
            enqueue(ri.entities().get(procedureId).get(idx), value, primaryStreamId, offset, replay);
            return;
        }
        if (procedureDescriptor.entityKeys().isEmpty()) {
            enqueue(entities.get(0), value, primaryStreamId, offset, replay);
            return;
        }
        // Procedure.Input is erased at this boundary; topology wiring guarantees value's runtime type matches.
        List<String> keys = routingInstance.entitiesFor(value);
        Map<String, Integer> keyToIdx = ri.entityKeyToIdx().get(procedureId);
        for (String key : keys) {
            Integer idx = keyToIdx.get(key);
            if (idx != null) {
                enqueue(entities.get(idx), value, primaryStreamId, offset, replay);
            }
        }
    }

    private void enqueue(EntitySlot slot, Object value, long primaryStreamId, OptionalLong offset) {
        enqueue(slot, value, primaryStreamId, offset, false);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void enqueue(EntitySlot slot, Object value, long primaryStreamId, OptionalLong offset, boolean replay) {
        if (correctionWindowMs > 0) {
            long eventTime = routingInstance.eventTime(value);
            if (slot.bufferIfSliceOpen(
                    new EntitySlot.PendingInput(value, primaryStreamId, offset, eventTime, replay))) {
                return;
            }
            if (isBeyondWindow(slot, eventTime) && beyondWindowPolicy == BeyondWindowPolicy.DROP) {
                LOG.warn(
                        "procedure `{}`: dropping input at event time {} — beyond the {} ms correction window",
                        procedureDescriptor.name(),
                        eventTime,
                        correctionWindowMs);
                return;
            }
            // Fold the mark when the input is ADMITTED, not when it runs: dispatch
            // is asynchronous, so classifying against a mark that only moves at
            // dispatch time would race every burst.
            slot.observeEventTime(eventTime);
        }
        admit(slot, new InputEnvelope(value, primaryStreamId, offset, replay));
    }

    private void admit(EntitySlot slot, InputEnvelope envelope) {
        slot.touch();
        boolean shouldDrain = slot.mailbox().push(envelope);
        if (shouldDrain) {
            pool.execute(() -> drain(slot));
        }
    }

    /** Older than the high-water by more than the window: the past it would repair is gone. */
    private boolean isBeyondWindow(EntitySlot slot, long eventTime) {
        long highWater = slot.eventTimeHighWater();
        return highWater != Long.MIN_VALUE && eventTime < highWater - correctionWindowMs;
    }

    /**
     * Close the correction replay slice: dispatch its members in <b>event-time
     * order</b> — the whole point of the correction — and only then the live
     * inputs that arrived behind it, in arrival order.
     *
     * <p>That ordering is load-bearing. A live input released ahead of the slice
     * would jump in front of the very inputs being re-ordered, and the corrected
     * state would be wrong in a way nothing detects.
     */
    public void endReplaySlice(EntitySlot slot) {
        for (EntitySlot.PendingInput p : slot.closeReplaySlice()) {
            push(slot, p);
        }
    }

    private void push(EntitySlot slot, EntitySlot.PendingInput p) {
        // Max-only: a replayed input's event time can be BELOW the mark (that is
        // what makes it late), and lowering the mark would make a genuinely late
        // input read as on-time.
        slot.observeEventTime(p.eventTime());
        admit(slot, new InputEnvelope(p.input(), p.primaryStreamId(), p.offset(), p.replay()));
    }

    /** Close any open slice on every Entity of this Procedure. */
    public void endReplaySliceAllEntities() {
        UnboundedOperativeInstance ri = instance.get();
        if (ri == null) {
            return;
        }
        for (EntitySlot slot : ri.entities().get(procedureId)) {
            endReplaySlice(slot);
        }
    }

    private void drain(EntitySlot slot) {
        try {
            while (true) {
                Object item = slot.mailbox().pop();
                while (item != null) {
                    InputEnvelope env = (InputEnvelope) item;
                    runOnce(slot, env.input, env.primaryStreamId, env.offset, env.replay);
                    item = slot.mailbox().pop();
                }
                slot.mailbox().releaseInflight();
                if (slot.mailbox().isEmpty()) {
                    return;
                }
                if (!slot.mailbox().tryMarkInflight()) {
                    return;
                }
            }
        } catch (Throwable t) {
            // Defense-in-depth: drain loop must release inflight even on
            // unexpected failure so subsequent pushes can resubmit.
            slot.mailbox().releaseInflight();
            LOG.error("dispatcher drain failed for procedure `{}`: {}", procedureDescriptor.name(), t.toString());
        }
    }

    /**
     * Evaluate the Procedure's declared {@code Clock} for this invocation. The
     * runtime does this once, before {@code run}; {@code ctx.now()} then serves
     * the single value. Mirrors the Rust dispatcher's {@code ClockFor<Input>}
     * evaluation — which the type system binds there, and registration
     * validation binds here.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private long evaluateClock(Object input, long invocationSeq) {
        Class<?> clock = procedureDescriptor.clockType();
        if (clock == com.flezha.sdk.state.EventTimeFrom.class) {
            return routingInstance.eventTime(input);
        }
        if (clock == com.flezha.sdk.state.LogicalClock.class) {
            return invocationSeq;
        }
        return System.currentTimeMillis();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void runOnce(EntitySlot slot, Object input, long primaryStreamId, OptionalLong offset, boolean replay) {
        slot.stateLock().lock();
        try {
            // run / applyChange execute on the Entity's OWN instance, so a
            // mutable field on a user's Procedure is per-Entity scratch — the
            // dispatcher's shared instance serves only pre-Entity routing.
            Procedure entityProcedure = slot.procedureInstance();
            Object state = slot.state().get();
            UnboundedOperativeInstance ri = instance.get();
            EntityId entityId =
                    EntityId.of(ri == null ? "" : ri.instanceId(), procedureDescriptor.name(), slot.entityKey());
            // The declared Clock is evaluated ONCE, before run: ctx.now() then
            // serves that single value for the whole invocation. The change seq
            // is the replay-stable half of ctx.stableId().
            long invocationSeq = slot.changeSeq().get();
            long clockValue = evaluateClock(input, invocationSeq);
            EmitContext ctxImpl = new EmitContext(primaryStreamId, slot, entityId, clockValue, invocationSeq, replay);
            // Build a StateHandle that records Changes onto the Entity's
            // flush buffer under the bufferLock. The runtime's flusher owns
            // the buffer's drain cadence; here we just append.
            //
            // Under the tier, changes go to a PER-INVOCATION list instead: they
            // ship inside this invocation's effect record, alongside its outputs
            // and its consumed offset, so the controller persists all three in
            // one WAL record under one fsync.
            BiConsumer<Object, Change> applyChangeFn = (s, c) -> {
                entityProcedure.applyChange(s, c);
            };
            List<Change> bufferView;
            if (tier) {
                bufferView = new ArrayList<>();
            } else {
                synchronized (slot.changeBufferLock()) {
                    bufferView = slot.changeBuffer();
                }
            }
            StateHandle<Object, Change> handle = StateHandle.create(state, bufferView, applyChangeFn);

            // Retry-then-park loop (dead-letter-queue): a failing
            // invocation is re-run up to the input channel's retryMax with
            // exponential backoff + jitter; a NonRetryable skips the
            // remaining budget. Panic-parity with the Rust dispatcher:
            // ProcedureException and RuntimeException share one policy.
            int maxAttempts = Math.max(1, retryPolicy.maxAttempts());
            int attempt = 0;
            String parkError = null;
            while (true) {
                attempt++;
                boolean isFinal = attempt >= maxAttempts;
                String error;
                boolean retryable;
                try {
                    entityProcedure.run(input, handle, ctxImpl);
                    break;
                } catch (ProcedureException e) {
                    error = e.getMessage();
                    retryable = !(e instanceof ProcedureException.NonRetryable);
                } catch (RuntimeException e) {
                    error = e.toString();
                    retryable = true;
                }
                if (!retryable || isFinal) {
                    parkError = error;
                    break;
                }
                DlqPark.RETRIES_TOTAL.incrementAndGet();
                LOG.warn(
                        "procedure `{}` entity `{}` attempt {}/{} failed (retrying): {}",
                        procedureDescriptor.name(),
                        slot.entityKey().orElse("<single>"),
                        attempt,
                        maxAttempts,
                        error);
                sleepQuietly(retryPolicy.backoffFor(attempt));
            }
            if (tier) {
                stageEffectRecord(slot, input, bufferView, ctxImpl, offset, parkError, attempt);
            } else if (parkError != null) {
                parkPoison(slot, input, parkError, attempt);
            }
        } finally {
            // Progress, whatever the outcome: the invocation ran. A frozen pool is
            // exactly the case where this stops advancing.
            com.flezha.sdk.runtime.entity.EntityCounters.noteDispatch();
            slot.stateLock().unlock();
        }
    }

    /**
     * Stage this invocation's effect record: its changes, its plugin outputs,
     * the offset of the input it consumed, and — if the input was poison and the
     * retry budget is spent — its dead-letter entry.
     *
     * <p>The park rides <b>inside</b> the record rather than as a separate
     * {@code park_payload} frame. That is what keeps the park and the
     * consumed-offset advance atomic: a crash between two frames could otherwise
     * acknowledge the poison input while losing the evidence of why.
     */
    private void stageEffectRecord(
            EntitySlot slot,
            Object input,
            List<Change> changes,
            EmitContext ec,
            OptionalLong offset,
            String parkError,
            int attempts) {
        Optional<DlqPark.DlqEntry> dlq = Optional.empty();
        if (parkError != null) {
            DlqPark.PARKED_TOTAL.incrementAndGet();
            LOG.error(
                    "procedure `{}` entity `{}` parking poison payload inside its effect record"
                            + " after {} attempt(s): {}",
                    procedureDescriptor.name(),
                    slot.entityKey().orElse("<single>"),
                    attempts,
                    parkError);
            dlq = Optional.of(
                    new DlqPark.DlqEntry(inputChannelName, parkError, attempts, DlqPark.extractPayloadBytes(input)));
        }

        List<byte[]> changeBytes = new ArrayList<>(changes.size());
        for (Change change : changes) {
            changeBytes.add(codec.serialize(change));
        }

        synchronized (slot.changeBufferLock()) {
            if (!checkpointAck) {
                slot.effectBuffer().add(new EffectRecord(changeBytes, ec.collected, offset, dlq, ec.now()));
                return;
            }

            // Checkpoint ack-granularity. The interval accumulates changes and
            // only CLOSES on an explicit ctx.checkpoint() — or on a park, which
            // must close it: otherwise the poison offset would be acknowledged
            // while the state built before it is still not durable.
            slot.stagedChanges().addAll(changeBytes);
            boolean closing = ec.checkpointed || dlq.isPresent();

            if (closing) {
                List<byte[]> interval = new ArrayList<>(slot.stagedChanges());
                slot.stagedChanges().clear();
                slot.effectBuffer().add(new EffectRecord(interval, ec.collected, offset, dlq, ec.now()));
            } else if (!ec.collected.isEmpty()) {
                // Mid-interval emits flush immediately, carrying NO offset: the
                // interval's inputs are re-forwarded on replay, so the outputs
                // are re-emitted either way, and the keyed sink absorbs that.
                // Holding them back would buy nothing.
                slot.effectBuffer()
                        .add(new EffectRecord(
                                List.of(), ec.collected, OptionalLong.empty(), Optional.empty(), ec.now()));
            }
        }
    }

    /**
     * Durably park an exhausted poison payload to the controller's DLQ and
     * hold the drain thread until the park is acked — park-then-advance,
     * never a drop.
     */
    private void parkPoison(EntitySlot slot, Object input, String error, int attempts) {
        DlqPark.PARKED_TOTAL.incrementAndGet();
        UnboundedOperativeInstance ri = instance.get();
        String instanceId = ri == null ? "<gone>" : ri.instanceId();
        EntityId entityId = EntityId.of(instanceId, procedureDescriptor.name(), slot.entityKey());
        LOG.error(
                "procedure `{}` entity `{}` parking poison payload to DLQ after {} attempt(s): {}",
                procedureDescriptor.name(),
                slot.entityKey().orElse("<single>"),
                attempts,
                error);
        DlqPark.parkBlocking(
                entityId,
                new DlqPark.ParkBody(
                        DlqPark.mintParkId(entityId.encoded()),
                        OptionalLong.empty(),
                        new DlqPark.DlqEntry(inputChannelName, error, attempts, DlqPark.extractPayloadBytes(input))));
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private record InputEnvelope(Object input, long primaryStreamId, OptionalLong offset, boolean replay) {}

    /**
     * Per-invocation {@link ProcedureCtx}. Single allocation per Procedure.run call.
     * Re-using the descriptor cached on the dispatcher avoids any per-emit
     * lookup.
     */
    private final class EmitContext implements ProcedureCtx {
        private final long primaryStreamId;
        private final EntitySlot slot;
        private final EntityId entityId;

        /** The declared Clock's value, evaluated once before {@code run}. */
        private final long now;

        /** The invocation's durable change seq — the replay-stable half of {@link #stableId}. */
        private final long invocationSeq;

        private byte[] identityBytes;
        private int stableIdOrdinal;
        private int emitOrdinal;

        /** Tier only: outputs staged into this invocation's effect record instead of shipped. */
        private final List<EffectRecord.OutputRecord> collected = new ArrayList<>();

        /** Set by {@code ctx.checkpoint()}: this invocation closes the interval. */
        private boolean checkpointed;

        /** Re-running already-processed input: {@code ctx.spawnBounded} issues nothing. */
        private final boolean replay;

        @Override
        public void checkpoint() {
            checkpointed = true;
        }

        @Override
        public boolean replaying() {
            return replay;
        }

        EmitContext(
                long primaryStreamId,
                EntitySlot slot,
                EntityId entityId,
                long now,
                long invocationSeq,
                boolean replay) {
            this.replay = replay;
            this.primaryStreamId = primaryStreamId;
            this.slot = slot;
            this.entityId = entityId;
            this.now = now;
            this.invocationSeq = invocationSeq;
        }

        @Override
        public long now() {
            return now;
        }

        @Override
        @Nonnull
        public StableId stableId() {
            if (identityBytes == null) {
                identityBytes = entityId.encodedBytes();
            }
            return StableId.derive(identityBytes, invocationSeq, stableIdOrdinal++);
        }

        @Override
        public <C> void emit(@Nonnull C value) throws ProcedureException {
            doEmit(outputChannelDescriptor.orElse(null), value, null, null);
        }

        @Override
        public <C> void emit(@Nonnull String channelName, @Nonnull C value) throws ProcedureException {
            doEmit(resolveChannel(channelName), value, null, null);
        }

        @Override
        public <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) throws ProcedureException {
            doEmit(outputChannelDescriptor.orElse(null), value, meta, null);
        }

        @Override
        public <C> void emitWithMeta(@Nonnull String channelName, @Nonnull C value, @Nonnull Map<String, String> meta)
                throws ProcedureException {
            doEmit(resolveChannel(channelName), value, meta, null);
        }

        @Override
        public <C> void emitKeyed(@Nonnull String key, @Nonnull C value) throws ProcedureException {
            doEmit(outputChannelDescriptor.orElse(null), value, null, key);
        }

        @Override
        public <C> void emitKeyedWithMeta(@Nonnull String key, @Nonnull C value, @Nonnull Map<String, String> meta)
                throws ProcedureException {
            doEmit(outputChannelDescriptor.orElse(null), value, meta, key);
        }

        private ChannelDescriptor resolveChannel(String name) throws ProcedureException {
            Integer id = topology.lookupChannelId(name);
            if (id == null) {
                throw new ProcedureException.UnknownChannel(name);
            }
            return topology.channel(id);
        }

        /**
         * The single emit funnel: enforces the keyed-channel contract (the
         * declaration is the source of truth for whether a key is required),
         * rejects reserved {@code flz.} metadata keys, then routes by shape.
         */
        private void doEmit(ChannelDescriptor descriptor, Object value, Map<String, String> meta, String key)
                throws ProcedureException {
            if (descriptor == null) {
                throw new ProcedureException.UnknownChannel("<no output channel declared on this procedure>");
            }
            if (descriptor.keyed() && key == null) {
                throw new ProcedureException.KeyRequiredOnKeyedChannel(descriptor.name());
            }
            if (!descriptor.keyed() && key != null) {
                throw new ProcedureException.KeyNotAllowedOnNonKeyedChannel(descriptor.name());
            }
            if (meta != null) {
                for (String k : meta.keySet()) {
                    if (k.startsWith(ProcedureCtx.RESERVED_METADATA_PREFIX)) {
                        throw new ProcedureException.ReservedMetadataKey(k);
                    }
                }
            }
            switch (descriptor.kind()) {
                case INTER_PROCEDURE -> {
                    Optional<PluginSink> sink = descriptor.pluginSink();
                    if (meta != null && sink.isEmpty()) {
                        throw new ProcedureException.MetadataNotAllowedOnInterProcedureChannel(descriptor.name());
                    }
                    fanoutInterProcedure(descriptor, value, primaryStreamId);
                    if (sink.isPresent()) {
                        emitPluginSink(descriptor, sink.get(), value, meta, key, this);
                    }
                }
                case PLUGIN_BOUND -> emitPlugin(descriptor, value, meta, key, this);
                case ENTRY -> throw new ProcedureException(
                        "cannot emit on entry channel `" + descriptor.name() + "` from a procedure");
            }
        }
    }

    /**
     * Route one channel value to every subscribed consumer, each through its
     * own edge's merge mapping. The single seam a {@link ConsumerEdge} mapping
     * is applied on: the wire-reader path (live delivery and the controller's
     * re-forwarded replay slice, via {@code PrimaryStreamReader}) and an
     * inter-procedure emit both come through here, so the two can never drift.
     * Mirrors the Rust SDK's {@code dispatcher::route_to_consumers}.
     */
    public static void routeToConsumers(
            @Nonnull UnboundedOperativeInstance instance,
            @Nonnull ChannelDescriptor channel,
            @Nonnull Object value,
            long primaryStreamId,
            @Nonnull OptionalLong offset,
            boolean replay) {
        for (ConsumerEdge edge : channel.consumers()) {
            ProcedureDispatcher consumer = instance.dispatcher(edge.procedure());
            if (consumer != null) {
                consumer.fanoutEntities(edge.mapValue(value), primaryStreamId, offset, replay);
            }
        }
    }

    private void fanoutInterProcedure(ChannelDescriptor descriptor, Object value, long primaryStreamId) {
        UnboundedOperativeInstance ri = instance.get();
        if (ri == null) {
            return;
        }
        // A value derived downstream carries no input-log offset — it was never
        // journaled — and is never a replay member: the downstream procedure
        // receives the corrected outputs already in order.
        routeToConsumers(ri, descriptor, value, primaryStreamId, OptionalLong.empty(), false);
    }

    private void emitPlugin(
            ChannelDescriptor descriptor, Object value, Map<String, String> userMeta, String key, EmitContext ec) {
        UnboundedOperativeInstance ri = instance.get();
        if (ri == null) {
            return;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        java.util.function.Function<Object, byte[]> serializer =
                (java.util.function.Function) descriptor.pluginSerializer().orElse(null);
        if (serializer == null) {
            LOG.error("plugin channel `{}` is missing a serializer", descriptor.name());
            return;
        }

        byte[] bytes;
        try {
            bytes = serializer.apply(value);
        } catch (RuntimeException e) {
            LOG.error("plugin channel `{}` serializer failed: {}", descriptor.name(), e.toString());
            return;
        }

        if (!stageOutput(descriptor, bytes, userMeta, key, ec)) {
            writePluginFrame(ri, descriptor, bytes, userMeta, key, ec);
        }
    }

    /**
     * Under the tier, a plugin output is STAGED into the invocation's effect
     * record rather than shipped: the controller derives the identity stamps
     * when it flushes the record, so none ride here. Returns {@code true} when
     * the output was staged and must not also be written to the wire.
     */
    private boolean stageOutput(
            ChannelDescriptor descriptor, byte[] bytes, Map<String, String> userMeta, String key, EmitContext ec) {
        if (!tier) {
            return false;
        }
        List<Map.Entry<String, String>> meta = new ArrayList<>();
        if (userMeta != null) {
            for (Map.Entry<String, String> e : userMeta.entrySet()) {
                if (!"channel".equals(e.getKey())) {
                    meta.add(Map.entry(e.getKey(), e.getValue()));
                }
            }
        }
        ec.collected.add(new EffectRecord.OutputRecord(descriptor.name(), key == null ? "" : key, meta, bytes));
        return true;
    }

    private void emitPluginSink(
            ChannelDescriptor descriptor,
            PluginSink sink,
            Object value,
            Map<String, String> userMeta,
            String key,
            EmitContext ec)
            throws ProcedureException {
        UnboundedOperativeInstance ri = instance.get();
        if (ri == null) {
            return;
        }
        byte[] bytes;
        try {
            bytes = sink.serializer().apply(value);
        } catch (RuntimeException | Error e) {
            throw new ProcedureException.PluginSerializerFailure(descriptor.name(), e.toString(), e);
        }
        if (!stageOutput(descriptor, bytes, userMeta, key, ec)) {
            writePluginFrame(ri, descriptor, bytes, userMeta, key, ec);
        }
    }

    private void writePluginFrame(
            UnboundedOperativeInstance ri,
            ChannelDescriptor descriptor,
            byte[] bytes,
            Map<String, String> userMeta,
            String key,
            EmitContext ec) {
        Map<String, String> metadata = new LinkedHashMap<>();
        if (userMeta != null) {
            for (Map.Entry<String, String> e : userMeta.entrySet()) {
                if (!"channel".equals(e.getKey())) {
                    metadata.put(e.getKey(), e.getValue());
                }
            }
        }
        metadata.put("channel", descriptor.name());

        // Identity stamps are appended AFTER user metadata so they can never be
        // clobbered (a user `flz.` key is already refused at the emit funnel).
        // A non-keyed channel gets no stamps at all, keeping its frame
        // byte-identical to the pre-change layout.
        if (descriptor.keyed()) {
            metadata.put("flz.entity", ec.entityId.encoded());
            metadata.put("flz.seq", ec.slot.changeSeq().get() + "." + ec.emitOrdinal);
            metadata.put("flz.key", key);
        }
        ec.emitOrdinal++;

        long secondaryStreamId = ctx.streamRegistry().secondaryFor(ec.primaryStreamId);
        if (secondaryStreamId == 0L) {
            LOG.warn(
                    "plugin emit on `{}`: no secondary stream paired with primary {}",
                    descriptor.name(),
                    ec.primaryStreamId);
            return;
        }

        byte[] frame = FrameEncoder.build(ri.instanceId(), bytes, metadata);
        int writeStatus = WIRE_WRITER.write(ctx.nativeHandle(), secondaryStreamId, frame);
        if (writeStatus != 0) {
            LOG.error("plugin emit on `{}`: stream write failed (status {})", descriptor.name(), writeStatus);
        }
    }
}
