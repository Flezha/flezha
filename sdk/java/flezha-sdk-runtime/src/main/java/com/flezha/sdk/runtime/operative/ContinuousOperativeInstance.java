package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.segment.SegmentSlot;
import com.flezha.sdk.runtime.state.FlusherEntry;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.SegmentFlusher;
import com.flezha.sdk.runtime.state.SegmentId;
import com.flezha.sdk.runtime.state.SegmentStateSink;
import com.flezha.sdk.runtime.worker.OperationDispatcher;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.state.NoChange;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Per-{@code AddOperative} runtime state. Holds:
 * <ul>
 *   <li>the shared {@link ContinuousOperativeTopology} (shared across all instances of
 *       this ContinuousOperative class),</li>
 *   <li>the controller-facing {@code instanceId} (= what the wire calls
 *       {@code operative_id}),</li>
 *   <li>per-Operation segment slots (one inner-list entry per Segment;
 *       single-mode Operations have inner length 1),</li>
 *   <li>per-Operation reverse index from segment key → segment index
 *       (empty for single-mode Operations),</li>
 *   <li>the resolved {@code persistenceEnabled} toggle (executor override
 *       &gt; ContinuousOperative default &gt; {@code true}; frozen for the instance's
 *       lifetime),</li>
 *   <li>the aggregated Kryo type list across all Operations in this ContinuousOperative class
 *       (used by the per-Segment flusher and the Compact handler).</li>
 * </ul>
 */
public final class ContinuousOperativeInstance implements Operative {

    private final ContinuousOperativeTopology topology;
    private final String instanceId;
    private final List<List<SegmentSlot>> segments;
    private final List<Map<String, Integer>> segmentKeyToIdx;
    private final List<OperationDispatcher> dispatchers;
    private final boolean persistenceEnabled;
    private final List<Class<?>> kryoTypes;

    private ContinuousOperativeInstance(
            ContinuousOperativeTopology topology,
            String instanceId,
            List<List<SegmentSlot>> segments,
            List<Map<String, Integer>> segmentKeyToIdx,
            List<OperationDispatcher> dispatchers,
            boolean persistenceEnabled,
            List<Class<?>> kryoTypes) {
        this.topology = topology;
        this.instanceId = instanceId;
        this.segments = segments;
        this.segmentKeyToIdx = segmentKeyToIdx;
        this.dispatchers = dispatchers;
        this.persistenceEnabled = persistenceEnabled;
        this.kryoTypes = kryoTypes;
    }

    /** Construct a fresh {@link ContinuousOperativeInstance} with persistence enabled. */
    @Nonnull
    public static ContinuousOperativeInstance create(
            @Nonnull ContinuousOperativeTopology topology,
            @Nonnull String instanceId,
            @Nonnull ExecutorContext ctx,
            @Nonnull ExecutorService pool) {
        return create(topology, instanceId, ctx, pool, true);
    }

    /**
     * Construct a fresh {@link ContinuousOperativeInstance}, threading the resolved
     * {@code persistenceEnabled} toggle from {@code AddOperative} metadata.
     * Calls {@code Operation.initState} for each Operation×Segment.
     */
    @Nonnull
    public static ContinuousOperativeInstance create(
            @Nonnull ContinuousOperativeTopology topology,
            @Nonnull String instanceId,
            @Nonnull ExecutorContext ctx,
            @Nonnull ExecutorService pool,
            boolean persistenceEnabled) {
        int operationCount = topology.operations().size();
        List<List<SegmentSlot>> segments = new ArrayList<>(operationCount);
        List<Map<String, Integer>> segmentKeyToIdx = new ArrayList<>(operationCount);
        List<OperationDispatcher> dispatchers = new ArrayList<>(operationCount);

        // Aggregate Kryo type registrations across all operations in declaration
        // order. Duplicate entries (same Class<?>) collapse — registration
        // order across operations is the order the topology emits them.
        LinkedHashSet<Class<?>> kryoTypeSet = new LinkedHashSet<>();
        for (OperationDescriptor desc : topology.operations()) {
            kryoTypeSet.addAll(desc.kryoTypes());
        }
        List<Class<?>> kryoTypes = List.copyOf(kryoTypeSet);

        // Shared per-instance flusher inputs: one codec (stable Kryo IDs across
        // the ContinuousOperative class), a stateless header template, and the global state
        // sink as the send path. Each changeful Segment registers below.
        KryoCodec codec = new KryoCodec(kryoTypes);
        HeaderTemplate headerTemplate = new HeaderTemplate();
        Function<byte[], Boolean> sendFrame = SegmentStateSink::send;

        ContinuousOperativeInstance shell = new ContinuousOperativeInstance(
                topology, instanceId, segments, segmentKeyToIdx, dispatchers, persistenceEnabled, kryoTypes);

        for (int operationId = 0; operationId < operationCount; operationId++) {
            OperationDescriptor desc = topology.operations().get(operationId);
            List<SegmentSlot> slots;
            Map<String, Integer> keyMap = new HashMap<>();
            boolean changeful = desc.changeType() != NoChange.class;
            int mailboxCapacity = desc.mailboxCapacity();
            OverflowPolicy policy = desc.inputChannelId().isPresent()
                    ? topology.channel(desc.inputChannelId().getAsInt()).overflowPolicy()
                    : OverflowPolicy.BLOCK;

            if (desc.segmentKeys().isEmpty()) {
                Operation<?, ?, ?, ?, ?, ?> operationInstance = instantiate(desc.operationClass());
                OperationDispatcher dispatcher = new OperationDispatcher(ctx, pool, shell, topology, operationId, operationInstance);
                @SuppressWarnings({"unchecked", "rawtypes"})
                Object initial = ((Operation) operationInstance).initState(Optional.empty(), Optional.empty());
                SegmentSlot slot =
                        new SegmentSlot(dispatcher, operationId, Optional.empty(), changeful, mailboxCapacity, policy);
                slot.state().set(initial);
                registerWithFlusher(
                        instanceId, desc, operationInstance, slot, persistenceEnabled, codec, headerTemplate, sendFrame);
                slots = List.of(slot);
                dispatchers.add(dispatcher);
            } else {
                slots = new ArrayList<>(desc.segmentKeys().size());
                Operation<?, ?, ?, ?, ?, ?> dispatcherOperationInstance = instantiate(desc.operationClass());
                OperationDispatcher dispatcher =
                        new OperationDispatcher(ctx, pool, shell, topology, operationId, dispatcherOperationInstance);
                dispatchers.add(dispatcher);
                for (int idx = 0; idx < desc.segmentKeys().size(); idx++) {
                    String key = desc.segmentKeys().get(idx);
                    Operation<?, ?, ?, ?, ?, ?> operationInstance = instantiate(desc.operationClass());
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    Object initial = ((Operation) operationInstance).initState(Optional.of(key), Optional.empty());
                    SegmentSlot slot = new SegmentSlot(
                            dispatcher, operationId, Optional.of(key), changeful, mailboxCapacity, policy);
                    slot.state().set(initial);
                    registerWithFlusher(
                            instanceId,
                            desc,
                            operationInstance,
                            slot,
                            persistenceEnabled,
                            codec,
                            headerTemplate,
                            sendFrame);
                    slots.add(slot);
                    keyMap.put(key, idx);
                }
                slots = List.copyOf(slots);
            }
            segments.add(slots);
            segmentKeyToIdx.add(Map.copyOf(keyMap));
        }
        return shell;
    }

    /**
     * Register a changeful Segment with the per-process flusher so its
     * recorded Changes are framed and shipped to the controller's per-Segment
     * WAL. {@code NoChange} Operations are skipped (the flusher no-ops them too).
     * The identity is keyed by the operative INSTANCE id so two operatives of the same
     * factory never collide on one Segment's WAL.
     */
    private static void registerWithFlusher(
            String instanceId,
            OperationDescriptor desc,
            Operation<?, ?, ?, ?, ?, ?> operationInstance,
            SegmentSlot slot,
            boolean persistenceEnabled,
            KryoCodec codec,
            HeaderTemplate headerTemplate,
            Function<byte[], Boolean> sendFrame) {
        if (!slot.changeful()) {
            return;
        }
        SegmentId segmentId = SegmentId.of(instanceId, desc.name(), slot.operationKey());
        FlusherEntry entry = new FlusherEntry(
                slot.changeBufferLock(),
                slot.changeBuffer(),
                persistenceEnabled,
                slot.changeSeq(),
                sendFrame,
                headerTemplate,
                segmentId,
                operationInstance.schemaVersion(),
                codec);
        SegmentFlusher.getOrStart().register(segmentId, entry, desc.changeType());
    }

    private static Operation<?, ?, ?, ?, ?, ?> instantiate(Class<? extends Operation<?, ?, ?, ?, ?, ?>> operationClass) {
        try {
            return operationClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "operation class `" + operationClass.getName() + "` must have a public no-arg constructor: " + e.toString(),
                    e);
        }
    }

    @Nonnull
    public ContinuousOperativeTopology topology() {
        return topology;
    }

    @Nonnull
    public String instanceId() {
        return instanceId;
    }

    @Nonnull
    public List<List<SegmentSlot>> segments() {
        return segments;
    }

    @Nonnull
    public List<Map<String, Integer>> segmentKeyToIdx() {
        return segmentKeyToIdx;
    }

    public OperationDispatcher dispatcher(int operationId) {
        if (operationId < 0 || operationId >= dispatchers.size()) {
            return null;
        }
        return dispatchers.get(operationId);
    }

    /** Resolved persistence toggle, frozen at construction time. */
    public boolean persistenceEnabled() {
        return persistenceEnabled;
    }

    /** Aggregated Kryo type list across all Operations in this ContinuousOperative class. */
    @Nonnull
    public List<Class<?>> kryoTypes() {
        return kryoTypes;
    }
}
