package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.entity.EntitySlot;
import com.flezha.sdk.runtime.state.EntityFlusher;
import com.flezha.sdk.runtime.state.EntityId;
import com.flezha.sdk.runtime.state.EntityStateSink;
import com.flezha.sdk.runtime.state.FlusherEntry;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.worker.ProcedureDispatcher;
import com.flezha.sdk.state.NoChange;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;

/**
 * Per-{@code AddOperative} runtime state. Holds:
 * <ul>
 *   <li>the shared {@link UnboundedOperativeTopology} (shared across all instances of
 *       this UnboundedOperative class),</li>
 *   <li>the controller-facing {@code instanceId} (= what the wire calls
 *       {@code operative_id}),</li>
 *   <li>per-Procedure entity slots (one inner-list entry per Entity;
 *       single-mode Procedures have inner length 1),</li>
 *   <li>per-Procedure reverse index from entity key → entity index
 *       (empty for single-mode Procedures),</li>
 *   <li>the resolved {@code persistenceEnabled} toggle (runner override
 *       &gt; UnboundedOperative default &gt; {@code true}; frozen for the instance's
 *       lifetime),</li>
 *   <li>the aggregated Kryo type list across all Procedures in this UnboundedOperative class
 *       (used by the per-Entity flusher and the Compact handler).</li>
 * </ul>
 */
public final class UnboundedOperativeInstance implements Operative {

    private final UnboundedOperativeTopology topology;
    private final String instanceId;
    private final List<List<EntitySlot>> entities;
    private final List<Map<String, Integer>> entityKeyToIdx;
    private final List<ProcedureDispatcher> dispatchers;
    private final boolean persistenceEnabled;
    private final List<Class<?>> kryoTypes;

    private UnboundedOperativeInstance(
            UnboundedOperativeTopology topology,
            String instanceId,
            List<List<EntitySlot>> entities,
            List<Map<String, Integer>> entityKeyToIdx,
            List<ProcedureDispatcher> dispatchers,
            boolean persistenceEnabled,
            List<Class<?>> kryoTypes) {
        this.topology = topology;
        this.instanceId = instanceId;
        this.entities = entities;
        this.entityKeyToIdx = entityKeyToIdx;
        this.dispatchers = dispatchers;
        this.persistenceEnabled = persistenceEnabled;
        this.kryoTypes = kryoTypes;
    }

    /** Construct a fresh {@link UnboundedOperativeInstance} with persistence enabled. */
    @Nonnull
    public static UnboundedOperativeInstance create(
            @Nonnull UnboundedOperativeTopology topology,
            @Nonnull String instanceId,
            @Nonnull RunnerContext ctx,
            @Nonnull ExecutorService pool) {
        return create(topology, instanceId, ctx, pool, true);
    }

    /**
     * Construct a fresh {@link UnboundedOperativeInstance}, threading the resolved
     * {@code persistenceEnabled} toggle from {@code AddOperative} metadata.
     * Calls {@code Procedure.initState} for each Procedure×Entity.
     */
    @Nonnull
    public static UnboundedOperativeInstance create(
            @Nonnull UnboundedOperativeTopology topology,
            @Nonnull String instanceId,
            @Nonnull RunnerContext ctx,
            @Nonnull ExecutorService pool,
            boolean persistenceEnabled) {
        int procedureCount = topology.procedures().size();
        List<List<EntitySlot>> entities = new ArrayList<>(procedureCount);
        List<Map<String, Integer>> entityKeyToIdx = new ArrayList<>(procedureCount);
        List<ProcedureDispatcher> dispatchers = new ArrayList<>(procedureCount);

        // Aggregate Kryo type registrations across all procedures in declaration
        // order. Duplicate entries (same Class<?>) collapse — registration
        // order across procedures is the order the topology emits them.
        LinkedHashSet<Class<?>> kryoTypeSet = new LinkedHashSet<>();
        for (ProcedureDescriptor desc : topology.procedures()) {
            kryoTypeSet.addAll(desc.kryoTypes());
        }
        List<Class<?>> kryoTypes = List.copyOf(kryoTypeSet);

        // Shared per-instance flusher inputs: one codec (stable Kryo IDs across
        // the UnboundedOperative class), a stateless header template, and the global state
        // sink as the send path. Each changeful Entity registers below.
        KryoCodec codec = new KryoCodec(kryoTypes);
        HeaderTemplate headerTemplate = new HeaderTemplate();
        BiFunction<String, byte[], Boolean> sendFrame = EntityStateSink::send;

        UnboundedOperativeInstance shell = new UnboundedOperativeInstance(
                topology, instanceId, entities, entityKeyToIdx, dispatchers, persistenceEnabled, kryoTypes);

        for (int procedureId = 0; procedureId < procedureCount; procedureId++) {
            ProcedureDescriptor desc = topology.procedures().get(procedureId);
            List<EntitySlot> slots;
            Map<String, Integer> keyMap = new HashMap<>();
            boolean changeful = changefulOf(desc);
            int mailboxCapacity = desc.mailboxCapacity();
            OverflowPolicy policy = overflowPolicyFor(topology, desc);

            boolean dynamicMode =
                    desc.entityKeyOverridden() && desc.entityKeys().isEmpty();
            if (dynamicMode) {
                // No Entities up front: a dynamic Procedure's keys are whatever the
                // input stream turns out to carry, so each is created on first sight.
                Procedure<?, ?, ?, ?, ?, ?> procedureInstance = instantiate(desc.procedureClass());
                dispatchers.add(new ProcedureDispatcher(ctx, pool, shell, topology, procedureId, procedureInstance));
                slots = new ArrayList<>();
            } else if (desc.entityKeys().isEmpty()) {
                Procedure<?, ?, ?, ?, ?, ?> procedureInstance = instantiate(desc.procedureClass());
                ProcedureDispatcher dispatcher =
                        new ProcedureDispatcher(ctx, pool, shell, topology, procedureId, procedureInstance);
                @SuppressWarnings({"unchecked", "rawtypes"})
                Object initial = ((Procedure) procedureInstance).initState(Optional.empty(), Optional.empty());
                EntitySlot slot = new EntitySlot(
                        dispatcher,
                        procedureInstance,
                        procedureId,
                        Optional.empty(),
                        changeful,
                        mailboxCapacity,
                        policy);
                slot.state().set(initial);
                registerWithFlusher(
                        instanceId,
                        desc,
                        procedureInstance,
                        slot,
                        persistenceEnabled,
                        codec,
                        headerTemplate,
                        sendFrame);
                slots = List.of(slot);
                dispatchers.add(dispatcher);
            } else {
                slots = new ArrayList<>(desc.entityKeys().size());
                // The dispatcher's instance serves only the pre-Entity routing
                // calls (entitiesFor / entityKey / eventTime); each Entity runs
                // on its own instance below.
                Procedure<?, ?, ?, ?, ?, ?> dispatcherProcedureInstance = instantiate(desc.procedureClass());
                ProcedureDispatcher dispatcher =
                        new ProcedureDispatcher(ctx, pool, shell, topology, procedureId, dispatcherProcedureInstance);
                dispatchers.add(dispatcher);
                for (int idx = 0; idx < desc.entityKeys().size(); idx++) {
                    String key = desc.entityKeys().get(idx);
                    Procedure<?, ?, ?, ?, ?, ?> procedureInstance = instantiate(desc.procedureClass());
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    Object initial = ((Procedure) procedureInstance).initState(Optional.of(key), Optional.empty());
                    EntitySlot slot = new EntitySlot(
                            dispatcher,
                            procedureInstance,
                            procedureId,
                            Optional.of(key),
                            changeful,
                            mailboxCapacity,
                            policy);
                    slot.state().set(initial);
                    registerWithFlusher(
                            instanceId,
                            desc,
                            procedureInstance,
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
            // A dynamic-mode Procedure creates its Entities lazily, one per key
            // seen, so its slot table must stay mutable.
            entities.add(dynamicMode ? new java.util.concurrent.CopyOnWriteArrayList<>(slots) : slots);
            entityKeyToIdx.add(dynamicMode ? new java.util.concurrent.ConcurrentHashMap<>(keyMap) : Map.copyOf(keyMap));
        }
        return shell;
    }

    /**
     * Evict dynamic Entities idle past their declared TTL.
     *
     * <p><b>Destructive</b>: ships {@code delete_entity_state}, so the
     * controller unlinks the Entity's WAL and snapshots. A later input for the
     * same key cold-starts a FRESH Entity — it does not read the old state back.
     * That is why eviction is off unless a TTL is declared.
     *
     * @return how many Entities were evicted
     */
    public int reapIdleDynamicEntities() {
        int evicted = 0;
        for (int procedureId = 0; procedureId < topology.procedures().size(); procedureId++) {
            ProcedureDescriptor desc = topology.procedure(procedureId);
            if (!desc.entityKeyOverridden() || !desc.entityKeys().isEmpty()) {
                continue;
            }
            Optional<java.time.Duration> ttl = desc.entityIdleTtl();
            if (ttl.isEmpty()) {
                continue; // eviction is opt-in
            }
            long ttlMs = ttl.get().toMillis();
            Map<String, Integer> keyMap = entityKeyToIdx.get(procedureId);
            List<EntitySlot> slots = entities.get(procedureId);

            synchronized (keyMap) {
                for (Map.Entry<String, Integer> e : List.copyOf(keyMap.entrySet())) {
                    EntitySlot slot = slots.get(e.getValue());
                    if (slot.idleMs() < ttlMs || !slot.mailbox().isEmpty()) {
                        continue;
                    }
                    EntityStateSink.send(
                            EntityStateSink.PAYLOAD_ID_DELETE_ENTITY_STATE,
                            EntityFlusher.buildEntityStateChangeFrame(
                                    EntityId.of(instanceId, desc.name(), Optional.of(e.getKey())),
                                    0L,
                                    new byte[16],
                                    new byte[0]));
                    keyMap.remove(e.getKey());
                    evicted++;
                }
            }
        }
        return evicted;
    }

    /**
     * Resolve a dynamic-mode Entity by key, creating it on first sight.
     *
     * <p>Cold-starts fresh: a dynamic Entity's state is built by
     * {@code initState(key)}, not restored — which is also why an evicted one may
     * be recreated without reading anything back from disk.
     */
    public int getOrCreateDynamicEntity(int procedureId, @Nonnull String key) {
        Map<String, Integer> keyMap = entityKeyToIdx.get(procedureId);
        Integer existing = keyMap.get(key);
        if (existing != null) {
            return existing;
        }
        synchronized (keyMap) {
            Integer raced = keyMap.get(key);
            if (raced != null) {
                return raced;
            }
            List<EntitySlot> slots = entities.get(procedureId);
            ProcedureDescriptor desc = topology.procedure(procedureId);
            ProcedureDispatcher dispatcher = dispatchers.get(procedureId);
            // A fresh instance per dynamic Entity — the dispatcher's own instance
            // is reserved for routing and must not be the run target for every key.
            Procedure<?, ?, ?, ?, ?, ?> procedureInstance = instantiate(desc.procedureClass());

            int idx = slots.size();
            EntitySlot slot = new EntitySlot(
                    dispatcher,
                    procedureInstance,
                    procedureId,
                    Optional.of(key),
                    changefulOf(desc),
                    desc.mailboxCapacity(),
                    overflowPolicyFor(topology, desc));
            slot.state().set(procedureInstance.initState(Optional.of(key), Optional.empty()));
            slots.add(slot);
            keyMap.put(key, idx);
            return idx;
        }
    }

    /**
     * Register a changeful Entity with the per-process flusher so its
     * recorded Changes are framed and shipped to the controller's per-Entity
     * WAL. {@code NoChange} Procedures are skipped (the flusher no-ops them too).
     * The identity is keyed by the operative INSTANCE id so two operatives of the same
     * factory never collide on one Entity's WAL.
     */
    private static void registerWithFlusher(
            String instanceId,
            ProcedureDescriptor desc,
            Procedure<?, ?, ?, ?, ?, ?> procedureInstance,
            EntitySlot slot,
            boolean persistenceEnabled,
            KryoCodec codec,
            HeaderTemplate headerTemplate,
            BiFunction<String, byte[], Boolean> sendFrame) {
        if (!slot.changeful()) {
            return;
        }
        EntityId entityId = EntityId.of(instanceId, desc.name(), slot.entityKey());
        FlusherEntry entry = new FlusherEntry(
                slot.changeBufferLock(),
                slot.changeBuffer(),
                slot.effectBuffer(),
                persistenceEnabled,
                slot.changeSeq(),
                sendFrame,
                headerTemplate,
                entityId,
                procedureInstance.schemaVersion(),
                codec);
        EntityFlusher.getOrStart().register(entityId, entry, desc.changeType());
    }

    /** Whether a Procedure persists state (declares a non-{@code NoChange} Change). */
    private static boolean changefulOf(ProcedureDescriptor desc) {
        return desc.changeType() != NoChange.class;
    }

    /** An Entity's mailbox overflow policy: the input channel's, or {@code BLOCK} when unwired. */
    private static OverflowPolicy overflowPolicyFor(UnboundedOperativeTopology topology, ProcedureDescriptor desc) {
        return desc.inputChannelId().isPresent()
                ? topology.channel(desc.inputChannelId().getAsInt()).overflowPolicy()
                : OverflowPolicy.BLOCK;
    }

    private static Procedure<?, ?, ?, ?, ?, ?> instantiate(
            Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass) {
        try {
            return procedureClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "procedure class `" + procedureClass.getName() + "` must have a public no-arg constructor: "
                            + e.toString(),
                    e);
        }
    }

    @Nonnull
    public UnboundedOperativeTopology topology() {
        return topology;
    }

    @Nonnull
    public String instanceId() {
        return instanceId;
    }

    @Nonnull
    public List<List<EntitySlot>> entities() {
        return entities;
    }

    @Nonnull
    public List<Map<String, Integer>> entityKeyToIdx() {
        return entityKeyToIdx;
    }

    public ProcedureDispatcher dispatcher(int procedureId) {
        if (procedureId < 0 || procedureId >= dispatchers.size()) {
            return null;
        }
        return dispatchers.get(procedureId);
    }

    /** Resolved persistence toggle, frozen at construction time. */
    public boolean persistenceEnabled() {
        return persistenceEnabled;
    }

    /** Aggregated Kryo type list across all Procedures in this UnboundedOperative class. */
    @Nonnull
    public List<Class<?>> kryoTypes() {
        return kryoTypes;
    }
}
