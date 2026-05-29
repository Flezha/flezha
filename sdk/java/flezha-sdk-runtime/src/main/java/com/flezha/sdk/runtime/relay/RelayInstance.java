package com.flezha.sdk.runtime.relay;

import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.partition.PartitionSlot;
import com.flezha.sdk.runtime.state.FlusherEntry;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.PartitionFlusher;
import com.flezha.sdk.runtime.state.PartitionId;
import com.flezha.sdk.runtime.state.PartitionStateSink;
import com.flezha.sdk.runtime.worker.ShardDispatcher;
import com.flezha.sdk.shard.Shard;
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
 * Per-{@code AddRelay} runtime state. Holds:
 * <ul>
 *   <li>the shared {@link RelayTopology} (shared across all instances of
 *       this Relay class),</li>
 *   <li>the controller-facing {@code instanceId} (= what the wire calls
 *       {@code relay_id}),</li>
 *   <li>per-Shard partition slots (one inner-list entry per Partition;
 *       single-mode Shards have inner length 1),</li>
 *   <li>per-Shard reverse index from partition key → partition index
 *       (empty for single-mode Shards),</li>
 *   <li>the resolved {@code persistenceEnabled} toggle (executor override
 *       &gt; Relay default &gt; {@code true}; frozen for the instance's
 *       lifetime),</li>
 *   <li>the aggregated Kryo type list across all Shards in this Relay class
 *       (used by the per-Partition flusher and the Compact handler).</li>
 * </ul>
 */
public final class RelayInstance {

    private final RelayTopology topology;
    private final String instanceId;
    private final List<List<PartitionSlot>> partitions;
    private final List<Map<String, Integer>> partitionKeyToIdx;
    private final List<ShardDispatcher> dispatchers;
    private final boolean persistenceEnabled;
    private final List<Class<?>> kryoTypes;

    private RelayInstance(
            RelayTopology topology,
            String instanceId,
            List<List<PartitionSlot>> partitions,
            List<Map<String, Integer>> partitionKeyToIdx,
            List<ShardDispatcher> dispatchers,
            boolean persistenceEnabled,
            List<Class<?>> kryoTypes) {
        this.topology = topology;
        this.instanceId = instanceId;
        this.partitions = partitions;
        this.partitionKeyToIdx = partitionKeyToIdx;
        this.dispatchers = dispatchers;
        this.persistenceEnabled = persistenceEnabled;
        this.kryoTypes = kryoTypes;
    }

    /** Construct a fresh {@link RelayInstance} with persistence enabled. */
    @Nonnull
    public static RelayInstance create(
            @Nonnull RelayTopology topology,
            @Nonnull String instanceId,
            @Nonnull ExecutorContext ctx,
            @Nonnull ExecutorService pool) {
        return create(topology, instanceId, ctx, pool, true);
    }

    /**
     * Construct a fresh {@link RelayInstance}, threading the resolved
     * {@code persistenceEnabled} toggle from {@code AddRelay} metadata.
     * Calls {@code Shard.initState} for each Shard×Partition.
     */
    @Nonnull
    public static RelayInstance create(
            @Nonnull RelayTopology topology,
            @Nonnull String instanceId,
            @Nonnull ExecutorContext ctx,
            @Nonnull ExecutorService pool,
            boolean persistenceEnabled) {
        int shardCount = topology.shards().size();
        List<List<PartitionSlot>> partitions = new ArrayList<>(shardCount);
        List<Map<String, Integer>> partitionKeyToIdx = new ArrayList<>(shardCount);
        List<ShardDispatcher> dispatchers = new ArrayList<>(shardCount);

        // Aggregate Kryo type registrations across all shards in declaration
        // order. Duplicate entries (same Class<?>) collapse — registration
        // order across shards is the order the topology emits them.
        LinkedHashSet<Class<?>> kryoTypeSet = new LinkedHashSet<>();
        for (ShardDescriptor desc : topology.shards()) {
            kryoTypeSet.addAll(desc.kryoTypes());
        }
        List<Class<?>> kryoTypes = List.copyOf(kryoTypeSet);

        // Shared per-instance flusher inputs: one codec (stable Kryo IDs across
        // the Relay class), a stateless header template, and the global state
        // sink as the send path. Each changeful Partition registers below.
        KryoCodec codec = new KryoCodec(kryoTypes);
        HeaderTemplate headerTemplate = new HeaderTemplate();
        Function<byte[], Boolean> sendFrame = PartitionStateSink::send;

        RelayInstance shell = new RelayInstance(
                topology, instanceId, partitions, partitionKeyToIdx, dispatchers, persistenceEnabled, kryoTypes);

        for (int shardId = 0; shardId < shardCount; shardId++) {
            ShardDescriptor desc = topology.shards().get(shardId);
            List<PartitionSlot> slots;
            Map<String, Integer> keyMap = new HashMap<>();
            boolean changeful = desc.changeType() != NoChange.class;
            int mailboxCapacity = desc.mailboxCapacity();
            OverflowPolicy policy = desc.inputChannelId().isPresent()
                    ? topology.channel(desc.inputChannelId().getAsInt()).overflowPolicy()
                    : OverflowPolicy.BLOCK;

            if (desc.partitionKeys().isEmpty()) {
                Shard<?, ?, ?, ?, ?, ?> shardInstance = instantiate(desc.shardClass());
                ShardDispatcher dispatcher = new ShardDispatcher(ctx, pool, shell, topology, shardId, shardInstance);
                @SuppressWarnings({"unchecked", "rawtypes"})
                Object initial = ((Shard) shardInstance).initState(Optional.empty(), Optional.empty());
                PartitionSlot slot =
                        new PartitionSlot(dispatcher, shardId, Optional.empty(), changeful, mailboxCapacity, policy);
                slot.state().set(initial);
                registerWithFlusher(
                        instanceId, desc, shardInstance, slot, persistenceEnabled, codec, headerTemplate, sendFrame);
                slots = List.of(slot);
                dispatchers.add(dispatcher);
            } else {
                slots = new ArrayList<>(desc.partitionKeys().size());
                Shard<?, ?, ?, ?, ?, ?> dispatcherShardInstance = instantiate(desc.shardClass());
                ShardDispatcher dispatcher =
                        new ShardDispatcher(ctx, pool, shell, topology, shardId, dispatcherShardInstance);
                dispatchers.add(dispatcher);
                for (int idx = 0; idx < desc.partitionKeys().size(); idx++) {
                    String key = desc.partitionKeys().get(idx);
                    Shard<?, ?, ?, ?, ?, ?> shardInstance = instantiate(desc.shardClass());
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    Object initial = ((Shard) shardInstance).initState(Optional.of(key), Optional.empty());
                    PartitionSlot slot = new PartitionSlot(
                            dispatcher, shardId, Optional.of(key), changeful, mailboxCapacity, policy);
                    slot.state().set(initial);
                    registerWithFlusher(
                            instanceId,
                            desc,
                            shardInstance,
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
            partitions.add(slots);
            partitionKeyToIdx.add(Map.copyOf(keyMap));
        }
        return shell;
    }

    /**
     * Register a changeful Partition with the per-process flusher so its
     * recorded Changes are framed and shipped to the controller's per-Partition
     * WAL. {@code NoChange} Shards are skipped (the flusher no-ops them too).
     * The identity is keyed by the relay INSTANCE id so two relays of the same
     * factory never collide on one Partition's WAL.
     */
    private static void registerWithFlusher(
            String instanceId,
            ShardDescriptor desc,
            Shard<?, ?, ?, ?, ?, ?> shardInstance,
            PartitionSlot slot,
            boolean persistenceEnabled,
            KryoCodec codec,
            HeaderTemplate headerTemplate,
            Function<byte[], Boolean> sendFrame) {
        if (!slot.changeful()) {
            return;
        }
        PartitionId partitionId = PartitionId.of(instanceId, desc.name(), slot.shardKey());
        FlusherEntry entry = new FlusherEntry(
                slot.changeBufferLock(),
                slot.changeBuffer(),
                persistenceEnabled,
                slot.changeSeq(),
                sendFrame,
                headerTemplate,
                partitionId,
                shardInstance.schemaVersion(),
                codec);
        PartitionFlusher.getOrStart().register(partitionId, entry, desc.changeType());
    }

    private static Shard<?, ?, ?, ?, ?, ?> instantiate(Class<? extends Shard<?, ?, ?, ?, ?, ?>> shardClass) {
        try {
            return shardClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "shard class `" + shardClass.getName() + "` must have a public no-arg constructor: " + e.toString(),
                    e);
        }
    }

    @Nonnull
    public RelayTopology topology() {
        return topology;
    }

    @Nonnull
    public String instanceId() {
        return instanceId;
    }

    @Nonnull
    public List<List<PartitionSlot>> partitions() {
        return partitions;
    }

    @Nonnull
    public List<Map<String, Integer>> partitionKeyToIdx() {
        return partitionKeyToIdx;
    }

    public ShardDispatcher dispatcher(int shardId) {
        if (shardId < 0 || shardId >= dispatchers.size()) {
            return null;
        }
        return dispatchers.get(shardId);
    }

    /** Resolved persistence toggle, frozen at construction time. */
    public boolean persistenceEnabled() {
        return persistenceEnabled;
    }

    /** Aggregated Kryo type list across all Shards in this Relay class. */
    @Nonnull
    public List<Class<?>> kryoTypes() {
        return kryoTypes;
    }
}
