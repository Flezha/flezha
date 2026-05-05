package com.flezha.sdk.runtime.relay;

import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.partition.PartitionSlot;
import com.flezha.sdk.runtime.worker.ShardDispatcher;
import com.flezha.sdk.shard.Shard;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;

/**
 * Per-{@code AddProcessor} runtime state. Holds:
 * <ul>
 *   <li>the shared {@link RelayTopology} (shared across all instances of
 *       this Relay class),</li>
 *   <li>the agent-facing {@code instanceId} (= what the wire calls
 *       {@code processor_id}),</li>
 *   <li>per-Shard partition slots (one inner-list entry per Partition;
 *       single-mode Shards have inner length 1),</li>
 *   <li>per-Shard reverse index from partition key → partition index
 *       (empty for single-mode Shards).</li>
 * </ul>
 */
public final class RelayInstance {

    private final RelayTopology topology;
    private final String instanceId;
    private final List<List<PartitionSlot>> partitions;
    private final List<Map<String, Integer>> partitionKeyToIdx;
    private final List<ShardDispatcher> dispatchers;

    private RelayInstance(
            RelayTopology topology,
            String instanceId,
            List<List<PartitionSlot>> partitions,
            List<Map<String, Integer>> partitionKeyToIdx,
            List<ShardDispatcher> dispatchers) {
        this.topology = topology;
        this.instanceId = instanceId;
        this.partitions = partitions;
        this.partitionKeyToIdx = partitionKeyToIdx;
        this.dispatchers = dispatchers;
    }

    /**
     * Construct a fresh {@link RelayInstance}. Calls
     * {@link Shard#initState(Optional)} for each Shard×Partition; instantiates
     * one {@link Shard} instance per Partition (Open Question 3 in
     * {@code design.md}: per-Partition Shard instance enables per-partition
     * scratch space).
     */
    @Nonnull
    public static RelayInstance create(
            @Nonnull RelayTopology topology,
            @Nonnull String instanceId,
            @Nonnull ComputeEngineContext ctx,
            @Nonnull ExecutorService pool) {
        int shardCount = topology.shards().size();
        List<List<PartitionSlot>> partitions = new ArrayList<>(shardCount);
        List<Map<String, Integer>> partitionKeyToIdx = new ArrayList<>(shardCount);
        List<ShardDispatcher> dispatchers = new ArrayList<>(shardCount);

        // Two-pass init: build the dispatchers once we have an instance
        // reference. We allocate the empty RelayInstance shell first to
        // satisfy the WeakReference each ShardDispatcher needs for emit
        // dispatch.
        RelayInstance shell = new RelayInstance(
                topology,
                instanceId,
                partitions,
                partitionKeyToIdx,
                dispatchers);

        for (int shardId = 0; shardId < shardCount; shardId++) {
            ShardDescriptor desc = topology.shards().get(shardId);
            List<PartitionSlot> slots;
            Map<String, Integer> keyMap = new HashMap<>();

            if (desc.partitionKeys().isEmpty()) {
                Shard<?, ?, ?> shardInstance = instantiate(desc.shardClass());
                ShardDispatcher dispatcher = new ShardDispatcher(
                        ctx, pool, shell, topology, shardId, shardInstance);
                @SuppressWarnings({"unchecked", "rawtypes"})
                Object initial = ((Shard) shardInstance).initState(Optional.empty());
                PartitionSlot slot = new PartitionSlot(dispatcher, shardId, Optional.empty());
                slot.state().set(initial);
                slots = List.of(slot);
                dispatchers.add(dispatcher);
            } else {
                slots = new ArrayList<>(desc.partitionKeys().size());
                Shard<?, ?, ?> dispatcherShardInstance = instantiate(desc.shardClass());
                ShardDispatcher dispatcher = new ShardDispatcher(
                        ctx, pool, shell, topology, shardId, dispatcherShardInstance);
                dispatchers.add(dispatcher);
                for (int idx = 0; idx < desc.partitionKeys().size(); idx++) {
                    String key = desc.partitionKeys().get(idx);
                    Shard<?, ?, ?> shardInstance = instantiate(desc.shardClass());
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    Object initial = ((Shard) shardInstance).initState(Optional.of(key));
                    PartitionSlot slot = new PartitionSlot(dispatcher, shardId, Optional.of(key));
                    slot.state().set(initial);
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

    private static Shard<?, ?, ?> instantiate(Class<? extends Shard<?, ?, ?>> shardClass) {
        try {
            return shardClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "shard class `" + shardClass.getName()
                            + "` must have a public no-arg constructor: " + e.toString(), e);
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
}
