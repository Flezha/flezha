package com.flezha.sdk.runtime.relay;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Frozen, immutable per-Relay-class topology produced by
 * {@link com.flezha.sdk.runtime.relay.builder.RelayTopologyBuilder} at
 * {@link com.flezha.sdk.relay.RelayRegistry#registerRelay(Class) registerRelay}
 * time. Shared across every {@link RelayInstance} of this Relay class.
 *
 * <p>The hot path (entry-channel input → consumer-shard partition) reads
 * {@link #channels} by index after one {@link #nameToId} lookup; the
 * inter-shard {@code emit} hot path reads {@link #channels} by the
 * pre-cached {@code outputChannelId} stamped on the {@code ShardDispatcher}
 * at build time.
 */
public final class RelayTopology {

    private final String factoryId;
    private final List<ChannelDescriptor> channels;
    private final Map<String, Integer> nameToId;
    private final List<ShardDescriptor> shards;
    private final Map<String, Integer> shardNameToId;

    public RelayTopology(
            @Nonnull String factoryId,
            @Nonnull List<ChannelDescriptor> channels,
            @Nonnull Map<String, Integer> nameToId,
            @Nonnull List<ShardDescriptor> shards,
            @Nonnull Map<String, Integer> shardNameToId) {
        this.factoryId = factoryId;
        this.channels = List.copyOf(channels);
        this.nameToId = Map.copyOf(nameToId);
        this.shards = List.copyOf(shards);
        this.shardNameToId = Map.copyOf(shardNameToId);
    }

    @Nonnull
    public String factoryId() {
        return factoryId;
    }

    @Nonnull
    public List<ChannelDescriptor> channels() {
        return channels;
    }

    @Nonnull
    public Map<String, Integer> nameToId() {
        return nameToId;
    }

    @Nonnull
    public List<ShardDescriptor> shards() {
        return shards;
    }

    @Nonnull
    public Map<String, Integer> shardNameToId() {
        return shardNameToId;
    }

    public ChannelDescriptor channel(int id) {
        if (id < 0 || id >= channels.size()) {
            return null;
        }
        return channels.get(id);
    }

    public ShardDescriptor shard(int id) {
        if (id < 0 || id >= shards.size()) {
            return null;
        }
        return shards.get(id);
    }

    public Integer lookupChannelId(@Nonnull String name) {
        return nameToId.get(name);
    }
}
