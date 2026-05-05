package com.flezha.sdk.runtime.relay;

import com.flezha.sdk.shard.Shard;
import java.util.List;
import java.util.OptionalInt;
import javax.annotation.Nonnull;

/**
 * Frozen runtime descriptor for one Shard inside a {@link RelayTopology}.
 *
 * <p>Carries the Shard's class so the runtime can instantiate one Shard per
 * Partition at relay-instance startup, plus the resolved input/output
 * channel ids (cached at registration time so the hot path needs zero name
 * lookups).
 */
public final class ShardDescriptor {

    private final String name;
    private final Class<? extends Shard<?, ?, ?>> shardClass;
    private final List<String> partitionKeys;
    private final boolean partitionForOverridden;
    private final boolean shardKeyOverridden;
    private final OptionalInt inputChannelId;
    private final OptionalInt outputChannelId;
    private final Class<?> inputType;
    private final Class<?> outputType;

    public ShardDescriptor(
            @Nonnull String name,
            @Nonnull Class<? extends Shard<?, ?, ?>> shardClass,
            @Nonnull List<String> partitionKeys,
            boolean partitionForOverridden,
            boolean shardKeyOverridden,
            @Nonnull OptionalInt inputChannelId,
            @Nonnull OptionalInt outputChannelId,
            @Nonnull Class<?> inputType,
            @Nonnull Class<?> outputType) {
        this.name = name;
        this.shardClass = shardClass;
        this.partitionKeys = List.copyOf(partitionKeys);
        this.partitionForOverridden = partitionForOverridden;
        this.shardKeyOverridden = shardKeyOverridden;
        this.inputChannelId = inputChannelId;
        this.outputChannelId = outputChannelId;
        this.inputType = inputType;
        this.outputType = outputType;
    }

    @Nonnull
    public String name() {
        return name;
    }

    @Nonnull
    public Class<? extends Shard<?, ?, ?>> shardClass() {
        return shardClass;
    }

    @Nonnull
    public List<String> partitionKeys() {
        return partitionKeys;
    }

    public boolean partitionForOverridden() {
        return partitionForOverridden;
    }

    public boolean shardKeyOverridden() {
        return shardKeyOverridden;
    }

    @Nonnull
    public OptionalInt inputChannelId() {
        return inputChannelId;
    }

    @Nonnull
    public OptionalInt outputChannelId() {
        return outputChannelId;
    }

    @Nonnull
    public Class<?> inputType() {
        return inputType;
    }

    @Nonnull
    public Class<?> outputType() {
        return outputType;
    }
}
