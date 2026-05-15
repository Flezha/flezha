package com.flezha.sdk.runtime.relay;

import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.Clock;
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
    private final Class<? extends Shard<?, ?, ?, ?, ?, ?>> shardClass;
    private final List<String> partitionKeys;
    private final boolean partitionForOverridden;
    private final boolean shardKeyOverridden;
    private final OptionalInt inputChannelId;
    private final OptionalInt outputChannelId;
    private final Class<?> inputType;
    private final Class<?> outputType;
    private final Class<? extends Change> changeType;
    private final Class<? extends Clock> clockType;
    private final Class<?> configType;
    private final List<Class<?>> kryoTypes;
    private final int schemaVersion;
    private final long compactAfterBytes;
    private final int mailboxCapacity;
    private final boolean mailboxCapacityOverridden;

    public ShardDescriptor(
            @Nonnull String name,
            @Nonnull Class<? extends Shard<?, ?, ?, ?, ?, ?>> shardClass,
            @Nonnull List<String> partitionKeys,
            boolean partitionForOverridden,
            boolean shardKeyOverridden,
            @Nonnull OptionalInt inputChannelId,
            @Nonnull OptionalInt outputChannelId,
            @Nonnull Class<?> inputType,
            @Nonnull Class<?> outputType,
            @Nonnull Class<? extends Change> changeType,
            @Nonnull Class<? extends Clock> clockType,
            @Nonnull Class<?> configType,
            @Nonnull List<Class<?>> kryoTypes,
            int schemaVersion,
            long compactAfterBytes,
            int mailboxCapacity,
            boolean mailboxCapacityOverridden) {
        this.name = name;
        this.shardClass = shardClass;
        this.partitionKeys = List.copyOf(partitionKeys);
        this.partitionForOverridden = partitionForOverridden;
        this.shardKeyOverridden = shardKeyOverridden;
        this.inputChannelId = inputChannelId;
        this.outputChannelId = outputChannelId;
        this.inputType = inputType;
        this.outputType = outputType;
        this.changeType = changeType;
        this.clockType = clockType;
        this.configType = configType;
        this.kryoTypes = List.copyOf(kryoTypes);
        this.schemaVersion = schemaVersion;
        this.compactAfterBytes = compactAfterBytes;
        this.mailboxCapacity = mailboxCapacity;
        this.mailboxCapacityOverridden = mailboxCapacityOverridden;
    }

    @Nonnull
    public Class<? extends Change> changeType() {
        return changeType;
    }

    @Nonnull
    public Class<? extends Clock> clockType() {
        return clockType;
    }

    @Nonnull
    public Class<?> configType() {
        return configType;
    }

    @Nonnull
    public List<Class<?>> kryoTypes() {
        return kryoTypes;
    }

    public int schemaVersion() {
        return schemaVersion;
    }

    public long compactAfterBytes() {
        return compactAfterBytes;
    }

    @Nonnull
    public String name() {
        return name;
    }

    @Nonnull
    public Class<? extends Shard<?, ?, ?, ?, ?, ?>> shardClass() {
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

    /** Resolved per-Partition mailbox capacity (Shard override → env var → default). */
    public int mailboxCapacity() {
        return mailboxCapacity;
    }

    /** Whether the Shard explicitly overrode {@code mailboxCapacity()}. */
    public boolean mailboxCapacityOverridden() {
        return mailboxCapacityOverridden;
    }
}
