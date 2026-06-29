package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.Clock;
import java.util.List;
import java.util.OptionalInt;
import javax.annotation.Nonnull;

/**
 * Frozen runtime descriptor for one Operation inside a {@link ContinuousOperativeTopology}.
 *
 * <p>Carries the Operation's class so the runtime can instantiate one Operation per
 * Segment at operative-instance startup, plus the resolved input/output
 * channel ids (cached at registration time so the hot path needs zero name
 * lookups).
 */
public final class OperationDescriptor {

    private final String name;
    private final Class<? extends Operation<?, ?, ?, ?, ?, ?>> operationClass;
    private final List<String> segmentKeys;
    private final boolean segmentForOverridden;
    private final boolean operationKeyOverridden;
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

    public OperationDescriptor(
            @Nonnull String name,
            @Nonnull Class<? extends Operation<?, ?, ?, ?, ?, ?>> operationClass,
            @Nonnull List<String> segmentKeys,
            boolean segmentForOverridden,
            boolean operationKeyOverridden,
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
        this.operationClass = operationClass;
        this.segmentKeys = List.copyOf(segmentKeys);
        this.segmentForOverridden = segmentForOverridden;
        this.operationKeyOverridden = operationKeyOverridden;
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
    public Class<? extends Operation<?, ?, ?, ?, ?, ?>> operationClass() {
        return operationClass;
    }

    @Nonnull
    public List<String> segmentKeys() {
        return segmentKeys;
    }

    public boolean segmentForOverridden() {
        return segmentForOverridden;
    }

    public boolean operationKeyOverridden() {
        return operationKeyOverridden;
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

    /** Resolved per-Segment mailbox capacity (Operation override → env var → default). */
    public int mailboxCapacity() {
        return mailboxCapacity;
    }

    /** Whether the Operation explicitly overrode {@code mailboxCapacity()}. */
    public boolean mailboxCapacityOverridden() {
        return mailboxCapacityOverridden;
    }
}
