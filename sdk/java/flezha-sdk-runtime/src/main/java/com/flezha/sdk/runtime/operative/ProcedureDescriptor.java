package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.Clock;
import java.util.List;
import java.util.OptionalInt;
import javax.annotation.Nonnull;

/**
 * Frozen runtime descriptor for one Procedure inside a {@link StreamOperativeTopology}.
 *
 * <p>Carries the Procedure's class so the runtime can instantiate one Procedure per
 * Entity at operative-instance startup, plus the resolved input/output
 * channel ids (cached at registration time so the hot path needs zero name
 * lookups).
 */
public final class ProcedureDescriptor {

    private final String name;
    private final Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass;
    private final List<String> entityKeys;
    private final boolean entitiesForOverridden;
    private final boolean dynamic;
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
    private final boolean compensateOverridden;
    private final java.util.Optional<java.time.Duration> entityIdleTtl;

    private final java.util.OptionalLong maxEntities;

    public ProcedureDescriptor(
            @Nonnull String name,
            @Nonnull Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass,
            @Nonnull List<String> entityKeys,
            boolean entitiesForOverridden,
            boolean dynamic,
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
            boolean mailboxCapacityOverridden,
            boolean compensateOverridden,
            @Nonnull java.util.Optional<java.time.Duration> entityIdleTtl,
            @Nonnull java.util.OptionalLong maxEntities) {
        this.entityIdleTtl = entityIdleTtl;
        this.maxEntities = maxEntities;
        this.compensateOverridden = compensateOverridden;
        this.name = name;
        this.procedureClass = procedureClass;
        this.entityKeys = List.copyOf(entityKeys);
        this.entitiesForOverridden = entitiesForOverridden;
        this.dynamic = dynamic;
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
    public Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass() {
        return procedureClass;
    }

    @Nonnull
    public List<String> entityKeys() {
        return entityKeys;
    }

    public boolean entitiesForOverridden() {
        return entitiesForOverridden;
    }

    /**
     * Whether the Procedure declares a {@code compensate} hook. Read by the
     * declaration map's {@code compensable_procs}: the controller refuses to
     * rewind a procedure whose plugin output is not semantically undoable, so
     * it needs this before it wakes the instance.
     */
    public boolean compensateOverridden() {
        return compensateOverridden;
    }

    /** Idle TTL for a dynamic Entity; empty = eviction off. */
    @Nonnull
    public java.util.Optional<java.time.Duration> entityIdleTtl() {
        return entityIdleTtl;
    }

    /** The declared dynamic-mode Entity cap, absent when the class takes the default. */
    public java.util.OptionalLong maxEntities() {
        return maxEntities;
    }

    /**
     * Whether some consumer edge into this node declares a {@code keyBy} —
     * dynamic Entity mode is read off the WIRING, not off the procedure.
     */
    public boolean dynamic() {
        return dynamic;
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

    /** Resolved per-Entity mailbox capacity (Procedure override → env var → default). */
    public int mailboxCapacity() {
        return mailboxCapacity;
    }

    /** Whether the Procedure explicitly overrode {@code mailboxCapacity()}. */
    public boolean mailboxCapacityOverridden() {
        return mailboxCapacityOverridden;
    }
}
