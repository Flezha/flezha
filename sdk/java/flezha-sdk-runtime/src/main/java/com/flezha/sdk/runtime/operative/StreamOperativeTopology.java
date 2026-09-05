package com.flezha.sdk.runtime.operative;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Frozen, immutable per-StreamOperative-class topology produced by
 * {@link com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder} at
 * {@code FlezhaRunner.defineStream(Class)}
 * time. Shared across every {@link StreamOperativeInstance} of this StreamOperative class.
 *
 * <p>The hot path (entry-channel input → consumer-procedure entity) reads
 * {@link #channels} by index after one {@link #nameToId} lookup; the
 * inter-procedure {@code emit} hot path reads {@link #channels} by the
 * pre-cached {@code outputChannelId} stamped on the {@code ProcedureDispatcher}
 * at build time.
 */
public final class StreamOperativeTopology {

    private final String factoryId;
    private final List<ChannelDescriptor> channels;
    private final Map<String, Integer> nameToId;
    private final List<ProcedureDescriptor> procedures;
    private final Map<String, Integer> procedureNameToId;
    private final boolean effectivelyOnce;
    private final Map<String, Object> declaration;

    public StreamOperativeTopology(
            @Nonnull String factoryId,
            @Nonnull List<ChannelDescriptor> channels,
            @Nonnull Map<String, Integer> nameToId,
            @Nonnull List<ProcedureDescriptor> procedures,
            @Nonnull Map<String, Integer> procedureNameToId,
            boolean effectivelyOnce,
            @Nonnull Map<String, Object> declaration) {
        this.effectivelyOnce = effectivelyOnce;
        this.declaration = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(declaration));
        this.factoryId = factoryId;
        this.channels = List.copyOf(channels);
        this.nameToId = Map.copyOf(nameToId);
        this.procedures = List.copyOf(procedures);
        this.procedureNameToId = Map.copyOf(procedureNameToId);
    }

    /**
     * Whether this Operative opted into the effectively-once output tier. Rides
     * the {@code effectively_once} key of the declaration map, which is how the
     * controller learns to run the tier for it.
     */
    public boolean effectivelyOnce() {
        return effectivelyOnce;
    }

    /**
     * The {@code HostClassDecl} record this class was built from — what the
     * builder was told, submitted to the core's one check and re-sent
     * verbatim at declare time. Written once, so the bytes the core judged
     * and the bytes it registers cannot drift.
     */
    @Nonnull
    public Map<String, Object> declaration() {
        return declaration;
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
    public List<ProcedureDescriptor> procedures() {
        return procedures;
    }

    @Nonnull
    public Map<String, Integer> procedureNameToId() {
        return procedureNameToId;
    }

    public ChannelDescriptor channel(int id) {
        if (id < 0 || id >= channels.size()) {
            return null;
        }
        return channels.get(id);
    }

    public ProcedureDescriptor procedure(int id) {
        if (id < 0 || id >= procedures.size()) {
            return null;
        }
        return procedures.get(id);
    }

    public Integer lookupChannelId(@Nonnull String name) {
        return nameToId.get(name);
    }
}
