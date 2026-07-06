package com.flezha.sdk.runtime.operative;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Frozen, immutable per-UnboundedOperative-class topology produced by
 * {@link com.flezha.sdk.runtime.operative.builder.UnboundedOperativeTopologyBuilder} at
 * {@link com.flezha.sdk.operative.UnboundedOperativeRegistry#registerUnbounded(Class) registerUnbounded}
 * time. Shared across every {@link UnboundedOperativeInstance} of this UnboundedOperative class.
 *
 * <p>The hot path (entry-channel input → consumer-operation segment) reads
 * {@link #channels} by index after one {@link #nameToId} lookup; the
 * inter-operation {@code emit} hot path reads {@link #channels} by the
 * pre-cached {@code outputChannelId} stamped on the {@code OperationDispatcher}
 * at build time.
 */
public final class UnboundedOperativeTopology {

    private final String factoryId;
    private final List<ChannelDescriptor> channels;
    private final Map<String, Integer> nameToId;
    private final List<OperationDescriptor> operations;
    private final Map<String, Integer> operationNameToId;

    public UnboundedOperativeTopology(
            @Nonnull String factoryId,
            @Nonnull List<ChannelDescriptor> channels,
            @Nonnull Map<String, Integer> nameToId,
            @Nonnull List<OperationDescriptor> operations,
            @Nonnull Map<String, Integer> operationNameToId) {
        this.factoryId = factoryId;
        this.channels = List.copyOf(channels);
        this.nameToId = Map.copyOf(nameToId);
        this.operations = List.copyOf(operations);
        this.operationNameToId = Map.copyOf(operationNameToId);
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
    public List<OperationDescriptor> operations() {
        return operations;
    }

    @Nonnull
    public Map<String, Integer> operationNameToId() {
        return operationNameToId;
    }

    public ChannelDescriptor channel(int id) {
        if (id < 0 || id >= channels.size()) {
            return null;
        }
        return channels.get(id);
    }

    public OperationDescriptor operation(int id) {
        if (id < 0 || id >= operations.size()) {
            return null;
        }
        return operations.get(id);
    }

    public Integer lookupChannelId(@Nonnull String name) {
        return nameToId.get(name);
    }
}
