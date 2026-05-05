package com.flezha.sdk.runtime.relay;

import com.flezha.sdk.channel.ChannelKind;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Frozen runtime descriptor for one channel inside a {@link RelayTopology}.
 * Indexed by interned channel id (the position in {@link RelayTopology#channels}).
 */
public final class ChannelDescriptor {

    private final String name;
    private final ChannelKind kind;
    private final OptionalInt producer;
    private final List<Integer> consumers;
    private final Optional<String> pluginName;
    private final Class<?> payloadType;
    private final Optional<Function<Object, byte[]>> pluginSerializer;

    public ChannelDescriptor(
            @Nonnull String name,
            @Nonnull ChannelKind kind,
            @Nonnull OptionalInt producer,
            @Nonnull List<Integer> consumers,
            @Nonnull Optional<String> pluginName,
            @Nonnull Class<?> payloadType,
            @Nonnull Optional<Function<Object, byte[]>> pluginSerializer) {
        this.name = name;
        this.kind = kind;
        this.producer = producer;
        this.consumers = List.copyOf(consumers);
        this.pluginName = pluginName;
        this.payloadType = payloadType;
        this.pluginSerializer = pluginSerializer;
    }

    @Nonnull
    public String name() {
        return name;
    }

    @Nonnull
    public ChannelKind kind() {
        return kind;
    }

    @Nonnull
    public OptionalInt producer() {
        return producer;
    }

    @Nonnull
    public List<Integer> consumers() {
        return consumers;
    }

    @Nonnull
    public Optional<String> pluginName() {
        return pluginName;
    }

    @Nonnull
    public Class<?> payloadType() {
        return payloadType;
    }

    @Nonnull
    public Optional<Function<Object, byte[]>> pluginSerializer() {
        return pluginSerializer;
    }
}
