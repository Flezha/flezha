package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.channel.RetryPolicy;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Frozen runtime descriptor for one channel inside a {@link UnboundedOperativeTopology}.
 * Indexed by interned channel id (the position in {@link UnboundedOperativeTopology#channels}).
 */
public final class ChannelDescriptor {

    private final String name;
    private final ChannelKind kind;
    private final OptionalInt producer;
    private final List<Integer> consumers;
    private final Optional<String> pluginName;
    private final Class<?> payloadType;
    private final Optional<Function<Object, byte[]>> pluginSerializer;
    private final Optional<PluginSink> pluginSink;
    private final OverflowPolicy overflowPolicy;
    private final RetryPolicy retry;

    public ChannelDescriptor(
            @Nonnull String name,
            @Nonnull ChannelKind kind,
            @Nonnull OptionalInt producer,
            @Nonnull List<Integer> consumers,
            @Nonnull Optional<String> pluginName,
            @Nonnull Class<?> payloadType,
            @Nonnull Optional<Function<Object, byte[]>> pluginSerializer,
            @Nonnull Optional<PluginSink> pluginSink,
            @Nonnull OverflowPolicy overflowPolicy,
            @Nonnull RetryPolicy retry) {
        this.name = name;
        this.kind = kind;
        this.producer = producer;
        this.consumers = List.copyOf(consumers);
        this.pluginName = pluginName;
        this.payloadType = payloadType;
        this.pluginSerializer = pluginSerializer;
        this.pluginSink = pluginSink;
        this.overflowPolicy = overflowPolicy;
        this.retry = retry;
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

    /**
     * The parallel plugin sink, if this channel was declared with
     * {@code alsoToPluginWith(...)}. Pure {@link ChannelKind#PLUGIN_BOUND}
     * channels (the closed-form {@code toPlugin(...)} path) leave this
     * empty and use {@link #pluginName()} / {@link #pluginSerializer()} instead.
     */
    @Nonnull
    public Optional<PluginSink> pluginSink() {
        return pluginSink;
    }

    /**
     * Per-channel backpressure policy applied at the producer-side push
     * site when a consumer Segment's bounded mailbox is full.
     */
    @Nonnull
    public OverflowPolicy overflowPolicy() {
        return overflowPolicy;
    }

    /**
     * Retry-then-park policy applied by this channel's consumer dispatchers.
     * Plugin-bound channels have no SDK-side consumer and ignore this value.
     */
    @Nonnull
    public RetryPolicy retry() {
        return retry;
    }
}
