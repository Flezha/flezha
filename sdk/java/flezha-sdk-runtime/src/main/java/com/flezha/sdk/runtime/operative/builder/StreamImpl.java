package com.flezha.sdk.runtime.operative.builder;

import com.flezha.sdk.channel.Channel;
import com.flezha.sdk.channel.EdgeStream;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.channel.Stream;
import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.ChannelEntry;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

/**
 * The procedure's output stream handle: a node id into the enclosing builder.
 * The channel out of the node is created lazily on first use.
 */
final class StreamImpl<T> extends Stream<T> implements EdgeSite {

    private final StreamOperativeTopologyBuilder owner;
    private final int node;

    /**
     * The shared surface, held rather than inherited: {@link Stream} is an
     * abstract class (so {@code branch} can be {@code @SafeVarargs}) and
     * Java has no second superclass to spend, so the producer-side handle
     * forwards to the one implementation instead of re-stating it.
     */
    private final EdgeStreamImpl<T> edge;

    StreamImpl(StreamOperativeTopologyBuilder owner, int node) {
        this(owner, node, null, null);
    }

    StreamImpl(StreamOperativeTopologyBuilder owner, int node, Integer pending, Class<?> payloadType) {
        this.owner = owner;
        this.node = node;
        this.edge = new NodeEdge<>(owner, node, pending, payloadType);
    }

    private int channelId() {
        return owner.ensureOutputChannel(node);
    }

    @Override
    public int siteChannelId() {
        return channelId();
    }

    @Override
    public Integer sitePending() {
        return edge.pending;
    }

    @Override
    @Nonnull
    public <O> Stream<O> into(@Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedureClass) {
        return edge.into(procedureClass);
    }

    @Override
    @Nonnull
    public <O> Stream<O> into(
            @Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedureClass, @Nonnull String nodeName) {
        return edge.into(procedureClass, nodeName);
    }

    @Override
    @Nonnull
    public <U> EdgeStream<U> map(@Nonnull Class<U> type, @Nonnull Function<T, U> mapper) {
        return edge.map(type, mapper);
    }

    @Override
    @Nonnull
    public <U> EdgeStream<U> flatMap(@Nonnull Class<U> type, @Nonnull Function<T, ? extends Iterable<U>> mapper) {
        return edge.flatMap(type, mapper);
    }

    @Override
    @Nonnull
    public EdgeStream<T> filter(@Nonnull Predicate<T> predicate) {
        return edge.filter(predicate);
    }

    @Override
    @Nonnull
    public EdgeStream<T> keyBy(@Nonnull Function<T, String> key) {
        return edge.keyBy(key);
    }

    @Override
    @Nonnull
    protected Stream<T> branchArms(@Nonnull List<Consumer<Stream<T>>> arms) {
        owner.checkMutable();
        if (arms.size() < 2) {
            owner.error(StreamOperativeConfigurationException.branchTooFewArms(arms.size())
                    .getMessage());
        }
        for (Consumer<Stream<T>> arm : arms) {
            arm.accept(this);
        }
        return this;
    }

    @Override
    @Nonnull
    public Stream<T> named(@Nonnull String name) {
        owner.checkMutable();
        owner.channel(channelId()).name = name;
        return this;
    }

    @Override
    @Nonnull
    public Stream<T> keyed() {
        owner.checkMutable();
        owner.channel(channelId()).keyed = true;
        return this;
    }

    @Override
    @Nonnull
    public Stream<T> codec() {
        // Reuse the SDK's default JSON serialization — the same Jackson
        // mapper a plugin-bound channel falls back to.
        return codecWith(
                PayloadCodecs.defaultSerializer(payloadType()), PayloadCodecs.defaultDeserializer(payloadType()));
    }

    @Override
    @Nonnull
    public Stream<T> codecWith(@Nonnull Function<T, byte[]> encoder, @Nonnull Function<byte[], T> decoder) {
        owner.checkMutable();
        ChannelEntry entry = owner.channel(channelId());
        entry.interProcEncoder = value -> encoder.apply(cast(value));
        entry.interProcDecoder = bytes -> decoder.apply(bytes);
        return this;
    }

    @Override
    @Nonnull
    public Stream<T> onFull(@Nonnull OverflowPolicy policy) {
        edge.onFull(policy);
        return this;
    }

    @Override
    @Nonnull
    public Stream<T> retryMax(int maxAttempts) {
        edge.retryMax(maxAttempts);
        return this;
    }

    @Override
    @Nonnull
    public Stream<T> retryBackoffMs(long baseMs) {
        edge.retryBackoffMs(baseMs);
        return this;
    }

    @Override
    @Nonnull
    public Stream<T> sink(@Nonnull String pluginName, @Nonnull Function<T, byte[]> serializer) {
        owner.checkMutable();
        ChannelEntry entry = owner.channel(channelId());
        if (entry.pluginSinkName != null) {
            owner.error(StreamOperativeConfigurationException.duplicatePluginSink(entry.name)
                    .getMessage());
            return this;
        }
        entry.pluginSinkName = pluginName;
        entry.pluginSinkSerializer = value -> serializer.apply(cast(value));
        return this;
    }

    @Override
    @Nonnull
    public Stream<T> sink(@Nonnull String pluginName) {
        return sink(pluginName, PayloadCodecs.defaultSerializer(payloadType()));
    }

    @Override
    @Nonnull
    public Channel<T> sealed() {
        owner.checkMutable();
        return new Channel<>();
    }

    /** The producer node's declared output type: this stream's payload type. */
    @SuppressWarnings("unchecked")
    private Class<T> payloadType() {
        return (Class<T>) owner.procedure(node).outputType;
    }

    @SuppressWarnings("unchecked")
    private T cast(Object value) {
        return (T) value;
    }
}
