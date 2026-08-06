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
    private final List<ConsumerEdge> consumers;
    private final Optional<String> pluginName;
    private final Class<?> payloadType;
    private final Optional<Function<byte[], Object>> entryParser;
    private final boolean entryParsed;
    private final Optional<Function<Object, byte[]>> pluginSerializer;
    private final Optional<PluginSink> pluginSink;
    private final OverflowPolicy overflowPolicy;
    private final RetryPolicy retry;
    private final boolean keyed;
    private final Optional<InterProcCodec> interProcCodec;

    public ChannelDescriptor(
            @Nonnull String name,
            @Nonnull ChannelKind kind,
            @Nonnull OptionalInt producer,
            @Nonnull List<ConsumerEdge> consumers,
            @Nonnull Optional<String> pluginName,
            @Nonnull Class<?> payloadType,
            @Nonnull Optional<Function<byte[], Object>> entryParser,
            boolean entryParsed,
            @Nonnull Optional<Function<Object, byte[]>> pluginSerializer,
            @Nonnull Optional<PluginSink> pluginSink,
            @Nonnull OverflowPolicy overflowPolicy,
            @Nonnull RetryPolicy retry,
            boolean keyed,
            @Nonnull Optional<InterProcCodec> interProcCodec) {
        this.interProcCodec = interProcCodec;
        this.name = name;
        this.kind = kind;
        this.producer = producer;
        this.consumers = List.copyOf(consumers);
        this.pluginName = pluginName;
        this.payloadType = payloadType;
        this.entryParser = entryParser;
        this.entryParsed = entryParsed;
        this.pluginSerializer = pluginSerializer;
        this.pluginSink = pluginSink;
        this.overflowPolicy = overflowPolicy;
        this.retry = retry;
        this.keyed = keyed;
    }

    /**
     * Whether emits on this channel carry a natural key ({@code .keyed()}).
     * The declaration is the single source of truth: a plain emit on a keyed
     * channel is refused, and a keyed emit on a non-keyed channel likewise.
     */
    public boolean keyed() {
        return keyed;
    }

    /**
     * Codec for this inter-procedure channel's payload, when declared. The
     * bounded proc-journal encodes the handoff between two procedures with it, so a
     * crash-resumed run can re-seed the next procedure instead of re-running the
     * chain from the top.
     */
    @Nonnull
    public Optional<InterProcCodec> interProcCodec() {
        return interProcCodec;
    }

    /** Encoder/decoder pair for an inter-procedure handoff payload. */
    public record InterProcCodec(
            @Nonnull Function<Object, byte[]> encoder, @Nonnull Function<byte[], Object> decoder) {}

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

    /** Consumer subscriptions, each carrying its optional merge mapping. */
    @Nonnull
    public List<ConsumerEdge> consumers() {
        return consumers;
    }

    /**
     * The consuming procedure ids, mappings stripped — what the graph-shape
     * walk and the bounded DAG builder need.
     */
    @Nonnull
    public List<Integer> consumerIds() {
        return consumers.stream().map(ConsumerEdge::procedure).toList();
    }

    @Nonnull
    public Optional<String> pluginName() {
        return pluginName;
    }

    @Nonnull
    public Class<?> payloadType() {
        return payloadType;
    }

    /**
     * Entry channels only: the wire-bytes → typed-payload parser evaluated
     * before dispatch (passthrough for {@code byte[]}, UTF-8 for
     * {@code String}, JSON or a custom parser otherwise).
     */
    @Nonnull
    public Optional<Function<byte[], Object>> entryParser() {
        return entryParser;
    }

    /**
     * Whether the entry parser is a real one (typed JSON or a custom
     * {@code input(type, name, parser)}). A failing parse on a parsed entry
     * rides this channel's retry→DLQ ladder; {@code byte[]}/{@code String}
     * passthrough entries keep the pre-change drop-and-log behaviour.
     */
    public boolean entryParsed() {
        return entryParsed;
    }

    @Nonnull
    public Optional<Function<Object, byte[]>> pluginSerializer() {
        return pluginSerializer;
    }

    /**
     * The parallel plugin sink, if this channel was declared with
     * {@code alsoOutput(...)}. Pure {@link ChannelKind#PLUGIN_BOUND}
     * channels (the closed-form {@code output(...)} path) leave this
     * empty and use {@link #pluginName()} / {@link #pluginSerializer()} instead.
     */
    @Nonnull
    public Optional<PluginSink> pluginSink() {
        return pluginSink;
    }

    /**
     * Per-channel backpressure policy applied at the producer-side push
     * site when a consumer Entity's bounded mailbox is full.
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
