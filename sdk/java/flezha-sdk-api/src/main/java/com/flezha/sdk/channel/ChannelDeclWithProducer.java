package com.flezha.sdk.channel;

import com.flezha.sdk.operation.Operation;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Channel with a producer wired. Accepts zero or more {@code .into(...)}
 * inter-operation consumers, OR exactly one {@code .toPlugin(...)} plugin
 * destination. A hybrid channel (inter-operation fan-out plus a parallel
 * plugin sink) is declared with {@link #alsoToPluginWith(String, Function)}.
 * Mixing the closed-form {@code .toPlugin(...)} call with inter-operation
 * consumers on the same channel is still rejected at build-topology time.
 */
public interface ChannelDeclWithProducer<T> {

    /**
     * Wire an inter-operation consumer. Calling {@code .into} repeatedly is
     * allowed (fan-out). The runtime verifies at {@code registerContinuous} time
     * that the consumer Operation's {@link Operation#inputType()} equals {@code T}.
     */
    @Nonnull
    ChannelDeclWithProducer<T> into(@Nonnull Class<? extends Operation<T, ?, ?, ?, ?, ?>> operation);

    /**
     * Mark this channel as plugin-bound. The controller's plugin layer receives
     * the values. {@code byte[]} and {@code String} payloads use a
     * passthrough/UTF-8 serializer; other payload types are serialized with
     * Jackson JSON by default.
     */
    @Nonnull
    Channel<T> toPlugin(@Nonnull String pluginName);

    /** Plugin-bound destination with an explicit serializer override. */
    @Nonnull
    Channel<T> toPlugin(@Nonnull String pluginName, @Nonnull Function<T, byte[]> serializer);

    /**
     * Attach a parallel plugin sink to an inter-operation channel: typed values
     * fan out to {@code .into(...)} consumers as usual, and a serialized copy
     * is published to the named plugin. The serializer is invoked exactly
     * once per {@code ctx.emit(...)} regardless of consumer count, after
     * consumer fan-out completes.
     *
     * <p>A serializer that throws is caught ({@code RuntimeException} or
     * {@code Error}) and surfaced to the producing Operation as
     * {@link com.flezha.sdk.operation.OperationException.PluginSerializerFailure};
     * the dispatcher thread stays alive, and inter-operation consumers have
     * already received the typed value.
     *
     * <p>Calling {@code alsoToPluginWith} more than once on the same channel
     * is rejected at {@code registerContinuous(...)} time with
     * {@link com.flezha.sdk.operative.ContinuousOperativeConfigurationException#duplicatePluginSink(String)}.
     *
     * <p>Returns {@code this} so further {@code .into(...)} wiring chains
     * naturally; the call order of {@code alsoToPluginWith} and {@code into}
     * is irrelevant.
     */
    @Nonnull
    ChannelDeclWithProducer<T> alsoToPluginWith(@Nonnull String pluginName, @Nonnull Function<T, byte[]> serializer);

    /**
     * Override the per-kind default {@link OverflowPolicy}. Available only
     * before {@link #toPlugin(String)}; the {@code Channel<T>} returned by
     * {@code toPlugin} has no further methods, so plugin-bound declarations
     * cannot redeclare a policy. Inter-operation channels default to
     * {@link OverflowPolicy#BLOCK}.
     */
    @Nonnull
    ChannelDeclWithProducer<T> onFull(@Nonnull OverflowPolicy policy);
}
