package com.flezha.sdk.channel;

import com.flezha.sdk.shard.Shard;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Channel with a producer wired. Accepts zero or more {@code .into(...)}
 * inter-shard consumers, OR exactly one {@code .toPlugin(...)} plugin
 * destination. Mixing inter-shard and plugin-bound usages of the same
 * channel is rejected at build-topology time.
 */
public interface ChannelDeclWithProducer<T> {

    /**
     * Wire an inter-shard consumer. Calling {@code .into} repeatedly is
     * allowed (fan-out). The runtime verifies at {@code registerRelay} time
     * that the consumer Shard's {@link Shard#inputType()} equals {@code T}.
     */
    @Nonnull ChannelDeclWithProducer<T> into(@Nonnull Class<? extends Shard<T, ?, ?, ?, ?, ?>> shard);

    /**
     * Mark this channel as plugin-bound. The agent's plugin layer receives
     * the values. {@code byte[]} and {@code String} payloads use a
     * passthrough/UTF-8 serializer; other payload types are serialized with
     * Jackson JSON by default.
     */
    @Nonnull Channel<T> toPlugin(@Nonnull String pluginName);

    /** Plugin-bound destination with an explicit serializer override. */
    @Nonnull Channel<T> toPlugin(@Nonnull String pluginName, @Nonnull Function<T, byte[]> serializer);
}
