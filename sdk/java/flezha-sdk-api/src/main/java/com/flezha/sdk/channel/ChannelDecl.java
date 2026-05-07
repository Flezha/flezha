package com.flezha.sdk.channel;

import com.flezha.sdk.shard.Shard;
import javax.annotation.Nonnull;

/**
 * Inter-shard or plugin-bound channel awaiting a producer. {@code .from(...)}
 * returns a {@link ChannelDeclWithProducer} which has no further
 * {@code .from} method, so multi-producer is a compile error.
 */
public interface ChannelDecl<T> {

    /**
     * Wire the producer Shard. The runtime verifies at
     * {@code registerRelay} time that the Shard's
     * {@link Shard#outputType()} equals {@code T}; a mismatch throws
     * {@link com.flezha.sdk.relay.RelayConfigurationException} naming both
     * types and the channel.
     */
    @Nonnull ChannelDeclWithProducer<T> from(@Nonnull Class<? extends Shard<?, ?, T, ?, ?, ?>> shard);
}
