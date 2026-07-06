package com.flezha.sdk.channel;

import com.flezha.sdk.operation.Operation;
import javax.annotation.Nonnull;

/**
 * Inter-operation or plugin-bound channel awaiting a producer. {@code .from(...)}
 * returns a {@link ChannelDeclWithProducer} which has no further
 * {@code .from} method, so multi-producer is a compile error.
 */
public interface ChannelDecl<T> {

    /**
     * Wire the producer Operation. The runtime verifies at
     * {@code registerUnbounded} time that the Operation's
     * {@link Operation#outputType()} equals {@code T}; a mismatch throws
     * {@link com.flezha.sdk.operative.UnboundedOperativeConfigurationException} naming both
     * types and the channel.
     */
    @Nonnull
    ChannelDeclWithProducer<T> from(@Nonnull Class<? extends Operation<?, ?, T, ?, ?, ?>> operation);

    /**
     * Override the per-kind default {@link OverflowPolicy} before wiring
     * the producer. Inter-operation channels default to
     * {@link OverflowPolicy#BLOCK}; switch to {@link OverflowPolicy#DROP_NEWEST}
     * or {@link OverflowPolicy#DROP_OLDEST} on a per-channel basis when
     * ordering can be sacrificed for liveness.
     */
    @Nonnull
    ChannelDecl<T> onFull(@Nonnull OverflowPolicy policy);
}
