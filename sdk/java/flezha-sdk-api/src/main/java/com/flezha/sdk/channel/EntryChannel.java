package com.flezha.sdk.channel;

import com.flezha.sdk.shard.Shard;
import javax.annotation.Nonnull;

/**
 * Entry-channel declaration. Only exposes {@link #into(Class)} to wire a
 * consumer Shard; entry channels are fed by the controller, so there is no
 * {@code .from(...)} on this type.
 */
public interface EntryChannel<T> {

    /**
     * Wire the entry-shard consumer. The runtime verifies at
     * {@code registerRelay} time that the Shard's {@link Shard#inputType()}
     * equals {@code T}; a mismatch throws
     * {@link com.flezha.sdk.relay.RelayConfigurationException} naming both
     * types and the channel.
     *
     * <p>The parametric bound {@code Class<? extends Shard<T, ?, ?, ?, ?, ?>>}
     * enforces the basic input-type match at compile time. Java's type
     * erasure means the runtime still has to verify the Shard's reflective
     * {@link Shard#inputType()} accessor agrees.
     */
    @Nonnull
    Channel<T> into(@Nonnull Class<? extends Shard<T, ?, ?, ?, ?, ?>> shard);

    /**
     * Override the per-kind default {@link OverflowPolicy}. Entry channels
     * default to {@link OverflowPolicy#DROP_NEWEST}; declare
     * {@link OverflowPolicy#BLOCK} here only if you accept stalling the
     * shared wire reader on this Relay's saturation.
     */
    @Nonnull
    EntryChannel<T> onFull(@Nonnull OverflowPolicy policy);
}
