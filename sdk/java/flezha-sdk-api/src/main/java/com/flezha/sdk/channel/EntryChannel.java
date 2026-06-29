package com.flezha.sdk.channel;

import com.flezha.sdk.operation.Operation;
import javax.annotation.Nonnull;

/**
 * Entry-channel declaration. Only exposes {@link #into(Class)} to wire a
 * consumer Operation; entry channels are fed by the controller, so there is no
 * {@code .from(...)} on this type.
 */
public interface EntryChannel<T> {

    /**
     * Wire the entry-operation consumer. The runtime verifies at
     * {@code registerContinuous} time that the Operation's {@link Operation#inputType()}
     * equals {@code T}; a mismatch throws
     * {@link com.flezha.sdk.operative.ContinuousOperativeConfigurationException} naming both
     * types and the channel.
     *
     * <p>The parametric bound {@code Class<? extends Operation<T, ?, ?, ?, ?, ?>>}
     * enforces the basic input-type match at compile time. Java's type
     * erasure means the runtime still has to verify the Operation's reflective
     * {@link Operation#inputType()} accessor agrees.
     */
    @Nonnull
    Channel<T> into(@Nonnull Class<? extends Operation<T, ?, ?, ?, ?, ?>> operation);

    /**
     * Override the per-kind default {@link OverflowPolicy}. Entry channels
     * default to {@link OverflowPolicy#DROP_NEWEST}; declare
     * {@link OverflowPolicy#BLOCK} here only if you accept stalling the
     * shared wire reader on this ContinuousOperative's saturation.
     */
    @Nonnull
    EntryChannel<T> onFull(@Nonnull OverflowPolicy policy);
}
