package com.flezha.sdk.channel;

import javax.annotation.Nonnull;

/**
 * Fluent builder consumed by {@link com.flezha.sdk.operative.ContinuousOperative#channels(ChannelBuilder)}.
 * Declarations land in a per-ContinuousOperative topology that the runtime freezes at
 * {@code registerContinuous} time.
 */
public interface ChannelBuilder {

    /**
     * Declare an entry channel fed by the controller. {@code T} must be
     * {@code byte[]} or {@code String}; other types throw
     * {@link com.flezha.sdk.operative.ContinuousOperativeConfigurationException} at
     * {@code registerContinuous} time (umbrella Decision 12).
     *
     * <p>The returned {@link EntryChannel} only exposes {@code .into(...)}
     * for wiring an entry-operation consumer.
     */
    @Nonnull
    <T> EntryChannel<T> input(@Nonnull Class<T> type, @Nonnull String name);

    /**
     * Declare an inter-operation or plugin-bound channel. The returned
     * {@link ChannelDecl} requires {@code .from(...)} to identify a producer
     * Operation; after {@code .from}, it requires either {@code .into(...)}
     * (inter-operation) or {@code .toPlugin(...)} (plugin-bound). A channel
     * with neither is rejected at build-topology time with
     * {@link com.flezha.sdk.operative.ContinuousOperativeConfigurationException}.
     */
    @Nonnull
    <T> ChannelDecl<T> channel(@Nonnull Class<T> type, @Nonnull String name);
}
