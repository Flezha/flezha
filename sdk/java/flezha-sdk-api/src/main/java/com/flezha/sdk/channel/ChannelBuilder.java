package com.flezha.sdk.channel;

import com.flezha.sdk.shard.Shard;
import javax.annotation.Nonnull;

/**
 * Fluent builder consumed by {@link com.flezha.sdk.relay.Relay#channels(ChannelBuilder)}.
 * Declarations land in a per-Relay topology that the runtime freezes at
 * {@code registerRelay} time.
 */
public interface ChannelBuilder {

    /**
     * Declare an entry channel fed by the agent. {@code T} must be
     * {@code byte[]} or {@code String}; other types throw
     * {@link com.flezha.sdk.relay.RelayConfigurationException} at
     * {@code registerRelay} time (umbrella Decision 12).
     *
     * <p>The returned {@link EntryChannel} only exposes {@code .into(...)}
     * for wiring an entry-shard consumer.
     */
    @Nonnull <T> EntryChannel<T> input(@Nonnull Class<T> type, @Nonnull String name);

    /**
     * Declare an inter-shard or plugin-bound channel. The returned
     * {@link ChannelDecl} requires {@code .from(...)} to identify a producer
     * Shard; after {@code .from}, it requires either {@code .into(...)}
     * (inter-shard) or {@code .toPlugin(...)} (plugin-bound). A channel
     * with neither is rejected at build-topology time with
     * {@link com.flezha.sdk.relay.RelayConfigurationException}.
     */
    @Nonnull <T> ChannelDecl<T> channel(@Nonnull Class<T> type, @Nonnull String name);
}
