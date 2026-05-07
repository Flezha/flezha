package com.flezha.sdk.relay;

import com.flezha.sdk.channel.ChannelBuilder;
import javax.annotation.Nonnull;

/**
 * One Relay class. Implementors declare a stable {@link #factoryId()} and the
 * per-class topology of channels + Shards via {@link #channels(ChannelBuilder)}.
 *
 * <p>The runtime calls {@link #channels(ChannelBuilder)} once at
 * {@link RelayRegistry#registerRelay(Class) registerRelay} time and freezes
 * the resulting topology shared by every {@code RelayInstance} of this class.
 *
 * <p>Implementations MUST provide a public no-arg constructor — the runtime
 * instantiates the class once at registration time to read its topology.
 */
public interface Relay {

    /**
     * Stable name of this Relay class. Used as the agent-facing factory id —
     * the agent's {@code flz processor start <factory_id>} looks this up.
     * One factory_id per Relay class.
     */
    @Nonnull String factoryId();

    /**
     * One-shot topology declaration. Called exactly once per Relay class at
     * {@link RelayRegistry#registerRelay(Class) registerRelay} time. The
     * builder collects channel declarations into a topology shared by every
     * Relay instance spawned for this class.
     */
    void channels(@Nonnull ChannelBuilder c);

    /**
     * Whether per-Partition state persistence is enabled for this Relay class.
     * The agent ships the resolved value (operator override &gt; this default
     * &gt; {@code true}) in {@code AddProcessor} metadata; the runtime stamps
     * it onto each {@code RelayInstance} at construction.
     *
     * <p>Default: {@code true}. Override to {@code false} to opt-out at the
     * Relay-class level (the per-Partition flusher drains and discards
     * Changes without serialization or wire I/O).
     */
    default boolean persistenceEnabled() {
        return true;
    }
}
