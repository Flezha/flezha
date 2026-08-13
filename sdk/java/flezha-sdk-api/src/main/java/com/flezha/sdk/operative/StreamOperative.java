package com.flezha.sdk.operative;

import com.flezha.sdk.channel.ChannelBuilder;
import javax.annotation.Nonnull;

/**
 * One StreamOperative class. Implementors declare a stable {@link #factoryId()} and the
 * per-class topology of channels + Procedures via {@link #channels(ChannelBuilder)}.
 *
 * <p>The runtime calls {@link #channels(ChannelBuilder)} once at
 * {@link com.flezha.sdk.FlezhaRunner#defineStream(Class) defineStream} time and records
 * the resulting topology shared by every {@code StreamOperativeInstance} of this class.
 *
 * <p>Implementations MUST provide a public no-arg constructor — the runtime
 * instantiates the class once at registration time to read its topology.
 */
public interface StreamOperative extends Computation {

    /**
     * Stable name of this StreamOperative class. Used as the controller-facing factory id —
     * the controller's {@code flz operative start <factory_id>} looks this up.
     * One factory_id per StreamOperative class.
     */
    @Override
    @Nonnull
    String factoryId();

    /**
     * One-shot topology declaration. Called exactly once per StreamOperative class at
     * {@link com.flezha.sdk.FlezhaRunner#defineStream(Class) defineStream} time. The
     * builder collects channel declarations into a topology shared by every
     * StreamOperative instance spawned for this class.
     */
    void channels(@Nonnull ChannelBuilder c);

    /**
     * Whether per-Entity state persistence is enabled for this StreamOperative class.
     * The controller ships the resolved value (runner override &gt; this default
     * &gt; {@code true}) in {@code AddOperative} metadata; the runtime stamps
     * it onto each {@code StreamOperativeInstance} at construction.
     *
     * <p>Default: {@code true}. Override to {@code false} to opt-out at the
     * StreamOperative-class level (the per-Entity flusher drains and discards
     * Changes without serialization or wire I/O).
     */
    default boolean persistenceEnabled() {
        return true;
    }
}
