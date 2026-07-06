package com.flezha.sdk.operative;

import com.flezha.sdk.channel.ChannelBuilder;
import javax.annotation.Nonnull;

/**
 * One UnboundedOperative class. Implementors declare a stable {@link #factoryId()} and the
 * per-class topology of channels + Operations via {@link #channels(ChannelBuilder)}.
 *
 * <p>The runtime calls {@link #channels(ChannelBuilder)} once at
 * {@link UnboundedOperativeRegistry#registerUnbounded(Class) registerUnbounded} time and freezes
 * the resulting topology shared by every {@code UnboundedOperativeInstance} of this class.
 *
 * <p>Implementations MUST provide a public no-arg constructor — the runtime
 * instantiates the class once at registration time to read its topology.
 */
public interface UnboundedOperative extends Computation {

    /**
     * Stable name of this UnboundedOperative class. Used as the controller-facing factory id —
     * the controller's {@code flz operative start <factory_id>} looks this up.
     * One factory_id per UnboundedOperative class.
     */
    @Override
    @Nonnull
    String factoryId();

    /**
     * One-shot topology declaration. Called exactly once per UnboundedOperative class at
     * {@link UnboundedOperativeRegistry#registerUnbounded(Class) registerUnbounded} time. The
     * builder collects channel declarations into a topology shared by every
     * UnboundedOperative instance spawned for this class.
     */
    void channels(@Nonnull ChannelBuilder c);

    /**
     * Whether per-Segment state persistence is enabled for this UnboundedOperative class.
     * The controller ships the resolved value (executor override &gt; this default
     * &gt; {@code true}) in {@code AddOperative} metadata; the runtime stamps
     * it onto each {@code UnboundedOperativeInstance} at construction.
     *
     * <p>Default: {@code true}. Override to {@code false} to opt-out at the
     * UnboundedOperative-class level (the per-Segment flusher drains and discards
     * Changes without serialization or wire I/O).
     */
    default boolean persistenceEnabled() {
        return true;
    }
}
