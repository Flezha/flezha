package com.flezha.sdk.operative;

import com.flezha.sdk.channel.ChannelBuilder;
import javax.annotation.Nonnull;

/**
 * One ContinuousOperative class. Implementors declare a stable {@link #factoryId()} and the
 * per-class topology of channels + Operations via {@link #channels(ChannelBuilder)}.
 *
 * <p>The runtime calls {@link #channels(ChannelBuilder)} once at
 * {@link ContinuousOperativeRegistry#registerContinuous(Class) registerContinuous} time and freezes
 * the resulting topology shared by every {@code ContinuousOperativeInstance} of this class.
 *
 * <p>Implementations MUST provide a public no-arg constructor — the runtime
 * instantiates the class once at registration time to read its topology.
 */
public interface ContinuousOperative extends Computation {

    /**
     * Stable name of this ContinuousOperative class. Used as the controller-facing factory id —
     * the controller's {@code flz operative start <factory_id>} looks this up.
     * One factory_id per ContinuousOperative class.
     */
    @Override
    @Nonnull
    String factoryId();

    /**
     * One-shot topology declaration. Called exactly once per ContinuousOperative class at
     * {@link ContinuousOperativeRegistry#registerContinuous(Class) registerContinuous} time. The
     * builder collects channel declarations into a topology shared by every
     * ContinuousOperative instance spawned for this class.
     */
    void channels(@Nonnull ChannelBuilder c);

    /**
     * Whether per-Segment state persistence is enabled for this ContinuousOperative class.
     * The controller ships the resolved value (executor override &gt; this default
     * &gt; {@code true}) in {@code AddOperative} metadata; the runtime stamps
     * it onto each {@code ContinuousOperativeInstance} at construction.
     *
     * <p>Default: {@code true}. Override to {@code false} to opt-out at the
     * ContinuousOperative-class level (the per-Segment flusher drains and discards
     * Changes without serialization or wire I/O).
     */
    default boolean persistenceEnabled() {
        return true;
    }
}
