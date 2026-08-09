package com.flezha.sdk.runtime.core;

import com.flezha.sdk.state.TimerKind;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import javax.annotation.Nonnull;

/**
 * One member of a {@link ActivationKind.Work} batch. Variant order is the
 * postcard enum discriminant.
 */
public sealed interface WorkItem {

    /** Discriminant 0 — one input payload for the target Entity. */
    record Input(
            int channel,
            @Nonnull byte[] payload,
            @Nonnull List<Map.Entry<String, String>> metadata,
            @Nonnull OptionalLong consumedOffset,
            boolean replay,
            long now)
            implements WorkItem {}

    /**
     * Discriminant 1 — a validated timer firing. Its {@code now} IS
     * {@code scheduledAt} — a chained {@code timerAfter} anchors on the firing
     * that woke it, replay-stable.
     */
    record Timer(@Nonnull TimerKind kind, long scheduledAt) implements WorkItem {}
}
