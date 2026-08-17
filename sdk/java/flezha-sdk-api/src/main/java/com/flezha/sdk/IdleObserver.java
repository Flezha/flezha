package com.flezha.sdk;

import java.time.Duration;

/**
 * What an application does about an entry channel that has gone quiet.
 *
 * <p>Registered per channel with a threshold via
 * {@link FlezhaRunner#onEntryIdle(String, Duration, IdleObserver)}; the SDK
 * reports <b>episodes</b>, not ticks: {@link #idle(Duration)} fires once when
 * the silence first exceeds the threshold and not again while it continues,
 * and {@link #resumed(Duration)} fires once when a frame finally arrives,
 * re-arming the next episode. Silence before the very first frame is never an
 * episode — an operative waiting for its subscriptions to take is not an
 * incident.
 *
 * <p>Both calls run on the SDK's background tick, never on the delivery path.
 * A throw is caught and logged; delivery and later episodes are unaffected.
 *
 * <p>Mirrors the Rust SDK's {@code flezha_runner::IdleObserver}.
 */
@FunctionalInterface
public interface IdleObserver {

    /**
     * The channel has been silent for {@code gap}, which first exceeded the
     * registered threshold.
     */
    void idle(Duration gap);

    /**
     * A frame arrived, ending an episode that had lasted {@code gap}. Does
     * nothing by default — an alarm that only needs raising implements
     * {@link #idle(Duration)} alone.
     *
     * <p>{@code gap} is measured to the newest frame the tick saw, so a burst
     * that resumes and continues within one tick reports the silence plus that
     * burst — bounded by the tick, i.e. at most a quarter of the threshold.
     * The delivery path stores one stamp, not a first-frame-after-silence.
     */
    default void resumed(Duration gap) {}
}
