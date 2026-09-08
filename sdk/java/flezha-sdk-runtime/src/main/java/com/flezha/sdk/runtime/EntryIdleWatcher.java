package com.flezha.sdk.runtime;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.IdleObserver;
import com.flezha.sdk.Logger;
import com.flezha.sdk.native_.FlzCore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Idle observation for entry channels: the policy half of the input-liveness
 * signal the core records.
 *
 * <p>The core knows when a frame last arrived on each entry channel — the Java
 * SDK is a poll host over it, so there is one delivery path and one stamp for
 * both SDKs. Only the application knows whether the silence since then is
 * anomalous, so this class ships no default threshold and no default action:
 * {@link com.flezha.sdk.FlezhaRunner#onEntryIdle} supplies both.
 *
 * <p>Evaluation runs on one daemon tick thread, never on the delivery path, so
 * a slow or throwing observer costs the data plane nothing.
 *
 * <p>Reached reflectively from {@code flezha-sdk-api}; the JVM's one runner
 * owns the registrations and clears them in {@code close()}.
 */
public final class EntryIdleWatcher {

    private static final Logger LOG = Flezha.log();

    /**
     * Tick floor and ceiling, mirrored in the Rust SDK. The floor bounds how
     * often a fleet of observers wakes; the ceiling bounds how late an episode
     * is reported.
     */
    private static final Duration TICK_MIN = Duration.ofMillis(250);

    private static final Duration TICK_MAX = Duration.ofSeconds(1);

    /**
     * Copy-on-write because the tick thread iterates it without {@link #LOCK},
     * which guards only the ticker/period bookkeeping below.
     */
    private static final List<Watch> WATCHES = new CopyOnWriteArrayList<>();

    private static final Object LOCK = new Object();

    @Nullable
    private static ScheduledExecutorService ticker;

    @Nullable
    private static ScheduledFuture<?> tick;

    /** The period {@link #tick} runs at — the shortest any watch asks for. */
    @Nullable
    private static Duration period;

    private EntryIdleWatcher() {}

    /**
     * When {@code channel} last received a frame, or empty before its first
     * frame (and for a name no declared operative has — both mean "nothing has
     * arrived here").
     *
     * <p>The core's stamp is monotonic; the wall-clock instant is derived from
     * the gap, so it is unaffected by a clock step between then and now.
     */
    @Nonnull
    public static Optional<Instant> lastEntryDelivery(@Nonnull String channel) {
        Objects.requireNonNull(channel, "channel");
        return deliveryInstant(FlzCore.entryLastDeliveryNanos(channel), FlzCore.monotonicNanos(), Instant.now());
    }

    /**
     * The wall-clock instant a monotonic stamp names, derived from its gap so
     * a clock step between the delivery and the read cannot skew it.
     * {@code last == 0} is the core's never-delivered sentinel.
     */
    @Nonnull
    static Optional<Instant> deliveryInstant(long last, long nowNanos, @Nonnull Instant wallNow) {
        return last == 0 ? Optional.empty() : Optional.of(wallNow.minusNanos(Math.max(0, nowNanos - last)));
    }

    /**
     * Register {@code observer} against {@code channel}. Legal before and
     * after {@code start()}: the core interns the observation by channel name,
     * so a registration made before the class is defined watches the same
     * channel it later gets. Several observers may watch one channel.
     */
    public static void onEntryIdle(
            @Nonnull String channel, @Nonnull Duration threshold, @Nonnull IdleObserver observer) {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(threshold, "threshold");
        Objects.requireNonNull(observer, "observer");
        Duration wanted = tickPeriod(threshold);
        synchronized (LOCK) {
            WATCHES.add(new Watch(channel, threshold.toNanos(), observer));
            if (ticker == null) {
                ticker = Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread t = new Thread(runnable, "flezha-entry-idle");
                    t.setDaemon(true);
                    return t;
                });
            }
            if (period == null || wanted.compareTo(period) < 0) {
                if (tick != null) {
                    tick.cancel(false);
                }
                period = wanted;
                // No initial delay: a channel that went quiet before the
                // observer was registered is already in an episode, and the
                // latch makes an immediate first pass harmless otherwise.
                tick = ticker.scheduleWithFixedDelay(EntryIdleWatcher::tick, 0, wanted.toNanos(), TimeUnit.NANOSECONDS);
            }
        }
    }

    /**
     * Drop every registration and stop the tick — the runner's {@code close()}
     * hook, so a second runner in the same JVM starts with no inherited
     * observers. Idempotent.
     */
    public static void reset() {
        synchronized (LOCK) {
            WATCHES.clear();
            if (tick != null) {
                tick.cancel(false);
                tick = null;
            }
            if (ticker != null) {
                ticker.shutdownNow();
                ticker = null;
            }
            period = null;
        }
    }

    /** Evaluate every watch once and deliver the episodes it found. */
    static void tick() {
        long now = FlzCore.monotonicNanos();
        for (Watch watch : WATCHES) {
            Episode episode = watch.evaluate(now, FlzCore.entryLastDeliveryNanos(watch.channel));
            if (episode != null) {
                watch.deliver(episode);
            }
        }
    }

    /**
     * The tick a {@code threshold} asks for: quarter of it, so an episode is
     * reported within 25% of the threshold, bounded at both ends.
     */
    static Duration tickPeriod(@Nonnull Duration threshold) {
        Duration quarter = threshold.dividedBy(4);
        if (quarter.compareTo(TICK_MIN) < 0) {
            return TICK_MIN;
        }
        return quarter.compareTo(TICK_MAX) > 0 ? TICK_MAX : quarter;
    }

    /** One episode boundary a tick found. */
    record Episode(boolean idle, Duration gap) {}

    /**
     * One registration plus its episode latch. Only the tick thread mutates
     * the latch, so the state machine below is single-threaded by construction.
     */
    static final class Watch {

        private final String channel;
        private final long thresholdNanos;
        private final IdleObserver observer;

        /**
         * The delivery stamp the current episode fired against; {@code 0}
         * while armed. Comparing stamps (not clock readings) is what makes a
         * resume exact: the stamp moves if and only if a frame arrived. A real
         * stamp is never 0 — the core clamps it away from the never-delivered
         * sentinel — so 0 is free to mean "armed" here.
         */
        private long firedAt;

        Watch(String channel, long thresholdNanos, IdleObserver observer) {
            this.channel = channel;
            this.thresholdNanos = thresholdNanos;
            this.observer = observer;
        }

        /**
         * Advance this watch's episode latch to {@code nowNanos}, given the
         * channel's last-delivery stamp, and report the boundary it crossed —
         * {@code null} for none. Both clock readings are arguments so the
         * state machine is exercised without waiting on a clock.
         */
        @Nullable
        Episode evaluate(long nowNanos, long last) {
            if (last == 0) {
                // No frame has ever arrived: a cold start is not an episode.
                return null;
            }
            if (firedAt == 0) {
                long gap = Math.max(0, nowNanos - last);
                if (gap <= thresholdNanos) {
                    return null;
                }
                firedAt = last;
                return new Episode(true, Duration.ofNanos(gap));
            }
            if (last != firedAt) {
                long silence = Math.max(0, last - firedAt);
                firedAt = 0;
                return new Episode(false, Duration.ofNanos(silence));
            }
            return null;
        }

        /**
         * Invoke the observer, contained: application code runs on the SDK's
         * own thread, so a throw must become a log line rather than a dead
         * ticker (a task that throws is dropped by the scheduler for good).
         */
        void deliver(@Nonnull Episode episode) {
            try {
                if (episode.idle()) {
                    observer.idle(episode.gap());
                } else {
                    observer.resumed(episode.gap());
                }
            } catch (RuntimeException | Error e) {
                LOG.warn(
                        "entry-idle observer threw on the {} episode of channel '{}'",
                        episode.idle() ? "idle" : "resumed",
                        channel,
                        e);
            }
        }
    }
}
