package com.flezha.sdk.runtime.entity;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

/**
 * Per-Procedure timer counters plus the two process-global gauges the
 * {@code data_heartbeat} carries. Monotonic totals and a signed {@code pending}
 * gauge, read by observability at its leisure — the same posture as
 * {@link EntityCounters}.
 */
public final class TimerCounters {

    private static final Map<String, TimerCounters> BY_PROCEDURE = new ConcurrentHashMap<>();

    /**
     * Timers that have fired across the JVM. Shipped in every
     * {@code data_heartbeat} beside {@code dispatches} / {@code blocked_parks}.
     */
    private static final AtomicLong FIRED_TOTAL = new AtomicLong();

    /**
     * Processing-time timers currently armed across every Operative's wheel —
     * the occupancy a runaway registration shows up in before it is fatal.
     */
    private static final AtomicLong ARMED = new AtomicLong();

    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong registered = new AtomicLong();
    private final AtomicLong fired = new AtomicLong();
    private final AtomicLong cancelled = new AtomicLong();
    private final AtomicLong refused = new AtomicLong();

    private final String procedure;

    private TimerCounters(String procedure) {
        this.procedure = procedure;
    }

    /** The counter set for one Procedure, created on first use. */
    @Nonnull
    public static TimerCounters of(@Nonnull String procedure) {
        return BY_PROCEDURE.computeIfAbsent(procedure, TimerCounters::new);
    }

    /** Every Procedure's counters, name-ordered, for observability and tests. */
    @Nonnull
    public static List<TimerCounters> snapshot() {
        List<TimerCounters> out = new ArrayList<>(BY_PROCEDURE.values());
        out.sort(Comparator.comparing(TimerCounters::procedure));
        return out;
    }

    public static void noteFired() {
        FIRED_TOTAL.incrementAndGet();
    }

    public static void noteArmed(long delta) {
        ARMED.addAndGet(delta);
    }

    public static long firedTotal() {
        return FIRED_TOTAL.get();
    }

    public static long armed() {
        return ARMED.get();
    }

    @Nonnull
    public String procedure() {
        return procedure;
    }

    @Nonnull
    public AtomicLong pending() {
        return pending;
    }

    @Nonnull
    public AtomicLong registered() {
        return registered;
    }

    @Nonnull
    public AtomicLong fired() {
        return fired;
    }

    @Nonnull
    public AtomicLong cancelled() {
        return cancelled;
    }

    @Nonnull
    public AtomicLong refused() {
        return refused;
    }
}
