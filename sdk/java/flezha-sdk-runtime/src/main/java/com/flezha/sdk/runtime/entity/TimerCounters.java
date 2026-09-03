package com.flezha.sdk.runtime.entity;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

/**
 * Per-Procedure timer counters: monotonic totals and a signed {@code pending}
 * gauge, read through {@link #of(String)} whenever a reader wants them.
 */
public final class TimerCounters {

    private static final Map<String, TimerCounters> BY_PROCEDURE = new ConcurrentHashMap<>();

    /**
     * Timers that have fired across the JVM. Nothing reads it: the
     * {@code timers_fired} a runner heartbeat carries is the core's own
     * process-wide counter, not this one.
     */
    private static final AtomicLong FIRED_TOTAL = new AtomicLong();

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

    public static void noteFired() {
        FIRED_TOTAL.incrementAndGet();
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
