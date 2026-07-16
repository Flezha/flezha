package com.flezha.sdk.runtime.segment;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide dispatch-progress counters, reported on every
 * {@code data_heartbeat}.
 *
 * <p>The heartbeat thread stays alive even when the dispatch pool is wedged, so
 * a heartbeat on its own proves only that the process exists. These counters are
 * what let the controller's stall classifier tell <b>"alive but frozen"</b> apart
 * from <b>"healthy and idle"</b>: if inputs keep arriving while {@code dispatches}
 * stops advancing, the runner is wedged — and without them, it looks perfectly
 * healthy.
 */
public final class SegmentCounters {

    private static final AtomicLong DISPATCHES = new AtomicLong();
    private static final AtomicLong BLOCKED_PARKS = new AtomicLong();

    private SegmentCounters() {}

    /** One Operation invocation completed (successfully or not — it made progress). */
    public static void noteDispatch() {
        DISPATCHES.incrementAndGet();
    }

    /** A producer parked on a full mailbox under {@code OverflowPolicy.BLOCK}. */
    public static void noteBlockedPark() {
        BLOCKED_PARKS.incrementAndGet();
    }

    public static long dispatchesCompleted() {
        return DISPATCHES.get();
    }

    public static long blockedParkEpisodes() {
        return BLOCKED_PARKS.get();
    }
}
