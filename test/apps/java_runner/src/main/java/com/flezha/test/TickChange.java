package com.flezha.test;

import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.CoalesceKey;
import java.util.Optional;

/**
 * Per-tick Change carrying an interval bucket, timestamp, and delta. Used by
 * {@link CoalescingCounterUnboundedOperative} to demonstrate the per-Entity flusher's
 * coalescing path: Changes with equal {@code (interval, ts)} collapse to the
 * latest within a flush window.
 */
public record TickChange(int interval, long ts, long delta) implements Change {

    @Override
    public Optional<CoalesceKey> coalesceKey() {
        return Optional.of(CoalesceKey.fromHash(interval, ts));
    }
}
