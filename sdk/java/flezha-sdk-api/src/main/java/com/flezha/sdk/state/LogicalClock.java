package com.flezha.sdk.state;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Logical clock backed by a monotonic counter exposed as an {@link Instant}
 * via {@link Instant#ofEpochMilli(long)}. Type ships in v1 for forward
 * compatibility; the runtime currently rejects Shards declaring this Clock
 * with {@code RelayConfigurationException.clockNotImplementedYet}.
 */
public final class LogicalClock implements Clock {

    private final AtomicLong counter;

    public LogicalClock(AtomicLong counter) {
        this.counter = counter;
    }

    @Override
    public Instant now() {
        throw new UnsupportedOperationException("Clock not implemented yet");
    }
}
