package com.flezha.sdk.state;

import java.time.Instant;

/** Real-time wall clock backed by {@link Instant#now()}. Fully supported in v1. */
public final class WallClock implements Clock {

    @Override
    public Instant now() {
        return Instant.now();
    }
}
