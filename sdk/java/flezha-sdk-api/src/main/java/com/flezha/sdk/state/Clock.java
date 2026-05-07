package com.flezha.sdk.state;

import java.time.Instant;

/**
 * Sealed clock abstraction declared per-Shard via the {@code Shard}'s
 * {@code Clock} type parameter. v1 fully wires {@link WallClock}; declaring
 * {@link LogicalClock} or {@link EventTimeFrom} ships the type but the
 * runtime rejects the Shard with
 * {@code RelayConfigurationException.clockNotImplementedYet} until a
 * follow-up wires those clocks.
 */
public sealed interface Clock permits WallClock, LogicalClock, EventTimeFrom {

    /** Current time as observed by this clock. */
    Instant now();
}
