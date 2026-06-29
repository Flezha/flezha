package com.flezha.sdk.state;

import java.util.Optional;

/**
 * Marker interface for per-Segment state-changes recorded through
 * {@link StateHandle#apply(Change)}. Implementations are typically Java
 * records — small, immutable, Kryo-serializable.
 *
 * <p>Override {@link #coalesceKey()} on a Change variant to enable
 * coalescing in the per-Segment flusher: Changes with equal keys collapse
 * to the latest within a flush window.
 */
public interface Change {

    /**
     * Optional coalesce key. Default: no coalescing — every Change is recorded
     * as-is on the WAL.
     */
    default Optional<CoalesceKey> coalesceKey() {
        return Optional.empty();
    }
}
