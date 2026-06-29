package com.flezha.sdk.state;

/**
 * Coalesce key for the per-Segment flusher. Changes whose keys compare equal
 * collapse to the latest within a flush window.
 *
 * <p>The key is a 64-bit hash over arbitrary user-supplied values; collisions
 * are possible but bounded by the documented hash quality (same caveat as the
 * Rust SDK's {@code CoalesceKey}).
 */
public record CoalesceKey(long hash) {

    /**
     * Compute a coalesce key from arbitrary values. Combines each value's
     * {@link Object#hashCode()} with a stable polynomial accumulator. Nulls
     * hash to 0.
     */
    public static CoalesceKey fromHash(Object... values) {
        long h = 1L;
        for (Object v : values) {
            h = 31L * h + (v == null ? 0 : v.hashCode());
        }
        return new CoalesceKey(h);
    }
}
