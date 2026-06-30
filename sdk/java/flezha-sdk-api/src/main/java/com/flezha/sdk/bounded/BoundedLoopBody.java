package com.flezha.sdk.bounded;

import javax.annotation.Nonnull;

/**
 * The body of a {@link BoundedOperativeCtx#loopUntil} loop: one iteration's work,
 * re-run until the loop's terminal predicate is true or the operative is
 * cancelled / completed. Mirrors the Rust SDK's {@code loop_until} body closure.
 */
@FunctionalInterface
public interface BoundedLoopBody {

    /** Run one loop iteration against the bounded context. */
    void run(@Nonnull BoundedOperativeCtx ctx) throws BoundedOperativeException;
}
