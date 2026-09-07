package com.flezha.sdk.operative;

/**
 * The body of a {@link WorkflowOperativeCtx#loopUntil} loop: one iteration's work,
 * re-run until the loop's terminal predicate is true or the operative is
 * cancelled / completed. Mirrors the Rust SDK's {@code loop_until} body closure.
 */
@FunctionalInterface
public interface WorkflowLoopBody {

    /** Run one loop iteration against the workflow context. */
    void run(WorkflowOperativeCtx ctx) throws WorkflowOperativeException;
}
