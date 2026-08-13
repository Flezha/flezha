package com.flezha.sdk.workflow;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import javax.annotation.Nonnull;

/**
 * Lifecycle-aware context handed to {@link WorkflowOperative#run} (the run-sugar
 * path). {@code emit} writes a frame tagged with the workflow's instance id on a
 * secondary stream that the controller fans out to the operative's plugins;
 * {@code recv} drains the inbox (input delivered after start); {@code loopUntil}
 * repeats a body until a terminal predicate or cancellation (duration /
 * external stop); {@code complete} sets the terminal result.
 *
 * <p>The concrete implementation lives in {@code flezha-sdk-runtime}; this
 * interface is the surface a workflow author sees.
 */
public interface WorkflowOperativeCtx {

    /** Emit one output payload to the operative's bound plugins. Best-effort, at-least-once. */
    void emit(@Nonnull byte[] value) throws WorkflowOperativeException;

    /** Convenience: emit a UTF-8 string payload. */
    default void emit(@Nonnull String value) throws WorkflowOperativeException {
        emit(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Request terminal completion with {@code result}. The body should return
     * shortly after; the driver tears down with status {@code ok} and
     * {@code result} in the {@code WorkflowCompleted} payload. Loops observe
     * completion automatically.
     */
    void complete(@Nonnull byte[] result);

    /** Convenience: complete with a UTF-8 string result. */
    default void complete(@Nonnull String result) {
        complete(result.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * True once an external stop / {@code maxDuration} has tripped the
     * cancellation token. Long-running bodies should check this and unwind.
     */
    boolean isCancelled();

    /**
     * Block for the next inbox item (input delivered after start). Returns
     * {@link Optional#empty()} when cancellation trips, {@link #complete} was
     * called, or the inbox is permanently closed. Sleeps while idle (≤200 ms
     * wake granularity to honour cancellation) — never busy-spins.
     */
    @Nonnull
    Optional<byte[]> recv();

    /**
     * Repeat {@code body} until {@code predicate} returns {@code true} OR the
     * cancellation token trips (duration / external stop) OR {@link #complete}
     * is called. The construct observes cancellation automatically and blocks
     * while idle (a body that waits on {@link #recv} sleeps rather than
     * spinning). {@code predicate} provides the required non-timeout exit, so a
     * loop authored through this construct can always terminate without an
     * external kill.
     */
    void loopUntil(@Nonnull BooleanSupplier predicate, @Nonnull WorkflowLoopBody body)
            throws WorkflowOperativeException;
}
