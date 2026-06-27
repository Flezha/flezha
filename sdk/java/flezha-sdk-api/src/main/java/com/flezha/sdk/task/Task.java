package com.flezha.sdk.task;

import javax.annotation.Nonnull;

/**
 * The <strong>task</strong> compute-unit kind: a stateless, run-once job.
 *
 * <p>A task is started with {@code args}, runs its {@link #run} method
 * exactly once on the executor's worker pool, may emit output to bound
 * plugins via {@link TaskCtx#emit}, and signals completion to the
 * controller. It has no channels, no partitions, and no per-partition state
 * — it shares only the runtime plumbing (registries, executor bind, plugin
 * emit) with relays.
 *
 * <p>Authoring is a distinct, lightweight interface (not a degenerate
 * {@code Shard}). Register a task with
 * {@link com.flezha.sdk.Flezha#registerTask(Class)} — the sibling of
 * {@code registerRelay} — before {@code Flezha.newInstance(...)}:
 *
 * <pre>{@code
 * public final class GreetTask implements Task {
 *     public String factoryId() { return "greet"; }
 *     public void run(TaskCtx ctx, String args) throws TaskException {
 *         ctx.emit("hello " + args);
 *     }
 * }
 * Flezha.registerTask(GreetTask.class);
 * }</pre>
 */
public interface Task {

    /** Stable, controller-facing factory identifier. */
    @Nonnull
    String factoryId();

    /**
     * Run the job once. Emit output via {@code ctx}; return normally on
     * success or throw {@link TaskException} on a terminal failure (the
     * controller tears the task down either way — there is no retry).
     *
     * @param ctx output context bound to this task instance
     * @param args the {@code init_values} string shipped with the start
     */
    void run(@Nonnull TaskCtx ctx, @Nonnull String args) throws TaskException;
}
