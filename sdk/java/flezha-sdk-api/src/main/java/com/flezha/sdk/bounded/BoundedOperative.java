package com.flezha.sdk.bounded;

import com.flezha.sdk.operative.Computation;
import javax.annotation.Nonnull;

/**
 * The <strong>bounded</strong> compute-operative kind: a stateless, run-once job.
 *
 * <p>A bounded is started with {@code args}, runs its {@link #run} method
 * exactly once on the executor's worker pool, may emit output to bound
 * plugins via {@link BoundedOperativeCtx#emit}, and signals completion to the
 * controller. It has no channels, no segments, and no per-segment state
 * — it shares only the runtime plumbing (registries, executor bind, plugin
 * emit) with operatives.
 *
 * <p>Authoring is a distinct, lightweight interface (not a degenerate
 * {@code Operation}). Register a bounded with
 * {@link com.flezha.sdk.Flezha#registerBounded(Class)} — the sibling of
 * {@code registerContinuous} — before {@code Flezha.newInstance(...)}:
 *
 * <pre>{@code
 * public final class GreetBoundedOperative implements BoundedOperative {
 *     public String factoryId() { return "greet"; }
 *     public void run(BoundedOperativeCtx ctx, String args) throws BoundedOperativeException {
 *         ctx.emit("hello " + args);
 *     }
 * }
 * Flezha.registerBounded(GreetBoundedOperative.class);
 * }</pre>
 */
public interface BoundedOperative extends Computation {

    /** Stable, controller-facing factory identifier. */
    @Override
    @Nonnull
    String factoryId();

    /**
     * Run the job once. Emit output via {@code ctx}; return normally on
     * success or throw {@link BoundedOperativeException} on a terminal failure (the
     * controller tears the bounded down either way — there is no retry).
     *
     * @param ctx output context bound to this bounded instance
     * @param args the {@code init_values} string shipped with the start
     */
    void run(@Nonnull BoundedOperativeCtx ctx, @Nonnull String args) throws BoundedOperativeException;
}
