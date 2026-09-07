package com.flezha.sdk.operative;

import javax.annotation.Nonnull;

/**
 * The shared base of both operative kinds.
 *
 * <p>{@link StreamOperative} declares a graph of procedures that runs continuously over a stream,
 * holding per-entity state. {@link WorkflowOperative} declares ordered steps that run to completion
 * and resume at the step they reached. Both are operatives, both are declared on a runner, and both
 * are scheduled, restarted and relocated by the controller in the same way.
 *
 * <p>The only member common to both is {@link #factoryId()}, the stable controller-facing name that
 * {@code flz operative start <factory_id>} resolves to a registered class. Everything that differs
 * between the kinds, the channel graph on one side and the step sequence on the other, stays on the
 * per-kind interface.
 *
 * <p>You rarely implement this interface directly. Implement one of the two kinds, which extend it:
 *
 * <pre>{@code
 * public final class WordCount implements StreamOperative { ... }    // continuous, stateful
 * public final class Checkout implements WorkflowOperative { ... }   // ordered steps, resumable
 * }</pre>
 *
 * <p>Both are declared on the same runner, through the method matching their kind:
 *
 * <pre>{@code
 * Flezha.createRunner("my_app")
 *         .defineStream(WordCount.class)
 *         .defineWorkflow(Checkout.class)
 *         .run();
 * }</pre>
 *
 * <p>Adding a further operative kind means implementing this interface and adding the matching
 * {@code defineX} and {@code removeX} methods on the runner, without modifying either existing
 * authoring surface.
 */
public interface Operative {

    /** The stable, controller-facing factory identifier. One factory id per class. */
    @Nonnull
    String factoryId();
}
