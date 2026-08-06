package com.flezha.sdk.operative;

import javax.annotation.Nonnull;

/**
 * Shared base of the two operative kinds: a {@link UnboundedOperative}
 * (entity, stateful, long-running DAG) and a
 * {@link com.flezha.sdk.bounded.BoundedOperative} (stateless, run-once job).
 *
 * <p>The only member common to both kinds is the stable controller-facing
 * {@link #factoryId()} — the name {@code flz operative start <factory_id>}
 * resolves to a registered class.
 */
public interface Computation {

    /** Stable, controller-facing factory identifier. One factory id per class. */
    @Nonnull
    String factoryId();
}
