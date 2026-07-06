package com.flezha.sdk.runtime.operative;

import javax.annotation.Nonnull;

/**
 * Sealed compute-operative instance held in the per-executor instance registry.
 * One variant per compute-operative kind: a {@link UnboundedOperativeInstance} (a segmented,
 * stateful DAG) or a {@link BoundedOperativeInstance} (a stateless run-once job). The
 * dispatch boundary resolves the variant once per inbound frame and
 * {@code switch}es on it, so the operative hot path stays a single predictable
 * branch and every future kind must be handled at the {@code switch}.
 *
 * <p>{@link UnboundedOperativeInstance} and {@link BoundedOperativeInstance} are
 * co-located in this package because Java requires a sealed type's permitted
 * subtypes to share its package (this module has no {@code module-info}).
 */
public sealed interface Operative permits UnboundedOperativeInstance, BoundedOperativeInstance {

    /** The controller-facing instance id (= what the wire calls {@code operative_id}). */
    @Nonnull
    String instanceId();
}
