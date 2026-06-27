package com.flezha.sdk.runtime.relay;

import javax.annotation.Nonnull;

/**
 * Sealed compute-unit instance held in the per-executor instance registry.
 * One variant per compute-unit kind: a {@link RelayInstance} (a partitioned,
 * stateful DAG) or a {@link TaskInstance} (a stateless run-once job). The
 * dispatch boundary resolves the variant once per inbound frame and
 * {@code switch}es on it, so the relay hot path stays a single predictable
 * branch and every future kind must be handled at the {@code switch}.
 *
 * <p>The name {@code Unit} is provisional; the relay→pipeline rename change
 * owns final naming. {@link RelayInstance} and {@link TaskInstance} are
 * co-located in this package because Java requires a sealed type's permitted
 * subtypes to share its package (this module has no {@code module-info}).
 */
public sealed interface Unit permits RelayInstance, TaskInstance {

    /** The controller-facing instance id (= what the wire calls {@code relay_id}). */
    @Nonnull
    String instanceId();
}
