package com.flezha.sdk.runtime.relay;

import javax.annotation.Nonnull;

/**
 * Per-{@code AddRelay} runtime marker for a task-kind unit. Unlike
 * {@link RelayInstance} it spawns no partition workers and holds no
 * mailboxes — the task's {@code run} is scheduled once on the worker pool
 * at spawn time (see {@code TaskRunner}). The instance lives in the unit
 * registry so a duplicate {@code AddRelay} is idempotent and the dispatch
 * boundary can identify a task frame as a no-op.
 *
 * <p>Co-located with {@link RelayInstance} so the sealed {@link Unit} seal
 * holds (see {@link Unit}); the runtime task plumbing otherwise lives in
 * {@code com.flezha.sdk.runtime.task}.
 */
public final class TaskInstance implements Unit {

    private final String instanceId;
    private final String factoryId;

    public TaskInstance(@Nonnull String instanceId, @Nonnull String factoryId) {
        this.instanceId = instanceId;
        this.factoryId = factoryId;
    }

    @Override
    @Nonnull
    public String instanceId() {
        return instanceId;
    }

    @Nonnull
    public String factoryId() {
        return factoryId;
    }
}
