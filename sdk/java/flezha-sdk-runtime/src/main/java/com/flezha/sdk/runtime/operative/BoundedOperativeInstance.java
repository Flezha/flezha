package com.flezha.sdk.runtime.operative;

import javax.annotation.Nonnull;

/**
 * Per-{@code AddOperative} runtime marker for a bounded-kind operative. Unlike
 * {@link ContinuousOperativeInstance} it spawns no segment workers and holds no
 * mailboxes — the bounded's {@code run} is scheduled once on the worker pool
 * at spawn time (see {@code BoundedOperativeRunner}). The instance lives in the operative
 * registry so a duplicate {@code AddOperative} is idempotent and the dispatch
 * boundary can identify a bounded frame as a no-op.
 *
 * <p>Co-located with {@link ContinuousOperativeInstance} so the sealed {@link Operative} seal
 * holds (see {@link Operative}); the runtime bounded plumbing otherwise lives in
 * {@code com.flezha.sdk.runtime.bounded}.
 */
public final class BoundedOperativeInstance implements Operative {

    private final String instanceId;
    private final String factoryId;

    public BoundedOperativeInstance(@Nonnull String instanceId, @Nonnull String factoryId) {
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
