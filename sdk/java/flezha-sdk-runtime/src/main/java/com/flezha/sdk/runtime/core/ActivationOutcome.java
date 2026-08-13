package com.flezha.sdk.runtime.core;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * What became of an {@link Activation}. Variant order is the postcard enum
 * discriminant and must match {@code flz_core}'s {@code ActivationOutcome}.
 */
public sealed interface ActivationOutcome {

    /**
     * Discriminant 0 — one effect per processed {@link WorkItem}, in item
     * order. May be shorter than the batch only when an item's verdict is an
     * error — items after the failed one are re-handed by the core.
     */
    record Work(@Nonnull List<ItemEffect> effects) implements ActivationOutcome {}

    /** Discriminant 1 — the restore fold completed; the host holds the state. */
    record Restored() implements ActivationOutcome {}

    /** Discriminant 2 — the host-encoded compaction snapshot. */
    record Snapshot(@Nonnull byte[] state) implements ActivationOutcome {}

    /** Discriminant 3 — the live-read JSON serialization. */
    record StateJson(@Nonnull byte[] json) implements ActivationOutcome {}

    /** Discriminant 4 — the migrated-in state was materialized. */
    record StateApplied() implements ActivationOutcome {}

    /** Discriminant 5 — the migration-handover JSON serialization. */
    record MigrationState(@Nonnull byte[] json) implements ActivationOutcome {}

    /**
     * Discriminant 6 — one result per {@link ActivationKind.KeyRequest}
     * payload, in order. A prepare failure enters the input channel's
     * retry-then-park ladder.
     */
    record Prepared(@Nonnull List<PrepareResult> answers) implements ActivationOutcome {}

    /**
     * Discriminant 7 — the whole activation failed before per-item processing
     * (host-side panic equivalent).
     */
    record Failed(@Nonnull WorkError error) implements ActivationOutcome {}

    /**
     * Discriminant 8 — a workflow run-sugar body returned. {@code completed}
     * mirrors {@code ctx.complete(...)} having been called.
     */
    record WorkflowDone(boolean completed, @Nonnull byte[] result) implements ActivationOutcome {}
}
