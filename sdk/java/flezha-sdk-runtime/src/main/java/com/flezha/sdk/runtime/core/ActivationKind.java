package com.flezha.sdk.runtime.core;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * What an {@link Activation} asks the host to do. Variant order is the
 * postcard enum discriminant and must match {@code flz_core}'s
 * {@code ActivationKind} exactly.
 */
public sealed interface ActivationKind {

    /** Discriminant 0 — an ordered per-Entity batch of inputs and timer firings. */
    record Work(@Nonnull List<WorkItem> items) implements ActivationKind {}

    /**
     * Discriminant 1 — fold a persisted change log into a live state; input
     * activations for the Entity are gated until this completes.
     */
    record RestoreFold(@Nonnull Optional<byte[]> snapshot, @Nonnull List<byte[]> changes) implements ActivationKind {}

    /**
     * Discriminant 2 — encode the current state for compaction. The core
     * drained pending changes first and stamps {@code change_seq - 1} itself.
     */
    record SnapshotRequest() implements ActivationKind {}

    /** Discriminant 3 — serialize the current state to JSON for a live read. */
    record LiveStateRead() implements ActivationKind {}

    /** Discriminant 4 — materialize a migrated-in state from its JSON form. */
    record ApplyState(@Nonnull byte[] stateJson) implements ActivationKind {}

    /** Discriminant 5 — serialize the current state to JSON for a migration handover. */
    record MigrationSerialize() implements ActivationKind {}

    /**
     * Discriminant 6 — the batched prepare crossing: entry payloads whose
     * consumers need a key and/or an extracted event time. The host answers one
     * {@link PrepareAnswer} per payload, in order.
     */
    record KeyRequest(int channel, @Nonnull List<byte[]> payloads, boolean wantKeys, boolean wantEventTime)
            implements ActivationKind {}

    /**
     * Discriminant 7 — a bounded run-sugar body: the seed args cross once;
     * emit/recv/cancelled ride their own ABI calls.
     */
    record BoundedRun(@Nonnull byte[] args) implements ActivationKind {}

    /**
     * Discriminant 8 — saga compensation for one bounded Entity: the recorded
     * forward outputs cross; the host runs {@code compensate}.
     */
    record CompensateRun(@Nonnull List<byte[]> outputs) implements ActivationKind {}
}
