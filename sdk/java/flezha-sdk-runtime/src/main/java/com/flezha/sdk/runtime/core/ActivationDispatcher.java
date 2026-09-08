package com.flezha.sdk.runtime.core;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.Logger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * The pure host layer over the poll core: {@link #dispatch} turns one
 * {@link Activation} into one {@link ActivationResult} by running user code —
 * no native calls, which is what makes the whole typed layer unit-testable
 * without the core. The only impurities are injected: a per-instance
 * {@link WorkflowSession} for run-sugar bodies (and the api-side
 * {@code SpawnRequests} static, reached through {@code ProcedureCtx}'s
 * default {@code spawnWorkflow}).
 *
 * <p>The core guarantees per-Entity exclusivity — it never hands out a second
 * activation for the same Entity until the first completes — so there is no
 * Java-side Entity locking; the maps are merely concurrent-safe across
 * different Entities.
 */
public final class ActivationDispatcher {

    private static final Logger LOG = Flezha.log();

    private final ConcurrentHashMap<String, OperativeInstance> instances = new ConcurrentHashMap<>();
    private final Function<String, WorkflowSession> workflowSessions;

    public ActivationDispatcher(@Nonnull Function<String, WorkflowSession> workflowSessions) {
        this.workflowSessions = workflowSessions;
    }

    /** Live operative instances keyed by operative id — the {@code operatives()} view. */
    @Nonnull
    public Map<String, OperativeInstance> instances() {
        return instances;
    }

    /** Whether any live instance belongs to {@code factoryId}. */
    public boolean hasInstanceOf(@Nonnull String factoryId) {
        return instances.values().stream().anyMatch(i -> factoryId.equals(i.factoryId()));
    }

    @Nonnull
    public ActivationResult dispatch(@Nonnull Activation activation) {
        try {
            // Keyed by operative id, but the FACTORY the activation names is what
            // decides which class runs: an operative id rebound to a different
            // factory — a deploy putting the next version of a class behind the
            // same id — must not keep running the class we materialized first.
            OperativeInstance instance = instances.compute(
                    activation.target().operative(),
                    (id, existing) -> existing != null && existing.factoryId().equals(activation.factory())
                            ? existing
                            : OperativeInstance.materialize(id, activation.factory()));
            return new ActivationResult(activation.token(), instance.dispatch(activation, workflowSessions));
        } catch (Throwable t) {
            String message = t.getMessage() == null ? t.toString() : t.getMessage();
            LOG.error(
                    "activation dispatch failed for `{}`: {}",
                    activation.target().operative(),
                    message);
            return new ActivationResult(activation.token(), new ActivationOutcome.Failed(new WorkError(message, true)));
        }
    }
}
