package com.flezha.sdk.workflow;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.Computation;
import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * The <strong>workflow</strong> compute-operative kind: an on-demand,
 * terminating, possibly-interactive process.
 *
 * <p>A workflow is started with {@code args}, runs to an exit trigger, and is
 * then torn down. It reuses the stream <strong>building blocks</strong>
 * (Procedures, Channels, Entities, per-Entity state, {@code ctx.emit}) but
 * drives them under a workflow <strong>lifecycle</strong>:
 *
 * <ul>
 *   <li><strong>{@code run} sugar</strong> — the trivial case is a bare
 *       {@link #run} body: a single implicit Procedure/Entity.</li>
 *   <li><strong>Structured</strong> — override {@link #procedures} to author
 *       Procedures/Channels/Entities on the same {@link ChannelBuilder} a
 *       stream operative uses; the workflow driver runs the static DAG in
 *       topological order with a per-procedure count-based barrier (entities
 *       within one procedure still run in parallel) — fan-out branches are
 *       independent, and a fan-in node runs once, after every producer
 *       feeding it committed, with the accumulated batch.</li>
 *   <li><strong>Exit triggers</strong> — the operative terminates on the first
 *       of: the chain running to quiescence, {@link WorkflowOperativeCtx#complete},
 *       a loop's terminal predicate, a declared {@link #maxDuration}, or
 *       external {@code flz operative stop}. Every trigger converges on the
 *       single {@code WorkflowCompleted} teardown.</li>
 *   <li><strong>Inbox</strong> — a running operative receives input after it
 *       starts via {@link WorkflowOperativeCtx#recv}.</li>
 * </ul>
 *
 * <p>Authoring is a distinct, lightweight interface (not a degenerate
 * {@code Procedure}). Declare a workflow with
 * {@link com.flezha.sdk.FlezhaRunner#defineWorkflow(Class)} — the sibling of
 * {@code defineStream} — before {@code start()}/{@code run()}:
 *
 * <pre>{@code
 * public final class GreetWorkflowOperative implements WorkflowOperative {
 *     public String factoryId() { return "greet"; }
 *     public void run(WorkflowOperativeCtx ctx, String args) throws WorkflowOperativeException {
 *         ctx.complete(("hello " + args).getBytes(StandardCharsets.UTF_8));
 *     }
 * }
 * Flezha.createRunner("my_app").defineWorkflow(GreetWorkflowOperative.class).run();
 * }</pre>
 */
public interface WorkflowOperative extends Computation {

    /** Stable, controller-facing factory identifier. */
    @Override
    @Nonnull
    String factoryId();

    /**
     * Run-sugar body: the single-Procedure, single-Entity case. Invoked once
     * per started instance with {@code args} from {@code init_values}. Emit
     * output via {@code ctx}, optionally call {@link WorkflowOperativeCtx#complete}
     * with a result, and return normally; throwing
     * {@link WorkflowOperativeException} is a terminal failure (no retry).
     *
     * <p>The default throws — a class must implement either {@code run} or
     * {@link #procedures}. A structured operative (non-empty {@code procedures})
     * never has {@code run} invoked.
     *
     * @param ctx lifecycle context bound to this workflow instance
     * @param args the {@code init_values} string shipped with the start
     */
    default void run(@Nonnull WorkflowOperativeCtx ctx, @Nonnull String args) throws WorkflowOperativeException {
        throw new WorkflowOperativeException(
                "WorkflowOperative `" + factoryId() + "`: neither run() nor procedures() was implemented");
    }

    /**
     * Structured form: declare a static <strong>DAG</strong> of Procedures /
     * Channels / Entities on the same graph builder a stream operative
     * uses. The default is empty, selecting the {@link #run}-sugar path. The
     * workflow driver runs the declaration in topological order: a procedure
     * becomes runnable only after <strong>every</strong> producer feeding it
     * has committed — fan-out branches are independent, and a fan-in node is
     * dispatched once, with the accumulated batch of all its producers'
     * outputs. Cycles are rejected at registration.
     */
    default void procedures(@Nonnull ChannelBuilder c) {}

    /**
     * Optional wall-clock backstop. {@link Optional#empty()} (the default) means
     * no duration bound; when present, the SDK cooperatively cancels in-flight
     * work once it elapses.
     */
    @Nonnull
    default Optional<Duration> maxDuration() {
        return Optional.empty();
    }

    /**
     * Self-termination guarantee, validated at registration. Default {@code true}:
     * the operative ends on its own (a {@code run}/chain that returns, or a loop
     * with a terminal predicate / exhausted input). An operative whose
     * <strong>only</strong> possible exit is an external stop returns
     * {@code false}; combined with no {@link #maxDuration} it is
     * <strong>rejected</strong> at registration with a message directing the
     * author to add a terminal condition.
     */
    default boolean selfTerminating() {
        return true;
    }

    /**
     * How many times this Operative runs its chain. Defaults to
     * {@link LoopMode#ONCE} — one pass, byte-identical to the pre-loop behaviour.
     */
    @Nonnull
    default LoopMode loopMode() {
        return LoopMode.ONCE;
    }

    /**
     * How long a parked (suspended) instance sleeps before the controller wakes
     * it, even with no inbound message. Empty = wake only on a message.
     */
    @Nonnull
    default Optional<Duration> wakeAfter() {
        return Optional.empty();
    }

    /**
     * Total attempts for a failing entity's batch, including the first (default
     * 3). {@code 0} or {@code 1} collapses to fail-fast.
     *
     * <p>Retry is strictly PRE-commit: a journaled proc is never retried. Only the
     * failing entity re-runs — its siblings stay buffered at the barrier — and
     * each attempt gets a fresh sink, so a failed attempt's staged output is
     * dropped rather than double-emitted.
     */
    default int retryAttempts() {
        return 3;
    }

    /**
     * Exponential-backoff base between attempts (default 500 ms). A zero backoff
     * with more than one attempt is rejected at registration: it would hot-spin.
     */
    @Nonnull
    default Duration retryBackoff() {
        return Duration.ofMillis(500);
    }

    /**
     * Saga rollback scope, applied when the operative terminates abnormally
     * after partial progress. Default {@link RollbackMode#ALL_OR_NOTHING} undoes
     * every completed procedure in reverse, back to the start;
     * {@link RollbackMode#KEEP_PROGRESS} keeps each finished loop iteration and
     * undoes only the in-flight one. The mode is irrelevant for a single-pass
     * chain (where both behave all-or-nothing) and for an operative with no
     * compensation hooks.
     */
    default RollbackMode rollbackMode() {
        return RollbackMode.ALL_OR_NOTHING;
    }

    /**
     * Class-level persistence toggle (durable by default, mirroring
     * {@code StreamOperative.persistenceEnabled()}). When {@code true}, a
     * structured workflow operative's per-Entity Changes are written to the WAL
     * as they apply, so a crash resumes from persisted state. When
     * {@code false}, the per-Entity flush drains and discards Changes with no
     * serialization or wire I/O — the operative is in-memory only (a crash
     * re-runs from the top). The controller resolves this durable-by-default
     * with a per-class operator opt-out.
     */
    default boolean persistenceEnabled() {
        return true;
    }
}
