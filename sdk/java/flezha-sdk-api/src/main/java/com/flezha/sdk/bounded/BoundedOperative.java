package com.flezha.sdk.bounded;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.Computation;
import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * The <strong>bounded</strong> compute-operative kind: an on-demand,
 * terminating, possibly-interactive process.
 *
 * <p>A bounded is started with {@code args}, runs to an exit trigger, and is
 * then torn down. It reuses the unbounded <strong>building blocks</strong>
 * (Operations, Channels, Segments, per-Segment state, {@code ctx.emit}) but
 * drives them under a bounded <strong>lifecycle</strong>:
 *
 * <ul>
 *   <li><strong>{@code run} sugar</strong> — the trivial case is a bare
 *       {@link #run} body: a single implicit Operation/Segment.</li>
 *   <li><strong>Structured</strong> — override {@link #operations} to author
 *       Operations/Channels/Segments on the same {@link ChannelBuilder} a
 *       unbounded operative uses; the bounded driver runs them sequentially
 *       in a linear chain with a per-operation count-based barrier (segments
 *       within one operation still run in parallel).</li>
 *   <li><strong>Exit triggers</strong> — the operative terminates on the first
 *       of: the chain running to quiescence, {@link BoundedOperativeCtx#complete},
 *       a loop's terminal predicate, a declared {@link #maxDuration}, or
 *       external {@code flz operative stop}. Every trigger converges on the
 *       single {@code BoundedCompleted} teardown.</li>
 *   <li><strong>Inbox</strong> — a running operative receives input after it
 *       starts via {@link BoundedOperativeCtx#recv}.</li>
 * </ul>
 *
 * <p>Authoring is a distinct, lightweight interface (not a degenerate
 * {@code Operation}). Register a bounded with
 * {@link com.flezha.sdk.Flezha#registerBounded(Class)} — the sibling of
 * {@code registerUnbounded} — before {@code Flezha.newInstance(...)}:
 *
 * <pre>{@code
 * public final class GreetBoundedOperative implements BoundedOperative {
 *     public String factoryId() { return "greet"; }
 *     public void run(BoundedOperativeCtx ctx, String args) throws BoundedOperativeException {
 *         ctx.complete(("hello " + args).getBytes(StandardCharsets.UTF_8));
 *     }
 * }
 * Flezha.registerBounded(GreetBoundedOperative.class);
 * }</pre>
 */
public interface BoundedOperative extends Computation {

    /** Stable, controller-facing factory identifier. */
    @Override
    @Nonnull
    String factoryId();

    /**
     * Run-sugar body: the single-Operation, single-Segment case. Invoked once
     * per started instance with {@code args} from {@code init_values}. Emit
     * output via {@code ctx}, optionally call {@link BoundedOperativeCtx#complete}
     * with a result, and return normally; throwing
     * {@link BoundedOperativeException} is a terminal failure (no retry).
     *
     * <p>The default throws — a class must implement either {@code run} or
     * {@link #operations}. A structured operative (non-empty {@code operations})
     * never has {@code run} invoked.
     *
     * @param ctx lifecycle context bound to this bounded instance
     * @param args the {@code init_values} string shipped with the start
     */
    default void run(@Nonnull BoundedOperativeCtx ctx, @Nonnull String args) throws BoundedOperativeException {
        throw new BoundedOperativeException(
                "BoundedOperative `" + factoryId() + "`: neither run() nor operations() was implemented");
    }

    /**
     * Structured form: declare a <strong>linear chain</strong> of Operations /
     * Channels / Segments on the same {@link ChannelBuilder} an unbounded
     * operative uses. The default is empty, selecting the {@link #run}-sugar
     * path. A non-empty declaration is run sequentially by the bounded driver
     * (operation {@code N+1} starts only when every segment of operation
     * {@code N} has finished). Branching is rejected at registration.
     */
    default void operations(@Nonnull ChannelBuilder c) {}

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
     * Total attempts for a failing segment's batch, including the first (default
     * 3). {@code 0} or {@code 1} collapses to fail-fast.
     *
     * <p>Retry is strictly PRE-commit: a journaled op is never retried. Only the
     * failing segment re-runs — its siblings stay buffered at the barrier — and
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
     * every completed operation in reverse, back to the start;
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
     * {@code UnboundedOperative.persistenceEnabled()}). When {@code true}, a
     * structured bounded operative's per-Segment Changes are written to the WAL
     * as they apply, so a crash resumes from persisted state. When
     * {@code false}, the per-Segment flush drains and discards Changes with no
     * serialization or wire I/O — the operative is in-memory only (a crash
     * re-runs from the top). The controller resolves this durable-by-default
     * with a per-class operator opt-out.
     */
    default boolean persistenceEnabled() {
        return true;
    }
}
