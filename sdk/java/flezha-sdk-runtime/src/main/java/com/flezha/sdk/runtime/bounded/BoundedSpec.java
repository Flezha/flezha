package com.flezha.sdk.runtime.bounded;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeConfigurationException;
import com.flezha.sdk.bounded.RollbackMode;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.OperationDescriptor;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.operative.builder.UnboundedOperativeTopologyBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Frozen per-class authoring result for a {@link BoundedOperative}, produced and
 * validated once at registration and reused for every spawn of the class.
 * Mirrors the Rust SDK's {@code BoundedSpec}: {@link #topology} is empty for the
 * bare {@code run}-sugar form, present (with the ordered {@link #chain}) for the
 * structured linear-chain form.
 */
public final class BoundedSpec {

    private final String factoryId;
    private final Class<? extends BoundedOperative> operativeClass;
    private final Optional<UnboundedOperativeTopology> topology;
    private final List<Integer> chain;
    private final Optional<Duration> maxDuration;
    private final RollbackMode rollbackMode;
    private final boolean persistenceEnabled;
    private final com.flezha.sdk.bounded.LoopMode loopMode;
    private final com.flezha.sdk.channel.RetryPolicy retry;
    private final Optional<Duration> wakeAfter;

    private BoundedSpec(
            String factoryId,
            Class<? extends BoundedOperative> operativeClass,
            Optional<UnboundedOperativeTopology> topology,
            List<Integer> chain,
            Optional<Duration> maxDuration,
            RollbackMode rollbackMode,
            boolean persistenceEnabled,
            com.flezha.sdk.bounded.LoopMode loopMode,
            Optional<Duration> wakeAfter,
            com.flezha.sdk.channel.RetryPolicy retry) {
        this.retry = retry;
        this.loopMode = loopMode;
        this.wakeAfter = wakeAfter;
        this.factoryId = factoryId;
        this.operativeClass = operativeClass;
        this.topology = topology;
        this.chain = List.copyOf(chain);
        this.maxDuration = maxDuration;
        this.rollbackMode = rollbackMode;
        this.persistenceEnabled = persistenceEnabled;
    }

    @Nonnull
    public String factoryId() {
        return factoryId;
    }

    @Nonnull
    public Class<? extends BoundedOperative> operativeClass() {
        return operativeClass;
    }

    /** Structured topology, or empty for the {@code run}-sugar form. */
    @Nonnull
    public Optional<UnboundedOperativeTopology> topology() {
        return topology;
    }

    /** Operations in execution order (entry consumer → … → terminal). Empty for {@code run}-sugar. */
    @Nonnull
    public List<Integer> chain() {
        return chain;
    }

    @Nonnull
    public Optional<Duration> maxDuration() {
        return maxDuration;
    }

    /** Saga rollback scope applied on abnormal termination. */
    @Nonnull
    public RollbackMode rollbackMode() {
        return rollbackMode;
    }

    /** Class-level persistence toggle; when true, a structured bounded's Changes are flushed to the WAL. */
    /**
     * The failing-segment retry ladder. Resolved onto the SAME
     * {@link com.flezha.sdk.channel.RetryPolicy} the unbounded dead-letter ladder
     * uses, so both share one backoff implementation.
     */
    @Nonnull
    public com.flezha.sdk.channel.RetryPolicy retry() {
        return retry;
    }

    /** How many times this class runs its chain. */
    @Nonnull
    public com.flezha.sdk.bounded.LoopMode loopMode() {
        return loopMode;
    }

    /** How long a parked instance sleeps before the controller wakes it. */
    @Nonnull
    public Optional<Duration> wakeAfter() {
        return wakeAfter;
    }

    public boolean persistenceEnabled() {
        return persistenceEnabled;
    }

    /** True when this spec runs the bare {@code run} body (no declared operations). */
    public boolean isRunSugar() {
        return topology.isEmpty();
    }

    /**
     * Build and validate the spec for a bounded class. An empty
     * {@link BoundedOperative#operations} selects the {@code run}-sugar path; a
     * non-empty declaration is validated as a single connected linear chain.
     *
     * @throws BoundedOperativeConfigurationException on invalid authoring
     *     (external-stop-only, branching, cycle, dynamic segmentation,
     *     disconnected operation, or no entry channel).
     */
    @Nonnull
    public static BoundedSpec build(@Nonnull Class<? extends BoundedOperative> operativeClass) {
        BoundedOperative probe;
        try {
            probe = operativeClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "BoundedOperative class `" + operativeClass.getName() + "` must have a public no-arg constructor: "
                            + e,
                    e);
        }
        String factoryId = probe.factoryId();
        if (factoryId == null || factoryId.isEmpty()) {
            throw new IllegalArgumentException(
                    "BoundedOperative class `" + operativeClass.getName() + "` returned null/empty from factoryId()");
        }

        // Self-termination guard: an operative whose only exit is an external
        // stop, with no duration backstop, can never terminate cooperatively.
        if (!probe.selfTerminating() && probe.maxDuration().isEmpty()) {
            throw new BoundedOperativeConfigurationException("bounded operative `" + factoryId
                    + "` is not terminable: its only possible exit is an external stop. Add a terminal condition — "
                    + "let run()/the chain complete, give the loop a terminal predicate, or declare maxDuration().");
        }

        UnboundedOperativeTopology topology;
        try {
            topology = UnboundedOperativeTopologyBuilder.build(factoryId, probe::operations);
        } catch (UnboundedOperativeConfigurationException e) {
            // A structured bounded reuses the unbounded topology builder, which
            // rejects dynamic-segment operations and unwired channels.
            throw new BoundedOperativeConfigurationException(
                    "bounded operative `" + factoryId + "` has an invalid topology: " + e.getMessage());
        }

        // No declared operations ⇒ run-sugar.
        if (topology.operations().isEmpty()) {
            // A loop iterates the CHAIN. Run-sugar has no chain — its `run` body
            // owns its own control flow, so a declared loop mode has nothing to
            // iterate and is a contradiction, not a no-op.
            if (probe.loopMode() != com.flezha.sdk.bounded.LoopMode.ONCE) {
                throw BoundedOperativeConfigurationException.loopWithoutChain(factoryId);
            }
            return new BoundedSpec(
                    factoryId,
                    operativeClass,
                    Optional.empty(),
                    List.of(),
                    probe.maxDuration(),
                    probe.rollbackMode(),
                    probe.persistenceEnabled(),
                    com.flezha.sdk.bounded.LoopMode.ONCE,
                    java.util.Optional.empty(),
                    resolveRetry(factoryId, probe));
        }

        List<Integer> chain = linearChain(factoryId, topology);

        // A durable class journals its inter-op handoff at every op barrier, so
        // every inter-op channel needs a codec. An opted-out class re-runs from
        // the top by design and needs none.
        if (probe.persistenceEnabled()) {
            for (ChannelDescriptor channel : topology.channels()) {
                boolean interOp = channel.kind() == com.flezha.sdk.channel.ChannelKind.INTER_OPERATION
                        && !channel.consumers().isEmpty();
                if (interOp && channel.interOpCodec().isEmpty()) {
                    throw BoundedOperativeConfigurationException.missingInterOpCodec(factoryId, channel.name());
                }
            }
        }

        // A durable CHAINED loop re-seeds each iteration from the terminal op's
        // journaled handoff. With no codec there it would re-seed EMPTY on a
        // crash-resume — which the loop reads as "converged" and stops, silently
        // truncating the run. Refuse it at registration instead.
        if (probe.loopMode() == com.flezha.sdk.bounded.LoopMode.CHAINED && probe.persistenceEnabled()) {
            int terminalOp = chain.get(chain.size() - 1);
            boolean terminalHasCodec = topology.channels().stream()
                    .anyMatch(c -> c.producer().isPresent()
                            && c.producer().getAsInt() == terminalOp
                            && c.interOpCodec().isPresent());
            if (!terminalHasCodec) {
                throw BoundedOperativeConfigurationException.loopTerminalWithoutCodec(factoryId);
            }
        }

        return new BoundedSpec(
                factoryId,
                operativeClass,
                Optional.of(topology),
                chain,
                probe.maxDuration(),
                probe.rollbackMode(),
                probe.persistenceEnabled(),
                probe.loopMode(),
                probe.wakeAfter(),
                resolveRetry(factoryId, probe));
    }

    /** Resolve the retry ladder, rejecting a hot-spinning zero backoff. */
    private static com.flezha.sdk.channel.RetryPolicy resolveRetry(String factoryId, BoundedOperative probe) {
        int attempts = Math.max(1, probe.retryAttempts());
        long backoffMs = probe.retryBackoff().toMillis();
        if (attempts > 1 && backoffMs <= 0) {
            throw BoundedOperativeConfigurationException.zeroRetryBackoff(factoryId);
        }
        return new com.flezha.sdk.channel.RetryPolicy(attempts, backoffMs);
    }

    /**
     * Resolve the linear execution order of a structured bounded's operations
     * and reject anything that is not a single connected chain (no entry
     * channel, branching, dynamic segmentation, cycles, disconnected
     * operations). Operation ids are indices into {@code topology.operations()};
     * a channel's {@code consumers()} are downstream operation ids.
     */
    private static List<Integer> linearChain(String factoryId, UnboundedOperativeTopology topology) {
        // The entry channel seeds the first operation(s) from the spawn args.
        ChannelDescriptor entry = null;
        for (ChannelDescriptor c : topology.channels()) {
            if (c.kind() == ChannelKind.ENTRY) {
                entry = c;
                break;
            }
        }
        if (entry == null) {
            throw new BoundedOperativeConfigurationException("bounded operative `" + factoryId
                    + "` declares operations but no entry channel; a structured bounded must declare one input "
                    + "channel (c.input(...)) to seed its first operation");
        }
        if (entry.consumers().isEmpty()) {
            throw new BoundedOperativeConfigurationException(
                    "bounded operative `" + factoryId + "` declares an entry channel with no consumer");
        }

        // Breadth-first from the entry. BFS order IS topological here — an
        // operation is only enqueued once every producer feeding it has been
        // visited — which is why branching needs NO change to the compensation
        // planner: completion order is already topological, and the planner
        // already reverses it.
        List<Integer> order = new ArrayList<>(topology.operations().size());
        Set<Integer> seen = new HashSet<>();
        java.util.Deque<Integer> queue = new java.util.ArrayDeque<>(entry.consumers());

        while (!queue.isEmpty()) {
            int current = queue.poll();
            if (!seen.add(current)) {
                // Fan-in: two producers feed this operation. Not an error — it just
                // means it was already scheduled. (Silently OVERWRITING such an edge
                // is the bug branching has to avoid.)
                continue;
            }
            order.add(current);

            OperationDescriptor op = topology.operation(current);
            // Bounded has no dynamic-segments capability.
            if (op.operationKeyOverridden()) {
                throw new BoundedOperativeConfigurationException("bounded operative `" + factoryId
                        + "` declares a dynamic-segment operation `" + op.name()
                        + "`; bounded operatives do not support dynamic segmentation");
            }

            if (op.outputChannelId().isPresent()) {
                ChannelDescriptor ch = topology.channel(op.outputChannelId().getAsInt());
                if (ch != null) {
                    for (int consumer : ch.consumers()) {
                        if (consumer == current) {
                            throw branching(factoryId, "operation `" + op.name() + "` (self-cycle)");
                        }
                        queue.add(consumer);
                    }
                }
            }
        }

        if (order.size() != topology.operations().size()) {
            String disconnected = "<unknown>";
            for (int i = 0; i < topology.operations().size(); i++) {
                if (!seen.contains(i)) {
                    disconnected = topology.operations().get(i).name();
                    break;
                }
            }
            throw new BoundedOperativeConfigurationException("bounded operative `" + factoryId
                    + "` declares operation `" + disconnected
                    + "` that is not reachable from the entry channel");
        }

        return order;
    }

    private static BoundedOperativeConfigurationException branching(String factoryId, String at) {
        return new BoundedOperativeConfigurationException(
                "bounded operative `" + factoryId
                        + "` declares a branching topology at " + at
                        + "; bounded v1 supports a single linear chain only (each stage feeds exactly one downstream operation)");
    }
}
