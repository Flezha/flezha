package com.flezha.sdk.runtime.bounded;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeConfigurationException;
import com.flezha.sdk.bounded.RollbackMode;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.ProcedureDescriptor;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.operative.builder.UnboundedOperativeTopologyBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Frozen per-class authoring result for a {@link BoundedOperative}, produced and
 * validated once at registration and reused for every spawn of the class.
 * Mirrors the Rust SDK's {@code BoundedSpec}: {@link #topology} is empty for the
 * bare {@code run}-sugar form, present (with the ordered {@link #chain}) for the
 * structured DAG form.
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

    /** Procedures in execution order (entry consumer → … → terminal). Empty for {@code run}-sugar. */
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
     * The failing-entity retry ladder. Resolved onto the SAME
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

    /** True when this spec runs the bare {@code run} body (no declared procedures). */
    public boolean isRunSugar() {
        return topology.isEmpty();
    }

    /**
     * Build and validate the spec for a bounded class. An empty
     * {@link BoundedOperative#procedures} selects the {@code run}-sugar path; a
     * non-empty declaration is validated as a single entry-rooted DAG.
     *
     * @throws BoundedOperativeConfigurationException on invalid authoring
     *     (external-stop-only, branching, cycle, dynamic entity mode,
     *     disconnected procedure, or no entry channel).
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
            topology = UnboundedOperativeTopologyBuilder.build(factoryId, probe::procedures);
        } catch (UnboundedOperativeConfigurationException e) {
            // A structured bounded reuses the unbounded topology builder, which
            // rejects dynamic-entity procedures and unwired channels.
            throw new BoundedOperativeConfigurationException(
                    "bounded operative `" + factoryId + "` has an invalid topology: " + e.getMessage());
        }

        // No declared procedures ⇒ run-sugar.
        if (topology.procedures().isEmpty()) {
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

        List<Integer> chain = topologicalOrder(factoryId, topology);

        // A durable class journals its inter-proc handoff at every proc barrier, so
        // every inter-proc channel needs a codec. An opted-out class re-runs from
        // the top by design and needs none.
        if (probe.persistenceEnabled()) {
            for (ChannelDescriptor channel : topology.channels()) {
                boolean interProc = channel.kind() == com.flezha.sdk.channel.ChannelKind.INTER_PROCEDURE
                        && !channel.consumers().isEmpty();
                if (interProc && channel.interProcCodec().isEmpty()) {
                    throw BoundedOperativeConfigurationException.missingInterProcCodec(factoryId, channel.name());
                }
            }
        }

        // A durable CHAINED loop re-seeds each iteration from the terminal proc's
        // journaled handoff. With no codec there it would re-seed EMPTY on a
        // crash-resume — which the loop reads as "converged" and stops, silently
        // truncating the run. Refuse it at registration instead.
        if (probe.loopMode() == com.flezha.sdk.bounded.LoopMode.CHAINED && probe.persistenceEnabled()) {
            int terminalProc = chain.get(chain.size() - 1);
            boolean terminalHasCodec = topology.channels().stream()
                    .anyMatch(c -> c.producer().isPresent()
                            && c.producer().getAsInt() == terminalProc
                            && c.interProcCodec().isPresent());
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
     * Resolve the topological execution order of a structured bounded's DAG
     * and reject anything outside the shared shape rule (no entry channel,
     * multi-consumer entry, dynamic entity mode, cycles, disconnected
     * procedures). Procedure ids are indices into {@code topology.procedures()};
     * a channel's {@code consumerIds()} are downstream procedure ids.
     */
    private static List<Integer> topologicalOrder(String factoryId, UnboundedOperativeTopology topology) {
        // The entry channel seeds the first procedure(s) from the spawn args.
        ChannelDescriptor entry = null;
        for (ChannelDescriptor c : topology.channels()) {
            if (c.kind() == ChannelKind.ENTRY) {
                entry = c;
                break;
            }
        }
        if (entry == null) {
            throw new BoundedOperativeConfigurationException("bounded operative `" + factoryId
                    + "` declares procedures but no entry channel; a structured bounded must declare one input "
                    + "channel (c.input(...)) to seed its first procedure");
        }
        if (entry.consumers().isEmpty()) {
            throw new BoundedOperativeConfigurationException(
                    "bounded operative `" + factoryId + "` declares an entry channel with no consumer");
        }
        // The seed is a bare payload naming no channel — the spawn args, or a
        // PER_INPUT loop's inbox item — so a merge mapping on an entry edge
        // could never be selected and every entry channel past the first would
        // be unreadable. Refuse the shape rather than accept one that cannot
        // run (`multi-input-procedures`). Mirrors Rust's MergeFromEntryChannel.
        for (ChannelDescriptor ch : topology.channels()) {
            if (ch.kind() != ChannelKind.ENTRY) {
                continue;
            }
            for (ConsumerEdge edge : ch.consumers()) {
                if (edge.map().isPresent()) {
                    throw new BoundedOperativeConfigurationException("MergeFromEntryChannel: bounded operative `"
                            + factoryId + "` merges entry channel `" + ch.name() + "` into procedure `"
                            + topology.procedures().get(edge.procedure()).name()
                            + "`; a bounded operative is seeded from its spawn args (and, for a PER_INPUT loop, its"
                            + " inbox items) through ONE entry channel, and neither carries the channel a payload"
                            + " arrived on — so an entry channel cannot be one merge source among several. Merge the"
                            + " output streams of two or more of this operative's procedures instead");
                }
            }
        }

        // The shared graph-shape walk (GraphShape, the same mechanism the
        // unbounded validation runs): Kahn order makes the driver's sequential
        // walk dispatch a join only after ALL its producers, with the
        // accumulated batch — the former BFS enqueued a join on its FIRST
        // producer. Kahn-FIFO equals BFS numbering for a tree, so journaled
        // proc_idx values keep their meaning. Reachability is named before the
        // cycle so a genuinely disconnected procedure is not reported as a
        // cycle symptom.
        int n = topology.procedures().size();
        List<int[]> edges = new ArrayList<>();
        for (ChannelDescriptor ch : topology.channels()) {
            if (ch.producer().isPresent()) {
                for (int consumer : ch.consumerIds()) {
                    edges.add(new int[] {ch.producer().getAsInt(), consumer});
                }
            }
        }
        com.flezha.sdk.runtime.operative.builder.GraphShape.Order shape =
                com.flezha.sdk.runtime.operative.builder.GraphShape.order(n, edges, entry.consumerIds());
        if (shape.unreachable() >= 0 && shape.onCycle() < 0) {
            throw new BoundedOperativeConfigurationException("bounded operative `" + factoryId
                    + "` declares procedure `"
                    + topology.procedures().get(shape.unreachable()).name()
                    + "` that is not reachable from the entry channel");
        }
        if (shape.onCycle() >= 0) {
            throw new BoundedOperativeConfigurationException("bounded operative `" + factoryId
                    + "` declares procedure `"
                    + topology.procedures().get(shape.onCycle()).name()
                    + "` on a cycle; a bounded topology is a DAG — a procedure's output may never"
                    + " reach its own input");
        }
        for (ProcedureDescriptor proc : topology.procedures()) {
            // Bounded has no dynamic-entities capability.
            if (proc.entityKeyOverridden()) {
                throw new BoundedOperativeConfigurationException("bounded operative `" + factoryId
                        + "` declares a dynamic-entity procedure `" + proc.name()
                        + "`; bounded operatives do not support dynamic entity mode");
            }
        }

        return shape.order();
    }
}
