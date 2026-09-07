package com.flezha.sdk.runtime.workflow;

import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.channel.RetryPolicy;
import com.flezha.sdk.operative.LoopMode;
import com.flezha.sdk.operative.RollbackMode;
import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import com.flezha.sdk.operative.WorkflowOperative;
import com.flezha.sdk.operative.WorkflowOperativeConfigurationException;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.ProcedureDescriptor;
import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.runtime.operative.builder.GraphShape;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Frozen per-class authoring result for a {@link WorkflowOperative}, produced and
 * validated once at registration and reused for every spawn of the class.
 * Mirrors the Rust SDK's {@code WorkflowSpec}: {@link #topology} is empty for the
 * bare {@code run}-sugar form, present (with the ordered {@link #chain}) for the
 * structured DAG form.
 */
public final class WorkflowSpec {

    private final String factoryId;
    private final Class<? extends WorkflowOperative> operativeClass;
    private final Optional<StreamOperativeTopology> topology;
    private final List<Integer> chain;
    private final Optional<Duration> maxDuration;
    private final RollbackMode rollbackMode;
    private final boolean persistenceEnabled;
    private final boolean syncDurability;
    private final LoopMode loopMode;
    private final RetryPolicy retry;
    private final int barrierRetryAttempts;
    private final Optional<Duration> wakeAfter;

    /**
     * Every value but the shape is read off the probe here, so the two build
     * arms cannot record a class's declarations differently.
     */
    private WorkflowSpec(
            Class<? extends WorkflowOperative> operativeClass,
            WorkflowOperative probe,
            Optional<StreamOperativeTopology> topology,
            List<Integer> chain,
            LoopMode loopMode,
            Optional<Duration> wakeAfter) {
        this.factoryId = probe.factoryId();
        this.operativeClass = operativeClass;
        this.topology = topology;
        this.chain = List.copyOf(chain);
        this.loopMode = loopMode;
        this.wakeAfter = wakeAfter;
        this.maxDuration = probe.maxDuration();
        this.rollbackMode = probe.rollbackMode();
        this.persistenceEnabled = probe.persistenceEnabled();
        this.syncDurability = probe.syncDurability();
        this.retry = resolveRetry(this.factoryId, probe);
        this.barrierRetryAttempts = Math.max(1, probe.barrierRetryAttempts());
    }

    @Nonnull
    public String factoryId() {
        return factoryId;
    }

    @Nonnull
    public Class<? extends WorkflowOperative> operativeClass() {
        return operativeClass;
    }

    /** Structured topology, or empty for the {@code run}-sugar form. */
    @Nonnull
    public Optional<StreamOperativeTopology> topology() {
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

    /**
     * The failing-entity retry ladder. Resolved onto the SAME
     * {@link RetryPolicy} the stream dead-letter ladder uses, so both share one
     * backoff implementation.
     */
    @Nonnull
    public RetryPolicy retry() {
        return retry;
    }

    /** Sends of one commit barrier before an unacked commit fails the run (at least 1). */
    public int barrierRetryAttempts() {
        return barrierRetryAttempts;
    }

    /** How many times this class runs its chain. */
    @Nonnull
    public LoopMode loopMode() {
        return loopMode;
    }

    /** How long a parked instance sleeps before the controller wakes it. */
    @Nonnull
    public Optional<Duration> wakeAfter() {
        return wakeAfter;
    }

    /** Class-level persistence toggle; a structured workflow's Changes are flushed to the WAL. */
    public boolean persistenceEnabled() {
        return persistenceEnabled;
    }

    /**
     * Synchronous remote durability: the controller ships each commit barrier
     * to its durability backend before acknowledging it (RPO 0). Inert unless
     * the controller has a backend configured.
     */
    public boolean syncDurability() {
        return syncDurability;
    }

    /** True when this spec runs the bare {@code run} body (no declared procedures). */
    public boolean isRunSugar() {
        return topology.isEmpty();
    }

    /**
     * Build and validate the spec for a workflow class. An empty
     * {@link WorkflowOperative#procedures} selects the {@code run}-sugar path; a
     * non-empty declaration is validated as a single entry-rooted DAG.
     *
     * @throws WorkflowOperativeConfigurationException on invalid authoring
     *     (external-stop-only, branching, cycle, dynamic entity mode,
     *     disconnected procedure, or no entry channel).
     */
    @Nonnull
    public static WorkflowSpec build(@Nonnull Class<? extends WorkflowOperative> operativeClass) {
        WorkflowOperative probe = resolveProbe(operativeClass);
        String factoryId = probe.factoryId();

        StreamOperativeTopology topology;
        try {
            topology = StreamOperativeTopologyBuilder.build(factoryId, probe::procedures);
        } catch (StreamOperativeConfigurationException e) {
            // A structured workflow reuses the unbounded topology builder, which
            // rejects dynamic-entity procedures and unwired channels.
            throw new WorkflowOperativeConfigurationException(
                    "workflow operative `" + factoryId + "` has an invalid topology: " + e.getMessage());
        }

        return topology.procedures().isEmpty()
                ? buildRunSugar(operativeClass, probe, factoryId)
                : buildStructured(operativeClass, probe, factoryId, topology);
    }

    /**
     * Instantiate the class and refuse an operative that cannot terminate: one
     * whose only possible exit is an external stop, with no duration backstop.
     */
    @Nonnull
    private static WorkflowOperative resolveProbe(Class<? extends WorkflowOperative> operativeClass) {
        WorkflowOperative probe;
        try {
            probe = operativeClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "WorkflowOperative class `" + operativeClass.getName() + "` must have a public no-arg constructor: "
                            + e,
                    e);
        }
        String factoryId = probe.factoryId();
        if (factoryId == null || factoryId.isEmpty()) {
            throw new IllegalArgumentException(
                    "WorkflowOperative class `" + operativeClass.getName() + "` returned null/empty from factoryId()");
        }
        if (!probe.selfTerminating() && probe.maxDuration().isEmpty()) {
            throw new WorkflowOperativeConfigurationException("workflow operative `" + factoryId
                    + "` is not terminable: its only possible exit is an external stop. Add a terminal condition — "
                    + "let run()/the chain complete, give the loop a terminal predicate, or declare maxDuration().");
        }
        return probe;
    }

    /**
     * The bare {@code run} body. A loop iterates the CHAIN and run-sugar has
     * none — its body owns its own control flow, so a declared loop mode has
     * nothing to iterate and is a contradiction, not a no-op.
     */
    @Nonnull
    private static WorkflowSpec buildRunSugar(
            Class<? extends WorkflowOperative> operativeClass, WorkflowOperative probe, String factoryId) {
        if (probe.loopMode() != LoopMode.ONCE) {
            throw WorkflowOperativeConfigurationException.loopWithoutChain(factoryId);
        }
        return new WorkflowSpec(operativeClass, probe, Optional.empty(), List.of(), LoopMode.ONCE, Optional.empty());
    }

    /** The declared DAG: its ordered chain, and the codecs durability needs. */
    @Nonnull
    private static WorkflowSpec buildStructured(
            Class<? extends WorkflowOperative> operativeClass,
            WorkflowOperative probe,
            String factoryId,
            StreamOperativeTopology topology) {
        List<Integer> chain = chainOrder(factoryId, topology, entryChannelOf(factoryId, topology));
        requireInterProcCodecs(factoryId, probe, topology, chain);
        return new WorkflowSpec(
                operativeClass, probe, Optional.of(topology), chain, probe.loopMode(), probe.wakeAfter());
    }

    /**
     * A durable class journals its inter-proc handoff at every proc barrier, so
     * every inter-proc channel needs a codec; an opted-out class re-runs from
     * the top by design and needs none. A durable CHAINED loop additionally
     * re-seeds each iteration from the terminal proc's journaled handoff: with
     * no codec there it re-seeds EMPTY on a crash-resume, which the loop reads
     * as "converged" and stops, silently truncating the run.
     */
    private static void requireInterProcCodecs(
            String factoryId, WorkflowOperative probe, StreamOperativeTopology topology, List<Integer> chain) {
        if (!probe.persistenceEnabled()) {
            return;
        }
        for (ChannelDescriptor channel : topology.channels()) {
            boolean interProc = channel.kind() == ChannelKind.INTER_PROCEDURE
                    && !channel.consumers().isEmpty();
            if (interProc && channel.interProcCodec().isEmpty()) {
                throw WorkflowOperativeConfigurationException.missingInterProcCodec(factoryId, channel.name());
            }
        }
        if (probe.loopMode() != LoopMode.CHAINED) {
            return;
        }
        int terminalProc = chain.get(chain.size() - 1);
        boolean terminalHasCodec = topology.channels().stream()
                .anyMatch(c -> c.producer().isPresent()
                        && c.producer().getAsInt() == terminalProc
                        && c.interProcCodec().isPresent());
        if (!terminalHasCodec) {
            throw WorkflowOperativeConfigurationException.loopTerminalWithoutCodec(factoryId);
        }
    }

    /** Resolve the retry ladder, rejecting a hot-spinning zero backoff. */
    private static RetryPolicy resolveRetry(String factoryId, WorkflowOperative probe) {
        int attempts = Math.max(1, probe.retryAttempts());
        long backoffMs = probe.retryBackoff().toMillis();
        if (attempts > 1 && backoffMs <= 0) {
            throw WorkflowOperativeConfigurationException.zeroRetryBackoff(factoryId);
        }
        return new RetryPolicy(attempts, backoffMs);
    }

    /**
     * The entry channel that seeds the first procedure(s) from the spawn args.
     * The seed is a bare payload naming no channel — the spawn args, or a
     * PER_INPUT loop's inbox item — and it bypasses the routing seam entirely,
     * so no edge closure declared there could ever be selected: a merge mapping
     * (every entry channel past the first would be unreadable) or a plain
     * transform (it would silently never run). Refuse the shape rather than
     * accept one that cannot run. Mirrors Rust's MergeFromEntryChannel.
     */
    @Nonnull
    private static ChannelDescriptor entryChannelOf(String factoryId, StreamOperativeTopology topology) {
        ChannelDescriptor entry = null;
        for (ChannelDescriptor c : topology.channels()) {
            if (c.kind() == ChannelKind.ENTRY) {
                entry = c;
                break;
            }
        }
        if (entry == null) {
            throw new WorkflowOperativeConfigurationException("workflow operative `" + factoryId
                    + "` declares procedures but no entry channel; a structured workflow must declare one input "
                    + "channel (c.input(...)) to seed its first procedure");
        }
        if (entry.consumers().isEmpty()) {
            throw new WorkflowOperativeConfigurationException(
                    "workflow operative `" + factoryId + "` declares an entry channel with no consumer");
        }
        for (ChannelDescriptor ch : topology.channels()) {
            if (ch.kind() != ChannelKind.ENTRY) {
                continue;
            }
            for (ConsumerEdge edge : ch.consumers()) {
                if (edge.transform().isPresent() || edge.key().isPresent()) {
                    throw new WorkflowOperativeConfigurationException("MergeFromEntryChannel: workflow operative `"
                            + factoryId + "` merges entry channel `" + ch.name() + "` into procedure `"
                            + topology.procedures().get(edge.procedure()).name()
                            + "`; a workflow operative is seeded from its spawn args (and, for a PER_INPUT loop, its"
                            + " inbox items) through ONE entry channel, and neither carries the channel a payload"
                            + " arrived on — so an entry channel cannot be one merge source among several. Merge the"
                            + " output streams of two or more of this operative's procedures instead");
                }
            }
        }
        return entry;
    }

    /**
     * The execution order of the declared DAG, over the shared {@link
     * GraphShape} walk the stream validation also runs: Kahn order makes the
     * driver's sequential walk dispatch a join only after ALL its producers,
     * with the accumulated batch. Kahn-FIFO equals BFS numbering for a tree, so
     * journaled {@code proc_idx} values keep their meaning. Reachability is
     * named before the cycle so a genuinely disconnected procedure is not
     * reported as a cycle symptom. Procedure ids are indices into
     * {@code topology.procedures()}; a channel's {@code consumerIds()} are
     * downstream procedure ids.
     */
    @Nonnull
    private static List<Integer> chainOrder(
            String factoryId, StreamOperativeTopology topology, ChannelDescriptor entry) {
        List<int[]> edges = new ArrayList<>();
        for (ChannelDescriptor ch : topology.channels()) {
            if (ch.producer().isPresent()) {
                for (int consumer : ch.consumerIds()) {
                    edges.add(new int[] {ch.producer().getAsInt(), consumer});
                }
            }
        }
        GraphShape.Order shape = GraphShape.order(topology.procedures().size(), edges, entry.consumerIds());
        if (shape.unreachable() >= 0 && shape.onCycle() < 0) {
            throw new WorkflowOperativeConfigurationException("workflow operative `" + factoryId
                    + "` declares procedure `"
                    + topology.procedures().get(shape.unreachable()).name()
                    + "` that is not reachable from the entry channel");
        }
        if (shape.onCycle() >= 0) {
            throw new WorkflowOperativeConfigurationException("workflow operative `" + factoryId
                    + "` declares procedure `"
                    + topology.procedures().get(shape.onCycle()).name()
                    + "` on a cycle; a bounded topology is a DAG — a procedure's output may never"
                    + " reach its own input");
        }
        for (ProcedureDescriptor proc : topology.procedures()) {
            // Workflow has no dynamic-entities capability.
            if (proc.dynamic()) {
                throw new WorkflowOperativeConfigurationException("workflow operative `" + factoryId
                        + "` declares a dynamic-entity procedure `" + proc.name()
                        + "`; workflow operatives do not support dynamic entity mode");
            }
        }
        return shape.order();
    }
}
