package com.flezha.sdk.runtime.bounded;

import com.flezha.sdk.bounded.BoundedLoopBody;
import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeCtx;
import com.flezha.sdk.bounded.BoundedOperativeException;
import com.flezha.sdk.bounded.LoopMode;
import com.flezha.sdk.bounded.RollbackMode;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.channel.RetryPolicy;
import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.proto.BoundedCompleted;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.IterCommit;
import com.flezha.sdk.proto.IterPark;
import com.flezha.sdk.proto.ProcCommit;
import com.flezha.sdk.proto.ProcCommittedProc;
import com.flezha.sdk.proto.ProcHandoffItem;
import com.flezha.sdk.proto.ProcJournalState;
import com.flezha.sdk.proto.ProcOutboxItem;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.bounded.BoundedCompensation.EntityRun;
import com.flezha.sdk.runtime.bounded.BoundedCompensation.StageRecord;
import com.flezha.sdk.runtime.control.AckRegistry;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.ProcedureDescriptor;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.state.DlqPark;
import com.flezha.sdk.runtime.state.EntityFlusher;
import com.flezha.sdk.runtime.state.EntityId;
import com.flezha.sdk.runtime.state.EntityStateRestore;
import com.flezha.sdk.runtime.state.EntityStateSink;
import com.flezha.sdk.runtime.state.FlusherEntry;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sequential bounded driver. One driver runs per started bounded instance on a
 * <strong>dedicated thread</strong> (not a worker-pool thread): a {@code run}-sugar
 * body may block on its inbox during an interactive loop, and the structured
 * path joins pool entity sub-tasks — running the driver on the shared pool
 * would tie up a worker indefinitely and a single-thread pool would self-deadlock.
 * The driver:
 *
 * <ul>
 *   <li>runs the body — {@code run}-sugar ({@link #runSugar}) or the structured
 *       linear chain ({@link #runStructured}: procedures sequential, entities
 *       parallel within a procedure, count-based barrier between them);</li>
 *   <li>observes the lifecycle — the instance's cancellation token (tripped by
 *       {@code BoundedStop} / {@code RemOperative} / a {@code maxDuration}
 *       watcher), the inbox, and {@code ctx.complete};</li>
 *   <li>converges every exit trigger on the single {@code BoundedCompleted}
 *       teardown, emitting status ({@code ok | error | cancelled}) plus the
 *       result payload.</li>
 * </ul>
 *
 * <p>The control surface (cancel token + inbox) is registered on the
 * {@link BoundedOperativeInstance} by {@code AddOperativeHandler} <em>before</em>
 * this driver's thread starts, so an inbox frame or {@code BoundedStop} arriving
 * immediately after {@code AddOperative} is never lost to a startup race.
 *
 * <p>P1 is in-memory: per-entity Changes fold into state via {@code applyChange}
 * but are not persisted (durability + crash-resume land in Phase 3, where the
 * WAL flush wires onto the same {@link EntityId} identity computed here).
 */
public final class BoundedOperativeRunner {

    private static final Logger LOG = LoggerFactory.getLogger(BoundedOperativeRunner.class);

    /** Wire-level payload id for SDK→controller control commands (same as the operative path). */
    private static final String CONTROL_PAYLOAD_ID = "control_command";

    private static final byte[] EMPTY = new byte[0];

    /** Shared daemon scheduler arming per-instance {@code maxDuration} watchers. */
    private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "flezha-bounded-timer");
        t.setDaemon(true);
        return t;
    });

    /**
     * Indirection over {@link NativeTransport#streamWrite(long, long, byte[])}
     * so tests can capture bounded output / completion frames without a real
     * native context. Mirrors {@code ProcedureDispatcher.WIRE_WRITER}.
     */
    public interface WireWriter {
        int write(long handle, long streamId, byte[] frame);
    }

    private static volatile WireWriter wireWriter = NativeTransport::streamWrite;

    /** Test seam: install a capturing writer. */
    public static void setWireWriter(@Nonnull WireWriter writer) {
        wireWriter = writer;
    }

    /** Restore the default {@link NativeTransport#streamWrite} writer. */
    public static void resetWireWriter() {
        wireWriter = NativeTransport::streamWrite;
    }

    private BoundedOperativeRunner() {}

    /**
     * Launch the bounded driver for one instance on a dedicated thread.
     * Fire-and-forget: the thread exits at terminal teardown. The {@code instance}'s
     * control surface (cancel token + inbox) must already be registered (it is —
     * {@code AddOperativeHandler} adds the instance to the registry before
     * calling this).
     */
    public static void spawn(
            @Nonnull RunnerContext ctx,
            @Nonnull BoundedOperativeInstance instance,
            @Nonnull BoundedSpec spec,
            @Nonnull String args) {
        Thread t = new Thread(() -> run(ctx, instance, spec, args), "flezha-bounded-" + instance.instanceId());
        t.setDaemon(true);
        try {
            t.start();
        } catch (Throwable e) {
            // Thread spawn failed (e.g. OOM): report a terminal error so the
            // controller does not wait forever.
            emitCompletion(ctx, instance.instanceId(), "error", "failed to start bounded driver thread: " + e, EMPTY);
        }
    }

    private static void run(RunnerContext ctx, BoundedOperativeInstance instance, BoundedSpec spec, String args) {
        AtomicBoolean finished = new AtomicBoolean(false);

        // maxDuration backstop: trip the cancellation token once it elapses,
        // unless the body finished first.
        ScheduledFuture<?> watcher = null;
        if (spec.maxDuration().isPresent()) {
            long ms = Math.max(0L, spec.maxDuration().get().toMillis());
            watcher = SCHEDULER.schedule(
                    () -> {
                        if (!finished.get()) {
                            instance.cancel();
                        }
                    },
                    ms,
                    TimeUnit.MILLISECONDS);
        }

        Outcome outcome =
                spec.isRunSugar() ? runSugar(ctx, instance, spec, args) : runStructured(ctx, instance, spec, args);

        finished.set(true);
        if (watcher != null) {
            watcher.cancel(false);
        }

        // A parked run is SUSPENDED, not finished: no BoundedCompleted. Emitting
        // one would tear the instance down and lose the iteration it parked at.
        if (outcome.isParked()) {
            return;
        }
        emitCompletion(ctx, instance.instanceId(), outcome.status(), outcome.error(), outcome.result());
    }

    // ---- run-sugar path -----------------------------------------------------

    /** Run-sugar path: a single implicit Procedure/Entity whose body is {@code run}. */
    private static Outcome runSugar(
            RunnerContext ctx, BoundedOperativeInstance instance, BoundedSpec spec, String args) {
        BoundedOperativeCtxImpl ctxImpl = new BoundedOperativeCtxImpl(ctx, instance);
        try {
            BoundedOperative bounded =
                    spec.operativeClass().getDeclaredConstructor().newInstance();
            bounded.run(ctxImpl, args);
            // Precedence: a real failure (caught below) ⇒ error; otherwise a
            // tripped cancellation token ⇒ cancelled; otherwise ok.
            String status = instance.isCancelled() ? "cancelled" : "ok";
            return new Outcome(status, "", ctxImpl.result());
        } catch (BoundedOperativeException e) {
            return new Outcome("error", messageOf(e), ctxImpl.result());
        } catch (Throwable t) {
            LOG.warn("BoundedOperative {} run failed: {}", instance.instanceId(), t.toString());
            return new Outcome("error", messageOf(t), ctxImpl.result());
        }
    }

    // ---- structured path ----------------------------------------------------

    /**
     * Structured path: run the linear chain sequentially. Procedure {@code N+1}
     * starts only when every entity of procedure {@code N} has finished
     * (count-based barrier); entities within a procedure run in parallel on the
     * pool. Inter-procedure output collected at each barrier seeds the next stage.
     */
    private static Outcome runStructured(
            RunnerContext ctx, BoundedOperativeInstance instance, BoundedSpec spec, String args) {
        UnboundedOperativeTopology topology = spec.topology().orElseThrow();
        List<Integer> chain = spec.chain();
        String instanceId = instance.instanceId();

        // Entities were built + wired onto the instance by AddOperativeHandler up
        // front (before the ACK) so the Controller's RestoreInitState frames could
        // reach them. Hold the first Procedure on the crash-resume gate until those
        // restores have landed — so a durable respawn reads persisted state instead
        // of cold-starting. A no-op for a fresh spawn (nothing expected).
        List<List<BoundedEntity>> stageEntities = instance.stageEntities();
        instance.awaitRestore();

        // Crash-resume into an in-flight compensation (saga, §4.3): a non-empty
        // reconstruction means the Controller shipped back compensation-intent
        // markers, so the operative had terminated abnormally and (some of) its
        // undo is outstanding. Finish the remaining hooks — skipping already-done
        // ones — rather than re-running the forward chain, which would redo
        // already-undone work. Empty ⇒ a fresh run / crash mid-forward, re-run below.
        int resumeIter = resumeIter(instance);
        List<StageRecord> resumed = reconstructCompletedFromIntents(instance, chain, stageEntities, resumeIter);
        if (!resumed.isEmpty()) {
            runCompensation(
                    ctx,
                    topology,
                    instanceId,
                    chain,
                    stageEntities,
                    resumed,
                    spec.rollbackMode(),
                    instance,
                    spec.persistenceEnabled(),
                    resumeIter);
            return new Outcome("error", "resumed compensation after crash", EMPTY);
        }

        // Seed the first procedure from the spawn args via the entry channel's typed parser.
        ChannelDescriptor entry = entryChannel(topology);
        Object seed = wrapEntry(entry, args.getBytes(StandardCharsets.UTF_8));
        List<Object> batch = new ArrayList<>();
        batch.add(seed);

        // A loop iterates the SAME chain machinery below; ONCE is the degenerate
        // single-iteration case, so the forward path is unchanged for it.
        if (spec.loopMode() != LoopMode.ONCE) {
            return runLoop(ctx, instance, spec, topology, chain, stageEntities, entry, batch);
        }

        // A rewind's outstanding undo runs BEFORE any forward progress: the
        // controller has decided those committed iterations never happened, and
        // resuming the chain on top of un-undone effects would compound them.
        String undoError = runRewindUndo(ctx, instance, spec, topology, chain, stageEntities);
        if (undoError != null) {
            return new Outcome("error", undoError, EMPTY);
        }

        // ONCE is a single pass, so its one and only iteration is 0.
        IterationOut it = runIteration(ctx, instance, spec, topology, chain, stageEntities, batch, 0);
        if (it.abnormal() == null) {
            return new Outcome("ok", "", EMPTY);
        }
        runCompensation(
                ctx,
                topology,
                instanceId,
                chain,
                stageEntities,
                it.completed(),
                spec.rollbackMode(),
                instance,
                spec.persistenceEnabled(),
                0);
        return it.abnormal();
    }

    /**
     * The iteration a resumed instance runs next: the one AFTER the last the
     * journal recorded committed, or 0 on a cold start. The single definition
     * both the loop and the crash-resume compensation read.
     */
    private static int resumeIter(BoundedOperativeInstance instance) {
        return instance.opJournal().map(j -> j.getIter() + 1).orElse(0);
    }

    /**
     * Run the undo the controller's rewind marker says is still owed.
     *
     * <p>{@code rewind_undo} is DERIVED by the controller — the committed procs of
     * the rewound iterations that have no {@code compensated} record appended
     * after their own commit. So the SDK does not decide what to unwind; it
     * drains a list, which is what makes a crash mid-rewind resume with exactly
     * the outstanding work and no completeness oracle.
     *
     * <p>Two rules that are easy to get wrong:
     *
     * <ul>
     *   <li>The rollback mode is <b>forced</b> all-or-nothing, whatever the class
     *       declares. A rewind unwinds committed iterations by definition, so
     *       "keep progress" is meaningless here.
     *   <li>The per-entity crash-resume done markers are <b>not</b> consulted.
     *       They are per <em>entity</em>, and an entity that ran in two rewound
     *       iterations must be undone once per iteration.
     * </ul>
     *
     * @return an error message, or {@code null} when nothing was owed or the undo ran.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static String runRewindUndo(
            RunnerContext ctx,
            BoundedOperativeInstance instance,
            BoundedSpec spec,
            UnboundedOperativeTopology topology,
            List<Integer> chain,
            List<List<BoundedEntity>> stageEntities) {
        List<ProcCommittedProc> owed =
                instance.opJournal().map(ProcJournalState::getRewindUndoList).orElseGet(List::of);
        if (owed.isEmpty()) {
            return null;
        }

        // Rebuild the completed-stage records the planner consumes. Outputs are
        // empty: a committed proc's collected output was never durable, so a
        // compensable proc must keep its undo handle in State (which IS restored) —
        // the same at-least-once contract the crash-resume undo rests on.
        List<StageRecord> completed = new ArrayList<>();
        for (ProcCommittedProc proc : owed) {
            int stageIdx = proc.getProcIdx();
            if (stageIdx < 0) {
                continue;
            }
            List<EntityRun> ran = new ArrayList<>();
            for (int i = 0; i < stageEntities.get(stageIdx).size(); i++) {
                ran.add(new EntityRun(i, List.of()));
            }
            completed.add(new StageRecord(stageIdx, proc.getIter(), ran));
        }
        if (completed.isEmpty()) {
            return null;
        }

        String instanceId = instance.instanceId();
        LOG.info("bounded `{}`: rewind owes {} proc undo(s); unwinding before resuming", instanceId, completed.size());

        // FORCED all-or-nothing: a rewind unwinds committed iterations by
        // definition, so a "keep progress" declaration is meaningless here. The
        // planner is unchanged — it just reverses.
        List<StageRecord> plan =
                BoundedCompensation.planCompensation(RollbackMode.ALL_OR_NOTHING, List.of(), completed);

        for (StageRecord record : plan) {
            List<BoundedEntity> slots = stageEntities.get(record.stageIdx());
            // A FRESH sink per undone proc: each proc journals exactly ONE
            // `compensated` record, and that record is what drains this proc from
            // `rewind_undo`.
            BoundedEntitySink sink = new BoundedEntitySink(ctx, instanceId, spec.persistenceEnabled());

            // Deliberately does NOT consult the per-entity crash-resume done
            // markers: those are per ENTITY, and an entity that ran in two
            // rewound iterations must be undone once per iteration.
            boolean hasHook = !slots.isEmpty() && slots.get(0).proc().compensateOverridden();
            if (hasHook) {
                for (EntityRun segRun : record.entities()) {
                    compensateEntity(topology, instanceId, slots.get(segRun.entityIndex()), segRun.outputs(), sink);
                }
            }

            // ONE compensated record per undone proc — EVEN AN EMPTY ONE for a
            // hookless proc. Without it that proc is never drained from `rewind_undo`
            // and every later restore re-offers it forever.
            if (spec.persistenceEnabled()) {
                shipCompensationOutbox(ctx, instanceId, record.iter(), sink.staged());
            }
        }
        return null;
    }

    /** Idle hysteresis before a looping instance parks. */
    private static final long PARK_IDLE_MS =
            Long.getLong("flezha.bounded.park.idle.ms", envLong("FLEZHA_BOUNDED_PARK_IDLE_MS", 5_000L));

    private static long envLong(String name, long fallback) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * The durable loop: iterate the SAME chain machinery, committing at every
     * iteration boundary and suspending when idle.
     *
     * <p>A committed iteration is durable, so a crash resumes at the NEXT one
     * rather than replaying the loop. When the loop runs dry it ships
     * {@code IterPark} and exits WITHOUT {@code BoundedCompleted} — a parked
     * instance is suspended, not finished, and the controller keeps it in the
     * registry so a later message (or a wake timer) revives it.
     */
    private static Outcome runLoop(
            RunnerContext ctx,
            BoundedOperativeInstance instance,
            BoundedSpec spec,
            UnboundedOperativeTopology topology,
            List<Integer> chain,
            List<List<BoundedEntity>> stageEntities,
            ChannelDescriptor entry,
            List<Object> firstSeed) {
        String instanceId = instance.instanceId();
        // Resume at the iteration AFTER the last committed one.
        int iter = resumeIter(instance);
        List<Object> seed = firstSeed;

        while (!instance.isCancelled()) {
            if (spec.loopMode() == LoopMode.PER_INPUT) {
                byte[] item = awaitInboxItem(instance);
                if (item == null) {
                    // Idle past the hysteresis: suspend rather than spin or finish.
                    return park(ctx, instance, spec, iter);
                }
                seed = new ArrayList<>();
                seed.add(wrapEntry(entry, item));
            }

            IterationOut it = runIteration(ctx, instance, spec, topology, chain, stageEntities, seed, iter);
            if (it.abnormal() != null) {
                runCompensation(
                        ctx,
                        topology,
                        instanceId,
                        chain,
                        stageEntities,
                        it.completed(),
                        spec.rollbackMode(),
                        instance,
                        spec.persistenceEnabled(),
                        iter);
                return it.abnormal();
            }

            if (spec.persistenceEnabled()) {
                String err = commitIteration(ctx, instance, topology, iter, it.terminalBatch(), chain, stageEntities);
                if (err != null) {
                    return new Outcome("error", err, EMPTY);
                }
            }
            iter++;

            if (spec.loopMode() == LoopMode.CHAINED) {
                // The next iteration is seeded by this one's terminal handoff. An
                // empty handoff means the chain converged: the loop is done.
                if (it.terminalBatch().isEmpty()) {
                    return new Outcome("ok", "", EMPTY);
                }
                seed = it.terminalBatch();
            }
        }
        return new Outcome("cancelled", "", EMPTY);
    }

    /** Block for an inbox item up to the park hysteresis. {@code null} ⇒ the loop is idle. */
    private static byte[] awaitInboxItem(BoundedOperativeInstance instance) {
        try {
            return instance.inbox().poll(PARK_IDLE_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** Journal the iteration boundary: the consumed inbox offset and the terminal handoff. */
    private static String commitIteration(
            RunnerContext ctx,
            BoundedOperativeInstance instance,
            UnboundedOperativeTopology topology,
            int iter,
            List<Object> terminalBatch,
            List<Integer> chain,
            List<List<BoundedEntity>> stageEntities) {
        // The terminal handoff is what re-seeds a crash-resumed CHAINED iteration.
        List<BoundedEntity> terminal = stageEntities.get(chain.size() - 1);
        Optional<ChannelDescriptor> out =
                terminal.isEmpty() ? Optional.empty() : terminal.get(0).outputChannel();

        String instanceId = instance.instanceId();
        IterCommit commit = IterCommit.newBuilder()
                .setOperativeId(instanceId)
                .setIter(iter)
                .setConsumedOffset(instance.consumedInboxOffset())
                .addAllHandoff(encodeHandoff(topology, out, terminalBatch))
                .build();

        String referenceId = "iter-commit-" + instanceId + "-" + iter + "-" + System.nanoTime();
        return shipAndAwait(
                ctx,
                referenceId,
                CommandRequest.newBuilder()
                        .setReferenceId(referenceId)
                        .setIterCommit(commit)
                        .build(),
                "IterCommit for iteration " + iter);
    }

    /**
     * Suspend the instance: ship {@code IterPark} and exit WITHOUT
     * {@code BoundedCompleted}.
     *
     * <p>That distinction is the whole of suspension. A completion tears the
     * instance down; a park leaves it registered with zero runner resources,
     * so the next message — or the wake timer — revives it at the iteration it
     * left off.
     */
    private static Outcome park(RunnerContext ctx, BoundedOperativeInstance instance, BoundedSpec spec, int iter) {
        String instanceId = instance.instanceId();
        IterPark.Builder parkMsg =
                IterPark.newBuilder().setOperativeId(instanceId).setIter(iter);
        spec.wakeAfter().ifPresent(d -> parkMsg.setWakeDeadlineMs(System.currentTimeMillis() + d.toMillis()));

        sendControl(
                ctx,
                CommandRequest.newBuilder()
                        .setReferenceId("iter-park-" + instanceId + "-" + iter + "-" + System.nanoTime())
                        .setIterPark(parkMsg.build())
                        .build());

        LOG.info("bounded `{}`: idle at iteration {}; parked (suspended, not completed)", instanceId, iter);
        return Outcome.parked();
    }

    /** One pass of the chain: its terminal batch, the stages it completed, and how it ended. */
    private record IterationOut(Outcome abnormal, List<Object> terminalBatch, List<StageRecord> completed) {}

    /**
     * Run the chain once. Shared by the single-pass ({@link LoopMode#ONCE}) path
     * and by every iteration of a loop — the loop is an iteration around this,
     * not a second copy of it.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static IterationOut runIteration(
            RunnerContext ctx,
            BoundedOperativeInstance instance,
            BoundedSpec spec,
            UnboundedOperativeTopology topology,
            List<Integer> chain,
            List<List<BoundedEntity>> stageEntities,
            List<Object> seedBatch,
            int iter) {
        String instanceId = instance.instanceId();

        // Each procedure is fed by ITS producer, not by "whatever ran last". With a
        // branch, two procedures share one producer's output; with a fan-in, one
        // procedure is fed by two producers — and those two contributions must
        // ACCUMULATE. Overwriting there silently drops half the input, which is the
        // defect a naive "running batch" carries into a branching graph.
        Map<Integer, List<Object>> inputs = new HashMap<>();
        inputs.put(chain.get(0), new ArrayList<>(seedBatch));
        List<Object> batch = seedBatch;

        // Completed stages of this pass, recorded in completion order so an
        // abnormal exit can roll them back in reverse.
        List<StageRecord> completed = new ArrayList<>();
        // Non-null once the chain terminates abnormally (error / mid-chain stop).
        Outcome abnormal = null;

        // Crash-resume from the proc-journal: a procedure the controller recorded
        // as committed is NOT re-run — its effects are already durable, and
        // re-running it would redo them. The next procedure is seeded from the
        // handoff journaled at that barrier.
        Set<Integer> committedProcs = journaledCommittedProcs(instance);
        if (!committedProcs.isEmpty()) {
            try {
                bankJournaledHandoffs(instance, topology, chain, committedProcs, inputs);
            } catch (BoundedOperativeException e) {
                // Outside the stage loop's own catch, so a mapping that throws
                // while re-seeding is contained here instead of unwinding the
                // driver.
                return new IterationOut(new Outcome("error", messageOf(e), EMPTY), batch, completed);
            }
        }

        for (int stageIdx = 0; stageIdx < chain.size(); stageIdx++) {
            // A cooperative stop before a stage starts is partial progress: stop
            // advancing and roll back what completed.
            if (instance.isCancelled()) {
                abnormal = new Outcome("cancelled", "", EMPTY);
                break;
            }
            if (committedProcs.contains(stageIdx)) {
                LOG.info(
                        "bounded `{}`: proc {} was journaled as committed before the crash; skipping",
                        instanceId,
                        stageIdx);
                continue;
            }
            ProcedureDescriptor proc = topology.procedure(chain.get(stageIdx));
            List<BoundedEntity> segs = stageEntities.get(stageIdx);
            batch = inputs.getOrDefault(chain.get(stageIdx), List.of());
            List<List<Object>> partitioned = partition(proc, segs, batch);
            try {
                StageOut stageOut = runStageParallel(
                        ctx,
                        instanceId,
                        topology,
                        segs,
                        partitioned,
                        spec.persistenceEnabled(),
                        spec.retry(),
                        instance);
                batch = stageOut.batch();

                // Durable saga (§4.3): record this procedure's completion — one
                // compensation-intent marker per entity that ran — so a crash
                // mid-compensation knows what still needs undo. Only for
                // compensable procedures (a hookless proc is never rolled back).
                if (!segs.isEmpty() && segs.get(0).proc().compensateOverridden()) {
                    for (EntityRun segRun : stageOut.entities()) {
                        emitCompensationRecord(
                                EntityStateSink.PAYLOAD_ID_COMPENSATION_INTENT,
                                segs.get(segRun.entityIndex()).identity(),
                                spec.persistenceEnabled());
                    }
                }
                completed.add(new StageRecord(stageIdx, iter, stageOut.entities()));

                // Proc barrier: journal this procedure's completion + handoff and
                // block until it is durable. A durable structured class only.
                if (spec.persistenceEnabled()) {
                    String commitError = commitProcBoundary(
                            ctx,
                            topology,
                            instanceId,
                            stageIdx,
                            iter,
                            segs,
                            stageOut.entities(),
                            batch,
                            segs.isEmpty() ? Optional.empty() : segs.get(0).outputChannel(),
                            stageOut.outbox());
                    if (commitError != null) {
                        abnormal = new Outcome("error", commitError, EMPTY);
                        break;
                    }
                }

                // Release this procedure's output to EVERY consumer of its output
                // channel: a branch feeds several, and a fan-in ACCUMULATES. Assigning
                // instead of accumulating here is the bug branching must avoid — it
                // silently drops one producer's contribution.
                //
                // Ordered AFTER the intent markers, the completion record and the
                // proc barrier — the Rust `run_iteration` ordering. A merge mapping
                // can throw here, and the procedure that produced the batch has
                // already RUN: recording it first is what puts it in the saga's
                // undo plan (and its intent markers on disk) instead of leaving
                // its effects outstanding.
                if (proc.outputChannelId().isPresent()) {
                    ChannelDescriptor outCh =
                            topology.channel(proc.outputChannelId().getAsInt());
                    if (outCh != null) {
                        for (ConsumerEdge edge : outCh.consumers()) {
                            // A merged consumer receives each producer's
                            // contribution through that edge's mapping.
                            extendMapped(inputs.computeIfAbsent(edge.procedure(), k -> new ArrayList<>()), edge, batch);
                        }
                    }
                }
            } catch (BoundedOperativeException e) {
                abnormal = new Outcome("error", messageOf(e), EMPTY);
                break;
            }
        }

        return new IterationOut(abnormal, batch, completed);
    }

    /**
     * Roll back the completed stages of an abnormally-terminated chain. Completed
     * procedures are compensated in reverse order (per
     * {@link BoundedCompensation#planCompensation}) and, within a procedure,
     * once per entity that ran. Procedures without a {@code compensate} hook are
     * skipped. Best-effort: a failing hook is logged and the remaining steps
     * still run, so one broken undo cannot strand the rest. Runs on the driver
     * thread (not the work pool).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void runCompensation(
            RunnerContext ctx,
            UnboundedOperativeTopology topology,
            String instanceId,
            List<Integer> chain,
            List<List<BoundedEntity>> stageEntities,
            List<StageRecord> completed,
            RollbackMode rollbackMode,
            BoundedOperativeInstance instance,
            boolean persistenceEnabled,
            int iter) {
        // v1 runs a single pass, so there are no committed iterations to keep;
        // the structured-loop follow-up threads them in and planCompensation
        // already honours the mode difference.
        List<StageRecord> plan = BoundedCompensation.planCompensation(rollbackMode, List.of(), completed);
        if (plan.isEmpty()) {
            return;
        }
        // Compensation output stages too, and rides its own ProcCommit with
        // `compensating` set — an undo's effect must be as durable as the effect
        // it reverses.
        BoundedEntitySink sink = new BoundedEntitySink(ctx, instanceId, persistenceEnabled);
        for (StageRecord record : plan) {
            List<BoundedEntity> slots = stageEntities.get(record.stageIdx());
            // The hook flag is per procedure; a hookless procedure is skipped.
            if (slots.isEmpty() || !slots.get(0).proc().compensateOverridden()) {
                continue;
            }
            for (EntityRun segRun : record.entities()) {
                BoundedEntity seg = slots.get(segRun.entityIndex());
                // Crash-resume (§4.3): an entity whose compensation-done marker
                // was shipped back already ran its undo — skip it.
                if (instance.hasCompensationDone(seg.identity().encoded())) {
                    continue;
                }
                compensateEntity(topology, instanceId, seg, segRun.outputs(), sink);
                // Durable saga (§4.3): record this compensation's completion so a
                // later crash resumes the remainder and skips this entity.
                emitCompensationRecord(
                        EntityStateSink.PAYLOAD_ID_COMPENSATION_DONE, seg.identity(), persistenceEnabled);
            }
        }

        if (persistenceEnabled && !sink.staged().isEmpty()) {
            shipCompensationOutbox(ctx, instanceId, iter, sink.staged());
        }
    }

    /**
     * Invoke one entity's {@code compensate} hook. The shared EFFECT of both undo
     * passes — the marker-backed crash-resume one and the journal-backed rewind
     * one — which otherwise differ in what they decide to undo.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void compensateEntity(
            UnboundedOperativeTopology topology,
            String instanceId,
            BoundedEntity seg,
            List<Object> outputs,
            BoundedEntitySink sink) {
        BoundedCollectAdapter adapter = new BoundedCollectAdapter(topology, seg.outputChannel(), sink, seg.identity());
        Procedure proc = seg.proc();
        BiConsumer<Object, Change> applyChangeFn = (s, c) -> proc.applyChange(s, c);
        // Compensation Changes record onto the entity's flush buffer and ship to
        // the WAL (durable classes) so a crash mid-compensation leaves the undo
        // recorded.
        List<Change> buffer = seg.changeBuffer();
        StateHandle<Object, Change> handle = StateHandle.create(seg.state(), buffer, applyChangeFn);
        try {
            proc.compensate(outputs, handle, adapter);
        } catch (ProcedureException | RuntimeException e) {
            LOG.warn("bounded {} compensation for proc `{}` failed: {}", instanceId, proc.name(), e.toString());
        } finally {
            seg.flusher().ifPresent(EntityFlusher::flushEntry);
        }
    }

    /** Journal the undo's staged output under a {@code compensating} ProcCommit. */
    private static void shipCompensationOutbox(
            RunnerContext ctx, String instanceId, int iter, List<StagedOutput> staged) {
        // Stamped for the same reason the forward barrier is: the undo of
        // iteration N is a distinct journal entry from the undo of iteration N-1.
        ProcCommit.Builder commit =
                ProcCommit.newBuilder().setOperativeId(instanceId).setIter(iter).setCompensating(true);
        int seq = 0;
        for (StagedOutput out : staged) {
            commit.addOutbox(ProcOutboxItem.newBuilder()
                    .setSeqInProc(seq++)
                    .setChannel(out.channel())
                    .setPayload(ByteString.copyFrom(out.payload()))
                    .build());
        }
        // The undo wait deliberately ignores cancellation: a rollback IS the
        // response to that cancel, so abandoning it would leave the undo unshipped.
        String referenceId = "proc-commit-undo-" + instanceId + "-" + System.nanoTime();
        String error = shipAndAwait(
                ctx,
                referenceId,
                CommandRequest.newBuilder()
                        .setReferenceId(referenceId)
                        .setProcCommit(commit.build())
                        .build(),
                "compensation outbox");
        if (error != null) {
            LOG.error("bounded {}: {} — undo output may be lost", instanceId, error);
        }
    }

    /**
     * Rebuild the completed-stage records to compensate on a crash-resume from
     * the shipped-back compensation-intent markers: an Entity marked "intent"
     * completed forward and needs undo. Outputs are empty — a procedure's
     * collected inter-procedure output was never durable, so a durable
     * compensable procedure must persist its undo handle in State (which IS
     * restored), per the at-least-once author contract. Stages are gathered in
     * chain order; {@link BoundedCompensation#planCompensation} reverses them.
     */
    private static List<StageRecord> reconstructCompletedFromIntents(
            BoundedOperativeInstance instance, List<Integer> chain, List<List<BoundedEntity>> stageEntities, int iter) {
        List<StageRecord> completed = new ArrayList<>();
        for (int stageIdx = 0; stageIdx < chain.size(); stageIdx++) {
            List<BoundedEntity> slots = stageEntities.get(stageIdx);
            List<EntityRun> segs = new ArrayList<>();
            for (int i = 0; i < slots.size(); i++) {
                if (instance.hasCompensationIntent(slots.get(i).identity().encoded())) {
                    segs.add(new EntityRun(i, List.of()));
                }
            }
            if (!segs.isEmpty()) {
                completed.add(new StageRecord(stageIdx, iter, segs));
            }
        }
        return completed;
    }

    /** Ship one saga compensation marker — a no-op for an opted-out (non-durable) class. */
    private static void emitCompensationRecord(String payloadId, EntityId identity, boolean persistenceEnabled) {
        if (persistenceEnabled) {
            EntityStateSink.sendCompensationRecord(payloadId, identity);
        }
    }

    /**
     * Build every stage's Entities for a structured spec, in chain order. Called
     * synchronously by {@code AddOperativeHandler} (before the instance is
     * registered + the driver spawns) so the Entities are reachable by the
     * inbound {@code RestoreInitState} handler and the resume gate is armed before
     * any restore frame — or the driver thread — runs.
     */
    @Nonnull
    public static List<List<BoundedEntity>> buildAllStageEntities(
            @Nonnull UnboundedOperativeTopology topology,
            @Nonnull String instanceId,
            @Nonnull List<Integer> chain,
            boolean persistenceEnabled) {
        List<List<BoundedEntity>> stageEntities = new ArrayList<>(chain.size());
        for (int opId : chain) {
            stageEntities.add(buildStageEntities(topology, instanceId, opId, persistenceEnabled));
        }
        return stageEntities;
    }

    /**
     * Build the entities for one stage: a single entity in single mode, one per
     * declared key in static mode. Uses the same {@link EntityId} identity a
     * unbounded operative would (real procedure name + entity key, no
     * sentinels), so Phase-3 durability rides the same per-Entity identity.
     */
    static List<BoundedEntity> buildStageEntities(
            UnboundedOperativeTopology topology, String instanceId, int opId, boolean persistenceEnabled) {
        ProcedureDescriptor proc = topology.procedure(opId);
        Optional<ChannelDescriptor> outputChannel = proc.outputChannelId().isPresent()
                ? Optional.ofNullable(topology.channel(proc.outputChannelId().getAsInt()))
                : Optional.empty();
        List<BoundedEntity> segs = new ArrayList<>();
        if (proc.entityKeys().isEmpty()) {
            segs.add(buildEntity(instanceId, proc, Optional.empty(), outputChannel, persistenceEnabled));
        } else {
            for (String key : proc.entityKeys()) {
                segs.add(buildEntity(instanceId, proc, Optional.of(key), outputChannel, persistenceEnabled));
            }
        }
        return segs;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static BoundedEntity buildEntity(
            String instanceId,
            ProcedureDescriptor proc,
            Optional<String> key,
            Optional<ChannelDescriptor> outputChannel,
            boolean persistenceEnabled) {
        Procedure procedureInstance = instantiateProcedure(proc.procedureClass());
        Object state = procedureInstance.initState(key, Optional.empty());
        EntityId identity = EntityId.of(instanceId, proc.name(), key);

        // A changeful entity ships its Changes to the WAL on flush; the entry
        // carries the class-level persistence toggle (drain-and-discard when off).
        // Reuses the unbounded flusher's frame + sink path, byte-for-byte.
        List<Change> changeBuffer = new ArrayList<>();
        Optional<FlusherEntry> flusher = Optional.empty();
        if (proc.changeType() != NoChange.class) {
            flusher = Optional.of(new FlusherEntry(
                    new Object(),
                    changeBuffer,
                    // Bounded operatives do not run the effectively-once tier:
                    // their durability contract is the proc-journal, not the
                    // effect record.
                    List.of(),
                    // Nor timers: a bounded operative has its own wake model.
                    List.of(),
                    persistenceEnabled,
                    new AtomicLong(0L),
                    EntityStateSink::send,
                    new HeaderTemplate(),
                    identity,
                    proc.schemaVersion(),
                    new KryoCodec(proc.kryoTypes())));
        }
        return new BoundedEntity(procedureInstance, state, identity, key, outputChannel, changeBuffer, flusher);
    }

    /**
     * Partition an input batch across a stage's entities. Single mode routes
     * every item to entity 0; static mode routes by the procedure's
     * {@code entitiesFor} keys.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static List<List<Object>> partition(
            ProcedureDescriptor proc, List<BoundedEntity> segs, List<Object> batch) {
        List<List<Object>> buckets = new ArrayList<>(segs.size());
        for (int i = 0; i < segs.size(); i++) {
            buckets.add(new ArrayList<>());
        }
        if (proc.entityKeys().isEmpty()) {
            if (!buckets.isEmpty()) {
                buckets.get(0).addAll(batch);
            }
            return buckets;
        }
        Map<String, Integer> keyToIdx = new HashMap<>();
        for (int i = 0; i < proc.entityKeys().size(); i++) {
            keyToIdx.put(proc.entityKeys().get(i), i);
        }
        // entitiesFor is a stateless routing function; any entity's instance works.
        Procedure router = segs.get(0).proc();
        for (Object item : batch) {
            List<String> keys = router.entitiesFor(item);
            for (String key : keys) {
                Integer idx = keyToIdx.get(key);
                if (idx != null) {
                    buckets.get(idx).add(item);
                }
            }
        }
        return buckets;
    }

    /**
     * Run a stage's entities in parallel on the pool and join on a count-based
     * barrier: every dispatched item is processed across all entities, with no
     * entity mid-run, before the collected output is released to the next stage.
     */
    private static StageOut runStageParallel(
            RunnerContext ctx,
            String instanceId,
            UnboundedOperativeTopology topology,
            List<BoundedEntity> segs,
            List<List<Object>> partitioned,
            boolean staging,
            RetryPolicy retry,
            BoundedOperativeInstance instance)
            throws BoundedOperativeException {
        Map<Integer, List<ProcOutboxItem>> outboxByEntity = new java.util.concurrent.ConcurrentHashMap<>();
        List<Future<EntityRun>> futures = new ArrayList<>();
        for (int i = 0; i < segs.size(); i++) {
            List<Object> items = partitioned.get(i);
            if (items.isEmpty()) {
                continue;
            }
            final int segIdx = i;
            BoundedEntity seg = segs.get(i);
            futures.add(ctx.workerPool().submit(() -> {
                EntityOut out = runEntityRetrying(topology, instanceId, ctx, seg, items, staging, retry, instance);
                outboxByEntity.put(segIdx, out.outbox());
                return new EntityRun(segIdx, out.collected());
            }));
        }

        // Wait for every entity (the barrier) before returning, even on
        // failure, so no entity is mid-run when the stage completes. Each
        // entity's outputs are kept under its index for both the next-stage
        // batch and saga rollback.
        List<EntityRun> entities = new ArrayList<>();
        BoundedOperativeException firstErr = null;
        for (Future<EntityRun> f : futures) {
            try {
                entities.add(f.get());
            } catch (ExecutionException ee) {
                if (firstErr == null) {
                    firstErr = asBounded(ee.getCause());
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                if (firstErr == null) {
                    firstErr = new BoundedOperativeException("bounded entity task interrupted", ie);
                }
            }
        }
        if (firstErr != null) {
            throw firstErr;
        }
        // Deterministic next-stage batch: entities in index order (each entity's
        // own item order is preserved by runEntity).
        entities.sort(Comparator.comparingInt(EntityRun::entityIndex));
        List<Object> batch = new ArrayList<>();
        List<ProcOutboxItem> outbox = new ArrayList<>();
        for (EntityRun sr : entities) {
            batch.addAll(sr.outputs());
            outbox.addAll(outboxByEntity.getOrDefault(sr.entityIndex(), List.of()));
        }
        return new StageOut(batch, entities, outbox);
    }

    /**
     * Process one entity's items sequentially (per-entity ordering preserved),
     * running {@code Procedure.run} against the entity's in-memory state via a
     * {@link BoundedCollectAdapter} that collects inter-procedure output and
     * routes plugin-bound output. The Java analog of the Rust dispatcher's
     * {@code run_collecting} path.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static EntityOut runEntity(
            UnboundedOperativeTopology topology,
            String instanceId,
            RunnerContext ctx,
            BoundedEntity seg,
            List<Object> items,
            boolean staging)
            throws BoundedOperativeException {
        BoundedEntitySink sink = new BoundedEntitySink(ctx, instanceId, staging);
        BoundedCollectAdapter adapter = new BoundedCollectAdapter(topology, seg.outputChannel(), sink, seg.identity());
        Procedure proc = seg.proc();
        BiConsumer<Object, Change> applyChangeFn = (s, c) -> proc.applyChange(s, c);
        // Changes record onto the entity's flush buffer; the flush after the
        // loop ships them to the WAL when the class is durable.
        List<Change> buffer = seg.changeBuffer();
        try {
            for (Object item : items) {
                StateHandle<Object, Change> handle = StateHandle.create(seg.state(), buffer, applyChangeFn);
                try {
                    proc.run(item, handle, adapter);
                } catch (ProcedureException e) {
                    // Carry the procedure's own classification through to the retry
                    // ladder: an author who declared the failure permanent must not
                    // have it re-run.
                    String msg = "procedure `" + proc.name() + "` failed: " + e.getMessage();
                    throw e instanceof ProcedureException.NonRetryable
                            ? BoundedOperativeException.nonRetryable(msg)
                            : new BoundedOperativeException(msg, e);
                } catch (RuntimeException e) {
                    throw new BoundedOperativeException("procedure `" + proc.name() + "` threw: " + e, e);
                }
            }
        } finally {
            // Flush regardless of outcome so the WAL reflects the state applied
            // before any failure (at-least-once; the saga compensates a partial run).
            seg.flusher().ifPresent(EntityFlusher::flushEntry);
        }
        // A stage that threw never reaches here — its staged bytes die with the
        // exception, so a failed proc leaks nothing onto the outbox.
        List<ProcOutboxItem> outbox = new ArrayList<>(sink.staged().size());
        int seqInProc = 0;
        for (StagedOutput out : sink.staged()) {
            outbox.add(ProcOutboxItem.newBuilder()
                    .setEntityId(ByteString.copyFrom(seg.identity().encodedBytes()))
                    .setSeqInProc(seqInProc++)
                    .setChannel(out.channel())
                    .setPayload(ByteString.copyFrom(out.payload()))
                    .build());
        }
        return new EntityOut(sink.collected(), outbox);
    }

    /** One entity's run: the values it collected for the next stage, plus the output it staged. */
    private record EntityOut(List<Object> collected, List<ProcOutboxItem> outbox) {}

    /**
     * The failing-entity retry ladder: re-run ONLY this entity's batch, with a
     * fresh sink each attempt.
     *
     * <p>Freshness is what makes the retry safe. A failed attempt's staged output
     * is discarded with its sink, so a recovered entity emits its outputs once,
     * not once per attempt. Siblings stay buffered at the barrier throughout —
     * nothing else re-runs.
     *
     * <p>Strictly PRE-commit: this sits inside the stage, before the proc barrier,
     * so a journaled proc is never retried. Backoff reuses the dead-letter ladder's
     * helper ({@code base × 2^(n-1)}, ±50% jitter, capped) rather than a second
     * implementation, and the wait is cancellation-aware so a stop or
     * {@code maxDuration} ends it at once.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static EntityOut runEntityRetrying(
            UnboundedOperativeTopology topology,
            String instanceId,
            RunnerContext ctx,
            BoundedEntity seg,
            List<Object> items,
            boolean staging,
            RetryPolicy retry,
            BoundedOperativeInstance instance)
            throws BoundedOperativeException {
        int maxAttempts = Math.max(1, retry.maxAttempts());
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return runEntity(topology, instanceId, ctx, seg, items, staging);
            } catch (BoundedOperativeException e) {
                boolean last = attempt >= maxAttempts;
                if (e.isNonRetryable() || last || instance.isCancelled()) {
                    throw e;
                }
                DlqPark.RETRIES_TOTAL.incrementAndGet();
                LOG.warn(
                        "bounded `{}` entity `{}` attempt {}/{} failed (retrying): {}",
                        instanceId,
                        seg.identity().encoded(),
                        attempt,
                        maxAttempts,
                        e.getMessage());
                if (!sleepCancellable(retry.backoffFor(attempt), instance)) {
                    throw e;
                }
            }
        }
    }

    /** Sleep in short slices so a stop / maxDuration ends the wait promptly. Returns false if cancelled. */
    private static boolean sleepCancellable(long totalMs, BoundedOperativeInstance instance) {
        long deadline = System.currentTimeMillis() + totalMs;
        while (System.currentTimeMillis() < deadline) {
            if (instance.isCancelled()) {
                return false;
            }
            try {
                Thread.sleep(Math.min(200L, Math.max(1L, deadline - System.currentTimeMillis())));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return !instance.isCancelled();
    }

    private static ChannelDescriptor entryChannel(UnboundedOperativeTopology topology) {
        for (ChannelDescriptor c : topology.channels()) {
            if (c.kind() == ChannelKind.ENTRY) {
                return c;
            }
        }
        // Validated at registration: a structured bounded always has an entry channel.
        throw new IllegalStateException("structured bounded is missing an entry channel");
    }

    /**
     * Decode entry wire bytes via the channel's declared parser (passthrough
     * {@code byte[]}, UTF-8 {@code String}, JSON/custom otherwise) — the same
     * seam the unbounded dispatch path uses.
     */
    private static Object wrapEntry(ChannelDescriptor entry, byte[] bytes) {
        return entry.entryParser()
                .<Object>map(p -> p.apply(bytes))
                .orElseGet(
                        () -> entry.payloadType() == String.class ? new String(bytes, StandardCharsets.UTF_8) : bytes);
    }

    @SuppressWarnings("rawtypes")
    private static Procedure instantiateProcedure(Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass) {
        try {
            return procedureClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "procedure class `" + procedureClass.getName() + "` must have a public no-arg constructor: " + e,
                    e);
        }
    }

    private static BoundedOperativeException asBounded(Throwable cause) {
        if (cause instanceof BoundedOperativeException be) {
            return be;
        }
        return new BoundedOperativeException(cause == null ? "bounded entity task failed" : cause.toString(), cause);
    }

    private static String messageOf(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    // ---- output + completion emit -------------------------------------------

    /**
     * Emit one bounded output payload on a secondary stream: a frame whose
     * payload id is the instance id and whose metadata is empty (best-effort,
     * at-least-once). Shared by {@code ctx.emit} and the structured plugin route.
     */
    static void emitOutput(RunnerContext ctx, String instanceId, byte[] value) {
        long secondaryStreamId = ctx.streamRegistry().anySecondary();
        if (secondaryStreamId == 0L) {
            LOG.warn("bounded {} output dropped: no secondary stream available", instanceId);
            return;
        }
        byte[] frame = FrameEncoder.build(instanceId, value, Map.of());
        int writeStatus = wireWriter.write(ctx.nativeHandle(), secondaryStreamId, frame);
        if (writeStatus != 0) {
            LOG.warn("bounded {} output write failed (status {})", instanceId, writeStatus);
        }
    }

    /** The proc indices the controller journaled as committed before the crash. */
    private static Set<Integer> journaledCommittedProcs(BoundedOperativeInstance instance) {
        return instance.opJournal()
                .map(j -> new HashSet<>(j.getCommittedProcIdxsList()))
                .orElseGet(HashSet::new);
    }

    /**
     * Extend {@code dest} with a producer's released batch, put through the
     * edge's transform when it declares one. Shared by the forward accumulate
     * and the journal re-seed so a resumed chain sees exactly what the live one
     * did; a bare edge — every single-input chain — moves its values straight
     * in.
     *
     * <p>The transform is author code, and unlike every other author callback
     * on a bounded chain it runs on the driver thread rather than inside an
     * entity invocation. Left uncontained, an exception would unwind the driver
     * before it can emit {@code BoundedCompleted} and the instance would be
     * stranded with no terminal frame; rethrowing as
     * {@link BoundedOperativeException} routes it into the chain's existing
     * abort-and-compensate path. Retrying is not offered: an edge closure is
     * required to be pure, so a re-run would throw identically. An edge key
     * never reaches here — a keyed edge makes its consumer dynamic, and
     * {@code BoundedSpec} refuses a dynamic bounded procedure. Mirrors the Rust
     * driver's {@code extend_mapped}.
     */
    private static void extendMapped(List<Object> dest, ConsumerEdge edge, List<Object> batch)
            throws BoundedOperativeException {
        if (edge.transform().isEmpty()) {
            dest.addAll(batch);
            return;
        }
        try {
            for (Object value : batch) {
                dest.addAll(edge.transformValue(value).values());
            }
        } catch (RuntimeException | Error e) {
            throw new BoundedOperativeException("edge transform threw: " + messageOf(e));
        }
    }

    /**
     * Bank each committed producer's journaled handoff into its uncommitted
     * consumers' input batches (the Rust {@code resumed_start} mirror). This is
     * what makes a resume a SKIP rather than a re-run — and what lets a join
     * with one committed and one outstanding producer accumulate the JOURNALED
     * contribution now and the fresh one when the outstanding producer commits.
     */
    private static void bankJournaledHandoffs(
            BoundedOperativeInstance instance,
            UnboundedOperativeTopology topology,
            List<Integer> chain,
            Set<Integer> committedPositions,
            Map<Integer, List<Object>> inputs)
            throws BoundedOperativeException {
        var edges = instance.opJournal().map(j -> j.getProcHandoffsList()).orElseGet(List::of);
        for (var edge : edges) {
            int position = edge.getProcIdx();
            if (!committedPositions.contains(position) || position >= chain.size()) {
                continue;
            }
            ProcedureDescriptor producer = topology.procedure(chain.get(position));
            if (producer.outputChannelId().isEmpty()) {
                continue;
            }
            ChannelDescriptor channel =
                    topology.channel(producer.outputChannelId().getAsInt());
            if (channel == null || channel.interProcCodec().isEmpty()) {
                LOG.warn(
                        "bounded resume: no inter-proc codec on channel `{}`; cannot decode handoff",
                        channel == null ? "<missing>" : channel.name());
                continue;
            }
            List<Object> decoded = new ArrayList<>();
            for (ProcHandoffItem item : edge.getHandoffList()) {
                decoded.add(channel.interProcCodec()
                        .get()
                        .decoder()
                        .apply(item.getPayload().toByteArray()));
            }
            for (ConsumerEdge consumerEdge : channel.consumers()) {
                int consumer = consumerEdge.procedure();
                if (!committedPositions.contains(chain.indexOf(consumer))) {
                    extendMapped(inputs.computeIfAbsent(consumer, k -> new ArrayList<>()), consumerEdge, decoded);
                }
            }
        }
    }

    /** How long the driver waits for the controller to journal + fsync a commit. */
    private static final long PROC_COMMIT_TIMEOUT_MS = 30_000L;

    /** Write one control command to the control stream. Returns the wire status (0 = ok). */
    private static int sendControl(RunnerContext ctx, CommandRequest request) {
        byte[] frame = FrameEncoder.build(CONTROL_PAYLOAD_ID, request.toByteArray(), Map.of());
        return wireWriter.write(ctx.nativeHandle(), ctx.streamRegistry().controlStreamId(), frame);
    }

    /**
     * The driver's durability barrier: ship a control command and BLOCK until the
     * controller says its journal entry is durable.
     *
     * <p>Journal-then-ack is the contract every barrier rests on — the proc commit,
     * the iteration commit, and the compensation record all use this one path.
     * Registering before the write is what makes it race-free: the ack can never
     * outrun the waiter.
     *
     * @return an error message, or {@code null} once the commit is durable
     */
    private static String shipAndAwait(RunnerContext ctx, String referenceId, CommandRequest request, String what) {
        CompletableFuture<String> ack = AckRegistry.register(referenceId);
        int writeStatus = sendControl(ctx, request);
        if (writeStatus != 0) {
            AckRegistry.forget(referenceId);
            return what + " write failed (status=" + writeStatus + ")";
        }
        if (!AckRegistry.await(referenceId, ack, PROC_COMMIT_TIMEOUT_MS)) {
            return what + " was not acknowledged within " + PROC_COMMIT_TIMEOUT_MS + " ms";
        }
        return null;
    }

    /**
     * Encode an inter-procedure handoff for the journal. This blob is what a
     * crash-resumed run re-seeds from instead of re-running the chain, so it is
     * only produced for a channel that declared a codec.
     */
    private static List<ProcHandoffItem> encodeHandoff(
            UnboundedOperativeTopology topology, Optional<ChannelDescriptor> channel, List<Object> batch) {
        if (channel.isEmpty() || channel.get().interProcCodec().isEmpty()) {
            return List.of();
        }
        Integer channelId = topology.lookupChannelId(channel.get().name());
        Function<Object, byte[]> encoder = channel.get().interProcCodec().get().encoder();
        List<ProcHandoffItem> items = new ArrayList<>(batch.size());
        for (Object value : batch) {
            items.add(ProcHandoffItem.newBuilder()
                    .setChannelId(channelId == null ? 0 : channelId)
                    .setPayload(ByteString.copyFrom(encoder.apply(value)))
                    .build());
        }
        return items;
    }

    /**
     * Journal this procedure's completion at the proc barrier and block until the
     * controller says the entry is durable.
     *
     * <p>Journal-then-ack is the whole contract: once the controller replies, a
     * crash-resumed run SKIPS this proc and re-seeds the next one from the handoff
     * journaled here — instead of re-running the chain from the top. A failure or
     * timeout is terminal and rolls back, because continuing past an
     * un-journaled barrier would silently forfeit that guarantee.
     *
     * @return an error message, or {@code null} on success.
     */
    private static String commitProcBoundary(
            RunnerContext ctx,
            UnboundedOperativeTopology topology,
            String instanceId,
            int opIdx,
            int iter,
            List<BoundedEntity> segs,
            List<EntityRun> ran,
            List<Object> handoffBatch,
            Optional<ChannelDescriptor> handoffChannel,
            List<ProcOutboxItem> outbox) {

        // The enclosing iteration is part of the entry's journal identity: the
        // controller keys an outbox-flush marker by (type, proc_idx, iter), so an
        // unstamped commit on a later iteration reads as already-flushed and its
        // staged output is never delivered.
        ProcCommit.Builder commit = ProcCommit.newBuilder()
                .setOperativeId(instanceId)
                .setProcIdx(opIdx)
                .setIter(iter);
        // The transactional outbox: this proc's plugin output becomes durable in
        // the SAME journal record as the proc itself. The controller flushes it
        // after the fsync, so a crash either loses both or neither.
        commit.addAllOutbox(outbox);
        for (EntityRun run : ran) {
            commit.addEntitiesRan(
                    ByteString.copyFrom(segs.get(run.entityIndex()).identity().encodedBytes()));
        }

        // The handoff is what makes the resume a SKIP rather than a re-run.
        commit.addAllHandoff(encodeHandoff(topology, handoffChannel, handoffBatch));

        String referenceId = "proc-commit-" + instanceId + "-" + opIdx + "-" + System.nanoTime();
        return shipAndAwait(
                ctx,
                referenceId,
                CommandRequest.newBuilder()
                        .setReferenceId(referenceId)
                        .setProcCommit(commit.build())
                        .build(),
                "ProcCommit for proc " + opIdx);
    }

    private static void emitCompletion(
            RunnerContext ctx, String instanceId, String status, String error, byte[] result) {
        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("bounded-completed-" + instanceId + "-" + System.nanoTime())
                .setBoundedCompleted(BoundedCompleted.newBuilder()
                        .setOperativeId(instanceId)
                        .setStatus(status)
                        .setError(error)
                        .setResult(ByteString.copyFrom(result))
                        .build())
                .build();
        int writeStatus = sendControl(ctx, request);
        if (writeStatus != 0) {
            LOG.warn("BoundedCompleted write failed for {} (status={})", instanceId, writeStatus);
        }
    }

    // ---- inner types --------------------------------------------------------

    /**
     * Terminal outcome of a driver run: status ({@code ok | error | cancelled}),
     * error text, result payload — or {@link #parked()}, which is NOT terminal.
     *
     * <p>A parked run emits no {@code BoundedCompleted}: the instance is
     * suspended, not finished. Emitting one would tear it down and lose the
     * iteration it was about to resume.
     */
    private record Outcome(String status, String error, byte[] result) {
        static final String PARKED = "parked";

        static Outcome parked() {
            return new Outcome(PARKED, "", EMPTY);
        }

        boolean isParked() {
            return PARKED.equals(status);
        }
    }

    /**
     * One stage's parallel-join result: the inter-procedure {@code batch}
     * released to the next stage, plus the per-entity record (which entities
     * ran, with their collected outputs) retained for a possible saga rollback.
     */
    private record StageOut(List<Object> batch, List<EntityRun> entities, List<ProcOutboxItem> outbox) {}

    /**
     * One bounded per-entity state holder (the lightweight unbounded
     * {@code EntitySlot} analog): the per-entity {@link Procedure} instance,
     * its in-memory state, the {@link EntityId} identity (real proc name + key),
     * and the producer-edge output channel.
     *
     * <p>Public so the {@code RestoreInitState} handler (a different package) can
     * reach a durable bounded Entity by wire identity on crash-resume; its
     * internals stay package-private. Unlike the unbounded {@code EntitySlot},
     * {@code state} carries no lock: the driver is held on the resume gate until
     * every restore has landed, so restore-then-run is serialized.
     */
    public static final class BoundedEntity {
        @SuppressWarnings("rawtypes")
        private final Procedure proc;

        // Non-final: warm-restart (restore) replaces it in place.
        private Object state;
        private final EntityId identity;
        // Entity key (empty in single mode), needed to seed initState on restore.
        private final Optional<String> entityKey;
        private final Optional<ChannelDescriptor> outputChannel;
        // Per-entity flush buffer (the unbounded EntitySlot analog): the
        // StateHandle records Changes here; `flusher` (present iff the procedure
        // is changeful) ships them to the WAL on flush.
        private final List<Change> changeBuffer;
        private final Optional<FlusherEntry> flusher;

        @SuppressWarnings("rawtypes")
        BoundedEntity(
                Procedure proc,
                Object state,
                EntityId identity,
                Optional<String> entityKey,
                Optional<ChannelDescriptor> outputChannel,
                List<Change> changeBuffer,
                Optional<FlusherEntry> flusher) {
            this.proc = proc;
            this.state = state;
            this.identity = identity;
            this.entityKey = entityKey;
            this.outputChannel = outputChannel;
            this.changeBuffer = changeBuffer;
            this.flusher = flusher;
        }

        @SuppressWarnings("rawtypes")
        Procedure proc() {
            return proc;
        }

        Object state() {
            return state;
        }

        @Nonnull
        public EntityId identity() {
            return identity;
        }

        Optional<ChannelDescriptor> outputChannel() {
            return outputChannel;
        }

        List<Change> changeBuffer() {
            return changeBuffer;
        }

        Optional<FlusherEntry> flusher() {
            return flusher;
        }

        /**
         * Warm-restart this Entity in place from the controller's persisted
         * snapshot + WAL change-log — the bounded analog of the unbounded slot
         * restore. Rebuilds the committed state via {@link EntityStateRestore},
         * replaces the in-memory state, and re-arms the flusher's change-seq to
         * {@code terminal + 1} so post-resume appends stay contiguous (the
         * controller then skips a WAL realign). Returns the terminal WAL seq
         * (0 when the change-log is empty). Lock-free by design: the driver is
         * held on {@code BoundedOperativeInstance.awaitRestore} until every
         * restore has landed, so this never races the driver's read.
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        public long restore(
                @Nonnull byte[] snapshot, @Nonnull byte[] changeLog, @Nonnull HeaderTemplate headerTemplate) {
            KryoCodec codec = new KryoCodec(proc.kryoTypes());
            EntityStateRestore.Rebuilt rebuilt =
                    EntityStateRestore.rebuild(proc, codec, entityKey, snapshot, changeLog, headerTemplate);
            this.state = rebuilt.state();
            long terminal = rebuilt.terminalSeq();
            flusher.ifPresent(f -> f.changeSeq().set(terminal + 1));
            return terminal;
        }
    }

    /**
     * Per-entity collecting sink for the structured driver. Inter-procedure
     * output is buffered for the next stage; plugin-bound output is forwarded to
     * the operative's wire (identical to {@code run}-sugar {@code ctx.emit}).
     */
    /** One staged plugin-bound output awaiting its proc barrier. */
    record StagedOutput(String channel, byte[] payload) {}

    private static final class BoundedEntitySink implements BoundedCollectSink {
        private final RunnerContext ctx;
        private final String instanceId;
        private final List<Object> collected = new ArrayList<>();
        private final List<StagedOutput> staged = new ArrayList<>();

        /**
         * Whether this sink stages output for the proc barrier instead of shipping
         * it. True only for a durable structured class: its outputs ride the
         * {@code ProcCommit}'s outbox and become durable with the proc that produced
         * them. Run-sugar has no barrier to stage against, and an opted-out class
         * has no journal — both emit direct, exactly as before.
         */
        private final boolean staging;

        BoundedEntitySink(RunnerContext ctx, String instanceId, boolean staging) {
            this.ctx = ctx;
            this.instanceId = instanceId;
            this.staging = staging;
        }

        @Override
        public void collect(Object value) {
            collected.add(value);
        }

        @Override
        public void emitPluginBytes(String channel, byte[] bytes) {
            if (staging) {
                staged.add(new StagedOutput(channel, bytes));
                return;
            }
            emitOutput(ctx, instanceId, bytes);
        }

        List<Object> collected() {
            return collected;
        }

        /** Staged outputs, in emission order. Drained onto the proc barrier's outbox. */
        List<StagedOutput> staged() {
            return staged;
        }
    }

    /** {@link BoundedOperativeCtx} bound to one running bounded instance. */
    private static final class BoundedOperativeCtxImpl implements BoundedOperativeCtx {
        private final RunnerContext ctx;
        private final BoundedOperativeInstance instance;
        private byte[] result = EMPTY;
        private boolean completed = false;

        BoundedOperativeCtxImpl(RunnerContext ctx, BoundedOperativeInstance instance) {
            this.ctx = ctx;
            this.instance = instance;
        }

        @Override
        public void emit(@Nonnull byte[] value) {
            emitOutput(ctx, instance.instanceId(), value);
        }

        @Override
        public void complete(@Nonnull byte[] result) {
            this.result = result;
            this.completed = true;
        }

        @Override
        public boolean isCancelled() {
            return instance.isCancelled();
        }

        @Override
        @Nonnull
        public Optional<byte[]> recv() {
            while (true) {
                if (instance.isCancelled() || completed) {
                    return Optional.empty();
                }
                try {
                    byte[] item = instance.inbox().poll(200, TimeUnit.MILLISECONDS);
                    if (item != null) {
                        return Optional.of(item);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                }
            }
        }

        @Override
        public void loopUntil(@Nonnull BooleanSupplier predicate, @Nonnull BoundedLoopBody body)
                throws BoundedOperativeException {
            while (!instance.isCancelled() && !completed && !predicate.getAsBoolean()) {
                body.run(this);
            }
        }

        byte[] result() {
            return result;
        }
    }
}
