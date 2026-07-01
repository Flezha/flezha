package com.flezha.sdk.runtime.bounded;

import com.flezha.sdk.bounded.BoundedLoopBody;
import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeCtx;
import com.flezha.sdk.bounded.BoundedOperativeException;
import com.flezha.sdk.bounded.RollbackMode;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.proto.BoundedCompleted;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.bounded.BoundedCompensation.SegmentRun;
import com.flezha.sdk.runtime.bounded.BoundedCompensation.StageRecord;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ContinuousOperativeTopology;
import com.flezha.sdk.runtime.operative.OperationDescriptor;
import com.flezha.sdk.runtime.state.FlusherEntry;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.SegmentFlusher;
import com.flezha.sdk.runtime.state.SegmentId;
import com.flezha.sdk.runtime.state.SegmentStateSink;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sequential bounded driver. One driver runs per started bounded instance on a
 * <strong>dedicated thread</strong> (not a worker-pool thread): a {@code run}-sugar
 * body may block on its inbox during an interactive loop, and the structured
 * path joins pool segment sub-tasks — running the driver on the shared pool
 * would tie up a worker indefinitely and a single-thread pool would self-deadlock.
 * The driver:
 *
 * <ul>
 *   <li>runs the body — {@code run}-sugar ({@link #runSugar}) or the structured
 *       linear chain ({@link #runStructured}: operations sequential, segments
 *       parallel within an operation, count-based barrier between them);</li>
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
 * <p>P1 is in-memory: per-segment Changes fold into state via {@code applyChange}
 * but are not persisted (durability + crash-resume land in Phase 3, where the
 * WAL flush wires onto the same {@link SegmentId} identity computed here).
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
     * native context. Mirrors {@code OperationDispatcher.WIRE_WRITER}.
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
            @Nonnull ExecutorContext ctx,
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

    private static void run(ExecutorContext ctx, BoundedOperativeInstance instance, BoundedSpec spec, String args) {
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

        emitCompletion(ctx, instance.instanceId(), outcome.status(), outcome.error(), outcome.result());
    }

    // ---- run-sugar path -----------------------------------------------------

    /** Run-sugar path: a single implicit Operation/Segment whose body is {@code run}. */
    private static Outcome runSugar(
            ExecutorContext ctx, BoundedOperativeInstance instance, BoundedSpec spec, String args) {
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
     * Structured path: run the linear chain sequentially. Operation {@code N+1}
     * starts only when every segment of operation {@code N} has finished
     * (count-based barrier); segments within an operation run in parallel on the
     * pool. Inter-operation output collected at each barrier seeds the next stage.
     */
    private static Outcome runStructured(
            ExecutorContext ctx, BoundedOperativeInstance instance, BoundedSpec spec, String args) {
        ContinuousOperativeTopology topology = spec.topology().orElseThrow();
        List<Integer> chain = spec.chain();
        String instanceId = instance.instanceId();

        // Build each stage's segments up front (per-segment state lives for the run).
        List<List<BoundedSegment>> stageSegments = new ArrayList<>(chain.size());
        for (int opId : chain) {
            stageSegments.add(buildStageSegments(topology, instanceId, opId, spec.persistenceEnabled()));
        }

        // Seed the first operation from the spawn args via the entry channel's typed wrapper.
        ChannelDescriptor entry = entryChannel(topology);
        Object seed = entry.payloadType() == String.class ? args : args.getBytes(StandardCharsets.UTF_8);
        List<Object> batch = new ArrayList<>();
        batch.add(seed);

        // Completed stages of the current (and only, in v1) pass, recorded in
        // completion order so an abnormal exit can roll them back in reverse.
        List<StageRecord> completed = new ArrayList<>();
        // Non-null once the chain terminates abnormally (error / mid-chain stop).
        Outcome abnormal = null;

        for (int stageIdx = 0; stageIdx < chain.size(); stageIdx++) {
            // A cooperative stop before a stage starts is partial progress: stop
            // advancing and roll back what completed.
            if (instance.isCancelled()) {
                abnormal = new Outcome("cancelled", "", EMPTY);
                break;
            }
            OperationDescriptor op = topology.operation(chain.get(stageIdx));
            List<BoundedSegment> segs = stageSegments.get(stageIdx);
            List<List<Object>> partitioned = partition(op, segs, batch);
            try {
                StageOut stageOut = runStageParallel(ctx, instanceId, topology, segs, partitioned);
                batch = stageOut.batch();
                completed.add(new StageRecord(stageIdx, stageOut.segments()));
            } catch (BoundedOperativeException e) {
                abnormal = new Outcome("error", messageOf(e), EMPTY);
                break;
            }
        }

        // Clean run to quiescence: every stage completed, so there is nothing
        // partial to undo — a clean completion never compensates.
        if (abnormal == null) {
            return new Outcome("ok", "", EMPTY);
        }
        // Abnormal exit after partial progress: roll back the completed stages in
        // the rollback mode's order, then report the terminal status.
        runCompensation(ctx, topology, instanceId, chain, stageSegments, completed, spec.rollbackMode());
        return abnormal;
    }

    /**
     * Roll back the completed stages of an abnormally-terminated chain. Completed
     * operations are compensated in reverse order (per
     * {@link BoundedCompensation#planCompensation}) and, within an operation,
     * once per segment that ran. Operations without a {@code compensate} hook are
     * skipped. Best-effort: a failing hook is logged and the remaining steps
     * still run, so one broken undo cannot strand the rest. Runs on the driver
     * thread (not the work pool).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void runCompensation(
            ExecutorContext ctx,
            ContinuousOperativeTopology topology,
            String instanceId,
            List<Integer> chain,
            List<List<BoundedSegment>> stageSegments,
            List<StageRecord> completed,
            RollbackMode rollbackMode) {
        // v1 runs a single pass, so there are no committed iterations to keep;
        // the structured-loop follow-up threads them in and planCompensation
        // already honours the mode difference.
        List<StageRecord> plan = BoundedCompensation.planCompensation(rollbackMode, List.of(), completed);
        if (plan.isEmpty()) {
            return;
        }
        BoundedSegmentSink sink = new BoundedSegmentSink(ctx, instanceId);
        for (StageRecord record : plan) {
            List<BoundedSegment> slots = stageSegments.get(record.stageIdx());
            // The hook flag is per operation; a hookless operation is skipped.
            if (slots.isEmpty() || !slots.get(0).op().compensateOverridden()) {
                continue;
            }
            for (SegmentRun segRun : record.segments()) {
                BoundedSegment seg = slots.get(segRun.segmentIndex());
                BoundedCollectAdapter adapter = new BoundedCollectAdapter(topology, seg.outputChannel(), sink);
                Operation op = seg.op();
                BiConsumer<Object, Change> applyChangeFn = (s, c) -> op.applyChange(s, c);
                // Compensation Changes record onto the segment's flush buffer and
                // ship to the WAL (durable classes) so a crash mid-compensation
                // leaves the undo recorded.
                List<Change> buffer = seg.changeBuffer();
                StateHandle<Object, Change> handle = StateHandle.create(seg.state(), buffer, applyChangeFn);
                try {
                    op.compensate(segRun.outputs(), handle, adapter);
                } catch (OperationException | RuntimeException e) {
                    LOG.warn("bounded {} compensation for op `{}` failed: {}", instanceId, op.name(), e.toString());
                } finally {
                    seg.flusher().ifPresent(SegmentFlusher::flushEntry);
                }
            }
        }
    }

    /**
     * Build the segments for one stage: a single segment in single mode, one per
     * declared key in static mode. Uses the same {@link SegmentId} identity a
     * continuous operative would (real operation name + segment key, no
     * sentinels), so Phase-3 durability rides the same per-Segment identity.
     */
    static List<BoundedSegment> buildStageSegments(
            ContinuousOperativeTopology topology, String instanceId, int opId, boolean persistenceEnabled) {
        OperationDescriptor op = topology.operation(opId);
        Optional<ChannelDescriptor> outputChannel = op.outputChannelId().isPresent()
                ? Optional.ofNullable(topology.channel(op.outputChannelId().getAsInt()))
                : Optional.empty();
        List<BoundedSegment> segs = new ArrayList<>();
        if (op.segmentKeys().isEmpty()) {
            segs.add(buildSegment(instanceId, op, Optional.empty(), outputChannel, persistenceEnabled));
        } else {
            for (String key : op.segmentKeys()) {
                segs.add(buildSegment(instanceId, op, Optional.of(key), outputChannel, persistenceEnabled));
            }
        }
        return segs;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static BoundedSegment buildSegment(
            String instanceId,
            OperationDescriptor op,
            Optional<String> key,
            Optional<ChannelDescriptor> outputChannel,
            boolean persistenceEnabled) {
        Operation operationInstance = instantiateOperation(op.operationClass());
        Object state = operationInstance.initState(key, Optional.empty());
        SegmentId identity = SegmentId.of(instanceId, op.name(), key);

        // A changeful segment ships its Changes to the WAL on flush; the entry
        // carries the class-level persistence toggle (drain-and-discard when off).
        // Reuses the continuous flusher's frame + sink path, byte-for-byte.
        List<Change> changeBuffer = new ArrayList<>();
        Optional<FlusherEntry> flusher = Optional.empty();
        if (op.changeType() != NoChange.class) {
            flusher = Optional.of(new FlusherEntry(
                    new Object(),
                    changeBuffer,
                    persistenceEnabled,
                    new AtomicLong(0L),
                    SegmentStateSink::send,
                    new HeaderTemplate(),
                    identity,
                    op.schemaVersion(),
                    new KryoCodec(op.kryoTypes())));
        }
        return new BoundedSegment(operationInstance, state, identity, outputChannel, changeBuffer, flusher);
    }

    /**
     * Partition an input batch across a stage's segments. Single mode routes
     * every item to segment 0; static mode routes by the operation's
     * {@code segmentFor} keys.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static List<List<Object>> partition(OperationDescriptor op, List<BoundedSegment> segs, List<Object> batch) {
        List<List<Object>> buckets = new ArrayList<>(segs.size());
        for (int i = 0; i < segs.size(); i++) {
            buckets.add(new ArrayList<>());
        }
        if (op.segmentKeys().isEmpty()) {
            if (!buckets.isEmpty()) {
                buckets.get(0).addAll(batch);
            }
            return buckets;
        }
        Map<String, Integer> keyToIdx = new HashMap<>();
        for (int i = 0; i < op.segmentKeys().size(); i++) {
            keyToIdx.put(op.segmentKeys().get(i), i);
        }
        // segmentFor is a stateless routing function; any segment's instance works.
        Operation router = segs.get(0).op();
        for (Object item : batch) {
            List<String> keys = router.segmentFor(item);
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
     * Run a stage's segments in parallel on the pool and join on a count-based
     * barrier: every dispatched item is processed across all segments, with no
     * segment mid-run, before the collected output is released to the next stage.
     */
    private static StageOut runStageParallel(
            ExecutorContext ctx,
            String instanceId,
            ContinuousOperativeTopology topology,
            List<BoundedSegment> segs,
            List<List<Object>> partitioned)
            throws BoundedOperativeException {
        List<Future<SegmentRun>> futures = new ArrayList<>();
        for (int i = 0; i < segs.size(); i++) {
            List<Object> items = partitioned.get(i);
            if (items.isEmpty()) {
                continue;
            }
            final int segIdx = i;
            BoundedSegment seg = segs.get(i);
            futures.add(ctx.workerPool()
                    .submit(() -> new SegmentRun(segIdx, runSegment(topology, instanceId, ctx, seg, items))));
        }

        // Wait for every segment (the barrier) before returning, even on
        // failure, so no segment is mid-run when the stage completes. Each
        // segment's outputs are kept under its index for both the next-stage
        // batch and saga rollback.
        List<SegmentRun> segments = new ArrayList<>();
        BoundedOperativeException firstErr = null;
        for (Future<SegmentRun> f : futures) {
            try {
                segments.add(f.get());
            } catch (ExecutionException ee) {
                if (firstErr == null) {
                    firstErr = asBounded(ee.getCause());
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                if (firstErr == null) {
                    firstErr = new BoundedOperativeException("bounded segment task interrupted", ie);
                }
            }
        }
        if (firstErr != null) {
            throw firstErr;
        }
        // Deterministic next-stage batch: segments in index order (each segment's
        // own item order is preserved by runSegment).
        segments.sort(Comparator.comparingInt(SegmentRun::segmentIndex));
        List<Object> batch = new ArrayList<>();
        for (SegmentRun sr : segments) {
            batch.addAll(sr.outputs());
        }
        return new StageOut(batch, segments);
    }

    /**
     * Process one segment's items sequentially (per-segment ordering preserved),
     * running {@code Operation.run} against the segment's in-memory state via a
     * {@link BoundedCollectAdapter} that collects inter-operation output and
     * routes plugin-bound output. The Java analog of the Rust dispatcher's
     * {@code run_collecting} path.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static List<Object> runSegment(
            ContinuousOperativeTopology topology,
            String instanceId,
            ExecutorContext ctx,
            BoundedSegment seg,
            List<Object> items)
            throws BoundedOperativeException {
        BoundedSegmentSink sink = new BoundedSegmentSink(ctx, instanceId);
        BoundedCollectAdapter adapter = new BoundedCollectAdapter(topology, seg.outputChannel(), sink);
        Operation op = seg.op();
        BiConsumer<Object, Change> applyChangeFn = (s, c) -> op.applyChange(s, c);
        // Changes record onto the segment's flush buffer; the flush after the
        // loop ships them to the WAL when the class is durable.
        List<Change> buffer = seg.changeBuffer();
        try {
            for (Object item : items) {
                StateHandle<Object, Change> handle = StateHandle.create(seg.state(), buffer, applyChangeFn);
                try {
                    op.run(item, handle, adapter);
                } catch (OperationException e) {
                    throw new BoundedOperativeException("operation `" + op.name() + "` failed: " + e.getMessage(), e);
                } catch (RuntimeException e) {
                    throw new BoundedOperativeException("operation `" + op.name() + "` threw: " + e, e);
                }
            }
        } finally {
            // Flush regardless of outcome so the WAL reflects the state applied
            // before any failure (at-least-once; the saga compensates a partial run).
            seg.flusher().ifPresent(SegmentFlusher::flushEntry);
        }
        return sink.collected();
    }

    private static ChannelDescriptor entryChannel(ContinuousOperativeTopology topology) {
        for (ChannelDescriptor c : topology.channels()) {
            if (c.kind() == ChannelKind.ENTRY) {
                return c;
            }
        }
        // Validated at registration: a structured bounded always has an entry channel.
        throw new IllegalStateException("structured bounded is missing an entry channel");
    }

    @SuppressWarnings("rawtypes")
    private static Operation instantiateOperation(Class<? extends Operation<?, ?, ?, ?, ?, ?>> operationClass) {
        try {
            return operationClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "operation class `" + operationClass.getName() + "` must have a public no-arg constructor: " + e,
                    e);
        }
    }

    private static BoundedOperativeException asBounded(Throwable cause) {
        if (cause instanceof BoundedOperativeException be) {
            return be;
        }
        return new BoundedOperativeException(cause == null ? "bounded segment task failed" : cause.toString(), cause);
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
    static void emitOutput(ExecutorContext ctx, String instanceId, byte[] value) {
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

    private static void emitCompletion(
            ExecutorContext ctx, String instanceId, String status, String error, byte[] result) {
        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("bounded-completed-" + instanceId + "-" + System.nanoTime())
                .setBoundedCompleted(BoundedCompleted.newBuilder()
                        .setOperativeId(instanceId)
                        .setStatus(status)
                        .setError(error)
                        .setResult(ByteString.copyFrom(result))
                        .build())
                .build();
        byte[] frame = FrameEncoder.build(CONTROL_PAYLOAD_ID, request.toByteArray(), Map.of());
        long controlStreamId = ctx.streamRegistry().controlStreamId();
        int writeStatus = wireWriter.write(ctx.nativeHandle(), controlStreamId, frame);
        if (writeStatus != 0) {
            LOG.warn("BoundedCompleted write failed for {} (status={})", instanceId, writeStatus);
        }
    }

    // ---- inner types --------------------------------------------------------

    /** Terminal outcome of a driver run: status ({@code ok | error | cancelled}), error text, result payload. */
    private record Outcome(String status, String error, byte[] result) {}

    /**
     * One stage's parallel-join result: the inter-operation {@code batch}
     * released to the next stage, plus the per-segment record (which segments
     * ran, with their collected outputs) retained for a possible saga rollback.
     */
    private record StageOut(List<Object> batch, List<SegmentRun> segments) {}

    /**
     * One bounded per-segment state holder (the lightweight continuous
     * {@code SegmentSlot} analog): the per-segment {@link Operation} instance,
     * its in-memory state, the {@link SegmentId} identity (real op name + key),
     * and the producer-edge output channel.
     */
    static final class BoundedSegment {
        @SuppressWarnings("rawtypes")
        private final Operation op;

        private final Object state;
        private final SegmentId identity;
        private final Optional<ChannelDescriptor> outputChannel;
        // Per-segment flush buffer (the continuous SegmentSlot analog): the
        // StateHandle records Changes here; `flusher` (present iff the operation
        // is changeful) ships them to the WAL on flush.
        private final List<Change> changeBuffer;
        private final Optional<FlusherEntry> flusher;

        @SuppressWarnings("rawtypes")
        BoundedSegment(
                Operation op,
                Object state,
                SegmentId identity,
                Optional<ChannelDescriptor> outputChannel,
                List<Change> changeBuffer,
                Optional<FlusherEntry> flusher) {
            this.op = op;
            this.state = state;
            this.identity = identity;
            this.outputChannel = outputChannel;
            this.changeBuffer = changeBuffer;
            this.flusher = flusher;
        }

        @SuppressWarnings("rawtypes")
        Operation op() {
            return op;
        }

        Object state() {
            return state;
        }

        SegmentId identity() {
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
    }

    /**
     * Per-segment collecting sink for the structured driver. Inter-operation
     * output is buffered for the next stage; plugin-bound output is forwarded to
     * the operative's wire (identical to {@code run}-sugar {@code ctx.emit}).
     */
    private static final class BoundedSegmentSink implements BoundedCollectSink {
        private final ExecutorContext ctx;
        private final String instanceId;
        private final List<Object> collected = new ArrayList<>();

        BoundedSegmentSink(ExecutorContext ctx, String instanceId) {
            this.ctx = ctx;
            this.instanceId = instanceId;
        }

        @Override
        public void collect(Object value) {
            collected.add(value);
        }

        @Override
        public void emitPluginBytes(byte[] bytes) {
            emitOutput(ctx, instanceId, bytes);
        }

        List<Object> collected() {
            return collected;
        }
    }

    /** {@link BoundedOperativeCtx} bound to one running bounded instance. */
    private static final class BoundedOperativeCtxImpl implements BoundedOperativeCtx {
        private final ExecutorContext ctx;
        private final BoundedOperativeInstance instance;
        private byte[] result = EMPTY;
        private boolean completed = false;

        BoundedOperativeCtxImpl(ExecutorContext ctx, BoundedOperativeInstance instance) {
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
