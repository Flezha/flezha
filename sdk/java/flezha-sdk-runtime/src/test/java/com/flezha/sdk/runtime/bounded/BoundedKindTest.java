package com.flezha.sdk.runtime.bounded;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeConfigurationException;
import com.flezha.sdk.bounded.BoundedOperativeCtx;
import com.flezha.sdk.bounded.BoundedOperativeException;
import com.flezha.sdk.bounded.BoundedOperativeRegistry;
import com.flezha.sdk.bounded.RollbackMode;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.ContinuousOperative;
import com.flezha.sdk.operative.ContinuousOperativeRegistry;
import com.flezha.sdk.proto.AddOperative;
import com.flezha.sdk.proto.BoundedCompleted;
import com.flezha.sdk.proto.BoundedStop;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.RemOperative;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.handlers.AddOperativeHandler;
import com.flezha.sdk.runtime.control.handlers.BoundedStopHandler;
import com.flezha.sdk.runtime.control.handlers.RemOperativeHandler;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.ContinuousOperativeInstanceRegistry;
import com.flezha.sdk.runtime.operative.ContinuousOperativeTopology;
import com.flezha.sdk.runtime.operative.Operative;
import com.flezha.sdk.runtime.state.SegmentStateSink;
import com.flezha.sdk.runtime.stream.PrimaryStreamReader;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BoundedKindTest {

    private static final long CONTROL_STREAM = 1L;
    private static final long PRIMARY_STREAM = 2L;
    private static final long SECONDARY_STREAM = 3L;
    private static final java.nio.charset.Charset UTF8 = StandardCharsets.UTF_8;

    private static final AtomicInteger EMIT_RUNS = new AtomicInteger(0);

    /**
     * Saga compensation observation log. Compensable operations append a marker
     * so rollback order and per-segment firing are assertable without wire
     * plumbing (a compensable inter-operation op's {@code ctx.emit} is collected,
     * not wired).
     */
    private static final List<String> COMPENSATION_LOG = new CopyOnWriteArrayList<>();

    /**
     * Forced-stop handshake: {@code StopReserve} signals it has started, then
     * parks until the test trips cancellation, so the "stop after partial
     * progress" path is deterministic (cancel is observed before the next
     * stage's pre-check rather than racing the fast chain).
     */
    private static final AtomicBoolean STAGE0_RAN = new AtomicBoolean(false);

    private static final AtomicBoolean STAGE0_RELEASE = new AtomicBoolean(false);

    @BeforeEach
    void reset() {
        ContinuousOperativeRegistry.reset();
        BoundedOperativeRegistry.reset();
        EMIT_RUNS.set(0);
        COMPENSATION_LOG.clear();
        STAGE0_RAN.set(false);
        STAGE0_RELEASE.set(false);
    }

    @AfterEach
    void restoreWriter() {
        BoundedOperativeRunner.resetWireWriter();
        SegmentStateSink.resetForTesting();
    }

    // --- Registration --------------------------------------------------------

    @Test
    @DisplayName("registerBounded builds the spec and makes the factory resolvable")
    void registerBoundedOperativeResolvable() {
        assertThat(BoundedOperativeClassRegistry.getSpec("test-emit-bounded")).isNull();
        BoundedOperativeRegistry.registerBounded(EmitBoundedOperative.class);
        assertThat(BoundedOperativeClassRegistry.getSpec("test-emit-bounded")).isNotNull();
        assertThat(BoundedOperativeRegistry.factoryIds()).contains("test-emit-bounded");
    }

    @Test
    @DisplayName("registerBounded after freeze throws IllegalStateException")
    void registerAfterFreezeRejected() {
        BoundedOperativeRegistry.freeze();
        assertThatThrownBy(() -> BoundedOperativeRegistry.registerBounded(EmitBoundedOperative.class))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("a bounded factory colliding with a operative (either order) is rejected")
    void crossKindDuplicateRejected() {
        ContinuousOperativeRegistry.registerContinuous(CollideContinuousOperative.class);
        assertThatThrownBy(() -> BoundedOperativeRegistry.registerBounded(CollideBoundedOperative.class))
                .isInstanceOf(IllegalArgumentException.class);

        ContinuousOperativeRegistry.reset();
        BoundedOperativeRegistry.reset();

        BoundedOperativeRegistry.registerBounded(CollideBoundedOperative.class);
        assertThatThrownBy(() -> ContinuousOperativeRegistry.registerContinuous(CollideContinuousOperative.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("an external-stop-only operative (no self-termination, no maxDuration) is rejected at registration")
    void externalStopOnlyAuthoringRejected() {
        assertThatThrownBy(() -> BoundedOperativeRegistry.registerBounded(ExternalStopOnlyBoundedOperative.class))
                .isInstanceOf(BoundedOperativeConfigurationException.class)
                .hasMessageContaining("terminal condition");
    }

    @Test
    @DisplayName("a branching structured topology is rejected at registration")
    void branchingTopologyRejected() {
        assertThatThrownBy(() -> BoundedOperativeRegistry.registerBounded(BranchingBoundedOperative.class))
                .isInstanceOf(BoundedOperativeConfigurationException.class)
                .hasMessageContaining("branching");
    }

    @Test
    @DisplayName("a dynamic-segment operation is rejected at registration")
    void dynamicSegmentOperationRejected() {
        assertThatThrownBy(() -> BoundedOperativeRegistry.registerBounded(DynamicBoundedOperative.class))
                .isInstanceOf(BoundedOperativeConfigurationException.class)
                .hasMessageContaining("dynamic");
    }

    // --- Sealed Operative registry -------------------------------------------

    @Test
    @DisplayName(
            "the instance registry holds both variants; getContinuousOperative is empty for a bounded; RemOperative removes a bounded")
    void sealedUnitHoldsBothVariants() {
        ContinuousOperativeInstanceRegistry registry = new ContinuousOperativeInstanceRegistry();
        registry.add(new BoundedOperativeInstance("t1", "echo_bounded"));

        assertThat(registry.contains("t1")).isTrue();
        assertThat(registry.get("t1")).isInstanceOf(BoundedOperativeInstance.class);
        assertThat(registry.getContinuousOperative("t1"))
                .as("a bounded is not a operative")
                .isNull();
        assertThat(registry.operativeSnapshot()).doesNotContainKey("t1");

        Operative removed = registry.remove("t1");
        assertThat(removed).isInstanceOf(BoundedOperativeInstance.class);
        assertThat(registry.contains("t1")).isFalse();
    }

    // --- Run-sugar spawn + lifecycle -----------------------------------------

    @Test
    @DisplayName("AddOperative kind=bounded spawns a bounded, runs once with init_values, and is idempotent")
    void addBoundedOperativeRunsOnce() throws Exception {
        BoundedOperativeRegistry.registerBounded(EmitBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-emit-bounded", "bounded-ok", "hello"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(EMIT_RUNS.get()).as("run must execute exactly once").isEqualTo(1);
        assertThat(ctx.operativeInstanceRegistry().get("bounded-ok")).isInstanceOf(BoundedOperativeInstance.class);

        // Duplicate AddOperative for a live id is idempotent — no re-run.
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-emit-bounded", "bounded-ok", "hello"), new ArrayList<>());
        Thread.sleep(100);
        assertThat(EMIT_RUNS.get()).as("duplicate start must not re-run").isEqualTo(1);
    }

    @Test
    @DisplayName("AddOperative kind=bounded with an unregistered factory reports an error and creates nothing")
    void addBoundedOperativeUnknownFactory() {
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        List<String> errors = new ArrayList<>();
        new AddOperativeHandler().handle(ctx, addBoundedOperative("test-missing-bounded", "bounded-x", ""), errors);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("BoundedOperative class");
        assertThat(ctx.operativeInstanceRegistry().contains("bounded-x")).isFalse();
    }

    @Test
    @DisplayName("ctx.complete(result) carries the result payload into BoundedCompleted")
    void completeResultReachesCompletion() throws Exception {
        BoundedOperativeRegistry.registerBounded(CompleteBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-complete-bounded", "bounded-c", "z"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        BoundedCompleted completed = cap.decodeCompletion();
        assertThat(completed.getStatus()).isEqualTo("ok");
        assertThat(completed.getResult().toStringUtf8()).isEqualTo("result:z");
    }

    @Test
    @DisplayName("inbox delivers post-start input to a running loop; a terminal predicate completes ok")
    void inboxLoopProcessesInputAndTerminalPredicateCompletesOk() throws Exception {
        BoundedOperativeRegistry.registerBounded(LoopBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-loop-bounded", "loop-1", ""), new ArrayList<>());

        PrimaryStreamReader reader = new PrimaryStreamReader(ctx, PRIMARY_STREAM);
        pushInbox(reader, "loop-1", "alpha");
        pushInbox(reader, "loop-1", "beta");
        pushInbox(reader, "loop-1", "quit");

        assertThat(cap.awaitCompletions()).isTrue();
        assertThat(cap.outputs(SECONDARY_STREAM)).containsExactly("got:alpha", "got:beta");
        assertThat(cap.decodeCompletion().getStatus())
                .as("terminal predicate ⇒ ok")
                .isEqualTo("ok");
    }

    @Test
    @DisplayName("BoundedStop trips cooperative cancellation → terminal cancelled completion")
    void boundedStopCancelsCooperatively() throws Exception {
        BoundedOperativeRegistry.registerBounded(LoopBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-loop-bounded", "loop-2", ""), new ArrayList<>());
        new BoundedStopHandler().handle(ctx, boundedStop("loop-2"), new ArrayList<>());

        assertThat(cap.awaitCompletions()).isTrue();
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("cancelled");
    }

    @Test
    @DisplayName("hard RemOperative removes a running bounded and cancels its driver → cancelled completion")
    void hardRemOperativeCancelsBounded() throws Exception {
        BoundedOperativeRegistry.registerBounded(LoopBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-loop-bounded", "loop-3", ""), new ArrayList<>());
        new RemOperativeHandler().handle(ctx, remOperative("loop-3"), new ArrayList<>());

        assertThat(ctx.operativeInstanceRegistry().contains("loop-3")).isFalse();
        assertThat(cap.awaitCompletions()).isTrue();
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("cancelled");
    }

    @Test
    @DisplayName("maxDuration elapse cancels an interactive loop → cancelled completion")
    void maxDurationElapseCancels() throws Exception {
        BoundedOperativeRegistry.registerBounded(TimedBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-timed-bounded", "timed-1", ""), new ArrayList<>());

        assertThat(cap.awaitCompletions()).isTrue();
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("cancelled");
    }

    // --- Run-sugar emit + completion -----------------------------------------

    @Test
    @DisplayName("success: emits an instance-id-keyed empty-metadata output frame and BoundedCompleted ok")
    void successEmitsOutputAndOkCompletion() throws Exception {
        BoundedOperativeRegistry.registerBounded(EmitBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-emit-bounded", "bounded-ok", "hello"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        Frame output = cap.decodeOne(SECONDARY_STREAM);
        assertThat(output.payloadId()).isEqualTo("bounded-ok");
        assertThat(output.metadata())
                .as("bounded output carries no channel metadata")
                .isEmpty();
        assertThat(new String(output.message(), UTF8)).isEqualTo("hello");

        BoundedCompleted completed = cap.decodeCompletion();
        assertThat(completed.getOperativeId()).isEqualTo("bounded-ok");
        assertThat(completed.getStatus()).isEqualTo("ok");
        assertThat(completed.getError()).isEmpty();
    }

    @Test
    @DisplayName(
            "failure: a thrown BoundedOperativeException reports BoundedCompleted error with the message, no output")
    void failureEmitsErrorCompletion() throws Exception {
        BoundedOperativeRegistry.registerBounded(FailBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-fail-bounded", "bounded-err", ""), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.framesOn(SECONDARY_STREAM))
                .as("a failing bounded emits no output")
                .isEmpty();
        BoundedCompleted completed = cap.decodeCompletion();
        assertThat(completed.getStatus()).isEqualTo("error");
        assertThat(completed.getError()).isEqualTo("boom");
    }

    // --- Structured (sequential driver) --------------------------------------

    @Test
    @DisplayName("a structured single-operation bounded equals the run-sugar one-operation form")
    void structuredSingleOpEqualsSugar() throws Exception {
        BoundedOperativeRegistry.registerBounded(EchoStructuredOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-echo-structured", "echo-s", "foo"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.outputs(SECONDARY_STREAM)).containsExactly("echo:foo");
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
    }

    @Test
    @DisplayName("a multi-operation bounded runs its operations in order with a barrier")
    void structuredChainRunsInOrder() throws Exception {
        BoundedOperativeRegistry.registerBounded(ChainBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-chain-bounded", "chain-1", "x"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.outputs(SECONDARY_STREAM))
                .as("Stage1 then Stage2, in order")
                .containsExactly("x-1-2");
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
    }

    @Test
    @DisplayName("parallel static segments run with independent per-segment state and a barrier")
    void structuredParallelSegmentsKeepPerSegmentState() throws Exception {
        BoundedOperativeRegistry.registerBounded(FanBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-fan-bounded", "fan-1", "a,bb,ccc,dddd"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        // odd segment: "a"(1) then "ccc"(2); even segment: "bb"(1) then "dddd"(2).
        // Independent per-segment counters; the barrier collected all four.
        assertThat(cap.outputs(SECONDARY_STREAM)).containsExactlyInAnyOrder("1:a", "2:ccc", "1:bb", "2:dddd");
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
    }

    @Test
    @DisplayName("a hybrid inter-operation channel both feeds the next stage and routes to a plugin sink")
    void structuredHybridChannelRoutesBoth() throws Exception {
        BoundedOperativeRegistry.registerBounded(HybridBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-hybrid-bounded", "hybrid-1", "z"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        // "mid:h:z" from the hybrid sink on Stage1's output; "final:h:z" from
        // Stage2's terminal plugin — Stage1's value reached both Stage2 and the sink.
        assertThat(cap.outputs(SECONDARY_STREAM)).containsExactlyInAnyOrder("mid:h:z", "final:h:z");
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
    }

    @Test
    @DisplayName("bounded segments use the real {instance, operation, segment} identity — no sentinels (§2.3)")
    void structuredSegmentsUseRealIdentity() {
        BoundedSpec spec = BoundedSpec.build(FanBoundedOperative.class);
        ContinuousOperativeTopology topology = spec.topology().orElseThrow();
        // Chain: SplitOp (single segment) → FanOp (static even/odd); FanOp is the 2nd stage.
        int fanOpId = spec.chain().get(1);
        List<BoundedOperativeRunner.BoundedSegment> segs =
                BoundedOperativeRunner.buildStageSegments(topology, "inst-1", fanOpId, true);
        assertThat(segs)
                .extracting(s -> s.identity().encoded())
                .containsExactlyInAnyOrder("inst-1/bounded_fan_op/even", "inst-1/bounded_fan_op/odd");
    }

    // --- Durability (Phase 3 §5b.1 / §7c.1) ----------------------------------

    @Test
    @DisplayName("§5b.1/§7c.1 a durable bounded writes WAL frames; an opt-out one writes nothing")
    void durableBoundedWritesWalOptOutWritesNothing() throws Exception {
        BoundedOperativeRegistry.registerBounded(FanBoundedOperative.class);
        BoundedOperativeRegistry.registerBounded(FanOptOutBoundedOperative.class);

        List<byte[]> walFrames = new CopyOnWriteArrayList<>();
        SegmentStateSink.installGlobalSink(frame -> {
            walFrames.add(frame);
            return true;
        });

        // FanBoundedOperative is durable-by-default; its FanOp applies a Change
        // per item, which the driver flushes as a segment_state_change frame.
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);
        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-fan-bounded", "fan-wal", "a,bb"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
        assertThat(walFrames)
                .as("a durable bounded ships segment_state_change frames to the WAL")
                .isNotEmpty();

        // The opt-out twin does identical work with persistenceEnabled=false:
        // Changes drain-and-discard, nothing reaches the WAL.
        walFrames.clear();
        Captured cap2 = new Captured();
        ExecutorContext ctx2 = newCtx(cap2);
        cap2.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx2, addBoundedOperative("test-fan-optout-bounded", "fan-optout", "a,bb"), new ArrayList<>());
        assertThat(cap2.awaitCompletions()).isTrue();
        assertThat(cap2.decodeCompletion().getStatus()).isEqualTo("ok");
        assertThat(walFrames).as("an opt-out bounded writes nothing to the WAL").isEmpty();
    }

    // --- Saga compensation (Phase 2 §4 / §7b) --------------------------------

    @Test
    @DisplayName("§7b.1 a failed step rolls back completed steps in reverse, each with its own output")
    void sagaReverseOrderRollbackOnError() throws Exception {
        BoundedOperativeRegistry.registerBounded(SagaBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-saga-bounded", "saga-1", "X"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        BoundedCompleted done = cap.decodeCompletion();
        assertThat(done.getStatus()).isEqualTo("error");
        assertThat(done.getError()).contains("ship failed");
        // reserve → charge completed; ship failed (not compensated). Undo runs
        // reverse: charge then reserve, each receiving its own output.
        assertThat(COMPENSATION_LOG).containsExactly("undo-charge:charged:reserved:X", "undo-reserve:reserved:X");
    }

    @Test
    @DisplayName("§7b.2 keep-progress mode is plumbed to the driver; a single pass unwinds all-or-nothing")
    void sagaKeepProgressModeUnwindsSinglePass() throws Exception {
        BoundedOperativeRegistry.registerBounded(KeepProgressSagaOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-keep-progress-saga", "saga-kp", "Y"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("error");
        assertThat(COMPENSATION_LOG).containsExactly("undo-charge:charged:reserved:Y", "undo-reserve:reserved:Y");
    }

    @Test
    @DisplayName("§7b.1 a clean completion does not compensate")
    void sagaCleanCompletionDoesNotCompensate() throws Exception {
        BoundedOperativeRegistry.registerBounded(CleanSagaOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-clean-saga", "saga-ok", "Z"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.outputs(SECONDARY_STREAM)).containsExactly("echo:charged:reserved:Z");
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
        assertThat(COMPENSATION_LOG).as("a clean ok must not compensate").isEmpty();
    }

    @Test
    @DisplayName("§7b.1 compensation fires once per segment that ran; a hookless operation is skipped")
    void sagaPerSegmentRollback() throws Exception {
        BoundedOperativeRegistry.registerBounded(FanSagaOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-fan-saga", "fan-saga", "a,bb,ccc"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("error");
        // odd segment ran "a","ccc"; even ran "bb"; the hook fires once per
        // segment with that segment's own outputs. SplitOp (no hook) is skipped.
        assertThat(COMPENSATION_LOG)
                .containsExactlyInAnyOrder("undo-fan:even:[even:bb]", "undo-fan:odd:[odd:a,odd:ccc]");
    }

    @Test
    @DisplayName("§7b.1 a cooperative stop after partial progress compensates the completed steps")
    void sagaForcedStopCompensatesPartialProgress() throws Exception {
        BoundedOperativeRegistry.registerBounded(StopSagaOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-stop-saga", "stop-saga", "W"), new ArrayList<>());

        // Wait for stage 0 to start, then trip cancellation before stage 1.
        while (!STAGE0_RAN.get()) {
            Thread.sleep(5);
        }
        BoundedOperativeInstance instance =
                (BoundedOperativeInstance) ctx.operativeInstanceRegistry().get("stop-saga");
        instance.cancel();
        STAGE0_RELEASE.set(true);

        assertThat(cap.awaitCompletions()).isTrue();
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("cancelled");
        // StopReserve completed before the stop landed; Echo never ran.
        assertThat(COMPENSATION_LOG).containsExactly("undo-stop-reserve:reserved:W");
    }

    // --- helpers -------------------------------------------------------------

    private ExecutorContext newCtx(Captured cap) {
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ExecutorContext ctx = new ExecutorContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));
        ctx.streamRegistry().setControlStreamId(CONTROL_STREAM);
        ctx.streamRegistry().register(0, PRIMARY_STREAM, SECONDARY_STREAM);
        BoundedOperativeRunner.setWireWriter(cap);
        return ctx;
    }

    private static CommandRequest addBoundedOperative(String factoryId, String operativeId, String args) {
        return CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setAddOperative(AddOperative.newBuilder()
                        .setOperativeId(operativeId)
                        .setFactoryId(factoryId)
                        .setInitValues(args)
                        .setKind("bounded")
                        .build())
                .build();
    }

    private static CommandRequest boundedStop(String operativeId) {
        return CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setBoundedStop(
                        BoundedStop.newBuilder().setOperativeId(operativeId).build())
                .build();
    }

    private static CommandRequest remOperative(String operativeId) {
        return CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setRemOperative(
                        RemOperative.newBuilder().setOperativeId(operativeId).build())
                .build();
    }

    private static void pushInbox(PrimaryStreamReader reader, String instanceId, String payload) {
        reader.onData(FrameEncoder.build(instanceId, payload.getBytes(UTF8), Map.of()));
    }

    /** Capturing wire writer + decode helpers. */
    private static final class Captured implements BoundedOperativeRunner.WireWriter {
        private final List<long[]> meta = new CopyOnWriteArrayList<>();
        private final List<byte[]> frames = new CopyOnWriteArrayList<>();
        private volatile CountDownLatch completions;

        void expectCompletions(int n) {
            completions = new CountDownLatch(n);
        }

        boolean awaitCompletions() throws InterruptedException {
            return completions.await(5, TimeUnit.SECONDS);
        }

        @Override
        public int write(long handle, long streamId, byte[] frame) {
            meta.add(new long[] {streamId});
            frames.add(frame);
            if (streamId == CONTROL_STREAM && completions != null) {
                completions.countDown();
            }
            return 0;
        }

        List<byte[]> framesOn(long streamId) {
            List<byte[]> out = new ArrayList<>();
            for (int i = 0; i < frames.size(); i++) {
                if (meta.get(i)[0] == streamId) {
                    out.add(frames.get(i));
                }
            }
            return Collections.unmodifiableList(out);
        }

        Frame decodeOne(long streamId) {
            List<byte[]> on = framesOn(streamId);
            assertThat(on).hasSize(1);
            return new FrameParser().feed(on.get(0)).get(0);
        }

        /** All output payloads on a stream, in capture order, decoded as UTF-8 strings. */
        List<String> outputs(long streamId) {
            List<String> out = new ArrayList<>();
            for (byte[] f : framesOn(streamId)) {
                Frame frame = new FrameParser().feed(f).get(0);
                out.add(new String(frame.message(), UTF8));
            }
            return out;
        }

        BoundedCompleted decodeCompletion() throws Exception {
            Frame f = decodeOne(CONTROL_STREAM);
            assertThat(f.payloadId()).isEqualTo("control_command");
            return CommandRequest.parseFrom(f.message()).getBoundedCompleted();
        }
    }

    // --- test bounded operatives ---------------------------------------------

    public static final class EmitBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-emit-bounded";
        }

        @Override
        public void run(BoundedOperativeCtx ctx, String args) throws BoundedOperativeException {
            EMIT_RUNS.incrementAndGet();
            ctx.emit(args);
        }
    }

    public static final class FailBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-fail-bounded";
        }

        @Override
        public void run(BoundedOperativeCtx ctx, String args) throws BoundedOperativeException {
            throw new BoundedOperativeException("boom");
        }
    }

    public static final class CompleteBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-complete-bounded";
        }

        @Override
        public void run(BoundedOperativeCtx ctx, String args) {
            ctx.complete("result:" + args);
        }
    }

    /** Interactive: loops on the inbox, echoes {@code got:<msg>}, exits on "quit" or cancellation. */
    public static final class LoopBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-loop-bounded";
        }

        @Override
        public void run(BoundedOperativeCtx ctx, String args) throws BoundedOperativeException {
            AtomicBoolean done = new AtomicBoolean(false);
            BooleanSupplier predicate = done::get;
            ctx.loopUntil(predicate, c -> {
                Optional<byte[]> msg = c.recv();
                if (msg.isEmpty()) {
                    done.set(true); // cancellation tripped
                    return;
                }
                String s = new String(msg.get(), UTF8);
                if (s.equals("quit")) {
                    done.set(true);
                } else {
                    c.emit("got:" + s);
                }
            });
        }
    }

    /** Interactive loop bounded by {@code maxDuration}; the only exit is the duration backstop. */
    public static final class TimedBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-timed-bounded";
        }

        @Override
        public Optional<Duration> maxDuration() {
            return Optional.of(Duration.ofMillis(200));
        }

        @Override
        public void run(BoundedOperativeCtx ctx, String args) throws BoundedOperativeException {
            ctx.loopUntil(() -> false, c -> c.recv());
        }
    }

    /** External-stop-only: declares no self-termination and no duration backstop — rejected at registration. */
    public static final class ExternalStopOnlyBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-external-stop-only";
        }

        @Override
        public boolean selfTerminating() {
            return false;
        }

        @Override
        public void run(BoundedOperativeCtx ctx, String args) {}
    }

    public static final class CollideBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "collide";
        }

        @Override
        public void run(BoundedOperativeCtx ctx, String args) {}
    }

    public static final class CollideContinuousOperative implements ContinuousOperative {
        @Override
        public String factoryId() {
            return "collide";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(EchoOp.class);
        }
    }

    // --- structured bounded operatives ---------------------------------------

    public static final class EchoStructuredOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-echo-structured";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(EchoOp.class);
            c.channel(String.class, "out").from(EchoOp.class).toPlugin("sink");
        }
    }

    public static final class ChainBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-chain-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(Stage1.class);
            c.channel(String.class, "mid").from(Stage1.class).into(Stage2.class);
            c.channel(String.class, "out").from(Stage2.class).toPlugin("sink");
        }
    }

    public static final class FanBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-fan-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(SplitOp.class);
            c.channel(String.class, "mid").from(SplitOp.class).into(FanOp.class);
            c.channel(String.class, "out").from(FanOp.class).toPlugin("sink");
        }
    }

    /** Opt-out twin of {@link FanBoundedOperative}: identical topology, persistence disabled. */
    public static final class FanOptOutBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-fan-optout-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(SplitOp.class);
            c.channel(String.class, "mid").from(SplitOp.class).into(FanOp.class);
            c.channel(String.class, "out").from(FanOp.class).toPlugin("sink");
        }

        @Override
        public boolean persistenceEnabled() {
            return false;
        }
    }

    public static final class HybridBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-hybrid-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(HybridStage1.class);
            c.channel(String.class, "mid")
                    .from(HybridStage1.class)
                    .alsoToPluginWith("midsink", (String s) -> ("mid:" + s).getBytes(UTF8))
                    .into(HybridStage2.class);
            c.channel(String.class, "out").from(HybridStage2.class).toPlugin("out");
        }
    }

    public static final class BranchingBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-branching-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            // One channel feeding two downstream operations = branching.
            c.input(String.class, "in").into(Stage1.class);
            c.channel(String.class, "mid").from(Stage1.class).into(Stage2.class).into(EchoOp.class);
        }
    }

    public static final class DynamicBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-dynamic-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(DynOp.class);
        }
    }

    // --- saga (compensation) bounded operatives ------------------------------

    /** Reserve → Charge → Ship(fails). Default all-or-nothing rollback. */
    public static final class SagaBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-saga-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(SagaReserve.class);
            c.channel(String.class, "c1").from(SagaReserve.class).into(SagaCharge.class);
            c.channel(String.class, "c2").from(SagaCharge.class).into(SagaShipFails.class);
            c.channel(String.class, "out").from(SagaShipFails.class).toPlugin("sink");
        }
    }

    /**
     * Same chain, keep-progress mode — for a single no-loop pass it unwinds
     * identically (the mode only diverges with committed iterations, covered by
     * the planner unit tests). Proves {@code rollbackMode()} is plumbed through.
     */
    public static final class KeepProgressSagaOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-keep-progress-saga";
        }

        @Override
        public RollbackMode rollbackMode() {
            return RollbackMode.KEEP_PROGRESS;
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(SagaReserve.class);
            c.channel(String.class, "c1").from(SagaReserve.class).into(SagaCharge.class);
            c.channel(String.class, "c2").from(SagaCharge.class).into(SagaShipFails.class);
            c.channel(String.class, "out").from(SagaShipFails.class).toPlugin("sink");
        }
    }

    /** Reserve → Charge → Echo(succeeds): a clean completion MUST NOT compensate. */
    public static final class CleanSagaOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-clean-saga";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(SagaReserve.class);
            c.channel(String.class, "c1").from(SagaReserve.class).into(SagaCharge.class);
            c.channel(String.class, "c2").from(SagaCharge.class).into(EchoOp.class);
            c.channel(String.class, "out").from(EchoOp.class).toPlugin("sink");
        }
    }

    /** Split(hookless) → Fan(compensable, even/odd) → FailLast: per-segment rollback; Split is skipped. */
    public static final class FanSagaOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-fan-saga";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(SplitOp.class);
            c.channel(String.class, "mid").from(SplitOp.class).into(SagaFan.class);
            c.channel(String.class, "mid2").from(SagaFan.class).into(FailLast.class);
            c.channel(String.class, "out").from(FailLast.class).toPlugin("sink");
        }
    }

    /** StopReserve → Echo: a cooperative stop after StopReserve compensates only it; Echo never runs. */
    public static final class StopSagaOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-stop-saga";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(StopReserve.class);
            c.channel(String.class, "mid").from(StopReserve.class).into(EchoOp.class);
            c.channel(String.class, "out").from(EchoOp.class).toPlugin("sink");
        }
    }

    // --- test operations -----------------------------------------------------

    /** Stateless {@code String → String} base, factoring the type-handle boilerplate. */
    abstract static class StrOp implements Operation<String, Unit, String, NoChange, WallClock, Void> {
        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public Class<NoChange> changeType() {
            return NoChange.class;
        }

        @Override
        public Class<WallClock> clockType() {
            return WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public Unit initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new Unit();
        }
    }

    public record Unit() {}

    public static final class EchoOp extends StrOp {
        @Override
        public String name() {
            return "bounded_echo_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit("echo:" + input);
        }
    }

    public static final class Stage1 extends StrOp {
        @Override
        public String name() {
            return "bounded_stage1";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit(input + "-1");
        }
    }

    public static final class Stage2 extends StrOp {
        @Override
        public String name() {
            return "bounded_stage2";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit(input + "-2");
        }
    }

    public static final class SplitOp extends StrOp {
        @Override
        public String name() {
            return "bounded_split_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            for (String piece : input.split(",")) {
                ctx.emit(piece);
            }
        }
    }

    public static final class HybridStage1 extends StrOp {
        @Override
        public String name() {
            return "bounded_hybrid_stage1";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit("h:" + input);
        }
    }

    public static final class HybridStage2 extends StrOp {
        @Override
        public String name() {
            return "bounded_hybrid_stage2";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit("final:" + input);
        }
    }

    /** Dynamic-segment operation (operationKey overridden) — used only for the rejection test. */
    public static final class DynOp extends StrOp {
        @Override
        public String name() {
            return "bounded_dyn_op";
        }

        @Override
        public Optional<String> operationKey(String input) {
            return Optional.of(input);
        }

        @Override
        public boolean operationKeyOverridden() {
            return true;
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) {}
    }

    public static final class CountState {
        long n;
    }

    public static final class Inc implements Change {}

    /** Static-segmented (even/odd by length parity), counts per-segment, emits {@code <n>:<item>}. */
    public static final class FanOp implements Operation<String, CountState, String, Inc, WallClock, Void> {
        @Override
        public String name() {
            return "bounded_fan_op";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public Class<Inc> changeType() {
            return Inc.class;
        }

        // A durable operative must register its Change type for the Kryo codec
        // (the flusher serializes Changes); mirrors the continuous contract.
        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(Inc.class);
        }

        @Override
        public Class<WallClock> clockType() {
            return WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public List<String> operationKeys() {
            return List.of("even", "odd");
        }

        @Override
        public boolean segmentForOverridden() {
            return true;
        }

        @Override
        public List<String> segmentFor(String input) {
            return input.length() % 2 == 0 ? List.of("even") : List.of("odd");
        }

        @Override
        public CountState initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new CountState();
        }

        @Override
        public void applyChange(CountState state, Inc change) {
            state.n += 1;
        }

        @Override
        public void run(String input, StateHandle<CountState, Inc> state, OperationCtx ctx) throws OperationException {
            state.apply(new Inc());
            long n = state.get().n;
            ctx.emit(n + ":" + input);
        }
    }

    // --- compensable (saga) operations ---------------------------------------

    /** Compensable: reserves (emits {@code reserved:<arg>}); undo logs {@code undo-reserve}. */
    public static final class SagaReserve extends StrOp {
        @Override
        public String name() {
            return "saga_reserve";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit("reserved:" + input);
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<Unit, NoChange> state, OperationCtx ctx) {
            for (String out : completedOutput) {
                COMPENSATION_LOG.add("undo-reserve:" + out);
            }
        }
    }

    /** Compensable: charges (emits {@code charged:<input>}); undo logs {@code undo-charge}. */
    public static final class SagaCharge extends StrOp {
        @Override
        public String name() {
            return "saga_charge";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit("charged:" + input);
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<Unit, NoChange> state, OperationCtx ctx) {
            for (String out : completedOutput) {
                COMPENSATION_LOG.add("undo-charge:" + out);
            }
        }
    }

    /** Terminal step that always fails — triggers reverse rollback; itself never compensated. */
    public static final class SagaShipFails extends StrOp {
        @Override
        public String name() {
            return "saga_ship_fails";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            throw new OperationException("ship failed");
        }
    }

    /** Terminal failing op for the per-segment rollback chain. */
    public static final class FailLast extends StrOp {
        @Override
        public String name() {
            return "saga_fail_last";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            throw new OperationException("fan ship failed");
        }
    }

    /** Per-segment state holding the segment key so {@code compensate} can identify its segment. */
    public static final class FanKeyState {
        String key = "";
    }

    /**
     * Static-segmented compensable op: stores its segment key in state, emits
     * {@code <key>:<item>}, and compensates ONCE per segment (logs all that
     * segment's outputs together) so per-segment firing is assertable.
     */
    public static final class SagaFan implements Operation<String, FanKeyState, String, NoChange, WallClock, Void> {
        @Override
        public String name() {
            return "saga_fan";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public Class<NoChange> changeType() {
            return NoChange.class;
        }

        @Override
        public Class<WallClock> clockType() {
            return WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public List<String> operationKeys() {
            return List.of("even", "odd");
        }

        @Override
        public boolean segmentForOverridden() {
            return true;
        }

        @Override
        public List<String> segmentFor(String input) {
            return input.length() % 2 == 0 ? List.of("even") : List.of("odd");
        }

        @Override
        public FanKeyState initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            FanKeyState s = new FanKeyState();
            s.key = operationKey.orElse("");
            return s;
        }

        @Override
        public void run(String input, StateHandle<FanKeyState, NoChange> state, OperationCtx ctx)
                throws OperationException {
            ctx.emit(state.get().key + ":" + input);
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(
                List<String> completedOutput, StateHandle<FanKeyState, NoChange> state, OperationCtx ctx) {
            COMPENSATION_LOG.add("undo-fan:" + state.get().key + ":[" + String.join(",", completedOutput) + "]");
        }
    }

    /**
     * Compensable stage-0 op with a handshake so a cooperative stop lands after
     * it completes but before stage 1 (deterministic forced-stop rollback).
     */
    public static final class StopReserve extends StrOp {
        @Override
        public String name() {
            return "saga_stop_reserve";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            STAGE0_RAN.set(true);
            while (!STAGE0_RELEASE.get()) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            ctx.emit("reserved:" + input);
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<Unit, NoChange> state, OperationCtx ctx) {
            for (String out : completedOutput) {
                COMPENSATION_LOG.add("undo-stop-reserve:" + out);
            }
        }
    }
}
