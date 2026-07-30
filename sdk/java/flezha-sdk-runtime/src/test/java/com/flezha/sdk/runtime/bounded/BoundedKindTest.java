package com.flezha.sdk.runtime.bounded;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeConfigurationException;
import com.flezha.sdk.bounded.BoundedOperativeCtx;
import com.flezha.sdk.bounded.BoundedOperativeException;
import com.flezha.sdk.bounded.RollbackMode;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.proto.AddOperative;
import com.flezha.sdk.proto.BoundedCompleted;
import com.flezha.sdk.proto.BoundedStop;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.OpCommittedOp;
import com.flezha.sdk.proto.OpEdgeHandoff;
import com.flezha.sdk.proto.OpHandoffItem;
import com.flezha.sdk.proto.OpJournalState;
import com.flezha.sdk.proto.RemOperative;
import com.flezha.sdk.proto.RestoreInitState;
import com.flezha.sdk.proto.RestoreInitStateResponse;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.AckRegistry;
import com.flezha.sdk.runtime.control.handlers.AddOperativeHandler;
import com.flezha.sdk.runtime.control.handlers.BoundedStopHandler;
import com.flezha.sdk.runtime.control.handlers.RemOperativeHandler;
import com.flezha.sdk.runtime.control.handlers.RestoreInitStateHandler;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.Operative;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstanceRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.SegmentId;
import com.flezha.sdk.runtime.state.SegmentStateSink;
import com.flezha.sdk.runtime.stream.PrimaryStreamReader;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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
import java.util.zip.CRC32;
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
        UnboundedOperativeClassRegistry.reset();
        BoundedOperativeClassRegistry.reset();
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
        BoundedOperativeClassRegistry.registerClass(EmitBoundedOperative.class);
        assertThat(BoundedOperativeClassRegistry.getSpec("test-emit-bounded")).isNotNull();
        assertThat(BoundedOperativeClassRegistry.factoryIds()).contains("test-emit-bounded");
    }

    @Test
    @DisplayName("a bounded factory colliding with a operative (either order) is rejected")
    void crossKindDuplicateRejected() {
        UnboundedOperativeClassRegistry.registerClass(CollideUnboundedOperative.class);
        assertThatThrownBy(() -> BoundedOperativeClassRegistry.registerClass(CollideBoundedOperative.class))
                .isInstanceOf(IllegalArgumentException.class);

        UnboundedOperativeClassRegistry.reset();
        BoundedOperativeClassRegistry.reset();

        BoundedOperativeClassRegistry.registerClass(CollideBoundedOperative.class);
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(CollideUnboundedOperative.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("an external-stop-only operative (no self-termination, no maxDuration) is rejected at registration")
    void externalStopOnlyAuthoringRejected() {
        assertThatThrownBy(() -> BoundedOperativeClassRegistry.registerClass(ExternalStopOnlyBoundedOperative.class))
                .isInstanceOf(BoundedOperativeConfigurationException.class)
                .hasMessageContaining("terminal condition");
    }

    @Test
    @DisplayName("a branching structured topology registers, and both branches run")
    void branchingTopologyRunsBothBranches() throws Exception {
        // Formerly rejected outright ("branching"). One producer now feeds two
        // downstream operations, and BFS order keeps completion topological — which
        // is why the compensation planner needed no change at all.
        BoundedOperativeClassRegistry.registerClass(BranchingBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-branching-bounded", "branch-1", "x"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
        // Both branches were fed from the shared producer's output.
        assertThat(cap.committedOpIdxs()).hasSize(3);
    }

    @Test
    @DisplayName("a dynamic-segment operation is rejected at registration")
    void dynamicSegmentOperationRejected() {
        assertThatThrownBy(() -> BoundedOperativeClassRegistry.registerClass(DynamicBoundedOperative.class))
                .isInstanceOf(BoundedOperativeConfigurationException.class)
                .hasMessageContaining("dynamic");
    }

    // --- Sealed Operative registry -------------------------------------------

    @Test
    @DisplayName(
            "the instance registry holds both variants; getUnboundedOperative is empty for a bounded; RemOperative removes a bounded")
    void sealedUnitHoldsBothVariants() {
        UnboundedOperativeInstanceRegistry registry = new UnboundedOperativeInstanceRegistry();
        registry.add(new BoundedOperativeInstance("t1", "echo_bounded"));

        assertThat(registry.contains("t1")).isTrue();
        assertThat(registry.get("t1")).isInstanceOf(BoundedOperativeInstance.class);
        assertThat(registry.getUnboundedOperative("t1"))
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
        BoundedOperativeClassRegistry.registerClass(EmitBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        RunnerContext ctx = newCtx(cap);

        List<String> errors = new ArrayList<>();
        new AddOperativeHandler().handle(ctx, addBoundedOperative("test-missing-bounded", "bounded-x", ""), errors);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("BoundedOperative class");
        assertThat(ctx.operativeInstanceRegistry().contains("bounded-x")).isFalse();
    }

    @Test
    @DisplayName("ctx.complete(result) carries the result payload into BoundedCompleted")
    void completeResultReachesCompletion() throws Exception {
        BoundedOperativeClassRegistry.registerClass(CompleteBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(LoopBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(LoopBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(LoopBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(TimedBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(EmitBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(FailBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(EchoStructuredOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(ChainBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(FanBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(HybridBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        UnboundedOperativeTopology topology = spec.topology().orElseThrow();
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
        BoundedOperativeClassRegistry.registerClass(FanBoundedOperative.class);
        BoundedOperativeClassRegistry.registerClass(FanOptOutBoundedOperative.class);

        List<byte[]> walFrames = new CopyOnWriteArrayList<>();
        SegmentStateSink.installGlobalSink((payloadId, frame) -> {
            walFrames.add(frame);
            return true;
        });

        // FanBoundedOperative is durable-by-default; its FanOp applies a Change
        // per item, which the driver flushes as a segment_state_change frame.
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);
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
        RunnerContext ctx2 = newCtx(cap2);
        cap2.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx2, addBoundedOperative("test-fan-optout-bounded", "fan-optout", "a,bb"), new ArrayList<>());
        assertThat(cap2.awaitCompletions()).isTrue();
        assertThat(cap2.decodeCompletion().getStatus()).isEqualTo("ok");
        assertThat(walFrames).as("an opt-out bounded writes nothing to the WAL").isEmpty();
    }

    // --- Op-journal (bounded-op-journal) -------------------------------------

    @Test
    @DisplayName("a durable chain journals an OpCommit at every op barrier")
    void durableChainJournalsEveryOpBoundary() throws Exception {
        BoundedOperativeClassRegistry.registerClass(ChainBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-chain-bounded", "journal-1", "x"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        // One commit per operation, in chain order — the durability boundary the
        // resume later reads to decide what NOT to re-run.
        assertThat(cap.committedOpIdxs()).containsExactly(0, 1);
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
    }

    @Test
    @DisplayName("an opted-out class journals nothing (it re-runs from the top by design)")
    void optedOutClassJournalsNothing() throws Exception {
        BoundedOperativeClassRegistry.registerClass(FanOptOutBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(
                        ctx,
                        addBoundedOperative("test-fan-optout-bounded", "journal-optout", "a,bb"),
                        new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.committedOpIdxs())
                .as("persistence opted out ⇒ no journal, no handshake")
                .isEmpty();
    }

    @Test
    @DisplayName("a durable structured class with a codec-less inter-op channel is refused")
    void missingInterOpCodecIsRefused() {
        assertThatThrownBy(() -> BoundedOperativeClassRegistry.registerClass(NoCodecBoundedOperative.class))
                .isInstanceOf(com.flezha.sdk.bounded.BoundedOperativeConfigurationException.class)
                .hasMessageContaining("MissingInterOpCodec")
                .hasMessageContaining("mid");
    }

    /** Durable + structured, but its inter-op handoff has no codec to journal. */
    public static final class NoCodecBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-no-codec-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sStage1 = c.input(String.class, "in").into(Stage1.class);
            var sStage2 = sStage1.named("mid").into(Stage2.class);
            sStage2.named("out").output("sink");
        }
    }

    // --- Staged commit + transactional outbox (bounded-transactional-outbox) --

    @Test
    @DisplayName("a durable op STAGES its output onto the barrier's outbox instead of shipping it")
    void durableOpStagesOutputOntoTheOutbox() throws Exception {
        BoundedOperativeClassRegistry.registerClass(ChainBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-chain-bounded", "outbox-1", "x"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        // The terminal op's emit rode its OpCommit's outbox — the SDK never wrote
        // it to the wire itself.
        List<String> staged = cap.outboxPayloads();
        assertThat(staged).containsExactly("x-1-2");
    }

    @Test
    @DisplayName("a failing stage leaks no staged output")
    void failingStageLeaksNoStagedOutput() throws Exception {
        BoundedOperativeClassRegistry.registerClass(EmitThenFailBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-emit-then-fail", "outbox-fail", "x"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        // The op emitted and THEN threw. Its staged bytes die with the exception:
        // they never reach a barrier, so they never become durable.
        assertThat(cap.outboxPayloads()).isEmpty();
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("error");
    }

    /** Emits, then throws — the leak-nothing case. */
    public static final class EmitThenFailBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-emit-then-fail";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sEmitThenFail = c.input(String.class, "in").into(EmitThenFail.class);
            sEmitThenFail.named("out").output("sink");
        }
    }

    public static final class EmitThenFail extends StrOp {
        @Override
        public String name() {
            return "emit_then_fail";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit("leaked:" + input);
            throw new OperationException("boom after emit");
        }
    }

    // --- Durable loop + suspension (bounded-durable-loop / -suspension) -------

    @Test
    @DisplayName("a PER_INPUT loop commits one iteration per inbox item, then PARKS when idle")
    void perInputLoopCommitsPerIterationThenParks() throws Exception {
        BoundedOperativeClassRegistry.registerClass(LoopingBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-looping-bounded", "loop-1", "seed"), new ArrayList<>());

        BoundedOperativeInstance instance =
                (BoundedOperativeInstance) ctx.operativeInstanceRegistry().get("loop-1");
        instance.offerInbox("turn-a".getBytes(UTF8));
        instance.offerInbox("turn-b".getBytes(UTF8));

        // The loop parks once idle past the hysteresis; that IS its terminal act.
        assertThat(cap.awaitPark()).isTrue();

        // One iteration commit per inbox item.
        assertThat(cap.committedIterations()).containsExactly(0, 1);

        // Each op commit is stamped with the iteration that enclosed it. The
        // controller keys an entry's outbox-flush marker by (type, op_idx, iter),
        // so leaving this at 0 makes turn-b's commit read as already-flushed and
        // its staged output is never delivered.
        assertThat(cap.opCommitIters()).containsExactly(0, 1);

        // A park is SUSPENSION, not completion: emitting BoundedCompleted would
        // tear the instance down and lose the iteration it parked at.
        assertThat(cap.hasCompletion()).isFalse();
    }

    @Test
    @DisplayName(
            "a resuming bind carries the op-journal, so a class with NO per-Segment state resumes at its iteration")
    void resumingBindCarriesTheOpJournalWithoutAnySegmentRestore() throws Exception {
        BoundedOperativeClassRegistry.registerClass(LoopingBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        // The journal is instance-scoped, so it rides the bind. A class with no
        // per-Segment state gets NO RestoreInitState at all (resumeSegmentCount
        // 0) — deliver it on the restore frame instead and this loop resumes at
        // iteration 0 on every wake, colliding with iteration 0's outbox-flush
        // marker and silently dropping every turn after the first.
        OpJournalState journal = OpJournalState.newBuilder().setIter(1).build();
        new AddOperativeHandler()
                .handle(
                        ctx,
                        addBoundedResumable("test-looping-bounded", "loop-resumed", "seed", 0, journal),
                        new ArrayList<>());

        BoundedOperativeInstance instance =
                (BoundedOperativeInstance) ctx.operativeInstanceRegistry().get("loop-resumed");
        instance.offerInbox("turn-c".getBytes(UTF8));

        assertThat(cap.awaitPark()).isTrue();
        // The iteration AFTER the last journaled one — never 0 again.
        assertThat(cap.committedIterations()).containsExactly(2);
        assertThat(cap.opCommitIters()).containsExactly(2);
    }

    @Test
    @DisplayName("a loop on run-sugar is refused (there is no chain to iterate)")
    void loopOnRunSugarIsRefused() {
        assertThatThrownBy(() -> BoundedOperativeClassRegistry.registerClass(LoopingSugarOperative.class))
                .isInstanceOf(com.flezha.sdk.bounded.BoundedOperativeConfigurationException.class)
                .hasMessageContaining("LoopWithoutChain");
    }

    @Test
    @DisplayName("a durable CHAINED loop whose terminal op has no codec is refused")
    void chainedLoopWithoutTerminalCodecIsRefused() {
        assertThatThrownBy(() -> BoundedOperativeClassRegistry.registerClass(ChainedNoCodecOperative.class))
                .isInstanceOf(com.flezha.sdk.bounded.BoundedOperativeConfigurationException.class)
                .hasMessageContaining("LoopTerminalWithoutCodec");
    }

    /** One iteration per inbox turn; parks when the turns stop. */
    public static final class LoopingBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-looping-bounded";
        }

        @Override
        public com.flezha.sdk.bounded.LoopMode loopMode() {
            return com.flezha.sdk.bounded.LoopMode.PER_INPUT;
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sStage1 = c.input(String.class, "in").into(Stage1.class);
            sStage1.named("out").codec().output("sink");
        }
    }

    /** run-sugar + a loop mode: nothing to iterate. */
    public static final class LoopingSugarOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-looping-sugar";
        }

        @Override
        public com.flezha.sdk.bounded.LoopMode loopMode() {
            return com.flezha.sdk.bounded.LoopMode.PER_INPUT;
        }

        @Override
        public void run(BoundedOperativeCtx ctx, String args) {}
    }

    /** Durable CHAINED, but the terminal op's output has no codec to re-seed from. */
    public static final class ChainedNoCodecOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-chained-no-codec";
        }

        @Override
        public com.flezha.sdk.bounded.LoopMode loopMode() {
            return com.flezha.sdk.bounded.LoopMode.CHAINED;
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sStage1 = c.input(String.class, "in").into(Stage1.class);
            sStage1.named("out").output("sink");
        }
    }

    // --- Segment retry ladder (bounded-segment-retry) -------------------------

    /** Attempt counter for the retry fixtures; reset per test. */
    private static final AtomicInteger RETRY_ATTEMPTS = new AtomicInteger();

    @Test
    @DisplayName("a transient segment failure recovers on retry, and emits its output ONCE")
    void transientSegmentFailureRecoversOnRetry() throws Exception {
        RETRY_ATTEMPTS.set(0);
        BoundedOperativeClassRegistry.registerClass(FlakyBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-flaky-bounded", "retry-1", "x"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
        assertThat(RETRY_ATTEMPTS.get())
                .as("first attempt failed, second succeeded")
                .isEqualTo(2);

        // The failed attempt's staged output died with its sink: the recovered
        // segment emits once, not once per attempt.
        assertThat(cap.outboxPayloads()).containsExactly("flaky:x");
    }

    @Test
    @DisplayName("a non-retryable failure skips the ladder entirely")
    void nonRetryableFailureSkipsTheLadder() throws Exception {
        RETRY_ATTEMPTS.set(0);
        BoundedOperativeClassRegistry.registerClass(PoisonBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-poison-bounded", "retry-poison", "x"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("error");
        assertThat(RETRY_ATTEMPTS.get())
                .as("a non-retryable error is not re-run")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("retryAttempts > 1 with a zero backoff is refused (it would hot-spin)")
    void zeroRetryBackoffIsRefused() {
        assertThatThrownBy(() -> BoundedOperativeClassRegistry.registerClass(ZeroBackoffOperative.class))
                .isInstanceOf(com.flezha.sdk.bounded.BoundedOperativeConfigurationException.class)
                .hasMessageContaining("ZeroRetryBackoff");
    }

    /** Fails once, then succeeds — the recover case. */
    public static final class FlakyBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-flaky-bounded";
        }

        @Override
        public java.time.Duration retryBackoff() {
            return java.time.Duration.ofMillis(10);
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sFlakyOp = c.input(String.class, "in").into(FlakyOp.class);
            sFlakyOp.named("out").output("sink");
        }
    }

    public static final class PoisonBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-poison-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sPoisonOp = c.input(String.class, "in").into(PoisonOp.class);
            sPoisonOp.named("out").output("sink");
        }
    }

    public static final class ZeroBackoffOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-zero-backoff";
        }

        @Override
        public java.time.Duration retryBackoff() {
            return java.time.Duration.ZERO;
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sStage1 = c.input(String.class, "in").into(Stage1.class);
            sStage1.named("out").output("sink");
        }
    }

    /** Emits, then fails on the FIRST attempt only — proving the staged emit is dropped with it. */
    public static final class FlakyOp extends StrOp {
        @Override
        public String name() {
            return "flaky_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit("flaky:" + input);
            if (RETRY_ATTEMPTS.incrementAndGet() == 1) {
                throw new OperationException("transient");
            }
        }
    }

    public static final class PoisonOp extends StrOp {
        @Override
        public String name() {
            return "retry_poison_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            RETRY_ATTEMPTS.incrementAndGet();
            throw new OperationException.NonRetryable("bad record");
        }
    }

    // --- Loop rewind (bounded-loop-rewind) -----------------------------------

    /** Undo hooks that fired, in order. */
    private static final List<String> REWIND_UNDO_LOG = new CopyOnWriteArrayList<>();

    @Test
    @DisplayName("a rewind unwinds the owed ops in reverse, and a HOOKLESS op still journals its drain")
    void rewindUnwindsOwedOpsInReverseAndDrainsHooklessOnes() throws Exception {
        REWIND_UNDO_LOG.clear();
        BoundedOperativeClassRegistry.registerClass(RewindSagaOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        // The controller's rewind marker says ops 0 (compensable) and 1 (HOOKLESS)
        // of a rewound iteration are still owed an undo. It rides the bind.
        OpJournalState journal = OpJournalState.newBuilder()
                .addRewindUndo(OpCommittedOp.newBuilder().setOpIdx(0).setIter(1).build())
                .addRewindUndo(OpCommittedOp.newBuilder().setOpIdx(1).setIter(1).build())
                .build();
        new AddOperativeHandler()
                .handle(ctx, addBoundedResumable("test-rewind-saga", "rewind-1", "x", 1, journal), new ArrayList<>());

        // The restore frame still releases the driver's resume gate.
        byte[] segId =
                SegmentId.of("rewind-1", "rewind_reserve", Optional.empty()).encodedBytes();
        CommandRequest restore = CommandRequest.newBuilder()
                .setReferenceId("r")
                .setRestoreInitState(RestoreInitState.newBuilder()
                        .setSegmentId(ByteString.copyFrom(segId))
                        .build())
                .build();
        new RestoreInitStateHandler().handle(ctx, restore, new ArrayList<>());

        assertThat(cap.awaitCompletions()).isTrue();

        // Reverse order: the later op is undone first.
        assertThat(REWIND_UNDO_LOG).containsExactly("undo-charge", "undo-reserve");

        // TWO compensating commits — one per undone op, INCLUDING the hookless one.
        // That empty record is what drains it from rewind_undo; without it the
        // controller would re-offer that op on every later restore, forever.
        assertThat(cap.compensatingCommits()).isEqualTo(2);
    }

    /** op0 compensable (RewindReserve) → op1 compensable (RewindCharge) → op2 HOOKLESS. */
    public static final class RewindSagaOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-rewind-saga";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sRewindReserve = c.input(String.class, "in").into(RewindReserve.class);
            var sRewindCharge = sRewindReserve.named("r1").codec().into(RewindCharge.class);
            sRewindCharge.named("out").output("sink");
        }
    }

    public static final class RewindReserve extends StrOp {
        @Override
        public String name() {
            return "rewind_reserve";
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
            REWIND_UNDO_LOG.add("undo-reserve");
        }
    }

    public static final class RewindCharge extends StrOp {
        @Override
        public String name() {
            return "rewind_charge";
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
            REWIND_UNDO_LOG.add("undo-charge");
        }
    }

    // --- Crash-resume (Phase 3b §5b.2) ---------------------------------------

    @Test
    @DisplayName("§5b.2 a durable bounded holds its first Operation until a Segment restore lands, then resumes it")
    void durableBoundedResumesFromRestoreThroughGate() throws Exception {
        BoundedOperativeClassRegistry.registerClass(FanBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        // Respawn expecting one Segment restore: AddOperativeHandler builds + wires
        // the Segments (reachable by the restore handler) before the driver spawns,
        // and the driver parks on the resume gate — nothing runs until the restore
        // lands.
        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedResumable("test-fan-bounded", "resume-inst", "a,bb", 1), new ArrayList<>());

        // Ship a WAL replay for the 'even' FanOp Segment carrying 5 Inc changes, so
        // its counter resumes at 5 (not a cold 0). The restore handler finds the
        // bounded Segment by wire identity, rebuilds its state, and releases the gate.
        KryoCodec codec = new KryoCodec(new FanOp().kryoTypes());
        HeaderTemplate template = new HeaderTemplate();
        byte[] changeLog = incChangeLog(codec, template, 5);
        byte[] evenId = SegmentId.of("resume-inst", "bounded_fan_op", Optional.of("even"))
                .encodedBytes();
        CommandRequest restore = CommandRequest.newBuilder()
                .setReferenceId("r")
                .setRestoreInitState(RestoreInitState.newBuilder()
                        .setSegmentId(ByteString.copyFrom(evenId))
                        .setChangeLogBytes(ByteString.copyFrom(changeLog))
                        .build())
                .build();
        List<String> errors = new ArrayList<>();
        byte[] resp = new RestoreInitStateHandler().handle(ctx, restore, errors);
        assertThat(errors).isEmpty();
        assertThat(RestoreInitStateResponse.parseFrom(resp).getTerminalSeq())
                .as("terminal seq acked as the highest WAL seq")
                .isEqualTo(4L);

        // The gate released; the driver runs. "bb" routes to 'even' (resumed at
        // 5 → 6), "a" to a cold 'odd' (→ 1) — proving resume-not-cold-start.
        assertThat(cap.awaitCompletions()).isTrue();
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
        assertThat(cap.outputs(SECONDARY_STREAM)).containsExactlyInAnyOrder("1:a", "6:bb");
    }

    // --- Saga compensation (Phase 2 §4 / §7b) --------------------------------

    @Test
    @DisplayName("§7b.1 a failed step rolls back completed steps in reverse, each with its own output")
    void sagaReverseOrderRollbackOnError() throws Exception {
        BoundedOperativeClassRegistry.registerClass(SagaBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(KeepProgressSagaOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(CleanSagaOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(FanSagaOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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
        BoundedOperativeClassRegistry.registerClass(StopSagaOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

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

    // --- Saga durable records + crash-resume (Phase 3c §4.3) -----------------

    @Test
    @DisplayName("§4.3 a durable saga records compensation_intent per completed op and compensation_done per undo")
    void sagaDurableEmitsCompensationRecords() throws Exception {
        BoundedOperativeClassRegistry.registerClass(SagaBoundedOperative.class);
        List<String> records = new CopyOnWriteArrayList<>();
        SegmentStateSink.installGlobalSink((payloadId, frame) -> {
            records.add(payloadId);
            return true;
        });

        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);
        cap.expectCompletions(1);
        // reserve + charge complete (both compensable ⇒ 2 intents); ship fails ⇒
        // both roll back (2 dones). Hookless ship_fails never completed ⇒ no record.
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-saga-bounded", "saga-rec", "42"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("error");

        long intents = records.stream()
                .filter(SegmentStateSink.PAYLOAD_ID_COMPENSATION_INTENT::equals)
                .count();
        long dones = records.stream()
                .filter(SegmentStateSink.PAYLOAD_ID_COMPENSATION_DONE::equals)
                .count();
        assertThat(intents).as("one intent per completed compensable segment").isEqualTo(2L);
        assertThat(dones).as("one done per compensated segment").isEqualTo(2L);
    }

    @Test
    @DisplayName("§4.3 crash-resume finishes the remaining hooks, skipping already-done ones, no forward re-run")
    void sagaCrashResumeSkipsDoneCompensations() throws Exception {
        BoundedOperativeClassRegistry.registerClass(ResumeSagaOperative.class);
        List<String> records = new CopyOnWriteArrayList<>();
        SegmentStateSink.installGlobalSink((payloadId, frame) -> {
            records.add(payloadId);
            return true;
        });

        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);
        // Respawn expecting two Segment restores (A + B); the driver parks on the gate.
        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedResumable("test-resume-saga", "resume-saga", "x", 2), new ArrayList<>());

        // Original run: A + B both completed (intent); B already undone before the
        // crash (done). Resume must undo A and SKIP B, without re-running forward.
        byte[] aId =
                SegmentId.of("resume-saga", "resume_undo_a", Optional.empty()).encodedBytes();
        byte[] bId =
                SegmentId.of("resume-saga", "resume_undo_b", Optional.empty()).encodedBytes();
        shipCompensationRestore(ctx, aId, true, false);
        shipCompensationRestore(ctx, bId, true, true);

        assertThat(cap.awaitCompletions()).isTrue();
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("error");
        assertThat(COMPENSATION_LOG).as("A undone, B skipped").containsExactly("undo-A");
        assertThat(cap.outputs(SECONDARY_STREAM)).as("forward ops not re-run").isEmpty();
        long dones = records.stream()
                .filter(SegmentStateSink.PAYLOAD_ID_COMPENSATION_DONE::equals)
                .count();
        long intents = records.stream()
                .filter(SegmentStateSink.PAYLOAD_ID_COMPENSATION_INTENT::equals)
                .count();
        assertThat(dones)
                .as("only A emits a fresh compensation_done; B was skipped")
                .isEqualTo(1L);
        assertThat(intents)
                .as("compensation-resume records no new intent (forward chain not re-run)")
                .isZero();
    }

    // --- helpers -------------------------------------------------------------

    /** Ship one bounded-Segment RestoreInitState carrying its saga compensation markers. */
    private static void shipCompensationRestore(RunnerContext ctx, byte[] segmentId, boolean intent, boolean done) {
        CommandRequest restore = CommandRequest.newBuilder()
                .setReferenceId("r")
                .setRestoreInitState(RestoreInitState.newBuilder()
                        .setSegmentId(ByteString.copyFrom(segmentId))
                        .setHasCompensationIntent(intent)
                        .setHasCompensationDone(done)
                        .build())
                .build();
        new RestoreInitStateHandler().handle(ctx, restore, new ArrayList<>());
    }

    private RunnerContext newCtx(Captured cap) {
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        RunnerContext ctx = new RunnerContext("ns", cfg, 0L);
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

    private static CommandRequest addBoundedResumable(
            String factoryId, String operativeId, String args, int resumeSegmentCount) {
        return addBoundedResumable(factoryId, operativeId, args, resumeSegmentCount, null);
    }

    /** The same, carrying the op-journal the controller ships on a resuming bind. */
    private static CommandRequest addBoundedResumable(
            String factoryId, String operativeId, String args, int resumeSegmentCount, OpJournalState journal) {
        AddOperative.Builder add = AddOperative.newBuilder()
                .setOperativeId(operativeId)
                .setFactoryId(factoryId)
                .setInitValues(args)
                .setKind("bounded")
                .setResumeSegmentCount(resumeSegmentCount);
        if (journal != null) {
            add.setOpJournal(journal);
        }
        return CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setAddOperative(add.build())
                .build();
    }

    /** A change-log of {@code count} Inc records (seq 0..count-1) in the controller's WAL framing. */
    private static byte[] incChangeLog(KryoCodec codec, HeaderTemplate template, int count) throws Exception {
        ByteArrayOutputStream log = new ByteArrayOutputStream();
        for (long seq = 0; seq < count; seq++) {
            byte[] body = codec.serialize(new Inc());
            byte[] header = template.stamp(new FanOp().schemaVersion(), body);
            int len = 16 + body.length;
            CRC32 crc = new CRC32();
            crc.update(header);
            crc.update(body);
            try (ByteArrayOutputStream rec = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(rec)) {
                out.writeLong(seq);
                out.writeInt(len);
                out.write(header);
                out.write(body);
                out.writeInt((int) crc.getValue());
                log.writeBytes(rec.toByteArray());
            }
        }
        return log.toByteArray();
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
            if (streamId == CONTROL_STREAM) {
                handleControlFrame(frame);
            }
            return 0;
        }

        /**
         * Stand in for the controller on the control stream.
         *
         * <p>An {@code OpCommit} is acked under its own reference_id, as the
         * controller does after its journal fsync — the driver BLOCKS at every op
         * barrier waiting for it, so a fake controller that stayed silent would
         * hang the run until the 30 s timeout and fail it as un-journaled.
         *
         * <p>Only a {@code BoundedCompleted} counts down the completion latch: the
         * control stream now carries op commits too, and counting those would
         * release the test mid-chain.
         */
        private void handleControlFrame(byte[] frame) {
            try {
                FrameParser parser = new FrameParser();
                for (Frame f : parser.feed(frame)) {
                    CommandRequest request = CommandRequest.parseFrom(f.message());
                    switch (request.getCommandCase()) {
                        case OP_COMMIT -> {
                            // The real controller journals the commit, fsyncs, acks,
                            // and THEN flushes the outbox — the op's plugin output
                            // becomes durable with the op itself. Stand in for that
                            // flush so the output assertions still observe the
                            // emits, now arriving via the transactional outbox.
                            flushOutbox(request.getOpCommit());
                            AckRegistry.complete(request.getReferenceId());
                        }
                        case ITER_COMMIT -> AckRegistry.complete(request.getReferenceId());
                        case ITER_PARK -> parked.countDown();
                        case BOUNDED_COMPLETED -> {
                            if (completions != null) {
                                completions.countDown();
                            }
                        }
                        default -> {}
                    }
                }
            } catch (Exception ignored) {
                // Not a decodable control frame — nothing to do.
            }
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

        /**
         * The run's terminal {@code BoundedCompleted}. The control stream also
         * carries an {@code OpCommit} per op barrier now, so this selects the
         * completion rather than assuming a single frame.
         */
        BoundedCompleted decodeCompletion() throws Exception {
            for (byte[] raw : framesOn(CONTROL_STREAM)) {
                for (Frame f : new FrameParser().feed(raw)) {
                    assertThat(f.payloadId()).isEqualTo("control_command");
                    CommandRequest request = CommandRequest.parseFrom(f.message());
                    if (request.getCommandCase() == CommandRequest.CommandCase.BOUNDED_COMPLETED) {
                        return request.getBoundedCompleted();
                    }
                }
            }
            throw new AssertionError("no BoundedCompleted was emitted on the control stream");
        }

        /**
         * Replay a committed op's outbox onto the secondary stream, exactly as the
         * controller's post-ack flush does. Without this the staged output would
         * never reach the wire in a unit test and every emit assertion would fail
         * — which is precisely the point of the transactional outbox: the output
         * is no longer the SDK's to ship.
         */
        private void flushOutbox(com.flezha.sdk.proto.OpCommit commit) {
            for (com.flezha.sdk.proto.OpOutboxItem item : commit.getOutboxList()) {
                meta.add(new long[] {SECONDARY_STREAM});
                frames.add(FrameEncoder.build(
                        commit.getOperativeId(), item.getPayload().toByteArray(), Map.of()));
            }
        }

        private final java.util.concurrent.CountDownLatch parked = new java.util.concurrent.CountDownLatch(1);

        /** Wait for the loop to suspend. */
        boolean awaitPark() throws InterruptedException {
            return parked.await(15, TimeUnit.SECONDS);
        }

        /** The iterations the driver journaled, in commit order. */
        List<Integer> committedIterations() throws Exception {
            List<Integer> out = new ArrayList<>();
            for (byte[] raw : framesOn(CONTROL_STREAM)) {
                for (Frame f : new FrameParser().feed(raw)) {
                    CommandRequest request = CommandRequest.parseFrom(f.message());
                    if (request.getCommandCase() == CommandRequest.CommandCase.ITER_COMMIT) {
                        out.add(request.getIterCommit().getIter());
                    }
                }
            }
            return out;
        }

        boolean hasCompletion() throws Exception {
            for (byte[] raw : framesOn(CONTROL_STREAM)) {
                for (Frame f : new FrameParser().feed(raw)) {
                    if (CommandRequest.parseFrom(f.message()).getCommandCase()
                            == CommandRequest.CommandCase.BOUNDED_COMPLETED) {
                        return true;
                    }
                }
            }
            return false;
        }

        /** Every payload the driver staged onto an OpCommit's outbox, in commit order. */
        List<String> outboxPayloads() throws Exception {
            List<String> out = new ArrayList<>();
            for (byte[] raw : framesOn(CONTROL_STREAM)) {
                for (Frame f : new FrameParser().feed(raw)) {
                    CommandRequest request = CommandRequest.parseFrom(f.message());
                    if (request.getCommandCase() == CommandRequest.CommandCase.OP_COMMIT) {
                        for (com.flezha.sdk.proto.OpOutboxItem item :
                                request.getOpCommit().getOutboxList()) {
                            out.add(new String(item.getPayload().toByteArray(), UTF8));
                        }
                    }
                }
            }
            return out;
        }

        /** How many compensating (undo) commits the driver journaled. */
        int compensatingCommits() throws Exception {
            int n = 0;
            for (byte[] raw : framesOn(CONTROL_STREAM)) {
                for (Frame f : new FrameParser().feed(raw)) {
                    CommandRequest request = CommandRequest.parseFrom(f.message());
                    if (request.getCommandCase() == CommandRequest.CommandCase.OP_COMMIT
                            && request.getOpCommit().getCompensating()) {
                        n++;
                    }
                }
            }
            return n;
        }

        /** The op indices the driver journaled, in commit order. */
        List<Integer> committedOpIdxs() throws Exception {
            List<Integer> out = new ArrayList<>();
            for (byte[] raw : framesOn(CONTROL_STREAM)) {
                for (Frame f : new FrameParser().feed(raw)) {
                    CommandRequest request = CommandRequest.parseFrom(f.message());
                    if (request.getCommandCase() == CommandRequest.CommandCase.OP_COMMIT) {
                        out.add(request.getOpCommit().getOpIdx());
                    }
                }
            }
            return out;
        }

        /** The enclosing iteration each forward op commit carried, in commit order. */
        List<Integer> opCommitIters() throws Exception {
            List<Integer> out = new ArrayList<>();
            for (byte[] raw : framesOn(CONTROL_STREAM)) {
                for (Frame f : new FrameParser().feed(raw)) {
                    CommandRequest request = CommandRequest.parseFrom(f.message());
                    if (request.getCommandCase() == CommandRequest.CommandCase.OP_COMMIT
                            && !request.getOpCommit().getCompensating()) {
                        out.add(request.getOpCommit().getIter());
                    }
                }
            }
            return out;
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

    public static final class CollideUnboundedOperative implements UnboundedOperative {
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
            var sEchoOp = c.input(String.class, "in").into(EchoOp.class);
            sEchoOp.named("out").output("sink");
        }
    }

    public static final class ChainBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-chain-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sStage1 = c.input(String.class, "in").into(Stage1.class);
            var sStage2 = sStage1.named("mid").codec().into(Stage2.class);
            sStage2.named("out").output("sink");
        }
    }

    public static final class FanBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-fan-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sSplitOp = c.input(String.class, "in").into(SplitOp.class);
            var sFanOp = sSplitOp.named("mid").codec().into(FanOp.class);
            sFanOp.named("out").output("sink");
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
            var sSplitOp = c.input(String.class, "in").into(SplitOp.class);
            var sFanOp = sSplitOp.named("mid").codec().into(FanOp.class);
            sFanOp.named("out").output("sink");
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
            var sHybridStage1 = c.input(String.class, "in").into(HybridStage1.class);
            var sHybridStage2 = sHybridStage1
                    .named("mid")
                    .codec()
                    .alsoOutput("midsink", (String s) -> ("mid:" + s).getBytes(UTF8))
                    .into(HybridStage2.class);
            sHybridStage2.named("out").output("out");
        }
    }

    public static final class BranchingBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-branching-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sStage1 = c.input(String.class, "in").into(Stage1.class);
            var chmid = sStage1.named("mid").codec();
            var sStage2 = chmid.into(Stage2.class);
            var sEchoOp = chmid.into(EchoOp.class);
            sStage2.named("out2").output("sink");
            sEchoOp.named("out3").output("sink");
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
            var sSagaReserve = c.input(String.class, "in").into(SagaReserve.class);
            var sSagaCharge = sSagaReserve.named("c1").codec().into(SagaCharge.class);
            var sSagaShipFails = sSagaCharge.named("c2").codec().into(SagaShipFails.class);
            sSagaShipFails.named("out").output("sink");
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
            var sSagaReserve = c.input(String.class, "in").into(SagaReserve.class);
            var sSagaCharge = sSagaReserve.named("c1").codec().into(SagaCharge.class);
            var sSagaShipFails = sSagaCharge.named("c2").codec().into(SagaShipFails.class);
            sSagaShipFails.named("out").output("sink");
        }
    }

    /** A → B → ship(fails). Durable-by-default; both A and B are compensable (§4.3 resume). */
    public static final class ResumeSagaOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-resume-saga";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var sResumeUndoA = c.input(String.class, "in").into(ResumeUndoA.class);
            var sResumeUndoB = sResumeUndoA.named("c1").codec().into(ResumeUndoB.class);
            var sSagaShipFails = sResumeUndoB.named("c2").codec().into(SagaShipFails.class);
            sSagaShipFails.named("out").output("sink");
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
            var sSagaReserve = c.input(String.class, "in").into(SagaReserve.class);
            var sSagaCharge = sSagaReserve.named("c1").codec().into(SagaCharge.class);
            var sEchoOp = sSagaCharge.named("c2").codec().into(EchoOp.class);
            sEchoOp.named("out").output("sink");
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
            var sSplitOp = c.input(String.class, "in").into(SplitOp.class);
            var sSagaFan = sSplitOp.named("mid").codec().into(SagaFan.class);
            var sFailLast = sSagaFan.named("mid2").codec().into(FailLast.class);
            sFailLast.named("out").output("sink");
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
            var sStopReserve = c.input(String.class, "in").into(StopReserve.class);
            var sEchoOp = sStopReserve.named("mid").codec().into(EchoOp.class);
            sEchoOp.named("out").output("sink");
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
        // (the flusher serializes Changes); mirrors the unbounded contract.
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

    /**
     * Compensable; undo logs {@code undo-A} UNCONDITIONALLY (not per output) so a
     * crash-resume — which reconstructs completed stages with EMPTY outputs (an
     * operation's collected output was never durable) — is still observable.
     */
    public static final class ResumeUndoA extends StrOp {
        @Override
        public String name() {
            return "resume_undo_a";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit("a:" + input);
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<Unit, NoChange> state, OperationCtx ctx) {
            COMPENSATION_LOG.add("undo-A");
        }
    }

    /** Compensable twin of {@link ResumeUndoA}; undo logs {@code undo-B} unconditionally. */
    public static final class ResumeUndoB extends StrOp {
        @Override
        public String name() {
            return "resume_undo_b";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit("b:" + input);
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<Unit, NoChange> state, OperationCtx ctx) {
            COMPENSATION_LOG.add("undo-B");
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

    // --- Fan-in diamond (`bounded-branching` D4 / `sdk-graph-authoring`) -----

    /** Forward runs + join dispatches of the diamond fixtures, in order. */
    private static final List<String> DIAMOND_LOG = new CopyOnWriteArrayList<>();

    @Test
    @DisplayName("a join runs once, after BOTH producers committed, with the accumulated batch")
    void joinAccumulatesBothProducersAndRunsOnce() throws Exception {
        DIAMOND_LOG.clear();
        BoundedOperativeClassRegistry.registerClass(DiamondBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-diamond-bounded", "diamond-1", "x"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
        // Kahn positions: A=0, B=1, C=2, join=3 — the join commits once, last.
        assertThat(cap.committedOpIdxs()).containsExactly(0, 1, 2, 3);
        assertThat(DIAMOND_LOG)
                .as("both producers run before the join; the join sees BOTH contributions once")
                .containsExactly("a:x", "b:a:x", "c:a:x", "j:b:a:x", "j:c:a:x");
    }

    @Test
    @DisplayName("a join resumes with one JOURNALED and one fresh contribution; the committed branch never re-runs")
    void joinResumesWithJournaledAndFreshContributions() throws Exception {
        DIAMOND_LOG.clear();
        BoundedOperativeClassRegistry.registerClass(DiamondBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        var topology =
                BoundedSpec.build(DiamondBoundedOperative.class).topology().orElseThrow();
        // The journal says A (position 0) and B (position 1) committed before the
        // crash, each with its edge handoff.
        OpJournalState journal = abCommittedJournal(topology, "dfan", "db");
        new AddOperativeHandler()
                .handle(
                        ctx,
                        addBoundedResumable("test-diamond-bounded", "diamond-r", "x", 1, journal),
                        new ArrayList<>());

        // The restore frame still releases the driver's resume gate.
        byte[] segId = SegmentId.of("diamond-r", "diam_a", Optional.empty()).encodedBytes();
        CommandRequest restore = CommandRequest.newBuilder()
                .setReferenceId("r")
                .setRestoreInitState(RestoreInitState.newBuilder()
                        .setSegmentId(ByteString.copyFrom(segId))
                        .build())
                .build();
        new RestoreInitStateHandler().handle(ctx, restore, new ArrayList<>());

        assertThat(cap.awaitCompletions()).isTrue();
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
        // Only the uncommitted branch (C, position 2) and the join re-run.
        assertThat(cap.committedOpIdxs()).containsExactly(2, 3);
        assertThat(DIAMOND_LOG)
                .as("committed producers never re-run; the join accumulates B's"
                        + " journaled handoff with C's fresh output")
                .containsExactly("c:a:x", "j:b:a:x", "j:c:a:x");
    }

    @Test
    @DisplayName("reverse-topological undo visits the join before either producer, the shared root last")
    void reverseTopologicalUndoVisitsJoinFirst() throws Exception {
        DIAMOND_LOG.clear();
        BoundedOperativeClassRegistry.registerClass(DiamondSagaOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-diamond-saga", "diamond-s", "x"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("error");
        assertThat(DIAMOND_LOG.stream().filter(e -> e.startsWith("undo-")).toList())
                .containsExactly("undo-join", "undo-c", "undo-b", "undo-a");
    }

    // --- Fan-in wired as a merge (`multi-input-operations`) -------------------

    @Test
    @DisplayName("a merged fan-in maps each producer's contribution on the edge it arrived on")
    void mergedJoinMapsEachProducersContribution() throws Exception {
        DIAMOND_LOG.clear();
        BoundedOperativeClassRegistry.registerClass(MergeBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-merge-bounded", "merge-1", "x"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
        assertThat(cap.committedOpIdxs()).containsExactly(0, 1, 2, 3);
        assertThat(DIAMOND_LOG)
                .as("the join's input is neither producer's Output; each edge's mapping lifts it")
                .containsExactly("a:x", "b:a:x", "c:a:x", "mb:b:a:x", "mc:c:a:x");
    }

    @Test
    @DisplayName("a resumed merge lifts the journaled handoff with the same mapping the live run used")
    void mergedJoinResumesWithMappedJournaledHandoff() throws Exception {
        DIAMOND_LOG.clear();
        BoundedOperativeClassRegistry.registerClass(MergeBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        var topology = BoundedSpec.build(MergeBoundedOperative.class).topology().orElseThrow();
        OpJournalState journal = abCommittedJournal(topology, "mfan", "mb");
        new AddOperativeHandler()
                .handle(ctx, addBoundedResumable("test-merge-bounded", "merge-r", "x", 1, journal), new ArrayList<>());

        byte[] segId = SegmentId.of("merge-r", "diam_a", Optional.empty()).encodedBytes();
        new RestoreInitStateHandler()
                .handle(
                        ctx,
                        CommandRequest.newBuilder()
                                .setReferenceId("r")
                                .setRestoreInitState(RestoreInitState.newBuilder()
                                        .setSegmentId(ByteString.copyFrom(segId))
                                        .build())
                                .build(),
                        new ArrayList<>());

        assertThat(cap.awaitCompletions()).isTrue();
        assertThat(cap.decodeCompletion().getStatus()).isEqualTo("ok");
        assertThat(cap.committedOpIdxs()).containsExactly(2, 3);
        assertThat(DIAMOND_LOG)
                .as("B's journaled handoff is decoded then mapped, exactly as the live run mapped it")
                .containsExactly("c:a:x", "mb:b:a:x", "mc:c:a:x");
    }

    @Test
    @DisplayName("a throwing merge mapping is contained: the instance still reports and compensates")
    void aThrowingMergeMappingIsContained() throws Exception {
        // The mapping runs on the driver thread, outside any segment
        // invocation. Uncontained it would unwind the driver before
        // BoundedCompleted is emitted and strand the instance with no terminal
        // frame — so the observable outcome is what this asserts.
        DIAMOND_LOG.clear();
        BoundedOperativeClassRegistry.registerClass(ThrowingMergeBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler()
                .handle(ctx, addBoundedOperative("test-throwing-merge-bounded", "merge-throw", "x"), new ArrayList<>());
        assertThat(cap.awaitCompletions())
                .as("the operative still reports — the throw did not take the driver with it")
                .isTrue();

        BoundedCompleted completion = cap.decodeCompletion();
        assertThat(completion.getStatus()).isEqualTo("error");
        assertThat(completion.getError()).contains("merge mapping threw");
        assertThat(completion.getError()).contains("merge mapping blew up");
        // The saga fires over exactly what was RECORDED as completed when the
        // abort began: the producer whose edge threw (B) and its parent (A),
        // reverse-topologically. Same exact list the Rust arm asserts — this is
        // where the two would silently diverge if the release were ordered
        // before the completion record.
        List<String> undo =
                DIAMOND_LOG.stream().filter(e -> e.startsWith("undo-")).toList();
        assertThat(undo).containsExactly("undo-b", "undo-a");
    }

    @Test
    @DisplayName("a merge mapping that throws while re-seeding from the journal is contained")
    void aThrowingMergeMappingIsContainedOnCrashResume() throws Exception {
        // The second mapping site: bankJournaledHandoffs maps a committed
        // producer's JOURNALED handoff into its uncommitted consumers, on the
        // driver thread and before any stage runs. It needs the same
        // containment as the forward release, and it is reached only on a
        // resume — hence the journal below rather than a plain start.
        DIAMOND_LOG.clear();
        BoundedOperativeClassRegistry.registerClass(ThrowingMergeBoundedOperative.class);
        Captured cap = new Captured();
        RunnerContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        var topology = BoundedSpec.build(ThrowingMergeBoundedOperative.class)
                .topology()
                .orElseThrow();
        OpJournalState journal = abCommittedJournal(topology, "tmfan", "tmb");
        new AddOperativeHandler()
                .handle(
                        ctx,
                        addBoundedResumable("test-throwing-merge-bounded", "merge-throw-r", "x", 1, journal),
                        new ArrayList<>());

        byte[] segId = SegmentId.of("merge-throw-r", "diam_a", Optional.empty()).encodedBytes();
        new RestoreInitStateHandler()
                .handle(
                        ctx,
                        CommandRequest.newBuilder()
                                .setReferenceId("r")
                                .setRestoreInitState(RestoreInitState.newBuilder()
                                        .setSegmentId(ByteString.copyFrom(segId))
                                        .build())
                                .build(),
                        new ArrayList<>());

        assertThat(cap.awaitCompletions())
                .as("the operative still reports — the throw did not take the driver with it")
                .isTrue();
        BoundedCompleted completion = cap.decodeCompletion();
        assertThat(completion.getStatus()).isEqualTo("error");
        assertThat(completion.getError()).contains("merge mapping threw");
        assertThat(completion.getError()).contains("merge mapping blew up");
        // The undo list is deliberately NOT asserted here. A resumed Java chain
        // does not seed its completed list from the op-journal, so nothing is
        // compensated on this path — a pre-existing gap recorded under "Known
        // limitation" in docs/multi_input_operations.md. Pinning that value
        // would turn the limitation into a contract and make closing it a
        // test failure; containment and the terminal report are what this
        // capability actually guarantees.
    }

    /** The merged fan-in whose mapping throws. */
    public static final class ThrowingMergeBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-throwing-merge-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var a = c.input(String.class, "in").into(DiamA.class).named("tmfan").codec();
            var b = a.into(DiamB.class).named("tmb").codec();
            var cc = a.into(DiamC.class).named("tmc").codec();
            c.merge(MergedJoinInput.class)
                    .from(b, v -> {
                        throw new IllegalStateException("merge mapping blew up");
                    })
                    .from(cc, MergedJoinInput::fromC)
                    .into(MergeJoinOp.class)
                    .named("tmout")
                    .output("sink");
        }
    }

    @Test
    @DisplayName("a bounded merge over an entry channel is refused at registration, by name")
    void boundedMergeOverAnEntryChannelIsRefused() {
        // The seed is a bare payload naming no channel, so the mapping could
        // never be selected and every entry channel past the first would be
        // unreadable. Refused at definition time instead of accepted and
        // failing every invocation on a type mismatch.
        assertThatThrownBy(() -> BoundedOperativeClassRegistry.registerClass(EntryMergeBoundedOperative.class))
                .isInstanceOf(BoundedOperativeConfigurationException.class)
                .hasMessageContaining("MergeFromEntryChannel")
                .hasMessageContaining("`in`")
                .hasMessageContaining("bounded_merge_join_op");
    }

    /** The unsupported shape: a merge whose sources are entry channels. */
    public static final class EntryMergeBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-entry-merge-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.merge(MergedJoinInput.class)
                    .from(c.input(String.class, "in"), MergedJoinInput::fromB)
                    .from(c.input(String.class, "also"), MergedJoinInput::fromC)
                    .into(MergeJoinOp.class);
        }
    }

    /**
     * The merged input of the fan-in below: both producers carry the same
     * payload type, so only the mapping tells the join which branch a
     * contribution came from.
     */
    public record MergedJoinInput(String branch, String value) {
        static MergedJoinInput fromB(String v) {
            return new MergedJoinInput("mb", v);
        }

        static MergedJoinInput fromC(String v) {
            return new MergedJoinInput("mc", v);
        }
    }

    /** The diamond's join re-authored as a merge over one sum-type input. */
    public static final class MergeJoinOp
            implements Operation<MergedJoinInput, Unit, String, NoChange, WallClock, Void> {
        @Override
        public String name() {
            return "bounded_merge_join_op";
        }

        @Override
        public Class<MergedJoinInput> inputType() {
            return MergedJoinInput.class;
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

        @Override
        public void run(MergedJoinInput input, StateHandle<Unit, NoChange> state, OperationCtx ctx)
                throws OperationException {
            String out = input.branch() + ":" + input.value();
            DIAMOND_LOG.add(out);
            ctx.emit(out);
        }
    }

    /** A→(B,C)→merge(Join): the fan-in wired as a merge. */
    public static final class MergeBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-merge-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var a = c.input(String.class, "in").into(DiamA.class).named("mfan").codec();
            var b = a.into(DiamB.class).named("mb").codec();
            var cc = a.into(DiamC.class).named("mc").codec();
            c.merge(MergedJoinInput.class)
                    .from(b, MergedJoinInput::fromB)
                    .from(cc, MergedJoinInput::fromC)
                    .into(MergeJoinOp.class)
                    .named("mout")
                    .output("sink");
        }
    }

    private static int channelIdByName(UnboundedOperativeTopology topology, String name) {
        for (int i = 0; i < topology.channels().size(); i++) {
            if (topology.channels().get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalStateException("channel `" + name + "` missing");
    }

    /**
     * The journal a diamond-shaped chain leaves when A (the fan, position 0)
     * and B (one branch, position 1) committed before the crash: each
     * producer's edge handoff, encoded with its channel's inter-op codec. The
     * branching resume and both merge resumes differ only in the two channel
     * names.
     */
    private static OpJournalState abCommittedJournal(
            UnboundedOperativeTopology topology, String fanChannel, String bChannel) {
        return OpJournalState.newBuilder()
                .addCommittedOpIdxs(0)
                .addCommittedOpIdxs(1)
                .addOpHandoffs(handoff(topology, 0, fanChannel, "a:x"))
                .addOpHandoffs(handoff(topology, 1, bChannel, "b:a:x"))
                .build();
    }

    private static OpEdgeHandoff handoff(
            UnboundedOperativeTopology topology, int opIdx, String channelName, String value) {
        int channelId = channelIdByName(topology, channelName);
        byte[] payload = topology.channel(channelId)
                .interOpCodec()
                .orElseThrow()
                .encoder()
                .apply(value);
        return OpEdgeHandoff.newBuilder()
                .setOpIdx(opIdx)
                .addHandoff(OpHandoffItem.newBuilder()
                        .setChannelId(channelId)
                        .setPayload(ByteString.copyFrom(payload))
                        .build())
                .build();
    }

    /** A→(B,C)→Join: the fan-in diamond, join plugin-terminal. */
    public static final class DiamondBoundedOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-diamond-bounded";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var a = c.input(String.class, "in").into(DiamA.class).named("dfan").codec();
            a.into(DiamB.class).named("db").codec().into(DiamJoin.class);
            a.into(DiamC.class)
                    .named("dc")
                    .codec()
                    .into(DiamJoin.class)
                    .named("dout")
                    .output("sink");
        }
    }

    /** The diamond with a failing tail after the join: A→(B,C)→Join→ShipFails. */
    public static final class DiamondSagaOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "test-diamond-saga";
        }

        @Override
        public void operations(ChannelBuilder c) {
            var a = c.input(String.class, "in").into(DiamA.class).named("dfan").codec();
            var join = a.into(DiamB.class).named("db").codec().into(DiamJoin.class);
            a.into(DiamC.class).named("dc").codec().into(DiamJoin.class);
            join.named("djoin").codec().into(DiamShipFails.class).named("dout").output("sink");
        }
    }

    abstract static class DiamondOp extends StrOp {
        abstract String tag();

        @Override
        public String name() {
            return "diam_" + tag();
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            String out = tag().substring(0, 1) + ":" + input;
            DIAMOND_LOG.add(out);
            ctx.emit(out);
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<Unit, NoChange> state, OperationCtx ctx) {
            DIAMOND_LOG.add("undo-" + tag());
        }
    }

    public static final class DiamA extends DiamondOp {
        @Override
        String tag() {
            return "a";
        }
    }

    public static final class DiamB extends DiamondOp {
        @Override
        String tag() {
            return "b";
        }
    }

    public static final class DiamC extends DiamondOp {
        @Override
        String tag() {
            return "c";
        }
    }

    public static final class DiamJoin extends DiamondOp {
        @Override
        String tag() {
            return "join";
        }
    }

    public static final class DiamShipFails extends StrOp {
        @Override
        public String name() {
            return "diam_ship_fails";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            throw new OperationException("diamond ship failed");
        }
    }
}
