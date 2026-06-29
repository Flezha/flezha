package com.flezha.sdk.runtime.bounded;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.proto.AddOperative;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.BoundedCompleted;
import com.flezha.sdk.operative.ContinuousOperative;
import com.flezha.sdk.operative.ContinuousOperativeRegistry;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.handlers.AddOperativeHandler;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.operative.ContinuousOperativeInstanceRegistry;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.Operative;
import com.flezha.sdk.runtime.stream.PrimaryStreamReader;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeCtx;
import com.flezha.sdk.bounded.BoundedOperativeException;
import com.flezha.sdk.bounded.BoundedOperativeRegistry;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BoundedKindTest {

    private static final long CONTROL_STREAM = 1L;
    private static final long PRIMARY_STREAM = 2L;
    private static final long SECONDARY_STREAM = 3L;

    private static final AtomicInteger EMIT_RUNS = new AtomicInteger(0);

    @BeforeEach
    void reset() {
        ContinuousOperativeRegistry.reset();
        BoundedOperativeRegistry.reset();
        EMIT_RUNS.set(0);
    }

    @AfterEach
    void restoreWriter() {
        BoundedOperativeRunner.resetWireWriter();
    }

    // --- 5.1 Registration ---

    @Test
    @DisplayName("registerBounded makes the factory resolvable")
    void registerBoundedOperativeResolvable() {
        assertThat(BoundedOperativeClassRegistry.getBoundedOperativeClass("test-emit-bounded")).isNull();
        BoundedOperativeRegistry.registerBounded(EmitBoundedOperative.class);
        assertThat(BoundedOperativeClassRegistry.getBoundedOperativeClass("test-emit-bounded")).isNotNull();
        assertThat(BoundedOperativeRegistry.factoryIds()).contains("test-emit-bounded");
    }

    @Test
    @DisplayName("registerBounded after freeze throws IllegalStateException")
    void registerAfterFreezeRejected() {
        BoundedOperativeRegistry.freeze();
        assertThatThrownBy(() -> BoundedOperativeRegistry.registerBounded(EmitBoundedOperative.class)).isInstanceOf(IllegalStateException.class);
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

    // --- 5.2 Sealed Operative registry ---

    @Test
    @DisplayName("the instance registry holds both variants; getContinuousOperative is empty for a bounded; RemOperative removes a bounded")
    void sealedUnitHoldsBothVariants() {
        ContinuousOperativeInstanceRegistry registry = new ContinuousOperativeInstanceRegistry();
        registry.add(new BoundedOperativeInstance("t1", "echo_bounded"));

        assertThat(registry.contains("t1")).isTrue();
        assertThat(registry.get("t1")).isInstanceOf(BoundedOperativeInstance.class);
        assertThat(registry.getContinuousOperative("t1")).as("a bounded is not a operative").isNull();
        assertThat(registry.operativeSnapshot()).doesNotContainKey("t1");

        Operative removed = registry.remove("t1");
        assertThat(removed).isInstanceOf(BoundedOperativeInstance.class);
        assertThat(registry.contains("t1")).isFalse();
    }

    // --- 5.3 Dispatch + run-once ---

    @Test
    @DisplayName("AddOperative kind=bounded spawns a bounded, runs once with init_values, and is idempotent")
    void addContinuousOperativeBoundedOperativeRunsOnce() throws Exception {
        BoundedOperativeRegistry.registerBounded(EmitBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler().handle(ctx, addBoundedOperative("test-emit-bounded", "bounded-ok", "hello"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(EMIT_RUNS.get()).as("run must execute exactly once").isEqualTo(1);
        assertThat(ctx.operativeInstanceRegistry().get("bounded-ok")).isInstanceOf(BoundedOperativeInstance.class);

        // Duplicate AddOperative for a live id is idempotent — no re-run.
        new AddOperativeHandler().handle(ctx, addBoundedOperative("test-emit-bounded", "bounded-ok", "hello"), new ArrayList<>());
        Thread.sleep(100);
        assertThat(EMIT_RUNS.get()).as("duplicate start must not re-run").isEqualTo(1);
    }

    @Test
    @DisplayName("AddOperative kind=bounded with an unregistered factory reports an error and creates nothing")
    void addContinuousOperativeBoundedOperativeUnknownFactory() {
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        List<String> errors = new ArrayList<>();
        new AddOperativeHandler().handle(ctx, addBoundedOperative("test-missing-bounded", "bounded-x", ""), errors);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("BoundedOperative class");
        assertThat(ctx.operativeInstanceRegistry().contains("bounded-x")).isFalse();
    }

    @Test
    @DisplayName("a data frame addressed to a bounded instance is a logged no-op (run not re-invoked)")
    void dataFrameToBoundedOperativeIsNoOp() {
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);
        ctx.operativeInstanceRegistry().add(new BoundedOperativeInstance("t1", "echo_bounded"));

        PrimaryStreamReader reader = new PrimaryStreamReader(ctx, PRIMARY_STREAM);
        byte[] frame = FrameEncoder.build("t1", "ignored".getBytes(StandardCharsets.UTF_8), Map.of());
        reader.onData(frame); // must not throw

        assertThat(EMIT_RUNS.get()).isZero();
    }

    // --- 5.4 Emit + completion frames ---

    @Test
    @DisplayName("success: emits an instance-id-keyed empty-metadata output frame and BoundedCompleted ok")
    void successEmitsOutputAndOkCompletion() throws Exception {
        BoundedOperativeRegistry.registerBounded(EmitBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler().handle(ctx, addBoundedOperative("test-emit-bounded", "bounded-ok", "hello"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        Frame output = cap.decodeOne(SECONDARY_STREAM);
        assertThat(output.payloadId()).isEqualTo("bounded-ok");
        assertThat(output.metadata())
                .as("bounded output carries no channel metadata")
                .isEmpty();
        assertThat(new String(output.message(), StandardCharsets.UTF_8)).isEqualTo("hello");

        BoundedCompleted completed = cap.decodeCompletion();
        assertThat(completed.getOperativeId()).isEqualTo("bounded-ok");
        assertThat(completed.getStatus()).isEqualTo("ok");
        assertThat(completed.getError()).isEmpty();
    }

    @Test
    @DisplayName("failure: a thrown BoundedOperativeException reports BoundedCompleted error with the message, no output")
    void failureEmitsErrorCompletion() throws Exception {
        BoundedOperativeRegistry.registerBounded(FailBoundedOperative.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddOperativeHandler().handle(ctx, addBoundedOperative("test-fail-bounded", "bounded-err", ""), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.framesOn(SECONDARY_STREAM))
                .as("a failing bounded emits no output")
                .isEmpty();
        BoundedCompleted completed = cap.decodeCompletion();
        assertThat(completed.getStatus()).isEqualTo("error");
        assertThat(completed.getError()).isEqualTo("boom");
    }

    // --- helpers ---

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

        BoundedCompleted decodeCompletion() throws Exception {
            Frame f = decodeOne(CONTROL_STREAM);
            assertThat(f.payloadId()).isEqualTo("control_command");
            return CommandRequest.parseFrom(f.message()).getBoundedCompleted();
        }
    }

    // --- test operatives ---

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
            c.input(String.class, "in").into(NoopOperation.class);
        }
    }

    public static final class NoopOperation
            implements Operation<
                    String,
                    NoopOperation.Empty,
                    String,
                    com.flezha.sdk.state.NoChange,
                    com.flezha.sdk.state.WallClock,
                    Void> {
        public record Empty() {}

        @Override
        public String name() {
            return "noop_operation";
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
        public Class<com.flezha.sdk.state.NoChange> changeType() {
            return com.flezha.sdk.state.NoChange.class;
        }

        @Override
        public Class<com.flezha.sdk.state.WallClock> clockType() {
            return com.flezha.sdk.state.WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public Empty initState(
                java.util.Optional<String> operationKey,
                java.util.Optional<com.flezha.sdk.state.RestoreInitPayload> initPayload) {
            return new Empty();
        }

        @Override
        public void run(
                String input,
                com.flezha.sdk.state.StateHandle<Empty, com.flezha.sdk.state.NoChange> state,
                com.flezha.sdk.operation.OperationCtx ctx) {}
    }
}
