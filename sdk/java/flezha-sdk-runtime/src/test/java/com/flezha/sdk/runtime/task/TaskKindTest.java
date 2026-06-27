package com.flezha.sdk.runtime.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.proto.AddRelay;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.TaskCompleted;
import com.flezha.sdk.relay.Relay;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.handlers.AddRelayHandler;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.relay.RelayInstanceRegistry;
import com.flezha.sdk.runtime.relay.TaskInstance;
import com.flezha.sdk.runtime.relay.Unit;
import com.flezha.sdk.runtime.stream.PrimaryStreamReader;
import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.task.Task;
import com.flezha.sdk.task.TaskCtx;
import com.flezha.sdk.task.TaskException;
import com.flezha.sdk.task.TaskRegistry;
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

class TaskKindTest {

    private static final long CONTROL_STREAM = 1L;
    private static final long PRIMARY_STREAM = 2L;
    private static final long SECONDARY_STREAM = 3L;

    private static final AtomicInteger EMIT_RUNS = new AtomicInteger(0);

    @BeforeEach
    void reset() {
        RelayRegistry.reset();
        TaskRegistry.reset();
        EMIT_RUNS.set(0);
    }

    @AfterEach
    void restoreWriter() {
        TaskRunner.resetWireWriter();
    }

    // --- 5.1 Registration ---

    @Test
    @DisplayName("registerTask makes the factory resolvable")
    void registerTaskResolvable() {
        assertThat(TaskClassRegistry.getTaskClass("test-emit-task")).isNull();
        TaskRegistry.registerTask(EmitTask.class);
        assertThat(TaskClassRegistry.getTaskClass("test-emit-task")).isNotNull();
        assertThat(TaskRegistry.factoryIds()).contains("test-emit-task");
    }

    @Test
    @DisplayName("registerTask after freeze throws IllegalStateException")
    void registerAfterFreezeRejected() {
        TaskRegistry.freeze();
        assertThatThrownBy(() -> TaskRegistry.registerTask(EmitTask.class)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("a task factory colliding with a relay (either order) is rejected")
    void crossKindDuplicateRejected() {
        RelayRegistry.registerRelay(CollideRelay.class);
        assertThatThrownBy(() -> TaskRegistry.registerTask(CollideTask.class))
                .isInstanceOf(IllegalArgumentException.class);

        RelayRegistry.reset();
        TaskRegistry.reset();

        TaskRegistry.registerTask(CollideTask.class);
        assertThatThrownBy(() -> RelayRegistry.registerRelay(CollideRelay.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --- 5.2 Sealed Unit registry ---

    @Test
    @DisplayName("the instance registry holds both variants; getRelay is empty for a task; RemRelay removes a task")
    void sealedUnitHoldsBothVariants() {
        RelayInstanceRegistry registry = new RelayInstanceRegistry();
        registry.add(new TaskInstance("t1", "echo_task"));

        assertThat(registry.contains("t1")).isTrue();
        assertThat(registry.get("t1")).isInstanceOf(TaskInstance.class);
        assertThat(registry.getRelay("t1")).as("a task is not a relay").isNull();
        assertThat(registry.relaySnapshot()).doesNotContainKey("t1");

        Unit removed = registry.remove("t1");
        assertThat(removed).isInstanceOf(TaskInstance.class);
        assertThat(registry.contains("t1")).isFalse();
    }

    // --- 5.3 Dispatch + run-once ---

    @Test
    @DisplayName("AddRelay kind=task spawns a task, runs once with init_values, and is idempotent")
    void addRelayTaskRunsOnce() throws Exception {
        TaskRegistry.registerTask(EmitTask.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddRelayHandler().handle(ctx, addTask("test-emit-task", "task-ok", "hello"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(EMIT_RUNS.get()).as("run must execute exactly once").isEqualTo(1);
        assertThat(ctx.relayInstanceRegistry().get("task-ok")).isInstanceOf(TaskInstance.class);

        // Duplicate AddRelay for a live id is idempotent — no re-run.
        new AddRelayHandler().handle(ctx, addTask("test-emit-task", "task-ok", "hello"), new ArrayList<>());
        Thread.sleep(100);
        assertThat(EMIT_RUNS.get()).as("duplicate start must not re-run").isEqualTo(1);
    }

    @Test
    @DisplayName("AddRelay kind=task with an unregistered factory reports an error and creates nothing")
    void addRelayTaskUnknownFactory() {
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        List<String> errors = new ArrayList<>();
        new AddRelayHandler().handle(ctx, addTask("test-missing-task", "task-x", ""), errors);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("Task class");
        assertThat(ctx.relayInstanceRegistry().contains("task-x")).isFalse();
    }

    @Test
    @DisplayName("a data frame addressed to a task instance is a logged no-op (run not re-invoked)")
    void dataFrameToTaskIsNoOp() {
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);
        ctx.relayInstanceRegistry().add(new TaskInstance("t1", "echo_task"));

        PrimaryStreamReader reader = new PrimaryStreamReader(ctx, PRIMARY_STREAM);
        byte[] frame = FrameEncoder.build("t1", "ignored".getBytes(StandardCharsets.UTF_8), Map.of());
        reader.onData(frame); // must not throw

        assertThat(EMIT_RUNS.get()).isZero();
    }

    // --- 5.4 Emit + completion frames ---

    @Test
    @DisplayName("success: emits an instance-id-keyed empty-metadata output frame and TaskCompleted ok")
    void successEmitsOutputAndOkCompletion() throws Exception {
        TaskRegistry.registerTask(EmitTask.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddRelayHandler().handle(ctx, addTask("test-emit-task", "task-ok", "hello"), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        Frame output = cap.decodeOne(SECONDARY_STREAM);
        assertThat(output.payloadId()).isEqualTo("task-ok");
        assertThat(output.metadata())
                .as("task output carries no channel metadata")
                .isEmpty();
        assertThat(new String(output.message(), StandardCharsets.UTF_8)).isEqualTo("hello");

        TaskCompleted completed = cap.decodeCompletion();
        assertThat(completed.getRelayId()).isEqualTo("task-ok");
        assertThat(completed.getStatus()).isEqualTo("ok");
        assertThat(completed.getError()).isEmpty();
    }

    @Test
    @DisplayName("failure: a thrown TaskException reports TaskCompleted error with the message, no output")
    void failureEmitsErrorCompletion() throws Exception {
        TaskRegistry.registerTask(FailTask.class);
        Captured cap = new Captured();
        ExecutorContext ctx = newCtx(cap);

        cap.expectCompletions(1);
        new AddRelayHandler().handle(ctx, addTask("test-fail-task", "task-err", ""), new ArrayList<>());
        assertThat(cap.awaitCompletions()).isTrue();

        assertThat(cap.framesOn(SECONDARY_STREAM))
                .as("a failing task emits no output")
                .isEmpty();
        TaskCompleted completed = cap.decodeCompletion();
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
        TaskRunner.setWireWriter(cap);
        return ctx;
    }

    private static CommandRequest addTask(String factoryId, String relayId, String args) {
        return CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setAddRelay(AddRelay.newBuilder()
                        .setRelayId(relayId)
                        .setFactoryId(factoryId)
                        .setInitValues(args)
                        .setKind("task")
                        .build())
                .build();
    }

    /** Capturing wire writer + decode helpers. */
    private static final class Captured implements TaskRunner.WireWriter {
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

        TaskCompleted decodeCompletion() throws Exception {
            Frame f = decodeOne(CONTROL_STREAM);
            assertThat(f.payloadId()).isEqualTo("control_command");
            return CommandRequest.parseFrom(f.message()).getTaskCompleted();
        }
    }

    // --- test units ---

    public static final class EmitTask implements Task {
        @Override
        public String factoryId() {
            return "test-emit-task";
        }

        @Override
        public void run(TaskCtx ctx, String args) throws TaskException {
            EMIT_RUNS.incrementAndGet();
            ctx.emit(args);
        }
    }

    public static final class FailTask implements Task {
        @Override
        public String factoryId() {
            return "test-fail-task";
        }

        @Override
        public void run(TaskCtx ctx, String args) throws TaskException {
            throw new TaskException("boom");
        }
    }

    public static final class CollideTask implements Task {
        @Override
        public String factoryId() {
            return "collide";
        }

        @Override
        public void run(TaskCtx ctx, String args) {}
    }

    public static final class CollideRelay implements Relay {
        @Override
        public String factoryId() {
            return "collide";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(NoopShard.class);
        }
    }

    public static final class NoopShard
            implements Shard<
                    String,
                    NoopShard.Empty,
                    String,
                    com.flezha.sdk.state.NoChange,
                    com.flezha.sdk.state.WallClock,
                    Void> {
        public record Empty() {}

        @Override
        public String name() {
            return "noop_shard";
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
                java.util.Optional<String> shardKey,
                java.util.Optional<com.flezha.sdk.state.RestoreInitPayload> initPayload) {
            return new Empty();
        }

        @Override
        public void run(
                String input,
                com.flezha.sdk.state.StateHandle<Empty, com.flezha.sdk.state.NoChange> state,
                com.flezha.sdk.shard.ShardCtx ctx) {}
    }
}
