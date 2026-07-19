package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.operative.StatelessTestOperation;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Java port of {@code sdk/rust/tests/hybrid_channel.rs}. Exercises the
 * hybrid-channel surface added by {@code operative-channel-fan-out-to-plugin-java}:
 * one channel fans out typed values to consumers AND publishes serialized
 * bytes to a plugin sink.
 */
class HybridChannelTest {

    // Shared captures across test cases. Cleared in @BeforeEach so test order
    // does not bleed observations. Static because the UnboundedOperative/Operation pipeline
    // re-instantiates operations reflectively (no-arg ctor), so test cases cannot
    // close over instance-level mutable state.
    private static final List<Integer> SINGLE_OBSERVED = new CopyOnWriteArrayList<>();
    private static final List<Integer> THREE_A = new CopyOnWriteArrayList<>();
    private static final List<Integer> THREE_B = new CopyOnWriteArrayList<>();
    private static final List<Integer> THREE_C = new CopyOnWriteArrayList<>();
    private static final AtomicInteger SERIALIZER_CALLS = new AtomicInteger(0);
    private static final List<Integer> PANIC_OBSERVED = new CopyOnWriteArrayList<>();
    private static final List<String> PANIC_EMIT_OUTCOMES = new CopyOnWriteArrayList<>();
    private static final List<Integer> META_OBSERVED = new CopyOnWriteArrayList<>();
    private static final List<Integer> PURE_OBSERVED = new CopyOnWriteArrayList<>();
    private static final List<String> PURE_EMIT_OUTCOMES = new CopyOnWriteArrayList<>();

    private final List<byte[]> capturedWrites = new CopyOnWriteArrayList<>();
    private RunnerContext ctx;

    @BeforeEach
    void reset() {
        SINGLE_OBSERVED.clear();
        THREE_A.clear();
        THREE_B.clear();
        THREE_C.clear();
        SERIALIZER_CALLS.set(0);
        PANIC_OBSERVED.clear();
        PANIC_EMIT_OUTCOMES.clear();
        META_OBSERVED.clear();
        PURE_OBSERVED.clear();
        PURE_EMIT_OUTCOMES.clear();
        capturedWrites.clear();
        UnboundedOperativeClassRegistry.reset();

        OperationDispatcher.setWireWriter((handle, streamId, frame) -> {
            capturedWrites.add(frame);
            return 0;
        });

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "2"));
        ctx = new RunnerContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(2));
        ctx.streamRegistry().register(0, 100L, 200L);
    }

    @AfterEach
    void close() {
        OperationDispatcher.resetWireWriter();
        if (ctx != null && ctx.workerPool() != null) {
            ctx.workerPool().shutdownNow();
        }
    }

    private UnboundedOperativeInstance instantiate(
            Class<? extends UnboundedOperative> operativeClass, String factoryId) {
        UnboundedOperativeClassRegistry.registerClass(operativeClass);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology(factoryId);
        return UnboundedOperativeInstance.create(topology, factoryId + "-instance", ctx, ctx.workerPool());
    }

    private void feed(UnboundedOperativeInstance instance, String producerOperationName, Object input) {
        int operationId = instance.topology().operationNameToId().get(producerOperationName);
        instance.dispatcher(operationId).fanoutSegments(input, 100L);
    }

    private static boolean waitFor(java.util.function.BooleanSupplier predicate, long millis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            if (predicate.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return predicate.getAsBoolean();
    }

    private List<Frame> parsedFrames() {
        FrameParser parser = new FrameParser();
        List<Frame> all = new ArrayList<>();
        for (byte[] write : capturedWrites) {
            all.addAll(parser.feed(write));
        }
        return all;
    }

    private static byte[] beU32(int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(v).array();
    }

    // ---------- 1 consumer + plugin sink ----------

    @Test
    @DisplayName("hybrid single consumer receives typed value and plugin bytes")
    void hybridSingleConsumerReceivesTypedAndPluginBytes() {
        UnboundedOperativeInstance instance = instantiate(SingleHybridUnboundedOperative.class, "hybrid_single");
        feed(instance, "hybrid_single_producer", "42");

        assertThat(waitFor(() -> SINGLE_OBSERVED.size() == 1 && capturedWrites.size() == 1, 5_000))
                .as("consumer or plugin frame did not arrive")
                .isTrue();

        assertThat(SINGLE_OBSERVED).containsExactly(42);
        List<Frame> frames = parsedFrames();
        assertThat(frames).hasSize(1);
        assertThat(frames.get(0).payloadId().trim()).isEqualTo("hybrid_single-instance");
        assertThat(frames.get(0).message()).isEqualTo(beU32(42));
        assertThat(frames.get(0).metadata()).containsEntry("channel", "hybrid_out");
    }

    // ---------- 3 consumers + plugin sink: serializer called once ----------

    @Test
    @DisplayName("hybrid three consumers fan out + serializer runs exactly once")
    void hybridThreeConsumersSingleSerializerCall() {
        UnboundedOperativeInstance instance = instantiate(ThreeHybridUnboundedOperative.class, "hybrid_three");
        feed(instance, "hybrid_three_producer", "7");

        assertThat(waitFor(
                        () -> THREE_A.size() == 1
                                && THREE_B.size() == 1
                                && THREE_C.size() == 1
                                && capturedWrites.size() == 1,
                        5_000))
                .as("delivery to all 3 consumers + plugin did not complete")
                .isTrue();

        assertThat(THREE_A).containsExactly(7);
        assertThat(THREE_B).containsExactly(7);
        assertThat(THREE_C).containsExactly(7);
        assertThat(SERIALIZER_CALLS.get())
                .as("serializer must run exactly once regardless of consumer count")
                .isEqualTo(1);
    }

    // ---------- throwing serializer caught; dispatcher survives ----------

    @Test
    @DisplayName("hybrid serializer that throws surfaces as PluginSerializerFailure")
    void hybridSerializerThrowIsCaught() {
        UnboundedOperativeInstance instance = instantiate(PanicHybridUnboundedOperative.class, "hybrid_panic");

        // First input: serializer throws on 13.
        feed(instance, "hybrid_panic_producer", "13");
        // Second input: dispatcher should still be alive.
        feed(instance, "hybrid_panic_producer", "5");

        assertThat(waitFor(() -> PANIC_OBSERVED.size() == 2 && PANIC_EMIT_OUTCOMES.size() == 2, 5_000))
                .as("consumer did not see both inputs")
                .isTrue();

        // Consumer fan-out completes for both — panic happens after.
        assertThat(PANIC_OBSERVED).containsExactly(13, 5);

        String first = PANIC_EMIT_OUTCOMES.get(0);
        assertThat(first).contains("plugin serializer for channel `panic_out`");
        assertThat(first).contains("test serializer rejected 13");
        assertThat(PANIC_EMIT_OUTCOMES.get(1)).isEqualTo("ok");

        // Only the successful emit produced a wire frame.
        assertThat(waitFor(() -> capturedWrites.size() == 1, 2_000)).isTrue();
        List<Frame> frames = parsedFrames();
        assertThat(frames).hasSize(1);
        assertThat(frames.get(0).message()).isEqualTo(beU32(5));
    }

    // ---------- duplicate plugin sink rejected at registration ----------

    @Test
    @DisplayName("duplicate alsoOutput on the same channel is rejected at registerUnbounded time")
    void hybridDuplicatePluginSinkIsRejected() {
        UnboundedOperativeConfigurationException ex = assertThrows(
                UnboundedOperativeConfigurationException.class,
                () -> UnboundedOperativeClassRegistry.registerClass(DuplicateSinkUnboundedOperative.class));
        assertThat(ex.getMessage()).contains("alsoOutput");
        assertThat(ex.getMessage()).contains("dup_out");
    }

    // ---------- emit_with_meta on hybrid channel: meta reaches plugin only ----------

    @Test
    @DisplayName("emitWithMeta on hybrid channel forwards typed value to consumer and meta to plugin")
    void emitWithMetaOnHybridChannelForwardsToPluginOnly() {
        UnboundedOperativeInstance instance = instantiate(MetaHybridUnboundedOperative.class, "hybrid_meta");
        feed(instance, "hybrid_meta_producer", "9");

        assertThat(waitFor(() -> META_OBSERVED.size() == 1 && capturedWrites.size() == 1, 5_000))
                .as("consumer or plugin frame did not arrive")
                .isTrue();

        assertThat(META_OBSERVED).containsExactly(9);
        List<Frame> frames = parsedFrames();
        assertThat(frames).hasSize(1);
        assertThat(frames.get(0).message()).isEqualTo(beU32(9));
        assertThat(frames.get(0).metadata()).containsEntry("trace_id", "abc123").containsEntry("channel", "meta_out");
    }

    // ---------- emit_with_meta on pure inter-operation channel is still rejected ----------

    @Test
    @DisplayName(
            "emitWithMeta on a pure inter-operation channel is rejected with MetadataNotAllowedOnInterOperationChannel")
    void emitWithMetaOnPureInterOperationChannelStillRejected() {
        UnboundedOperativeInstance instance = instantiate(PureInterOperationUnboundedOperative.class, "hybrid_pure");
        feed(instance, "hybrid_pure_producer", "1");

        assertThat(waitFor(() -> PURE_EMIT_OUTCOMES.size() == 1, 5_000))
                .as("producer run did not complete")
                .isTrue();
        String outcome = PURE_EMIT_OUTCOMES.get(0);
        assertThat(outcome).contains("MetadataNotAllowedOnInterOperationChannel");
        assertThat(outcome).contains("pure_out");

        // Consumer fan-out happens via emit, not the rejecting emitWithMeta;
        // pure inter-operation channels still throw before fan-out, matching Rust.
        assertThat(PURE_OBSERVED).isEmpty();
        assertThat(capturedWrites).isEmpty();
    }

    // =============================================================================================
    // Test UnboundedOperative topologies + Operation impls. Each UnboundedOperative is registered exactly once per
    // test; producer
    // Operations close over the static CopyOnWriteArrayList captures above.
    // =============================================================================================

    // ---- 1 consumer ----

    public static final class SingleHybridUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "hybrid_single";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sSingleProducer = c.input(String.class, "input").into(SingleProducer.class);
            sSingleProducer
                    .named("hybrid_out")
                    .alsoOutput("plugin_a", HybridChannelTest::beU32)
                    .into(SingleConsumer.class);
        }
    }

    public static final class SingleProducer extends StatelessTestOperation<String, Object, Integer> {
        @Override
        public String name() {
            return "hybrid_single_producer";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
        }

        @Override
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) throws OperationException {
            int n = Integer.parseInt(input);
            ctx.emit(n);
        }
    }

    public static final class SingleConsumer extends StatelessTestOperation<Integer, Object, Void> {
        @Override
        public String name() {
            return "hybrid_single_consumer";
        }

        @Override
        public Class<Integer> inputType() {
            return Integer.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(Integer input, StateHandle<Object, NoChange> state, OperationCtx ctx) {
            SINGLE_OBSERVED.add(input);
        }
    }

    // ---- 3 consumers ----

    public static final class ThreeHybridUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "hybrid_three";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sThreeProducer = c.input(String.class, "input").into(ThreeProducer.class);
            var chthree_out = sThreeProducer.named("three_out").alsoOutput("plugin_three", v -> {
                SERIALIZER_CALLS.incrementAndGet();
                return beU32(v);
            });
            chthree_out.into(ThreeConsumerA.class);
            chthree_out.into(ThreeConsumerB.class);
            chthree_out.into(ThreeConsumerC.class);
        }
    }

    public static final class ThreeProducer extends StatelessTestOperation<String, Object, Integer> {
        @Override
        public String name() {
            return "hybrid_three_producer";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
        }

        @Override
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit(Integer.parseInt(input));
        }
    }

    private abstract static class CapturingConsumer extends StatelessTestOperation<Integer, Object, Void> {
        @Override
        public Class<Integer> inputType() {
            return Integer.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }
    }

    public static final class ThreeConsumerA extends CapturingConsumer {
        @Override
        public String name() {
            return "hybrid_three_a";
        }

        @Override
        public void run(Integer input, StateHandle<Object, NoChange> state, OperationCtx ctx) {
            THREE_A.add(input);
        }
    }

    public static final class ThreeConsumerB extends CapturingConsumer {
        @Override
        public String name() {
            return "hybrid_three_b";
        }

        @Override
        public void run(Integer input, StateHandle<Object, NoChange> state, OperationCtx ctx) {
            THREE_B.add(input);
        }
    }

    public static final class ThreeConsumerC extends CapturingConsumer {
        @Override
        public String name() {
            return "hybrid_three_c";
        }

        @Override
        public void run(Integer input, StateHandle<Object, NoChange> state, OperationCtx ctx) {
            THREE_C.add(input);
        }
    }

    // ---- throwing serializer ----

    public static final class PanicHybridUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "hybrid_panic";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sPanicProducer = c.input(String.class, "input").into(PanicProducer.class);
            sPanicProducer
                    .named("panic_out")
                    .alsoOutput("plugin_panic", v -> {
                        if (v == 13) {
                            throw new RuntimeException("test serializer rejected 13");
                        }
                        return beU32(v);
                    })
                    .into(PanicConsumer.class);
        }
    }

    public static final class PanicProducer extends StatelessTestOperation<String, Object, Integer> {
        @Override
        public String name() {
            return "hybrid_panic_producer";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
        }

        @Override
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) {
            int n = Integer.parseInt(input);
            try {
                ctx.emit(n);
                PANIC_EMIT_OUTCOMES.add("ok");
            } catch (OperationException e) {
                PANIC_EMIT_OUTCOMES.add(e.getMessage());
            }
        }
    }

    public static final class PanicConsumer extends StatelessTestOperation<Integer, Object, Void> {
        @Override
        public String name() {
            return "hybrid_panic_consumer";
        }

        @Override
        public Class<Integer> inputType() {
            return Integer.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(Integer input, StateHandle<Object, NoChange> state, OperationCtx ctx) {
            PANIC_OBSERVED.add(input);
        }
    }

    // ---- duplicate plugin sink ----

    public static final class DuplicateSinkUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "hybrid_dup";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sDupProducer = c.input(String.class, "input").into(DupProducer.class);
            sDupProducer
                    .named("dup_out")
                    .alsoOutput("plugin_a", HybridChannelTest::beU32)
                    .alsoOutput("plugin_b", HybridChannelTest::beU32);
        }
    }

    public static final class DupProducer extends StatelessTestOperation<String, Object, Integer> {
        @Override
        public String name() {
            return "hybrid_dup_producer";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
        }

        @Override
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) {}
    }

    // ---- emitWithMeta on hybrid ----

    public static final class MetaHybridUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "hybrid_meta";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sMetaProducer = c.input(String.class, "input").into(MetaProducer.class);
            sMetaProducer
                    .named("meta_out")
                    .alsoOutput("plugin_meta", HybridChannelTest::beU32)
                    .into(MetaConsumer.class);
        }
    }

    public static final class MetaProducer extends StatelessTestOperation<String, Object, Integer> {
        @Override
        public String name() {
            return "hybrid_meta_producer";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
        }

        @Override
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) throws OperationException {
            int n = Integer.parseInt(input);
            Map<String, String> meta = new HashMap<>();
            meta.put("trace_id", "abc123");
            meta.put("channel", "spoof");
            ctx.emitWithMeta(n, meta);
        }
    }

    public static final class MetaConsumer extends StatelessTestOperation<Integer, Object, Void> {
        @Override
        public String name() {
            return "hybrid_meta_consumer";
        }

        @Override
        public Class<Integer> inputType() {
            return Integer.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(Integer input, StateHandle<Object, NoChange> state, OperationCtx ctx) {
            META_OBSERVED.add(input);
        }
    }

    // ---- emitWithMeta on pure inter-operation channel ----

    public static final class PureInterOperationUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "hybrid_pure";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sPureProducer = c.input(String.class, "input").into(PureProducer.class);
            sPureProducer.named("pure_out").into(PureConsumer.class);
        }
    }

    public static final class PureProducer extends StatelessTestOperation<String, Object, Integer> {
        @Override
        public String name() {
            return "hybrid_pure_producer";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
        }

        @Override
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) {
            Map<String, String> meta = new HashMap<>();
            meta.put("trace_id", "abc123");
            try {
                ctx.emitWithMeta(Integer.parseInt(input), meta);
                PURE_EMIT_OUTCOMES.add("ok");
            } catch (OperationException e) {
                PURE_EMIT_OUTCOMES.add(e.getClass().getSimpleName() + ":" + e.getMessage());
            }
        }
    }

    public static final class PureConsumer extends StatelessTestOperation<Integer, Object, Void> {
        @Override
        public String name() {
            return "hybrid_pure_consumer";
        }

        @Override
        public Class<Integer> inputType() {
            return Integer.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(Integer input, StateHandle<Object, NoChange> state, OperationCtx ctx) {
            PURE_OBSERVED.add(input);
        }
    }

    // Suppress unused-import lint for transitively used types.
    @SuppressWarnings("unused")
    private static void _unused(WallClock c, NoChange n) {}
}
