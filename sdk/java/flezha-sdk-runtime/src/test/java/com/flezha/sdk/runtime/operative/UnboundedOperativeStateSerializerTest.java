package com.flezha.sdk.runtime.operative;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Per-Operation / per-Segment state-blob shape (per
 * {@code operative-mvp-rust/design.md} Decision 10):
 *
 * <pre>
 *   single-mode operation:  {"operationName": &lt;state&gt;}
 *   static-mode operation:  {"operationName": {"operationKey": &lt;state&gt;, ...}}
 * </pre>
 */
class UnboundedOperativeStateSerializerTest {

    @BeforeEach
    void reset() {
        UnboundedOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("serialize emits the documented per-Operation / per-Segment shape")
    void serializeShape() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(MixedUnboundedOperative.class);
        UnboundedOperativeInstance instance = newInstance("mixed");

        // Manually mutate the segment states so the test asserts the
        // serializer reads from the live AtomicReference.
        instance.segments().get(operationId(instance, "a")).get(0).state().set(new OperationAState(1));
        instance.segments().get(operationId(instance, "b")).get(0).state().set(new OperationBState(1));
        instance.segments().get(operationId(instance, "b")).get(1).state().set(new OperationBState(2));

        byte[] blob = UnboundedOperativeStateSerializer.serialize(instance);
        @SuppressWarnings("unchecked")
        Map<String, Object> root = new ObjectMapper().readValue(blob, Map.class);

        assertThat(root).containsKey("a").containsKey("b");
        @SuppressWarnings("unchecked")
        Map<String, Object> operationA = (Map<String, Object>) root.get("a");
        assertThat(operationA).containsEntry("n", 1);

        @SuppressWarnings("unchecked")
        Map<String, Object> operationB = (Map<String, Object>) root.get("b");
        assertThat(operationB).containsKey("1m").containsKey("1h");
        @SuppressWarnings("unchecked")
        Map<String, Object> b1m = (Map<String, Object>) operationB.get("1m");
        @SuppressWarnings("unchecked")
        Map<String, Object> b1h = (Map<String, Object>) operationB.get("1h");
        assertThat(b1m).containsEntry("x", 1);
        assertThat(b1h).containsEntry("x", 2);
    }

    @Test
    @DisplayName("deserialize round-trips identical segment states")
    void deserializeRoundTrips() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(MixedUnboundedOperative.class);
        UnboundedOperativeInstance instance = newInstance("mixed");
        instance.segments().get(operationId(instance, "a")).get(0).state().set(new OperationAState(7));
        instance.segments().get(operationId(instance, "b")).get(0).state().set(new OperationBState(11));
        instance.segments().get(operationId(instance, "b")).get(1).state().set(new OperationBState(13));

        byte[] blob = UnboundedOperativeStateSerializer.serialize(instance);

        UnboundedOperativeInstance second = newInstance("mixed");
        UnboundedOperativeStateSerializer.deserialize(second, blob);

        assertThat(second.segments()
                        .get(operationId(second, "a"))
                        .get(0)
                        .state()
                        .get())
                .isEqualTo(new OperationAState(7));
        assertThat(second.segments()
                        .get(operationId(second, "b"))
                        .get(0)
                        .state()
                        .get())
                .isEqualTo(new OperationBState(11));
        assertThat(second.segments()
                        .get(operationId(second, "b"))
                        .get(1)
                        .state()
                        .get())
                .isEqualTo(new OperationBState(13));
    }

    private static UnboundedOperativeInstance newInstance(String factoryId) {
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        RunnerContext ctx = new RunnerContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology(factoryId);
        return UnboundedOperativeInstance.create(topology, factoryId + "-instance", ctx, ctx.workerPool());
    }

    private static int operationId(UnboundedOperativeInstance instance, String operationName) {
        return instance.topology().operationNameToId().get(operationName);
    }

    public record OperationAState(int n) {}

    public record OperationBState(int x) {}

    public static final class OperationA extends StatelessTestOperation<String, OperationAState, String> {
        @Override
        public String name() {
            return "a";
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
        protected OperationAState initState(Optional<String> operationKey) {
            return new OperationAState(0);
        }

        @Override
        public void run(
                String input,
                com.flezha.sdk.state.StateHandle<OperationAState, com.flezha.sdk.state.NoChange> state,
                OperationCtx ctx)
                throws OperationException {}
    }

    public static final class OperationB extends StatelessTestOperation<String, OperationBState, String> {
        @Override
        public String name() {
            return "b";
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
        protected OperationBState initState(Optional<String> operationKey) {
            return new OperationBState(0);
        }

        @Override
        public List<String> operationKeys() {
            return List.of("1m", "1h");
        }

        @Override
        public boolean segmentForOverridden() {
            return true;
        }

        @Override
        public List<String> segmentFor(String input) {
            return List.of("1m");
        }

        @Override
        public void run(
                String input,
                com.flezha.sdk.state.StateHandle<OperationBState, com.flezha.sdk.state.NoChange> state,
                OperationCtx ctx)
                throws OperationException {}
    }

    public static final class MixedUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "mixed";
        }

        @Override
        public void channels(ChannelBuilder c) {
            // One entry (the graph-shape rule); A feeds B so both ops carry state.
            c.input(String.class, "input.a").into(OperationA.class).into(OperationB.class);
        }
    }
}
