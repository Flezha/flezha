package com.flezha.sdk.runtime.operative;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
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
 * Per-Procedure / per-Entity state-blob shape (per
 * {@code operative-mvp-rust/design.md} Decision 10):
 *
 * <pre>
 *   single-mode procedure:  {"procedureName": &lt;state&gt;}
 *   static-mode procedure:  {"procedureName": {"entityKey": &lt;state&gt;, ...}}
 * </pre>
 */
class UnboundedOperativeStateSerializerTest {

    @BeforeEach
    void reset() {
        UnboundedOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("serialize emits the documented per-Procedure / per-Entity shape")
    void serializeShape() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(MixedUnboundedOperative.class);
        UnboundedOperativeInstance instance = newInstance("mixed");

        // Manually mutate the entity states so the test asserts the
        // serializer reads from the live AtomicReference.
        instance.entities().get(procedureId(instance, "a")).get(0).state().set(new ProcedureAState(1));
        instance.entities().get(procedureId(instance, "b")).get(0).state().set(new ProcedureBState(1));
        instance.entities().get(procedureId(instance, "b")).get(1).state().set(new ProcedureBState(2));

        byte[] blob = UnboundedOperativeStateSerializer.serialize(instance);
        @SuppressWarnings("unchecked")
        Map<String, Object> root = new ObjectMapper().readValue(blob, Map.class);

        assertThat(root).containsKey("a").containsKey("b");
        @SuppressWarnings("unchecked")
        Map<String, Object> procedureA = (Map<String, Object>) root.get("a");
        assertThat(procedureA).containsEntry("n", 1);

        @SuppressWarnings("unchecked")
        Map<String, Object> procedureB = (Map<String, Object>) root.get("b");
        assertThat(procedureB).containsKey("1m").containsKey("1h");
        @SuppressWarnings("unchecked")
        Map<String, Object> b1m = (Map<String, Object>) procedureB.get("1m");
        @SuppressWarnings("unchecked")
        Map<String, Object> b1h = (Map<String, Object>) procedureB.get("1h");
        assertThat(b1m).containsEntry("x", 1);
        assertThat(b1h).containsEntry("x", 2);
    }

    @Test
    @DisplayName("deserialize round-trips identical entity states")
    void deserializeRoundTrips() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(MixedUnboundedOperative.class);
        UnboundedOperativeInstance instance = newInstance("mixed");
        instance.entities().get(procedureId(instance, "a")).get(0).state().set(new ProcedureAState(7));
        instance.entities().get(procedureId(instance, "b")).get(0).state().set(new ProcedureBState(11));
        instance.entities().get(procedureId(instance, "b")).get(1).state().set(new ProcedureBState(13));

        byte[] blob = UnboundedOperativeStateSerializer.serialize(instance);

        UnboundedOperativeInstance second = newInstance("mixed");
        UnboundedOperativeStateSerializer.deserialize(second, blob);

        assertThat(second.entities()
                        .get(procedureId(second, "a"))
                        .get(0)
                        .state()
                        .get())
                .isEqualTo(new ProcedureAState(7));
        assertThat(second.entities()
                        .get(procedureId(second, "b"))
                        .get(0)
                        .state()
                        .get())
                .isEqualTo(new ProcedureBState(11));
        assertThat(second.entities()
                        .get(procedureId(second, "b"))
                        .get(1)
                        .state()
                        .get())
                .isEqualTo(new ProcedureBState(13));
    }

    private static UnboundedOperativeInstance newInstance(String factoryId) {
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        RunnerContext ctx = new RunnerContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology(factoryId);
        return UnboundedOperativeInstance.create(topology, factoryId + "-instance", ctx, ctx.workerPool());
    }

    private static int procedureId(UnboundedOperativeInstance instance, String procedureName) {
        return instance.topology().procedureNameToId().get(procedureName);
    }

    public record ProcedureAState(int n) {}

    public record ProcedureBState(int x) {}

    public static final class ProcedureA extends StatelessTestProcedure<String, ProcedureAState, String> {
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
        protected ProcedureAState initState(Optional<String> entityKey) {
            return new ProcedureAState(0);
        }

        @Override
        public void run(
                String input,
                com.flezha.sdk.state.StateHandle<ProcedureAState, com.flezha.sdk.state.NoChange> state,
                ProcedureCtx ctx)
                throws ProcedureException {}
    }

    public static final class ProcedureB extends StatelessTestProcedure<String, ProcedureBState, String> {
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
        protected ProcedureBState initState(Optional<String> entityKey) {
            return new ProcedureBState(0);
        }

        @Override
        public List<String> entityKeys() {
            return List.of("1m", "1h");
        }

        @Override
        public boolean entitiesForOverridden() {
            return true;
        }

        @Override
        public List<String> entitiesFor(String input) {
            return List.of("1m");
        }

        @Override
        public void run(
                String input,
                com.flezha.sdk.state.StateHandle<ProcedureBState, com.flezha.sdk.state.NoChange> state,
                ProcedureCtx ctx)
                throws ProcedureException {}
    }

    public static final class MixedUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "mixed";
        }

        @Override
        public void channels(ChannelBuilder c) {
            // One entry (the graph-shape rule); A feeds B so both procs carry state.
            c.input(String.class, "input.a").into(ProcedureA.class).into(ProcedureB.class);
        }
    }
}
