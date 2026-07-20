package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.operative.StatelessTestOperation;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Each Segment runs {@code run} on its OWN Operation instance — as the Rust SDK
 * does, holding one Operation per Segment alongside that Segment's State — so a
 * mutable field on a user's Operation is per-Segment scratch, never a shared,
 * unsynchronised cross-key channel.
 */
class PerSegmentInstanceTest {

    private RunnerContext ctx;

    @BeforeEach
    void reset() {
        UnboundedOperativeClassRegistry.reset();
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ctx = new RunnerContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));
        ctx.streamRegistry().register(0, 100L, 200L);
    }

    @AfterEach
    void close() {
        if (ctx != null && ctx.workerPool() != null) {
            ctx.workerPool().shutdownNow();
        }
    }

    @Test
    @DisplayName("static-key Segments each hold their own instance; a field written by one is not seen by the other")
    void staticKeySegmentsDoNotLeak() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(MaxOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("max");
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "max-inst", ctx, ctx.workerPool());

        instance.dispatcher(0).fanoutSegments("A:100", 100L);
        instance.dispatcher(0).fanoutSegments("B:999", 100L);
        instance.dispatcher(0).fanoutSegments("A:50", 100L);
        drainPool();

        MaxOp opA = instanceFor(instance, "A");
        MaxOp opB = instanceFor(instance, "B");

        // Distinct objects: the whole guarantee. A shared instance would make the
        // two keys the same object.
        assertThat(opA).isNotSameAs(opB);
        // Each field reflects only its own Segment's inputs — no leak either way.
        assertThat(opA.max).isEqualTo(100L);
        assertThat(opB.max).isEqualTo(999L);
    }

    @Test
    @DisplayName("dynamic Segments each receive their own instance")
    void dynamicSegmentsEachGetAnInstance() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(DynMaxOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("dyn-max");
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "dyn-max-inst", ctx, ctx.workerPool());

        instance.dispatcher(0).fanoutSegments("BTC:5", 100L);
        instance.dispatcher(0).fanoutSegments("ETH:9", 100L);
        instance.dispatcher(0).fanoutSegments("BTC:3", 100L);
        drainPool();

        assertThat(instance.segments().get(0)).hasSize(2);
        MaxOp btc = instanceFor(instance, "BTC");
        MaxOp eth = instanceFor(instance, "ETH");

        assertThat(btc).isNotSameAs(eth);
        assertThat(btc.max).isEqualTo(5L);
        assertThat(eth.max).isEqualTo(9L);
    }

    private void drainPool() throws InterruptedException {
        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    private static MaxOp instanceFor(UnboundedOperativeInstance instance, String key) {
        int idx = instance.segmentKeyToIdx().get(0).get(key);
        return (MaxOp) instance.segments().get(0).get(idx).operationInstance();
    }

    // --- fixtures -----------------------------------------------------------

    public static final class MaxOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "max";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(MaxOp.class);
        }
    }

    public static final class DynMaxOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "dyn-max";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(DynMaxOp.class);
        }
    }

    public record Unit() {}

    /**
     * Records the running max of the values routed to it in a MUTABLE INSTANCE
     * FIELD — exactly the shape that leaks across keys on a shared instance.
     * Input is {@code "KEY:VALUE"}.
     */
    public static class MaxOp extends StatelessTestOperation<String, Unit, String> {
        public volatile long max = Long.MIN_VALUE;

        @Override
        public String name() {
            return "max_op";
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
        public List<String> operationKeys() {
            return List.of("A", "B");
        }

        @Override
        public boolean segmentForOverridden() {
            return true;
        }

        @Override
        public List<String> segmentFor(String input) {
            return List.of(input.split(":")[0]);
        }

        @Override
        protected Unit initState(Optional<String> operationKey) {
            return new Unit();
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            max = Math.max(max, Long.parseLong(input.split(":")[1]));
        }
    }

    /** Same field-leak shape, but routed dynamically (one Segment per key seen). */
    public static final class DynMaxOp extends MaxOp {
        @Override
        public String name() {
            return "dyn_max_op";
        }

        @Override
        public List<String> operationKeys() {
            return List.of();
        }

        @Override
        public boolean segmentForOverridden() {
            return false;
        }

        @Override
        public Optional<String> operationKey(String input) {
            return Optional.of(input.split(":")[0]);
        }

        @Override
        public boolean operationKeyOverridden() {
            return true;
        }
    }
}
