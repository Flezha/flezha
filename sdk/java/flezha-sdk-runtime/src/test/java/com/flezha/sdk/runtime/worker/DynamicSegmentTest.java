package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.state.SegmentStateSink;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Dynamic segmentation: an Operation that overrides {@code operationKey(input)}
 * without declaring {@code operationKeys()} gets one Segment per key the input
 * stream actually carries, created on first sight.
 *
 * <p>This SDK previously <em>rejected</em> such an Operation at registration
 * ("dynamic mode is reserved … and rejected in v1"), so the whole shape was
 * unavailable to Java authors.
 */
class DynamicSegmentTest {

    /** (segmentKey, input) pairs, in the order the Operation saw them. */
    private static final List<String> SEEN = new CopyOnWriteArrayList<>();

    private RunnerContext ctx;

    @BeforeEach
    void reset() {
        SEEN.clear();
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
    @DisplayName("a dynamic Operation registers and creates one Segment per key seen")
    void dynamicOperationCreatesSegmentsLazily() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(DynamicOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("dynamic");
        assertThat(topology)
                .as("dynamic mode is no longer refused at registration")
                .isNotNull();

        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "dynamic-inst", ctx, ctx.workerPool());

        // No Segments exist up front — the keys are not declared anywhere.
        assertThat(instance.segments().get(0)).isEmpty();

        instance.dispatcher(0).fanoutSegments("BTC:1", 100L);
        instance.dispatcher(0).fanoutSegments("ETH:2", 100L);
        instance.dispatcher(0).fanoutSegments("BTC:3", 100L);

        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // Exactly two Segments: one per distinct key, each created on first sight.
        assertThat(instance.segments().get(0)).hasSize(2);

        // Each input ran on the Segment its key selects.
        assertThat(SEEN).containsExactlyInAnyOrder("BTC/BTC:1", "ETH/ETH:2", "BTC/BTC:3");
    }

    @Test
    @DisplayName("eviction is OFF unless a TTL is declared")
    void evictionIsOffByDefault() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(DynamicOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("dynamic");
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "no-ttl-inst", ctx, ctx.workerPool());

        instance.dispatcher(0).fanoutSegments("BTC:1", 100L);
        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        Thread.sleep(30L);
        // No TTL declared ⇒ the Segment is kept, however idle. Eviction is
        // destructive, so it must be opted into, never inferred.
        assertThat(instance.reapIdleDynamicSegments()).isZero();
    }

    @Test
    @DisplayName("a declared TTL evicts an idle Segment destructively")
    void declaredTtlEvictsIdleSegment() throws Exception {
        List<String> deleted = new CopyOnWriteArrayList<>();
        SegmentStateSink.installGlobalSink((payloadId, frame) -> {
            deleted.add(payloadId);
            return true;
        });

        UnboundedOperativeClassRegistry.registerClass(TtlOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("dynamic-ttl");
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "ttl-inst", ctx, ctx.workerPool());

        instance.dispatcher(0).fanoutSegments("BTC:1", 100L);
        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        Thread.sleep(60L); // idle past the 20 ms TTL

        assertThat(instance.reapIdleDynamicSegments()).isEqualTo(1);
        // Destructive: the controller is told to unlink the Segment's state.
        assertThat(deleted).contains(SegmentStateSink.PAYLOAD_ID_DELETE_SEGMENT_STATE);
    }

    public static final class TtlOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "dynamic-ttl";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(TtlOp.class);
        }
    }

    /** Same as DynamicOp, but opts into a (very short) idle TTL. */
    public static final class TtlOp extends DynamicOp {
        @Override
        public String name() {
            return "ttl_op";
        }

        @Override
        public java.util.Optional<java.time.Duration> segmentIdleTtl() {
            return java.util.Optional.of(java.time.Duration.ofMillis(20));
        }
    }

    // --- fixtures -----------------------------------------------------------

    public static final class DynamicOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "dynamic";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(DynamicOp.class);
        }
    }

    public record Unit() {}

    /** Keys by the symbol prefix — the classic dynamic case (one Segment per instrument). */
    public static class DynamicOp implements Operation<String, Unit, String, NoChange, WallClock, Void> {
        @Override
        public String name() {
            return "dynamic_op";
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
        public Optional<String> operationKey(String input) {
            return Optional.of(input.split(":")[0]);
        }

        @Override
        public boolean operationKeyOverridden() {
            return true;
        }

        @Override
        public Unit initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new Unit();
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            SEEN.add(input.split(":")[0] + "/" + input);
        }
    }
}
