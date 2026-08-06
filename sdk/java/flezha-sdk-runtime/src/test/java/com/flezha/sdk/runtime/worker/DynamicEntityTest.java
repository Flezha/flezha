package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.state.EntityStateSink;
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
 * Dynamic entity mode: a Procedure that overrides {@code entityKey(input)}
 * without declaring {@code entityKeys()} gets one Entity per key the input
 * stream actually carries, created on first sight.
 *
 * <p>This SDK previously <em>rejected</em> such a Procedure at registration
 * ("dynamic mode is reserved … and rejected in v1"), so the whole shape was
 * unavailable to Java authors.
 */
class DynamicEntityTest {

    /** (entityKey, input) pairs, in the order the Procedure saw them. */
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
    @DisplayName("a dynamic Procedure registers and creates one Entity per key seen")
    void dynamicProcedureCreatesEntitiesLazily() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(DynamicOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("dynamic");
        assertThat(topology)
                .as("dynamic mode is no longer refused at registration")
                .isNotNull();

        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "dynamic-inst", ctx, ctx.workerPool());

        // No Entities exist up front — the keys are not declared anywhere.
        assertThat(instance.entities().get(0)).isEmpty();

        instance.dispatcher(0).fanoutEntities("BTC:1", 100L);
        instance.dispatcher(0).fanoutEntities("ETH:2", 100L);
        instance.dispatcher(0).fanoutEntities("BTC:3", 100L);

        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // Exactly two Entities: one per distinct key, each created on first sight.
        assertThat(instance.entities().get(0)).hasSize(2);

        // Each input ran on the Entity its key selects.
        assertThat(SEEN).containsExactlyInAnyOrder("BTC/BTC:1", "ETH/ETH:2", "BTC/BTC:3");
    }

    @Test
    @DisplayName("eviction is OFF unless a TTL is declared")
    void evictionIsOffByDefault() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(DynamicOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("dynamic");
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "no-ttl-inst", ctx, ctx.workerPool());

        instance.dispatcher(0).fanoutEntities("BTC:1", 100L);
        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        Thread.sleep(30L);
        // No TTL declared ⇒ the Entity is kept, however idle. Eviction is
        // destructive, so it must be opted into, never inferred.
        assertThat(instance.reapIdleDynamicEntities()).isZero();
    }

    @Test
    @DisplayName("a declared TTL evicts an idle Entity destructively")
    void declaredTtlEvictsIdleEntity() throws Exception {
        List<String> deleted = new CopyOnWriteArrayList<>();
        EntityStateSink.installGlobalSink((payloadId, frame) -> {
            deleted.add(payloadId);
            return true;
        });

        UnboundedOperativeClassRegistry.registerClass(TtlOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("dynamic-ttl");
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "ttl-inst", ctx, ctx.workerPool());

        instance.dispatcher(0).fanoutEntities("BTC:1", 100L);
        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        Thread.sleep(60L); // idle past the 20 ms TTL

        assertThat(instance.reapIdleDynamicEntities()).isEqualTo(1);
        // Destructive: the controller is told to unlink the Entity's state.
        assertThat(deleted).contains(EntityStateSink.PAYLOAD_ID_DELETE_ENTITY_STATE);
    }

    public static final class TtlOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "dynamic-ttl";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(TtlProc.class);
        }
    }

    /** Same as DynamicProc, but opts into a (very short) idle TTL. */
    public static final class TtlProc extends DynamicProc {
        @Override
        public String name() {
            return "ttl_op";
        }

        @Override
        public java.util.Optional<java.time.Duration> entityIdleTtl() {
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
            c.input(String.class, "in").into(DynamicProc.class);
        }
    }

    public record Unit() {}

    /** Keys by the symbol prefix — the classic dynamic case (one Entity per instrument). */
    public static class DynamicProc implements Procedure<String, Unit, String, NoChange, WallClock, Void> {
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
        public Optional<String> entityKey(String input) {
            return Optional.of(input.split(":")[0]);
        }

        @Override
        public boolean entityKeyOverridden() {
            return true;
        }

        @Override
        public Unit initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Unit();
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            SEEN.add(input.split(":")[0] + "/" + input);
        }
    }
}
