package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.EdgeStream;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.state.DlqPark;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@code stream-transforms}: edge-closure value transforms ({@code map},
 * {@code filter}, {@code flatMap}) and edge-side Entity keying
 * ({@code keyBy}). Rust→Java parity port of
 * {@code sdk/rust/tests/stream_transforms.rs}.
 */
class StreamTransformsTest {

    /** Values the keyed consumer observed. */
    private static final List<String> OBSERVED = new CopyOnWriteArrayList<>();
    /** Values the fan-out consumer observed. */
    private static final List<String> FANNED = new CopyOnWriteArrayList<>();
    /**
     * Bumped once per traversal of the composed closure, proving the chain did
     * not become three edges the runtime walks in turn.
     */
    private static final AtomicInteger TRAVERSALS = new AtomicInteger();

    private static final List<String> TIMEFRAMES = List.of("1m", "5m", "1h", "1d");

    private RunnerContext ctx;

    @BeforeEach
    void reset() {
        OBSERVED.clear();
        FANNED.clear();
        TRAVERSALS.set(0);
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

    // ----- 1: a chain of transforms is one closure on one edge -----

    @Test
    @DisplayName("filter → map → map → keyBy collapses onto one consumer edge, traversed once")
    void aTransformChainIsOneClosureOnOneEdge() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(ChainOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("transform_chain");

        assertThat(topology.procedures())
                .as("four transforms added no Procedure node")
                .hasSize(1);
        var edges = topology.channels().get(0).consumers();
        assertThat(edges).hasSize(1);
        assertThat(edges.get(0).transform()).isPresent();
        assertThat(edges.get(0).key()).isPresent();

        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "chain-inst", ctx, ctx.workerPool());
        for (String raw : List.of("AAPL:3", "#comment", "MSFT:7")) {
            route(instance, raw);
        }
        drain();

        assertThat(OBSERVED)
                .as("the filtered value never reached the consumer, and the two maps ran")
                .containsExactlyInAnyOrder("AAPLx3", "MSFTx7");
        assertThat(entityKeys(instance))
                .as("the key ran on the MAPPED value — it split `AAPLx3`, not `AAPL:3`")
                .containsExactlyInAnyOrder("AAPL", "MSFT");
        assertThat(TRAVERSALS)
                .as("one traversal per input — the chain is one closure, not three edges")
                .hasValue(3);
    }

    // ----- 2: flatMap fans one input out across distinct Entities -----

    @Test
    @DisplayName("flatMap yields four values, each keyed into its own Entity")
    void aFanOutKeysEveryYieldedValueIndependently() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(FanOutOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("transform_fanout");
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "fan-inst", ctx, ctx.workerPool());

        route(instance, "AAPL:3");
        drain();

        assertThat(FANNED).containsExactlyInAnyOrder("1m", "5m", "1h", "1d");
        assertThat(entityKeys(instance))
                .as("four yielded values reached four distinct Entities")
                .containsExactlyInAnyOrder("AAPL@1m", "AAPL@5m", "AAPL@1h", "AAPL@1d");
    }

    // ----- 3: replay runs the same closures the live delivery ran -----

    @Test
    @DisplayName("a replayed slice folds the same values into the same Entities")
    void aReplayedSliceTransformsExactlyAsTheLiveRunDid() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(ChainOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("transform_chain");
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "chain-replay", ctx, ctx.workerPool());

        List<String> feed = List.of("AAPL:1", "#skip", "MSFT:2");
        for (String raw : feed) {
            route(instance, raw);
        }
        drainOnly();
        List<String> live = List.copyOf(OBSERVED);

        // The same frames re-forwarded as a replayed slice. The closures live
        // on the channel's edge, not on the delivery, so a warm restart folds
        // the identical values into the identical Entities.
        for (int i = 0; i < feed.size(); i++) {
            ProcedureDispatcher.routeToConsumers(
                    instance, topology.channel(0), feed.get(i), 100L, OptionalLong.of(i + 1), true);
        }
        drain();

        assertThat(OBSERVED.subList(live.size(), OBSERVED.size()))
                .as("replay must fold the same values the live run folded")
                .containsExactlyInAnyOrderElementsOf(live);
        assertThat(entityKeys(instance))
                .as("replay must reach the same Entities, creating no others")
                .containsExactlyInAnyOrder("AAPL", "MSFT");
    }

    // ----- 4: a throwing transform is contained and parks -----

    @Test
    @DisplayName("a throwing transform parks to the DLQ and routing keeps serving later inputs")
    void aThrowingTransformParksAndRoutingContinues() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(ThrowingTransformOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("transform_throw");
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, "throw-inst", ctx, ctx.workerPool());

        long parkedBefore = DlqPark.PARKED_TOTAL.get();
        for (String raw : List.of("ok-1", "poison", "ok-2")) {
            route(instance, raw);
        }
        drain();

        assertThat(OBSERVED)
                .as("the routing thread survived and kept serving later inputs")
                .containsExactlyInAnyOrder("ok-1", "ok-2");
        // The park itself blocks on a controller ack no test controller sends,
        // so the observable end of the ladder is the counter it bumps on the
        // way in. Retries are exhausted first (retryMax(2), 1 ms backoff).
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (DlqPark.PARKED_TOTAL.get() == parkedBefore && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertThat(DlqPark.PARKED_TOTAL.get())
                .as("the poison value climbed the ladder and parked")
                .isGreaterThan(parkedBefore);
    }

    // ----- 5: definition-time refusals -----

    @Test
    @DisplayName("every transform declaration refusal fires, with the Rust error names")
    void transformDeclarationsAreValidated() {
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(DuplicateKeyByOperative.class))
                .isInstanceOf(UnboundedOperativeConfigurationException.class)
                .hasMessageContaining("DuplicateKeyBy");
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(DanglingOperative.class))
                .hasMessageContaining("DanglingTransformChain")
                .hasMessageContaining("other");
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(AfterTerminalOperative.class))
                .hasMessageContaining("TransformAfterTerminal");
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(AfterKeyByOperative.class))
                .hasMessageContaining("TransformAfterKeyBy");
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(ConflictingRedeclarationOperative.class))
                .hasMessageContaining("EdgeAlreadyWired");
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(MixedKeyingOperative.class))
                .hasMessageContaining("UnkeyedEdgeIntoDynamicProcedure");
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(KeyByOnStaticOperative.class))
                .hasMessageContaining("KeyByOnUnkeyableTarget")
                .hasMessageContaining("transform_static_fan");

        // The transform-free re-declaration stays the legal fan-in no-op.
        UnboundedOperativeClassRegistry.registerClass(BareRedeclarationOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("accept_bare");
        assertThat(topology.channels().get(0).consumers()).hasSize(1);
    }

    @Test
    @DisplayName("the consumer's inputType is checked against the TRANSFORMED payload type")
    void aMistypedTransformChainIsRefusedAtRegistration() {
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(MistypedMapOperative.class))
                .hasMessageContaining("edge transforms produce");
    }

    // ----- helpers -----

    private void route(UnboundedOperativeInstance instance, String raw) {
        ProcedureDispatcher.routeToConsumers(
                instance, instance.topology().channel(0), raw, 100L, OptionalLong.empty(), false);
    }

    /** Let the worker pool finish, then keep it usable for a second wave. */
    private void drainOnly() throws Exception {
        Thread.sleep(50L);
    }

    private void drain() throws Exception {
        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    /** The live Entity keys of the operative's one dynamic Procedure. */
    private static List<String> entityKeys(UnboundedOperativeInstance instance) {
        return instance.entityKeyToIdx().get(0).keySet().stream().toList();
    }

    private static String[] parse(String raw) {
        int colon = raw.indexOf(':');
        return colon < 0 ? new String[] {raw, "1"} : new String[] {raw.substring(0, colon), raw.substring(colon + 1)};
    }

    // ----- operatives -----

    public static final class ChainOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "transform_chain";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in")
                    .filter(raw -> {
                        TRAVERSALS.incrementAndGet();
                        return !raw.startsWith("#");
                    })
                    .map(String.class, raw -> parse(raw)[0] + "x" + parse(raw)[1])
                    .map(String.class, shaped -> shaped)
                    .keyBy(shaped -> shaped.split("x")[0])
                    .into(Recorder.class);
        }
    }

    public static final class FanOutOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "transform_fanout";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in")
                    .flatMap(String.class, raw -> TIMEFRAMES.stream()
                            .map(tf -> parse(raw)[0] + "@" + tf)
                            .toList())
                    .keyBy(slot -> slot)
                    .into(FanRecorder.class);
        }
    }

    public static final class ThrowingTransformOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "transform_throw";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in")
                    .retryMax(2)
                    .retryBackoffMs(1)
                    .map(String.class, raw -> {
                        if (raw.equals("poison")) {
                            throw new IllegalStateException("transform blew up");
                        }
                        return raw;
                    })
                    .keyBy(raw -> raw)
                    .into(Recorder.class);
        }
    }

    public static final class DuplicateKeyByOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "refuse_duplicate_key";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").keyBy(s -> s).keyBy(String::toUpperCase).into(Recorder.class);
        }
    }

    public static final class DanglingOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "refuse_dangling";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").keyBy(s -> s).into(Recorder.class);
            c.input(String.class, "other").map(String.class, String::toUpperCase);
        }
    }

    public static final class AfterTerminalOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "refuse_after_terminal";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in")
                    .keyBy(s -> s)
                    .into(Echo.class)
                    .alsoOutput("plugin")
                    .filter(s -> true)
                    .into(Recorder.class);
        }
    }

    public static final class AfterKeyByOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "refuse_after_key";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in")
                    .keyBy(s -> s)
                    .map(String.class, String::toUpperCase)
                    .into(Recorder.class);
        }
    }

    public static final class ConflictingRedeclarationOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "refuse_redeclared";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var entry = c.input(String.class, "in");
            entry.keyBy(s -> s).into(Recorder.class);
            entry.keyBy(String::toUpperCase).into(Recorder.class);
        }
    }

    public static final class MixedKeyingOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "refuse_mixed_keying";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "first").keyBy(s -> s).into(Recorder.class);
            c.input(String.class, "second").into(Recorder.class);
        }
    }

    public static final class BareRedeclarationOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "accept_bare";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var entry = c.input(String.class, "in");
            entry.into(Recorder.class);
            entry.into(Recorder.class);
        }
    }

    public static final class KeyByOnStaticOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "refuse_key_on_static";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").keyBy(s -> s).into(StaticFan.class);
        }
    }

    public static final class MistypedMapOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "refuse_mistyped_map";
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public void channels(ChannelBuilder c) {
            // Through the typed API this is a COMPILE error — the `Class<U>`
            // token flows into the handle's type parameter — so reaching the
            // registration-time backstop takes a raw handle, which is exactly
            // the hole it exists to close.
            EdgeStream raw = c.input(String.class, "in").map(Integer.class, String::length);
            raw.keyBy(v -> String.valueOf(v)).into(Recorder.class);
        }
    }

    // ----- procedures -----

    public static class Recorder implements Procedure<String, Void, String, NoChange, WallClock, Void> {
        @Override
        public String name() {
            return "transform_recorder";
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
        public Void initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return null;
        }

        @Override
        public void run(String input, StateHandle<Void, NoChange> state, ProcedureCtx ctx) {
            OBSERVED.add(input);
        }
    }

    public static final class FanRecorder extends Recorder {
        @Override
        public String name() {
            return "transform_fan_recorder";
        }

        @Override
        public void run(String input, StateHandle<Void, NoChange> state, ProcedureCtx ctx) {
            FANNED.add(input.substring(input.indexOf('@') + 1));
        }
    }

    public static final class Echo extends Recorder {
        @Override
        public String name() {
            return "transform_echo";
        }

        @Override
        public void run(String input, StateHandle<Void, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class StaticFan extends Recorder {
        @Override
        public String name() {
            return "transform_static_fan";
        }

        @Override
        public List<String> entityKeys() {
            return List.of("a", "b");
        }

        @Override
        public boolean entitiesForOverridden() {
            return true;
        }

        @Override
        public List<String> entitiesFor(String input) {
            return List.of("a", "b");
        }
    }
}
