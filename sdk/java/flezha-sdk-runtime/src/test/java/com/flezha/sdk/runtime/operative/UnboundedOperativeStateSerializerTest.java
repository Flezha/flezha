package com.flezha.sdk.runtime.operative;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.entity.EntitySlot;
import com.flezha.sdk.runtime.operative.UnboundedOperativeStateSerializer.Selection;
import com.flezha.sdk.runtime.operative.UnboundedOperativeStateSerializer.Selector;
import com.flezha.sdk.runtime.state.EntityFlusher;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
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

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ExecutorService ctxPool;

    @BeforeEach
    void reset() {
        UnboundedOperativeClassRegistry.reset();
    }

    @AfterEach
    void closePool() {
        if (ctxPool != null) {
            ctxPool.shutdownNow();
        }
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

    // ----- Live read: selector, dynamic coverage, seq, refusals -----

    @Test
    @DisplayName("a selector narrows to one Entity of many and carries its seq")
    void selectorNarrowsToOneEntity() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(MixedUnboundedOperative.class);
        UnboundedOperativeInstance instance = newInstance("mixed");

        Selection all = UnboundedOperativeStateSerializer.select(instance, Selector.all());
        assertThat(MAPPER.readValue(all.json(), Map.class)).containsKeys("a", "b");
        assertThat(all.changeSeq())
                .as("a multi-Entity read names no single version")
                .isEmpty();

        Selection procedureOnly = UnboundedOperativeStateSerializer.select(instance, Selector.of("b", ""));
        assertThat(MAPPER.readValue(procedureOnly.json(), Map.class))
                .as("a procedure selector excludes the other Procedure entirely")
                .containsOnlyKeys("b");

        Selection one = UnboundedOperativeStateSerializer.select(instance, Selector.of("b", "1m"));
        @SuppressWarnings("unchecked")
        Map<String, Object> root = MAPPER.readValue(one.json(), Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> procedureB = (Map<String, Object>) root.get("b");
        assertThat(procedureB).containsOnlyKeys("1m");
        assertThat(one.changeSeq()).hasValue(0L);
    }

    @Test
    @DisplayName("a dynamic Procedure serializes under the keys its registry holds")
    void dynamicEntitiesSerializeUnderTheirLiveKeys() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(CandleOperative.class);
        UnboundedOperativeInstance instance = newInstance("candle_operative");
        int procedureId = procedureId(instance, "candle");
        instance.getOrCreateDynamicEntity(procedureId, "BTCUSD@1m");
        instance.getOrCreateDynamicEntity(procedureId, "ETHUSD@1m");

        Selection all = UnboundedOperativeStateSerializer.select(instance, Selector.all());
        @SuppressWarnings("unchecked")
        Map<String, Object> root = MAPPER.readValue(all.json(), Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> candle = (Map<String, Object>) root.get("candle");
        assertThat(candle).containsOnlyKeys("BTCUSD@1m", "ETHUSD@1m");

        Selection one = UnboundedOperativeStateSerializer.select(instance, Selector.of("candle", "BTCUSD@1m"));
        @SuppressWarnings("unchecked")
        Map<String, Object> narrowed =
                (Map<String, Object>) MAPPER.readValue(one.json(), Map.class).get("candle");
        assertThat(narrowed).containsOnlyKeys("BTCUSD@1m");
        assertThat(one.changeSeq()).hasValue(0L);
    }

    @Test
    @DisplayName("an Entity key without a Procedure is refused")
    void entityKeyWithoutProcedureIsRefused() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(MixedUnboundedOperative.class);
        UnboundedOperativeInstance instance = newInstance("mixed");

        assertThatThrownBy(() -> UnboundedOperativeStateSerializer.select(instance, Selector.of("", "1m")))
                .isInstanceOf(UnboundedOperativeStateSerializer.RefusedException.class)
                .hasMessageStartingWith(UnboundedOperativeStateSerializer.ENTITY_KEY_WITHOUT_PROCEDURE);

        assertThatThrownBy(() -> UnboundedOperativeStateSerializer.select(instance, Selector.of("nope", "")))
                .hasMessageStartingWith(UnboundedOperativeStateSerializer.PROCEDURE_NOT_FOUND);
    }

    @Test
    @DisplayName("an unknown key is refused distinctly from an evicted one")
    void unknownKeyIsRefusedDistinctlyFromAnEvictedOne() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(EvictingCandleOperative.class);
        UnboundedOperativeInstance instance = newInstance("evicting_candle_operative");
        int procedureId = procedureId(instance, "evicting_candle");
        instance.getOrCreateDynamicEntity(procedureId, "BTCUSD@1m");

        assertThatThrownBy(() ->
                        UnboundedOperativeStateSerializer.select(instance, Selector.of("evicting_candle", "NEVER")))
                .hasMessageStartingWith(UnboundedOperativeStateSerializer.ENTITY_NOT_FOUND);

        Thread.sleep(30L); // idle past the 10 ms TTL
        assertThat(instance.reapIdleDynamicEntities()).isEqualTo(1);

        assertThatThrownBy(() ->
                        UnboundedOperativeStateSerializer.select(instance, Selector.of("evicting_candle", "BTCUSD@1m")))
                .as("eviction is a distinct diagnosis")
                .hasMessageStartingWith(UnboundedOperativeStateSerializer.ENTITY_EVICTED);

        @SuppressWarnings("unchecked")
        Map<String, Object> surviving = (Map<String, Object>) MAPPER.readValue(
                        UnboundedOperativeStateSerializer.select(instance, Selector.of("evicting_candle", ""))
                                .json(),
                        Map.class)
                .get("evicting_candle");
        assertThat(surviving).as("the refused read created nothing").isEmpty();
    }

    @Test
    @DisplayName(
            "a read reflects an applied-but-unflushed change, reports a seq above the persisted one, and drains nothing")
    void readReflectsAnUnflushedChangeWithoutDraining() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(TallyOperative.class);
        UnboundedOperativeInstance instance = newInstance("tally_operative");
        EntitySlot slot = instance.entities().get(0).get(0);

        // Drop the flusher registration so "not yet flushed" is a fact of the
        // test rather than a race against the linger window, and arm the seq at
        // 7 to stand for "records 0..6 are durable".
        EntityFlusher.resetForTesting();
        slot.changeSeq().set(7L);

        instance.dispatcher(0).fanoutEntities("5", 100L, OptionalLong.empty(), false);
        ctxPool.shutdown();
        assertThat(ctxPool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        Selection read = UnboundedOperativeStateSerializer.select(instance, Selector.of("tally", ""));
        @SuppressWarnings("unchecked")
        Map<String, Object> tally =
                (Map<String, Object>) MAPPER.readValue(read.json(), Map.class).get("tally");
        assertThat(tally).containsEntry("value", 5);
        assertThat(read.changeSeq())
                .as("the reported seq is the Entity's next WAL seq — above every persisted one")
                .hasValue(7L);

        for (int i = 0; i < 3; i++) {
            UnboundedOperativeStateSerializer.select(instance, Selector.of("tally", ""));
        }
        assertThat(slot.changeSeq().get()).as("reads do not consume seqs").isEqualTo(7L);
        assertThat(EntityFlusher.getOrStart().registeredCount())
                .as("a read registers nothing with the flusher and drains nothing")
                .isZero();
    }

    @Test
    @DisplayName("the JSON shape is byte-identical to the Rust SDK's for an equivalent state")
    void jsonShapeMatchesTheRustSdk() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(CrossLanguageOperative.class);
        UnboundedOperativeInstance instance = newInstance("cross_language_operative");
        instance.entities().get(procedureId(instance, "counter")).get(0).state().set(new CounterState(1));
        instance.entities().get(procedureId(instance, "ohlc")).get(0).state().set(new OhlcState(2));
        instance.entities().get(procedureId(instance, "ohlc")).get(1).state().set(new OhlcState(3));

        // Pinned cross-language fixture: the identical literal is asserted by
        // `a_selector_less_read_matches_the_java_sdk_byte_for_byte` in the Rust
        // SDK's `tests/state_serialize.rs`. Both encoders sort object keys.
        assertThat(new String(
                        UnboundedOperativeStateSerializer.serialize(instance), java.nio.charset.StandardCharsets.UTF_8))
                .isEqualTo("{\"counter\":{\"value\":1},\"ohlc\":{\"1h\":{\"high\":3},\"1m\":{\"high\":2}}}");
    }

    private UnboundedOperativeInstance newInstance(String factoryId) {
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        RunnerContext ctx = new RunnerContext("ns", cfg, 0L);
        ctxPool = Executors.newFixedThreadPool(1);
        ctx.setWorkerPool(ctxPool);
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

    // --- live-read fixtures -------------------------------------------------

    public record CandleState(long ticks) {}

    /** Dynamic-mode: Entities exist only once a key has been seen. */
    public static class CandleProcedure extends StatelessTestProcedure<String, CandleState, String> {
        @Override
        public String name() {
            return "candle";
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
        protected CandleState initState(Optional<String> entityKey) {
            return new CandleState(0);
        }

        @Override
        public void run(String input, StateHandle<CandleState, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {}
    }

    /** Same Procedure, opted into a very short idle TTL so a sweep evicts it. */
    public static final class EvictingCandleProcedure extends CandleProcedure {
        @Override
        public String name() {
            return "evicting_candle";
        }

        @Override
        public Optional<Duration> entityIdleTtl() {
            return Optional.of(Duration.ofMillis(10));
        }
    }

    public static final class CandleOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "candle_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").keyBy(input -> input).into(CandleProcedure.class);
        }
    }

    public static final class EvictingCandleOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "evicting_candle_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").keyBy(input -> input).into(EvictingCandleProcedure.class);
        }
    }

    public static final class TallyState {
        public long value;
    }

    public record Add(long delta) implements Change {}

    /** Changeful, so a dispatch leaves an applied change in the flush buffer. */
    public static final class TallyProcedure implements Procedure<String, TallyState, String, Add, WallClock, Void> {
        @Override
        public String name() {
            return "tally";
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
        public Class<Add> changeType() {
            return Add.class;
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
        public List<Class<?>> kryoTypes() {
            return List.of(Add.class, TallyState.class);
        }

        @Override
        public void applyChange(TallyState state, Add change) {
            state.value += change.delta();
        }

        @Override
        public TallyState initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new TallyState();
        }

        @Override
        public void run(String input, StateHandle<TallyState, Add> state, ProcedureCtx ctx) throws ProcedureException {
            state.apply(new Add(Long.parseLong(input.trim())));
        }
    }

    public static final class TallyOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "tally_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(TallyProcedure.class);
        }
    }

    public record CounterState(long value) {}

    public record OhlcState(long high) {}

    /** Named and shaped to match the Rust SDK's `PairOperative` fixture. */
    public static final class CounterProcedure extends StatelessTestProcedure<String, CounterState, String> {
        @Override
        public String name() {
            return "counter";
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
        protected CounterState initState(Optional<String> entityKey) {
            return new CounterState(0);
        }

        @Override
        public void run(String input, StateHandle<CounterState, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {}
    }

    public static final class OhlcProcedure extends StatelessTestProcedure<String, OhlcState, String> {
        @Override
        public String name() {
            return "ohlc";
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
        protected OhlcState initState(Optional<String> entityKey) {
            return new OhlcState(0);
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
        public void run(String input, StateHandle<OhlcState, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {}
    }

    public static final class CrossLanguageOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "cross_language_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(CounterProcedure.class).into(OhlcProcedure.class);
        }
    }
}
