package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.operative.StatelessTestProcedure;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.stream.PrimaryStreamReader;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Java port of {@code sdk/rust/tests/merged_input.rs}
 * ({@code multi-input-procedures}): a procedure fed by two or more source
 * channels, each subscription mapping its payload into one sum-type input.
 *
 * <p>Mirrors the Rust scenarios one for one — merged delivery through typed
 * inter-procedure channels, entry channels as sources, per-source FIFO under
 * interleaving, a replayed slice mapped exactly as live delivery mapped it,
 * {@code entitiesFor} on the mapped value, and the definition-time refusals.
 */
class MergedInputTest {

    // Procedures are re-instantiated reflectively (no-arg ctor), so captures
    // are static. Cleared in @BeforeEach so test order does not bleed.
    private static final List<SignalInput> SIGNALS = new CopyOnWriteArrayList<>();
    private static final List<String> SIGNAL_SINK = new CopyOnWriteArrayList<>();
    private static final List<TunedInput> TUNED = new CopyOnWriteArrayList<>();
    private static final List<String> PER_ENTITY = new CopyOnWriteArrayList<>();

    private RunnerContext ctx;

    @BeforeEach
    void reset() {
        SIGNALS.clear();
        SIGNAL_SINK.clear();
        TUNED.clear();
        PER_ENTITY.clear();
        UnboundedOperativeClassRegistry.reset();

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "2"));
        ctx = new RunnerContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(2));
        ctx.streamRegistry().register(0, 100L, 200L);
    }

    @AfterEach
    void close() {
        if (ctx != null && ctx.workerPool() != null) {
            ctx.workerPool().shutdownNow();
        }
    }

    // ---------- Payloads ----------

    public record Ticker(String symbol) {}

    public record Candle(long close) {}

    /** The author-declared sum type a merged procedure consumes. */
    public record SignalInput(Optional<Ticker> ticker, Optional<Candle> candle) {
        static SignalInput ofTicker(Ticker t) {
            return new SignalInput(Optional.of(t), Optional.empty());
        }

        static SignalInput ofCandle(Candle c) {
            return new SignalInput(Optional.empty(), Optional.of(c));
        }
    }

    /** A data feed plus a live config channel — the reconfigurable shape. */
    public record TunedInput(String kind, String value) {
        static TunedInput data(String v) {
            return new TunedInput("data", v);
        }

        static TunedInput config(String v) {
            return new TunedInput("config", v);
        }
    }

    // ---------- 1: two typed channels feed one procedure ----------

    @Test
    @DisplayName("two typed channels feed one procedure, whose own output still fans out")
    void twoTypedChannelsFeedOneProcedure() {
        UnboundedOperativeInstance instance = instantiate(TypedMergeOperative.class, "typed_merge_operative");
        PrimaryStreamReader reader = reader();

        reader.onData(FrameEncoder.build(instance.instanceId(), "abc".getBytes(), Map.of("channel", "raw")));

        // One raw frame fans out to both producers, so the merged procedure
        // sees one value per source — through its ordinary run, as one type.
        assertThat(waitFor(() -> SIGNALS.size() == 2 && SIGNAL_SINK.size() == 2, 5_000))
                .as("merged delivery did not complete: %s / %s", SIGNALS, SIGNAL_SINK)
                .isTrue();
        assertThat(SIGNALS)
                .containsExactlyInAnyOrder(
                        SignalInput.ofTicker(new Ticker("abc")), SignalInput.ofCandle(new Candle(3)));
        assertThat(SIGNAL_SINK).containsExactlyInAnyOrder("ticker:abc", "candle:3");
    }

    // ---------- 2: entry channels as merge sources ----------

    @Test
    @DisplayName("entry channels are merge sources, delivered mapped with no parser procedure")
    void entryChannelsAreMergeSources() {
        UnboundedOperativeInstance instance = instantiate(EntryMergeOperative.class, "entry_merge_operative");
        PrimaryStreamReader reader = reader();

        reader.onData(FrameEncoder.build(instance.instanceId(), "threshold=7".getBytes(), Map.of("channel", "config")));
        reader.onData(FrameEncoder.build(instance.instanceId(), "tick-1".getBytes(), Map.of("channel", "data")));

        assertThat(waitFor(() -> TUNED.size() == 2, 5_000))
                .as("entry-fed merge did not deliver: %s", TUNED)
                .isTrue();
        assertThat(TUNED).containsExactlyInAnyOrder(TunedInput.config("threshold=7"), TunedInput.data("tick-1"));
    }

    @Test
    @DisplayName("per-source FIFO holds under interleaving; nothing is promised across sources")
    void perSourceOrderHoldsUnderInterleaving() {
        UnboundedOperativeInstance instance = instantiate(EntryMergeOperative.class, "entry_merge_operative");
        PrimaryStreamReader reader = reader();

        int n = 200;
        int configs = 0;
        for (int i = 0; i < n; i++) {
            reader.onData(FrameEncoder.build(instance.instanceId(), ("d" + i).getBytes(), Map.of("channel", "data")));
            if (i % 20 == 0) {
                reader.onData(
                        FrameEncoder.build(instance.instanceId(), ("c" + i).getBytes(), Map.of("channel", "config")));
                configs++;
            }
        }
        int expected = n + configs;

        assertThat(waitFor(() -> TUNED.size() == expected, 5_000))
                .as("interleaved merge did not drain: %s", TUNED.size())
                .isTrue();

        List<String> data = TUNED.stream()
                .filter(v -> v.kind().equals("data"))
                .map(TunedInput::value)
                .toList();
        List<String> config = TUNED.stream()
                .filter(v -> v.kind().equals("config"))
                .map(TunedInput::value)
                .toList();
        assertThat(data)
                .as("source `data` lost its FIFO order")
                .isEqualTo(IntStream.range(0, n).mapToObj(i -> "d" + i).toList());
        assertThat(config)
                .as("source `config` lost its FIFO order")
                .isEqualTo(IntStream.range(0, n)
                        .filter(i -> i % 20 == 0)
                        .mapToObj(i -> "c" + i)
                        .toList());
    }

    @Test
    @DisplayName("a replayed slice maps exactly as the live run mapped it")
    void aReplayedMergeMapsExactlyAsTheLiveRunDid() {
        UnboundedOperativeInstance instance = instantiate(EntryMergeOperative.class, "entry_merge_operative");
        PrimaryStreamReader reader = reader();

        String[][] feed = {{"data", "d0"}, {"config", "c0"}, {"data", "d1"}};
        for (String[] f : feed) {
            reader.onData(FrameEncoder.build(instance.instanceId(), f[1].getBytes(), Map.of("channel", f[0])));
        }
        assertThat(waitFor(() -> TUNED.size() == 3, 5_000))
                .as("live run did not drain")
                .isTrue();
        List<TunedInput> live = List.copyOf(TUNED);

        // The same frames re-forwarded as a replayed slice: the mapping is the
        // channel's, not the delivery's, so a warm restart folds the same values.
        for (int i = 0; i < feed.length; i++) {
            reader.onData(FrameEncoder.build(
                    instance.instanceId(),
                    feed[i][1].getBytes(),
                    Map.of("channel", feed[i][0], "flz.replay", "1", "flz.offset", String.valueOf(i + 1))));
        }
        reader.onData(FrameEncoder.build(instance.instanceId(), new byte[0], Map.of("flz.replay_end", "1")));

        assertThat(waitFor(() -> TUNED.size() == 6, 5_000))
                .as("replayed slice did not drain: %s", TUNED)
                .isTrue();
        assertThat(TUNED.subList(3, 6))
                .as("replay must map identically to live delivery")
                .containsExactlyInAnyOrderElementsOf(live);
    }

    // ---------- 3: entity derivation runs on the MAPPED value ----------

    @Test
    @DisplayName("entitiesFor is evaluated on the mapped value, not the source payload")
    void entitiesForIsEvaluatedOnTheMappedValue() {
        UnboundedOperativeInstance instance = instantiate(EntityMergeOperative.class, "entity_merge_operative");
        PrimaryStreamReader reader = reader();

        reader.onData(FrameEncoder.build(instance.instanceId(), "d0".getBytes(), Map.of("channel", "data")));
        reader.onData(FrameEncoder.build(instance.instanceId(), "c0".getBytes(), Map.of("channel", "config")));

        assertThat(waitFor(() -> PER_ENTITY.size() == 2, 5_000))
                .as("entity merge did not drain: %s", PER_ENTITY)
                .isTrue();
        // The routing key came from the sum type: entitiesFor never sees the
        // pre-map payload.
        assertThat(PER_ENTITY).containsExactlyInAnyOrder("data/d0", "config/c0");
    }

    // ---------- 4: definition-time validation ----------

    @Test
    @DisplayName("a merge with fewer than two sources is refused, naming the procedure")
    void aMergeWithFewerThanTwoSourcesIsRefused() {
        UnboundedOperativeConfigurationException ex = assertThrows(
                UnboundedOperativeConfigurationException.class,
                () -> UnboundedOperativeClassRegistry.registerClass(OneSourceMergeOperative.class));
        assertThat(ex.getMessage()).contains("MergeTooFewSources");
        assertThat(ex.getMessage()).contains("merge_tuned_procedure");
    }

    @Test
    @DisplayName("a duplicated merge source is refused, naming the channel — and only that")
    void aDuplicatedMergeSourceIsRefused() {
        UnboundedOperativeConfigurationException ex = assertThrows(
                UnboundedOperativeConfigurationException.class,
                () -> UnboundedOperativeClassRegistry.registerClass(DuplicateSourceMergeOperative.class));
        assertThat(ex.getMessage()).contains("DuplicateMergeSource");
        assertThat(ex.getMessage()).contains("`data`");
        // The author declared two sources; reporting the merge as
        // under-declared as well would send them to fix the wrong thing.
        assertThat(ex.getMessage())
                .as("a duplicated source must not also read as too few sources")
                .doesNotContain("MergeTooFewSources");
    }

    @Test
    @DisplayName("a merge source may not re-wire an edge another merge already declared")
    void aRewiredMergeSourceIsRefused() {
        UnboundedOperativeConfigurationException ex = assertThrows(
                UnboundedOperativeConfigurationException.class,
                () -> UnboundedOperativeClassRegistry.registerClass(RewiredMergeSourceOperative.class));
        assertThat(ex.getMessage()).contains("MergeSourceAlreadyWired");
        assertThat(ex.getMessage()).contains("`data`");
        assertThat(ex.getMessage()).contains("merge_tuned_procedure");
    }

    @Test
    @DisplayName("a merge may not re-wire an edge a plain into() already declared")
    void aMergeOntoAPlainEdgeIsRefused() {
        UnboundedOperativeConfigurationException ex = assertThrows(
                UnboundedOperativeConfigurationException.class,
                () -> UnboundedOperativeClassRegistry.registerClass(PlainThenMergedOperative.class));
        assertThat(ex.getMessage()).contains("MergeSourceAlreadyWired");
        assertThat(ex.getMessage()).contains("`data`");
    }

    @Test
    @DisplayName("a merged type that is not the consumer's inputType is refused at registration")
    void aMergedTypeMismatchIsRefused() {
        // What Rust rejects at the `.into::<Proc>()` call site with an
        // `Input = I` bound, erasure forces Java to reject at registration.
        UnboundedOperativeConfigurationException ex = assertThrows(
                UnboundedOperativeConfigurationException.class,
                () -> UnboundedOperativeClassRegistry.registerClass(MismatchedMergeOperative.class));
        assertThat(ex.getMessage()).contains("merge maps into");
        assertThat(ex.getMessage()).contains("merge_tuned_procedure");
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private UnboundedOperativeInstance instantiate(
            Class<? extends UnboundedOperative> operativeClass, String factoryId) {
        UnboundedOperativeClassRegistry.registerClass(operativeClass);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology(factoryId);
        // The instance id IS the wire payload-id, a fixed 32-byte field: a
        // longer one truncates and the frame finds no operative.
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, factoryId, ctx, ctx.workerPool());
        ctx.operativeInstanceRegistry().add(instance);
        return instance;
    }

    private PrimaryStreamReader reader() {
        return new PrimaryStreamReader(ctx, 100L);
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

    // =========================================================================
    // Test topologies
    // =========================================================================

    public static final class TickerProcedure extends StatelessTestProcedure<String, Void, Ticker> {
        @Override
        public String name() {
            return "merge_ticker_procedure";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Ticker> outputType() {
            return Ticker.class;
        }

        @Override
        protected Void initState(Optional<String> entityKey) {
            return null;
        }

        @Override
        public void run(String input, StateHandle<Void, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emit(new Ticker(input));
        }
    }

    public static final class CandleProcedure extends StatelessTestProcedure<String, Void, Candle> {
        @Override
        public String name() {
            return "merge_candle_procedure";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Candle> outputType() {
            return Candle.class;
        }

        @Override
        protected Void initState(Optional<String> entityKey) {
            return null;
        }

        @Override
        public void run(String input, StateHandle<Void, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emit(new Candle(input.length()));
        }
    }

    public static final class DetectorProcedure extends StatelessTestProcedure<SignalInput, Void, String> {
        @Override
        public String name() {
            return "merge_detector_procedure";
        }

        @Override
        public Class<SignalInput> inputType() {
            return SignalInput.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        protected Void initState(Optional<String> entityKey) {
            return null;
        }

        @Override
        public void run(SignalInput input, StateHandle<Void, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            SIGNALS.add(input);
            String label = input.ticker()
                    .map(t -> "ticker:" + t.symbol())
                    .orElseGet(() -> "candle:" + input.candle().orElseThrow().close());
            ctx.emit(label);
        }
    }

    public static final class SignalSinkProcedure extends StatelessTestProcedure<String, Void, String> {
        @Override
        public String name() {
            return "merge_signal_sink_procedure";
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
        protected Void initState(Optional<String> entityKey) {
            return null;
        }

        @Override
        public void run(String input, StateHandle<Void, NoChange> state, ProcedureCtx ctx) {
            SIGNAL_SINK.add(input);
        }
    }

    public static final class TypedMergeOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "typed_merge_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var raw = c.input(String.class, "raw");
            var ticker = raw.into(TickerProcedure.class).named("ticker_out");
            var candles = raw.into(CandleProcedure.class).named("ohlc_out");
            c.merge(SignalInput.class)
                    .from(ticker, SignalInput::ofTicker)
                    .from(candles, SignalInput::ofCandle)
                    .into(DetectorProcedure.class)
                    .into(SignalSinkProcedure.class);
        }
    }

    public static final class TunedProcedure extends StatelessTestProcedure<TunedInput, Void, String> {
        @Override
        public String name() {
            return "merge_tuned_procedure";
        }

        @Override
        public Class<TunedInput> inputType() {
            return TunedInput.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        protected Void initState(Optional<String> entityKey) {
            return null;
        }

        @Override
        public void run(TunedInput input, StateHandle<Void, NoChange> state, ProcedureCtx ctx) {
            TUNED.add(input);
        }
    }

    public static final class EntryMergeOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "entry_merge_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.merge(TunedInput.class)
                    .from(c.input(String.class, "data"), TunedInput::data)
                    .from(c.input(String.class, "config"), TunedInput::config)
                    .into(TunedProcedure.class);
        }
    }

    public static final class EntityTunedProcedure extends StatelessTestProcedure<TunedInput, String, String> {
        @Override
        public String name() {
            return "merge_entity_tuned_procedure";
        }

        @Override
        public Class<TunedInput> inputType() {
            return TunedInput.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public List<String> entityKeys() {
            return List.of("data", "config");
        }

        @Override
        public boolean entitiesForOverridden() {
            return true;
        }

        @Override
        public List<String> entitiesFor(TunedInput input) {
            return List.of(input.kind());
        }

        @Override
        protected String initState(Optional<String> entityKey) {
            return entityKey.orElse("?");
        }

        @Override
        public void run(TunedInput input, StateHandle<String, NoChange> state, ProcedureCtx ctx) {
            PER_ENTITY.add(state.get() + "/" + input.value());
        }
    }

    public static final class EntityMergeOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "entity_merge_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.merge(TunedInput.class)
                    .from(c.input(String.class, "data"), TunedInput::data)
                    .from(c.input(String.class, "config"), TunedInput::config)
                    .into(EntityTunedProcedure.class);
        }
    }

    public static final class OneSourceMergeOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "one_source_merge_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.merge(TunedInput.class)
                    .from(c.input(String.class, "data"), TunedInput::data)
                    .into(TunedProcedure.class);
        }
    }

    public static final class DuplicateSourceMergeOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "duplicate_source_merge_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var data = c.input(String.class, "data");
            c.merge(TunedInput.class)
                    .from(data, TunedInput::data)
                    .from(data, TunedInput::config)
                    .into(TunedProcedure.class);
        }
    }

    /**
     * Two merges closing on ONE consumer that share a source channel — the
     * shape a graph assembled by helper methods falls into. The second
     * {@code from} cannot be honoured (an edge carries one mapping), so it is
     * refused rather than dropped: the procedure would otherwise receive the
     * first declaration's value forever, silently.
     */
    public static final class RewiredMergeSourceOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "rewired_merge_source_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var data = c.input(String.class, "data");
            c.merge(TunedInput.class)
                    .from(data, TunedInput::data)
                    .from(c.input(String.class, "config"), TunedInput::config)
                    .into(TunedProcedure.class);
            c.merge(TunedInput.class)
                    .from(data, TunedInput::config)
                    .from(c.input(String.class, "extra"), TunedInput::data)
                    .into(TunedProcedure.class);
        }
    }

    /** The same edge claimed first by a plain into(), then by a merge. */
    public static final class PlainThenMergedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "plain_then_merged_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var data = c.input(TunedInput.class, "data", b -> TunedInput.data(new String(b)));
            data.into(TunedProcedure.class);
            c.merge(TunedInput.class)
                    .from(data, v -> v)
                    .from(c.input(String.class, "config"), TunedInput::config)
                    .into(TunedProcedure.class);
        }
    }

    public static final class MismatchedMergeOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "mismatched_merge_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            // `TunedProcedure.inputType()` is TunedInput, not String.
            c.merge(String.class)
                    .from(c.input(String.class, "data"), s -> s)
                    .from(c.input(String.class, "config"), s -> s)
                    .into(mismatched());
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static Class<? extends Procedure<String, ?, String, ?, ?, ?>> mismatched() {
            return (Class) TunedProcedure.class;
        }
    }
}
