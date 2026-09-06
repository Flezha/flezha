package com.flezha.sdk.runtime.operative;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StreamOperativeTopologyBuilderTest {

    public record Trade(String symbol, double price) {}

    public record Candle(String symbol, double open, double close) {}

    @Nested
    class WiringValidation {

        @Test
        @DisplayName("single-mode entry channel + consumer compiles and freezes")
        void singleModeOk() {
            StreamOperativeTopology topology = StreamOperativeTopologyBuilder.build(new SingleModeStreamOperative());
            assertThat(topology.factoryId()).isEqualTo("single_mode");
            assertThat(topology.channels()).hasSize(1);
            assertThat(topology.channels().get(0).kind()).isEqualTo(ChannelKind.ENTRY);
            assertThat(topology.procedures()).hasSize(1);
            assertThat(topology.procedures().get(0).entityKeys()).isEmpty();
        }

        @Test
        @DisplayName("static-mode procedure allocates one descriptor with the declared keys")
        void staticModeAllocatesEntities() {
            StreamOperativeTopology topology = StreamOperativeTopologyBuilder.build(new StaticModeStreamOperative());
            assertThat(topology.procedures().get(0).entityKeys()).containsExactly("a", "b", "c", "d");
            assertThat(topology.procedures().get(0).entitiesForOverridden()).isTrue();
        }

        @Test
        @DisplayName("a typed entry channel is accepted and decodes at the edge")
        void typedEntryChannelIsAcceptedAsAParser() {
            // `sdk-graph-authoring`: any entry type is legal; non-byte[]/String
            // entries decode at the edge (default Jackson) and are flagged as
            // real parsers so a failing decode rides the retry→DLQ ladder.
            var topology = StreamOperativeTopologyBuilder.build(new BadEntryTypeStreamOperative());
            assertThat(topology.channels().get(0).entryParsed()).isTrue();
            assertThat(topology.channels().get(0).entryParser()).isPresent();
        }

        @Test
        @DisplayName("wiring mismatch fails registration with diagnosable error")
        void wiringMismatchRejected() {
            assertThatThrownBy(() -> StreamOperativeTopologyBuilder.build(new TypeMismatchStreamOperative()))
                    .isInstanceOf(StreamOperativeConfigurationException.class)
                    .hasMessageContaining("trade.parsed");
        }

        @Test
        @DisplayName("static-mode procedure without entitiesForOverridden is rejected")
        void staticWithoutEntitiesForOverriddenRejected() {
            assertThatThrownBy(() ->
                            StreamOperativeTopologyBuilder.build(new MissingEntitiesForOverriddenStreamOperative()))
                    .isInstanceOf(StreamOperativeConfigurationException.class)
                    .hasMessageContaining("EntitiesForMissing")
                    .hasMessageContaining("declares non-empty entity_keys() but does not override entities_for");
        }

        @Test
        @DisplayName("a type argument still generic at the procedure class is refused at definition time")
        void unresolvedTypeTokenRejected() {
            // The defaults read the implements clause; what they cannot read
            // they refuse by name here, rather than guessing and failing at
            // dispatch with a type mismatch far from this declaration.
            assertThatThrownBy(() -> StreamOperativeTopologyBuilder.build(new UnresolvedTokenStreamOperative()))
                    .isInstanceOf(StreamOperativeConfigurationException.class)
                    .hasMessageContaining(UnresolvedInputProcedure.class.getName())
                    .hasMessageContaining("Input");
        }

        @Test
        @DisplayName("a keyed edge builds a dynamic-mode consumer (Entities are created per key at dispatch)")
        void dynamicModeOnlyBuilds() {
            // The keying is on the WIRING: the procedure declares nothing, and
            // the edge's key function is what classifies it as dynamic.
            StreamOperativeTopology topology =
                    StreamOperativeTopologyBuilder.build(new DynamicModeOnlyStreamOperative());
            assertThat(topology.procedures()).hasSize(1);
            assertThat(topology.procedures().get(0).dynamic()).isTrue();
            assertThat(topology.procedures().get(0).entityKeys()).isEmpty();
            assertThat(topology.channels().get(0).consumers().get(0).key()).isPresent();
        }
    }

    // ---- Test operatives / procedures -----------------------------------------------

    /** A concrete procedure class that leaves its own Input open. */
    public static final class UnresolvedInputProcedure<I> extends StatelessTestProcedure<I, Object, String> {
        @Override
        public String name() {
            return "unresolved_input";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(I input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class UnresolvedTokenStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "unresolved_token";
        }

        @Override
        @SuppressWarnings("unchecked")
        public void channels(ChannelBuilder c) {
            // A generic procedure class has a raw class literal, so wiring it
            // takes a cast — which is the author's first hint that the runtime
            // will have nothing concrete to read either.
            c.input(String.class, "in").into((Class<? extends Procedure<String, ?, String, ?, ?, ?>>)
                    (Class<?>) UnresolvedInputProcedure.class);
        }
    }

    public static final class SingleProcedure extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "single_procedure";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class SingleModeStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "single_mode";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(SingleProcedure.class);
        }
    }

    public static final class StaticProcedure extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "static_procedure";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public List<String> entityKeys() {
            return List.of("a", "b", "c", "d");
        }

        @Override
        public boolean entitiesForOverridden() {
            return true;
        }

        @Override
        public List<String> entitiesFor(String input) {
            return List.of(input.substring(0, 1));
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class StaticModeStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "static_mode";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(StaticProcedure.class);
        }
    }

    public static final class BadEntryTypeStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "bad_entry";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(Trade.class, "trade.raw").into(TradeProcedure.class);
        }
    }

    public static final class TradeProcedure extends StatelessTestProcedure<Trade, Object, String> {
        @Override
        public String name() {
            return "trade_procedure";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(Trade input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class CandleProcedure extends StatelessTestProcedure<Candle, Object, String> {
        @Override
        public String name() {
            return "candle_procedure";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(Candle input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class TradeProducerProcedure extends StatelessTestProcedure<String, Object, Trade> {
        @Override
        public String name() {
            return "trade_producer";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {}
    }

    public static final class TypeMismatchStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "type_mismatch";
        }

        @Override
        public void channels(ChannelBuilder c) {
            ChannelDeclMismatch.declareTradeIntoCandle(
                    c.input(String.class, "input").into(TradeProducerProcedure.class));
        }
    }

    /** Helper to keep the erasure-mismatch construction off the operative. */
    private static final class ChannelDeclMismatch {
        @SuppressWarnings({"unchecked", "rawtypes"})
        static void declareTradeIntoCandle(com.flezha.sdk.channel.Stream<Trade> stream) {
            ((com.flezha.sdk.channel.Stream) stream.named("trade.parsed")).into((Class) CandleProcedure.class);
        }
    }

    public static final class MissingEntitiesForOverriddenProcedure
            extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "missing_pfo_procedure";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public List<String> entityKeys() {
            return List.of("a", "b");
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class MissingEntitiesForOverriddenStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "missing_pfo_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(MissingEntitiesForOverriddenProcedure.class);
        }
    }

    public static final class DynamicModeOnlyProcedure extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "dynamic_only_procedure";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class DynamicModeOnlyStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "dynamic_only_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").keyBy(input -> input).into(DynamicModeOnlyProcedure.class);
        }
    }

    // ---- sdk-graph-authoring pins (mirror of the Rust operative_topology tests) ----

    @Nested
    class GraphAuthoring {

        @Test
        @DisplayName("branch() builds the identical topology to handle reuse")
        void branchBuildsIdenticalTopology() {
            var byHandles = StreamOperativeTopologyBuilder.build("g1", c -> {
                var validated = c.input(String.class, "in").into(SingleProcedure.class);
                validated.into(GraphStage1.class);
                validated.into(GraphStage2.class);
            });
            var byBranch = StreamOperativeTopologyBuilder.build("g2", c -> c.input(String.class, "in")
                    .into(SingleProcedure.class)
                    .branch(b -> b.into(GraphStage1.class), b -> b.into(GraphStage2.class)));
            assertThat(shape(byBranch)).isEqualTo(shape(byHandles));
        }

        @Test
        @DisplayName("branch() with fewer than two arms is refused")
        void branchWithOneArmRefused() {
            assertThatThrownBy(() -> StreamOperativeTopologyBuilder.build("g3", c -> c.input(String.class, "in")
                            .into(SingleProcedure.class)
                            .branch(b -> b.into(GraphStage1.class))))
                    .isInstanceOf(StreamOperativeConfigurationException.class)
                    .hasMessageContaining("BranchTooFewArms")
                    .hasMessageContaining("branch(...) requires at least two arms; 1 given");
        }

        @Test
        @DisplayName("named nodes reuse one class at two positions; unnamed mentions unify into a fan-in")
        void namedNodesAndFanIn() {
            var t = StreamOperativeTopologyBuilder.build("g4", c -> {
                var validated = c.input(String.class, "in").into(SingleProcedure.class);
                validated.into(GraphStage1.class, "fraud_filter").into(GraphMerge.class);
                validated.into(GraphStage1.class, "spam_filter").into(GraphMerge.class);
            });
            assertThat(t.procedures().stream().map(o -> o.name()).toList())
                    .containsExactly("single_procedure", "fraud_filter", "graph_merge", "spam_filter");
            // Derived channel names are the node identities; the merge is ONE node.
            assertThat(t.nameToId()).containsKeys("fraud_filter", "spam_filter");
            long merges = t.procedures().stream()
                    .filter(o -> o.name().equals("graph_merge"))
                    .count();
            assertThat(merges).isEqualTo(1);
        }

        @Test
        @DisplayName("a node identity collision is refused")
        void nodeIdentityCollisionRefused() {
            assertThatThrownBy(() -> StreamOperativeTopologyBuilder.build("g5", c -> {
                        var validated = c.input(String.class, "in").into(SingleProcedure.class);
                        validated.into(GraphStage2.class, "graph_stage1");
                        validated.into(GraphStage1.class);
                    }))
                    .isInstanceOf(StreamOperativeConfigurationException.class)
                    .hasMessageContaining("NodeIdentityCollision")
                    .hasMessageContaining("graph_stage1");
        }

        @Test
        @DisplayName("channel names derive from the producer identity; .named overrides; collisions are refused")
        void derivedNamesAndCollision() {
            var t = StreamOperativeTopologyBuilder.build("g6", c -> c.input(String.class, "in")
                    .into(SingleProcedure.class)
                    .into(GraphStage1.class)
                    .named("orders.validated")
                    .into(GraphStage2.class));
            assertThat(t.nameToId()).containsKeys("single_procedure", "orders.validated");

            assertThatThrownBy(() ->
                            StreamOperativeTopologyBuilder.build("g7", c -> c.input(String.class, "single_procedure")
                                    .into(SingleProcedure.class)
                                    .into(GraphStage1.class)))
                    .isInstanceOf(StreamOperativeConfigurationException.class)
                    .hasMessageContaining("DuplicateChannel")
                    .hasMessageContaining("single_procedure");
        }

        @Test
        @DisplayName("a cycle is refused")
        void cycleRefused() {
            assertThatThrownBy(() -> StreamOperativeTopologyBuilder.build("g8", c -> c.input(String.class, "in")
                            .into(GraphStage1.class)
                            .into(GraphStage2.class)
                            .into(GraphStage1.class)))
                    .isInstanceOf(StreamOperativeConfigurationException.class)
                    .hasMessageContaining("Cycle")
                    .hasMessageContaining("is on a cycle");
        }

        @Test
        @DisplayName("multiple entry channels build")
        void multipleEntryChannelsBuild() {
            // One operative may declare several entry channels — one per
            // external source (e.g. a market pipeline reading a trade feed and
            // an order-book feed). The controller routes each frame to its
            // entry by metadata["channel"]. Both entries and subgraphs resolve.
            var t = StreamOperativeTopologyBuilder.build("g9", c -> {
                c.input(String.class, "in_a").into(GraphStage1.class);
                c.input(String.class, "in_b").into(GraphStage2.class);
            });
            var entries = t.channels().stream()
                    .filter(ch -> ch.kind() == ChannelKind.ENTRY)
                    .map(ch -> ch.name())
                    .toList();
            assertThat(entries).containsExactlyInAnyOrder("in_a", "in_b");
        }

        @Test
        @DisplayName("a custom entry parser replaces the default Jackson decode")
        void customParserReplacesDefaultJson() {
            var t = StreamOperativeTopologyBuilder.build(
                    "g10", c -> c.input(Trade.class, "orders", bytes -> new Trade(new String(bytes), 1.0))
                            .into(TradeProcedure.class));
            var entry = t.channels().get(0);
            assertThat(entry.entryParsed()).isTrue();
            Object decoded = entry.entryParser().orElseThrow().apply("BTC".getBytes());
            assertThat(decoded).isEqualTo(new Trade("BTC", 1.0));
        }

        @Test
        @DisplayName("byte[] and String entries stay passthrough (not parsers)")
        void bytesAndStringEntriesStayPassthrough() {
            var t = StreamOperativeTopologyBuilder.build(
                    "g11", c -> c.input(String.class, "in").into(SingleProcedure.class));
            assertThat(t.channels().get(0).entryParsed()).isFalse();
        }

        @Test
        @DisplayName("a stream handle used after the freeze throws naming the operative")
        void handleAfterFreezeThrows() {
            var leaked = new java.util.concurrent.atomic.AtomicReference<com.flezha.sdk.channel.Stream<String>>();
            StreamOperativeTopologyBuilder.build(
                    "g12", c -> leaked.set(c.input(String.class, "in").into(SingleProcedure.class)));
            assertThatThrownBy(() -> leaked.get().into(GraphStage1.class))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("g12");
        }

        @Test
        @DisplayName("a sink with no consumers is the terminal; sealed() changes nothing")
        void sinkTerminalityIsDerivedFromTheWiring() {
            var terminal = StreamOperativeTopologyBuilder.build("g13", c -> c.input(String.class, "in")
                    .into(SingleProcedure.class)
                    .named("egress")
                    .sink("plugin_u"));
            var sealed = StreamOperativeTopologyBuilder.build("g14", c -> c.input(String.class, "in")
                    .into(SingleProcedure.class)
                    .named("egress")
                    .sink("plugin_u")
                    .sealed());
            var hybrid = StreamOperativeTopologyBuilder.build("g15", c -> c.input(String.class, "in")
                    .into(SingleProcedure.class)
                    .named("egress")
                    .sink("plugin_u")
                    .into(GraphStage1.class));

            assertThat(shape(sealed))
                    .as("`.sealed()` must add no topology difference")
                    .isEqualTo(shape(terminal));
            assertThat(sinkShape(terminal)).isEqualTo(List.of(ChannelKind.PLUGIN_BOUND.name(), "0", "plugin_u"));
            assertThat(sinkShape(hybrid)).isEqualTo(List.of(ChannelKind.INTER_PROCEDURE.name(), "1", "plugin_u"));
        }

        @Test
        @DisplayName("sealing a leaf declares no channel")
        void sealingALeafDeclaresNoChannel() {
            // A leaf's output channel is created lazily, so this is the case
            // where "sealing changes no topology" is falsifiable: a sealed()
            // that reached the builder would conjure one.
            var sealed = StreamOperativeTopologyBuilder.build(
                    "g16",
                    c -> c.input(String.class, "in").into(SingleProcedure.class).sealed());
            var bare = StreamOperativeTopologyBuilder.build(
                    "g17", c -> c.input(String.class, "in").into(SingleProcedure.class));

            assertThat(shape(sealed)).isEqualTo(shape(bare));
        }

        /** Everything about the `egress` channel the sink declaration decides. */
        private java.util.List<String> sinkShape(StreamOperativeTopology t) {
            var channel = t.channels().stream()
                    .filter(ch -> ch.name().equals("egress"))
                    .findFirst()
                    .orElseThrow();
            return List.of(
                    channel.kind().name(),
                    String.valueOf(channel.consumerIds().size()),
                    channel.pluginSink().map(PluginSink::name).orElse("<none>"));
        }

        private java.util.List<String> shape(StreamOperativeTopology t) {
            var channels = t.channels().stream()
                    .map(ch -> ch.name() + "|" + ch.kind() + "|" + ch.producer() + "|" + ch.consumerIds())
                    .toList();
            var procs = t.procedures().stream().map(o -> o.name()).toList();
            var all = new java.util.ArrayList<String>(channels);
            all.addAll(procs);
            return all;
        }
    }

    /**
     * The verdict a build was refused with, or {@code null} if the build was accepted. Package
     * private: {@link ReferenceTopologyRowsTest} reads the same verdict off the same builder.
     */
    static StreamOperativeConfigurationException catchConfigException(Runnable build) {
        return org.assertj.core.api.Assertions.catchThrowableOfType(
                StreamOperativeConfigurationException.class, build::run);
    }

    /**
     * The refusals Java reports by NAME only because the core holds them: the
     * six shapes this SDK refused in its own words, or accepted, before the
     * builder started submitting its record. A verdict lists every refusal it
     * found, so a sibling name alongside the asserted one is expected.
     */
    @Nested
    class CoreRefusals {

        @Test
        @DisplayName("a zero correction window, and the two things a window rests on")
        void correctionWindowRefusals() {
            StreamOperativeConfigurationException refused =
                    catchConfigException(() -> StreamOperativeTopologyBuilder.build("r1", c -> {
                        c.correctionWindow(0);
                        c.input(String.class, "in").into(SingleProcedure.class);
                    }));
            assertThat(refused.refusals())
                    .as("`correctionWindow(0)` is the one shape Java accepted before the core held the rules")
                    .contains("CorrectionWindowZero", "CorrectionWithoutTier", "CorrectionWithoutEventTime");
            assertThat(refused).hasMessageContaining("a zero window can correct nothing");
        }

        @Test
        @DisplayName("a zero retry budget is refused by name")
        void retryMaxZeroRefused() {
            assertThatThrownBy(() -> StreamOperativeTopologyBuilder.build(
                            "r2", c -> c.input(String.class, "in").retryMax(0).into(SingleProcedure.class)))
                    .isInstanceOf(StreamOperativeConfigurationException.class)
                    .hasMessageContaining("RetryMaxZero")
                    .hasMessageContaining("channel `in` declared retry_max = 0");
        }

        @Test
        @DisplayName("a second sink on one channel is refused by name; the first still registered")
        void duplicatePluginSinkRefused() {
            assertThatThrownBy(() -> StreamOperativeTopologyBuilder.build("r3", c -> c.input(String.class, "in")
                            .into(SingleProcedure.class)
                            .named("egress")
                            .sink("plugin_a")
                            .sink("plugin_b")))
                    .isInstanceOf(StreamOperativeConfigurationException.class)
                    .hasMessageContaining("DuplicatePluginSink")
                    .hasMessageContaining("channel `egress` was declared with `sink(...)` more than once");
        }

        @Test
        @DisplayName("a plugin-bound channel that is not keyed is refused under the tier")
        void unkeyedChannelUnderTheTierRefused() {
            assertThatThrownBy(() -> StreamOperativeTopologyBuilder.build("r4", c -> {
                        c.effectivelyOnce();
                        c.input(String.class, "in")
                                .into(SingleProcedure.class)
                                .named("egress")
                                .sink("plugin_a");
                    }))
                    .isInstanceOf(StreamOperativeConfigurationException.class)
                    .hasMessageContaining("UnkeyedChannelUnderEffectivelyOnce")
                    .hasMessageContaining("must opt in with .keyed()");
        }
    }

    public static final class GraphStage1 extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "graph_stage1";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class GraphStage2 extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "graph_stage2";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class GraphMerge extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "graph_merge";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }
}
