package com.flezha.sdk.runtime.operative;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.operative.builder.UnboundedOperativeTopologyBuilder;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UnboundedOperativeTopologyBuilderTest {

    public record Trade(String symbol, double price) {}

    public record Candle(String symbol, double open, double close) {}

    @Nested
    class WiringValidation {

        @Test
        @DisplayName("single-mode entry channel + consumer compiles and freezes")
        void singleModeOk() {
            UnboundedOperativeTopology topology =
                    UnboundedOperativeTopologyBuilder.build(new SingleModeUnboundedOperative());
            assertThat(topology.factoryId()).isEqualTo("single_mode");
            assertThat(topology.channels()).hasSize(1);
            assertThat(topology.channels().get(0).kind()).isEqualTo(ChannelKind.ENTRY);
            assertThat(topology.procedures()).hasSize(1);
            assertThat(topology.procedures().get(0).entityKeys()).isEmpty();
        }

        @Test
        @DisplayName("static-mode procedure allocates one descriptor with the declared keys")
        void staticModeAllocatesEntities() {
            UnboundedOperativeTopology topology =
                    UnboundedOperativeTopologyBuilder.build(new StaticModeUnboundedOperative());
            assertThat(topology.procedures().get(0).entityKeys()).containsExactly("a", "b", "c", "d");
            assertThat(topology.procedures().get(0).entitiesForOverridden()).isTrue();
        }

        @Test
        @DisplayName("a typed entry channel is accepted and decodes at the edge")
        void typedEntryChannelIsAcceptedAsAParser() {
            // `sdk-graph-authoring`: any entry type is legal; non-byte[]/String
            // entries decode at the edge (default Jackson) and are flagged as
            // real parsers so a failing decode rides the retry→DLQ ladder.
            var topology = UnboundedOperativeTopologyBuilder.build(new BadEntryTypeUnboundedOperative());
            assertThat(topology.channels().get(0).entryParsed()).isTrue();
            assertThat(topology.channels().get(0).entryParser()).isPresent();
        }

        @Test
        @DisplayName("wiring mismatch fails registration with diagnosable error")
        void wiringMismatchRejected() {
            assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build(new TypeMismatchUnboundedOperative()))
                    .isInstanceOf(UnboundedOperativeConfigurationException.class)
                    .hasMessageContaining("trade.parsed");
        }

        @Test
        @DisplayName("static-mode procedure without entitiesForOverridden is rejected")
        void staticWithoutEntitiesForOverriddenRejected() {
            assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build(
                            new MissingEntitiesForOverriddenUnboundedOperative()))
                    .isInstanceOf(UnboundedOperativeConfigurationException.class)
                    .hasMessageContaining("entitiesFor");
        }

        @Test
        @DisplayName("a keyed edge builds a dynamic-mode consumer (Entities are created per key at dispatch)")
        void dynamicModeOnlyBuilds() {
            // The keying is on the WIRING: the procedure declares nothing, and
            // the edge's key function is what classifies it as dynamic.
            UnboundedOperativeTopology topology =
                    UnboundedOperativeTopologyBuilder.build(new DynamicModeOnlyUnboundedOperative());
            assertThat(topology.procedures()).hasSize(1);
            assertThat(topology.procedures().get(0).dynamic()).isTrue();
            assertThat(topology.procedures().get(0).entityKeys()).isEmpty();
            assertThat(topology.channels().get(0).consumers().get(0).key()).isPresent();
        }
    }

    // ---- Test operatives / procedures -----------------------------------------------

    public static final class SingleProcedure extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "single_procedure";
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
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class SingleModeUnboundedOperative implements UnboundedOperative {
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
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
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

    public static final class StaticModeUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "static_mode";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(StaticProcedure.class);
        }
    }

    public static final class BadEntryTypeUnboundedOperative implements UnboundedOperative {
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
        public Class<Trade> inputType() {
            return Trade.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
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
        public Class<Candle> inputType() {
            return Candle.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
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
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Trade> outputType() {
            return Trade.class;
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {}
    }

    public static final class TypeMismatchUnboundedOperative implements UnboundedOperative {
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
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
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

    public static final class MissingEntitiesForOverriddenUnboundedOperative implements UnboundedOperative {
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
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class DynamicModeOnlyUnboundedOperative implements UnboundedOperative {
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
            var byHandles = UnboundedOperativeTopologyBuilder.build("g1", c -> {
                var validated = c.input(String.class, "in").into(SingleProcedure.class);
                validated.into(GraphStage1.class);
                validated.into(GraphStage2.class);
            });
            var byBranch = UnboundedOperativeTopologyBuilder.build("g2", c -> c.input(String.class, "in")
                    .into(SingleProcedure.class)
                    .branch(b -> b.into(GraphStage1.class), b -> b.into(GraphStage2.class)));
            assertThat(shape(byBranch)).isEqualTo(shape(byHandles));
        }

        @Test
        @DisplayName("branch() with fewer than two arms is refused")
        void branchWithOneArmRefused() {
            assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build("g3", c -> c.input(String.class, "in")
                            .into(SingleProcedure.class)
                            .branch(b -> b.into(GraphStage1.class))))
                    .isInstanceOf(UnboundedOperativeConfigurationException.class)
                    .hasMessageContaining("at least two arms");
        }

        @Test
        @DisplayName("named nodes reuse one class at two positions; unnamed mentions unify into a fan-in")
        void namedNodesAndFanIn() {
            var t = UnboundedOperativeTopologyBuilder.build("g4", c -> {
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
            assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build("g5", c -> {
                        var validated = c.input(String.class, "in").into(SingleProcedure.class);
                        validated.into(GraphStage2.class, "graph_stage1");
                        validated.into(GraphStage1.class);
                    }))
                    .isInstanceOf(UnboundedOperativeConfigurationException.class)
                    .hasMessageContaining("graph_stage1");
        }

        @Test
        @DisplayName("channel names derive from the producer identity; .named overrides; collisions are refused")
        void derivedNamesAndCollision() {
            var t = UnboundedOperativeTopologyBuilder.build("g6", c -> c.input(String.class, "in")
                    .into(SingleProcedure.class)
                    .into(GraphStage1.class)
                    .named("orders.validated")
                    .into(GraphStage2.class));
            assertThat(t.nameToId()).containsKeys("single_procedure", "orders.validated");

            assertThatThrownBy(() ->
                            UnboundedOperativeTopologyBuilder.build("g7", c -> c.input(String.class, "single_procedure")
                                    .into(SingleProcedure.class)
                                    .into(GraphStage1.class)))
                    .isInstanceOf(UnboundedOperativeConfigurationException.class)
                    .hasMessageContaining("single_procedure");
        }

        @Test
        @DisplayName("a cycle is refused")
        void cycleRefused() {
            assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build("g8", c -> c.input(String.class, "in")
                            .into(GraphStage1.class)
                            .into(GraphStage2.class)
                            .into(GraphStage1.class)))
                    .isInstanceOf(UnboundedOperativeConfigurationException.class)
                    .hasMessageContaining("cycle");
        }

        @Test
        @DisplayName("multiple entry channels build")
        void multipleEntryChannelsBuild() {
            // One operative may declare several entry channels — one per
            // external source (e.g. a market pipeline reading a trade feed and
            // an order-book feed). The controller routes each frame to its
            // entry by metadata["channel"]. Both entries and subgraphs resolve.
            var t = UnboundedOperativeTopologyBuilder.build("g9", c -> {
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
            var t = UnboundedOperativeTopologyBuilder.build(
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
            var t = UnboundedOperativeTopologyBuilder.build(
                    "g11", c -> c.input(String.class, "in").into(SingleProcedure.class));
            assertThat(t.channels().get(0).entryParsed()).isFalse();
        }

        @Test
        @DisplayName("a stream handle used after the freeze throws naming the operative")
        void handleAfterFreezeThrows() {
            var leaked = new java.util.concurrent.atomic.AtomicReference<com.flezha.sdk.channel.Stream<String>>();
            UnboundedOperativeTopologyBuilder.build(
                    "g12", c -> leaked.set(c.input(String.class, "in").into(SingleProcedure.class)));
            assertThatThrownBy(() -> leaked.get().into(GraphStage1.class))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("g12");
        }

        private java.util.List<String> shape(UnboundedOperativeTopology t) {
            var channels = t.channels().stream()
                    .map(ch -> ch.name() + "|" + ch.kind() + "|" + ch.producer() + "|" + ch.consumerIds())
                    .toList();
            var procs = t.procedures().stream().map(o -> o.name()).toList();
            var all = new java.util.ArrayList<String>(channels);
            all.addAll(procs);
            return all;
        }
    }

    public static final class GraphStage1 extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "graph_stage1";
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
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
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
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }
}
