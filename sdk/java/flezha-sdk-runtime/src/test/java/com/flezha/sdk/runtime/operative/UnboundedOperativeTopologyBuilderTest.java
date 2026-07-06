package com.flezha.sdk.runtime.operative;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
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
            assertThat(topology.operations()).hasSize(1);
            assertThat(topology.operations().get(0).segmentKeys()).isEmpty();
        }

        @Test
        @DisplayName("static-mode operation allocates one descriptor with the declared keys")
        void staticModeAllocatesSegments() {
            UnboundedOperativeTopology topology =
                    UnboundedOperativeTopologyBuilder.build(new StaticModeUnboundedOperative());
            assertThat(topology.operations().get(0).segmentKeys()).containsExactly("a", "b", "c", "d");
            assertThat(topology.operations().get(0).segmentForOverridden()).isTrue();
        }

        @Test
        @DisplayName("entry channel of disallowed type fails registration")
        void entryChannelOfDisallowedTypeRejected() {
            assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build(new BadEntryTypeUnboundedOperative()))
                    .isInstanceOf(UnboundedOperativeConfigurationException.class)
                    .hasMessageContaining("byte[] or String");
        }

        @Test
        @DisplayName("wiring mismatch fails registration with diagnosable error")
        void wiringMismatchRejected() {
            assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build(new TypeMismatchUnboundedOperative()))
                    .isInstanceOf(UnboundedOperativeConfigurationException.class)
                    .hasMessageContaining("trade.parsed");
        }

        @Test
        @DisplayName("unwired channel fails registration")
        void unwiredChannelRejected() {
            assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build(new UnwiredChannelUnboundedOperative()))
                    .isInstanceOf(UnboundedOperativeConfigurationException.class)
                    .hasMessageContaining("ohlc.candles");
        }

        @Test
        @DisplayName("static-mode operation without segmentForOverridden is rejected")
        void staticWithoutSegmentForOverriddenRejected() {
            assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build(
                            new MissingSegmentForOverriddenUnboundedOperative()))
                    .isInstanceOf(UnboundedOperativeConfigurationException.class)
                    .hasMessageContaining("segmentFor");
        }

        @Test
        @DisplayName("dynamic-mode-only declaration is rejected in v1")
        void dynamicModeOnlyRejected() {
            assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build(new DynamicModeOnlyUnboundedOperative()))
                    .isInstanceOf(UnboundedOperativeConfigurationException.class)
                    .hasMessageContaining("dynamic mode");
        }
    }

    // ---- Test operatives / operations -----------------------------------------------

    public static final class SingleOperation extends StatelessTestOperation<String, Object, String> {
        @Override
        public String name() {
            return "single_operation";
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
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) {}
    }

    public static final class SingleModeUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "single_mode";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(SingleOperation.class);
        }
    }

    public static final class StaticOperation extends StatelessTestOperation<String, Object, String> {
        @Override
        public String name() {
            return "static_operation";
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
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public List<String> operationKeys() {
            return List.of("a", "b", "c", "d");
        }

        @Override
        public boolean segmentForOverridden() {
            return true;
        }

        @Override
        public List<String> segmentFor(String input) {
            return List.of(input.substring(0, 1));
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) {}
    }

    public static final class StaticModeUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "static_mode";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(StaticOperation.class);
        }
    }

    public static final class BadEntryTypeUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "bad_entry";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(Trade.class, "trade.raw").into(TradeOperation.class);
        }
    }

    public static final class TradeOperation extends StatelessTestOperation<Trade, Object, String> {
        @Override
        public String name() {
            return "trade_operation";
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
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(Trade input, StateHandle<Object, NoChange> state, OperationCtx ctx) {}
    }

    public static final class CandleOperation extends StatelessTestOperation<Candle, Object, String> {
        @Override
        public String name() {
            return "candle_operation";
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
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(Candle input, StateHandle<Object, NoChange> state, OperationCtx ctx) {}
    }

    public static final class TradeProducerOperation extends StatelessTestOperation<String, Object, Trade> {
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
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx)
                throws OperationException {}
    }

    public static final class TypeMismatchUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "type_mismatch";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(TradeProducerOperation.class);
            ChannelDeclMismatch.declareTradeIntoCandle(c);
        }
    }

    /** Helper to keep the reflection-mismatch construction off the operative. */
    private static final class ChannelDeclMismatch {
        @SuppressWarnings({"unchecked", "rawtypes"})
        static void declareTradeIntoCandle(ChannelBuilder c) {
            c.channel(Trade.class, "trade.parsed")
                    .from((Class) TradeProducerOperation.class)
                    .into((Class) CandleOperation.class);
        }
    }

    public static final class UnwiredChannelUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "unwired_channel";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(SingleOperation.class);
            c.channel(Candle.class, "ohlc.candles");
        }
    }

    public static final class MissingSegmentForOverriddenOperation
            extends StatelessTestOperation<String, Object, String> {
        @Override
        public String name() {
            return "missing_pfo_operation";
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
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public List<String> operationKeys() {
            return List.of("a", "b");
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) {}
    }

    public static final class MissingSegmentForOverriddenUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "missing_pfo_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(MissingSegmentForOverriddenOperation.class);
        }
    }

    public static final class DynamicModeOnlyOperation extends StatelessTestOperation<String, Object, String> {
        @Override
        public String name() {
            return "dynamic_only_operation";
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
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public boolean operationKeyOverridden() {
            return true;
        }

        @Override
        public Optional<String> operationKey(String input) {
            return Optional.of(input);
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) {}
    }

    public static final class DynamicModeOnlyUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "dynamic_only_operative";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(DynamicModeOnlyOperation.class);
        }
    }
}
