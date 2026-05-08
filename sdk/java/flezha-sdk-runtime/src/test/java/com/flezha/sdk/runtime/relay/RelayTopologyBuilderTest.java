package com.flezha.sdk.runtime.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.relay.Relay;
import com.flezha.sdk.relay.RelayConfigurationException;
import com.flezha.sdk.runtime.relay.builder.RelayTopologyBuilder;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RelayTopologyBuilderTest {

    public record Trade(String symbol, double price) {}

    public record Candle(String symbol, double open, double close) {}

    @Nested
    class WiringValidation {

        @Test
        @DisplayName("single-mode entry channel + consumer compiles and freezes")
        void singleModeOk() {
            RelayTopology topology = RelayTopologyBuilder.build(new SingleModeRelay());
            assertThat(topology.factoryId()).isEqualTo("single_mode");
            assertThat(topology.channels()).hasSize(1);
            assertThat(topology.channels().get(0).kind()).isEqualTo(ChannelKind.ENTRY);
            assertThat(topology.shards()).hasSize(1);
            assertThat(topology.shards().get(0).partitionKeys()).isEmpty();
        }

        @Test
        @DisplayName("static-mode shard allocates one descriptor with the declared keys")
        void staticModeAllocatesPartitions() {
            RelayTopology topology = RelayTopologyBuilder.build(new StaticModeRelay());
            assertThat(topology.shards().get(0).partitionKeys()).containsExactly("a", "b", "c", "d");
            assertThat(topology.shards().get(0).partitionForOverridden()).isTrue();
        }

        @Test
        @DisplayName("entry channel of disallowed type fails registration")
        void entryChannelOfDisallowedTypeRejected() {
            assertThatThrownBy(() -> RelayTopologyBuilder.build(new BadEntryTypeRelay()))
                    .isInstanceOf(RelayConfigurationException.class)
                    .hasMessageContaining("byte[] or String");
        }

        @Test
        @DisplayName("wiring mismatch fails registration with diagnosable error")
        void wiringMismatchRejected() {
            assertThatThrownBy(() -> RelayTopologyBuilder.build(new TypeMismatchRelay()))
                    .isInstanceOf(RelayConfigurationException.class)
                    .hasMessageContaining("trade.parsed");
        }

        @Test
        @DisplayName("unwired channel fails registration")
        void unwiredChannelRejected() {
            assertThatThrownBy(() -> RelayTopologyBuilder.build(new UnwiredChannelRelay()))
                    .isInstanceOf(RelayConfigurationException.class)
                    .hasMessageContaining("ohlc.candles");
        }

        @Test
        @DisplayName("static-mode shard without partitionForOverridden is rejected")
        void staticWithoutPartitionForOverriddenRejected() {
            assertThatThrownBy(() -> RelayTopologyBuilder.build(new MissingPartitionForOverriddenRelay()))
                    .isInstanceOf(RelayConfigurationException.class)
                    .hasMessageContaining("partitionFor");
        }

        @Test
        @DisplayName("dynamic-mode-only declaration is rejected in v1")
        void dynamicModeOnlyRejected() {
            assertThatThrownBy(() -> RelayTopologyBuilder.build(new DynamicModeOnlyRelay()))
                    .isInstanceOf(RelayConfigurationException.class)
                    .hasMessageContaining("dynamic mode");
        }
    }

    // ---- Test relays / shards -----------------------------------------------

    public static final class SingleShard extends StatelessTestShard<String, Object, String> {
        @Override
        public String name() {
            return "single_shard";
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
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ShardCtx ctx) {}
    }

    public static final class SingleModeRelay implements Relay {
        @Override
        public String factoryId() {
            return "single_mode";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(SingleShard.class);
        }
    }

    public static final class StaticShard extends StatelessTestShard<String, Object, String> {
        @Override
        public String name() {
            return "static_shard";
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
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public List<String> shardKeys() {
            return List.of("a", "b", "c", "d");
        }

        @Override
        public boolean partitionForOverridden() {
            return true;
        }

        @Override
        public List<String> partitionFor(String input) {
            return List.of(input.substring(0, 1));
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ShardCtx ctx) {}
    }

    public static final class StaticModeRelay implements Relay {
        @Override
        public String factoryId() {
            return "static_mode";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(StaticShard.class);
        }
    }

    public static final class BadEntryTypeRelay implements Relay {
        @Override
        public String factoryId() {
            return "bad_entry";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(Trade.class, "trade.raw").into(TradeShard.class);
        }
    }

    public static final class TradeShard extends StatelessTestShard<Trade, Object, String> {
        @Override
        public String name() {
            return "trade_shard";
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
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public void run(Trade input, StateHandle<Object, NoChange> state, ShardCtx ctx) {}
    }

    public static final class CandleShard extends StatelessTestShard<Candle, Object, String> {
        @Override
        public String name() {
            return "candle_shard";
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
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public void run(Candle input, StateHandle<Object, NoChange> state, ShardCtx ctx) {}
    }

    public static final class TradeProducerShard extends StatelessTestShard<String, Object, Trade> {
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
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ShardCtx ctx) throws ShardException {}
    }

    public static final class TypeMismatchRelay implements Relay {
        @Override
        public String factoryId() {
            return "type_mismatch";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(TradeProducerShard.class);
            ChannelDeclMismatch.declareTradeIntoCandle(c);
        }
    }

    /** Helper to keep the reflection-mismatch construction off the relay. */
    private static final class ChannelDeclMismatch {
        @SuppressWarnings({"unchecked", "rawtypes"})
        static void declareTradeIntoCandle(ChannelBuilder c) {
            c.channel(Trade.class, "trade.parsed")
                    .from((Class) TradeProducerShard.class)
                    .into((Class) CandleShard.class);
        }
    }

    public static final class UnwiredChannelRelay implements Relay {
        @Override
        public String factoryId() {
            return "unwired_channel";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(SingleShard.class);
            c.channel(Candle.class, "ohlc.candles");
        }
    }

    public static final class MissingPartitionForOverriddenShard extends StatelessTestShard<String, Object, String> {
        @Override
        public String name() {
            return "missing_pfo_shard";
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
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public List<String> shardKeys() {
            return List.of("a", "b");
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ShardCtx ctx) {}
    }

    public static final class MissingPartitionForOverriddenRelay implements Relay {
        @Override
        public String factoryId() {
            return "missing_pfo_relay";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(MissingPartitionForOverriddenShard.class);
        }
    }

    public static final class DynamicModeOnlyShard extends StatelessTestShard<String, Object, String> {
        @Override
        public String name() {
            return "dynamic_only_shard";
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
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public boolean shardKeyOverridden() {
            return true;
        }

        @Override
        public Optional<String> shardKey(String input) {
            return Optional.of(input);
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ShardCtx ctx) {}
    }

    public static final class DynamicModeOnlyRelay implements Relay {
        @Override
        public String factoryId() {
            return "dynamic_only_relay";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(DynamicModeOnlyShard.class);
        }
    }
}
