package com.flezha.sdk.runtime.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.relay.Relay;
import com.flezha.sdk.relay.RelayConfigurationException;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.runtime.relay.builder.RelayTopologyBuilder;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Topology-level tests for the bounded-mailbox surface added by the
 * Rust→Java parity port: {@code OverflowPolicy} + {@code .onFull(...)} +
 * {@code Shard.mailboxCapacity()} + {@code FLEZHA_MAILBOX_CAPACITY} env
 * resolution + the two range-validation exception factories.
 */
class BoundedMailboxTopologyTest {

    @BeforeEach
    void reset() {
        RelayRegistry.reset();
    }

    @Test
    @DisplayName("default policy: entry channels DROP_NEWEST, inter-shard BLOCK")
    void defaultPoliciesByKind() {
        RelayTopology topology = RelayTopologyBuilder.build(new TwoStageRelay());
        // Channel index 0 is the entry channel "input"; index 1 is the
        // inter-shard channel "mid".
        assertThat(topology.channels().get(0).overflowPolicy()).isEqualTo(OverflowPolicy.DROP_NEWEST);
        assertThat(topology.channels().get(1).overflowPolicy()).isEqualTo(OverflowPolicy.BLOCK);
    }

    @Test
    @DisplayName("onFull(DROP_OLDEST) overrides the per-kind default on an inter-shard channel")
    void onFullOverridesDefault() {
        RelayTopology topology = RelayTopologyBuilder.build(new InterShardDropOldestRelay());
        assertThat(topology.channels().get(1).overflowPolicy()).isEqualTo(OverflowPolicy.DROP_OLDEST);
    }

    @Test
    @DisplayName("onFull on an entry channel overrides the per-kind default")
    void onFullOnEntryChannelOverrides() {
        RelayTopology topology = RelayTopologyBuilder.build(new EntryBlockRelay());
        assertThat(topology.channels().get(0).overflowPolicy()).isEqualTo(OverflowPolicy.BLOCK);
    }

    @Test
    @DisplayName("Shard.mailboxCapacity() override flows into the descriptor when overridden flag is set")
    void shardOverrideHonored() {
        RelayTopology topology = RelayTopologyBuilder.build(new BigMailboxRelay());
        int shardId = topology.shardNameToId().get("big_mailbox_shard");
        assertThat(topology.shards().get(shardId).mailboxCapacity()).isEqualTo(8192);
        assertThat(topology.shards().get(shardId).mailboxCapacityOverridden()).isTrue();
    }

    @Test
    @DisplayName("default mailbox capacity is 1024 when no override and no env var present")
    void defaultMailboxCapacity() {
        RelayTopology topology = RelayTopologyBuilder.build(new TwoStageRelay());
        for (ShardDescriptor desc : topology.shards()) {
            assertThat(desc.mailboxCapacity()).isEqualTo(1024);
            assertThat(desc.mailboxCapacityOverridden()).isFalse();
        }
    }

    @Test
    @DisplayName("mailboxCapacity = 0 fails registration with named factory")
    void zeroCapacityRejected() {
        assertThatThrownBy(() -> RelayTopologyBuilder.build(new ZeroCapacityRelay()))
                .isInstanceOf(RelayConfigurationException.class)
                .hasMessageContaining("zero_capacity_shard")
                .hasMessageContaining("mailboxCapacity = 0");
    }

    @Test
    @DisplayName("mailboxCapacity above MAX is rejected with named factory + value")
    void tooLargeRejected() {
        assertThatThrownBy(() -> RelayTopologyBuilder.build(new HugeCapacityRelay()))
                .isInstanceOf(RelayConfigurationException.class)
                .hasMessageContaining("huge_capacity_shard")
                .hasMessageContaining("9999999");
    }

    // ---- Test shards / relays ----------------------------------------------------

    public static final class A extends StatelessTestShard<String, Object, Integer> {
        @Override
        public String name() {
            return "a";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
        }

        @Override
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ShardCtx ctx) {}
    }

    public static final class B extends StatelessTestShard<Integer, Object, Void> {
        @Override
        public String name() {
            return "b";
        }

        @Override
        public Class<Integer> inputType() {
            return Integer.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public void run(Integer input, StateHandle<Object, NoChange> state, ShardCtx ctx) {}
    }

    public static final class TwoStageRelay implements Relay {
        @Override
        public String factoryId() {
            return "two_stage";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(A.class);
            c.channel(Integer.class, "mid").from(A.class).into(B.class);
        }
    }

    public static final class InterShardDropOldestRelay implements Relay {
        @Override
        public String factoryId() {
            return "intershard_drop_oldest";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(A.class);
            c.channel(Integer.class, "mid")
                    .onFull(OverflowPolicy.DROP_OLDEST)
                    .from(A.class)
                    .into(B.class);
        }
    }

    public static final class EntryBlockRelay implements Relay {
        @Override
        public String factoryId() {
            return "entry_block";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").onFull(OverflowPolicy.BLOCK).into(A.class);
            c.channel(Integer.class, "mid").from(A.class).into(B.class);
        }
    }

    // Custom Shard with an explicit mailboxCapacity() override.
    public static final class BigMailboxShard extends StatelessTestShard<String, Object, Integer> {
        @Override
        public String name() {
            return "big_mailbox_shard";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
        }

        @Override
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public int mailboxCapacity() {
            return 8192;
        }

        @Override
        public boolean mailboxCapacityOverridden() {
            return true;
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ShardCtx ctx) {}
    }

    public static final class BigMailboxRelay implements Relay {
        @Override
        public String factoryId() {
            return "big_mailbox";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(BigMailboxShard.class);
        }
    }

    public static final class ZeroCapacityShard extends StatelessTestShard<String, Object, Integer> {
        @Override
        public String name() {
            return "zero_capacity_shard";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
        }

        @Override
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public int mailboxCapacity() {
            return 0;
        }

        @Override
        public boolean mailboxCapacityOverridden() {
            return true;
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ShardCtx ctx) {}
    }

    public static final class ZeroCapacityRelay implements Relay {
        @Override
        public String factoryId() {
            return "zero_cap";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(ZeroCapacityShard.class);
        }
    }

    public static final class HugeCapacityShard extends StatelessTestShard<String, Object, Integer> {
        @Override
        public String name() {
            return "huge_capacity_shard";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
        }

        @Override
        protected Object initState(Optional<String> shardKey) {
            return new Object();
        }

        @Override
        public int mailboxCapacity() {
            return 9_999_999;
        }

        @Override
        public boolean mailboxCapacityOverridden() {
            return true;
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ShardCtx ctx) {}
    }

    public static final class HugeCapacityRelay implements Relay {
        @Override
        public String factoryId() {
            return "huge_cap";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(HugeCapacityShard.class);
        }
    }
}
