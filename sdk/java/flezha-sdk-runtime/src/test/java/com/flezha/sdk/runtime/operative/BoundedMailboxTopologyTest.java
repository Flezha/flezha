package com.flezha.sdk.runtime.operative;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Topology-level tests for the bounded-mailbox surface added by the
 * Rust→Java parity port: {@code OverflowPolicy} + {@code .onFull(...)} +
 * {@code Procedure.mailboxCapacity()} + {@code FLEZHA_MAILBOX_CAPACITY} env
 * resolution + the two range refusals the core names off the resolved value.
 */
class BoundedMailboxTopologyTest {

    @BeforeEach
    void reset() {
        StreamOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("default policy: DROP_NOTHING on entry and inter-procedure channels alike")
    void defaultPoliciesByKind() {
        StreamOperativeTopology topology = StreamOperativeTopologyBuilder.build(new TwoStageStreamOperative());
        // Channel index 0 is the entry channel "input"; index 1 is the
        // inter-procedure channel "mid".
        assertThat(topology.channels().get(0).overflowPolicy()).isEqualTo(OverflowPolicy.DROP_NOTHING);
        assertThat(topology.channels().get(1).overflowPolicy()).isEqualTo(OverflowPolicy.DROP_NOTHING);
    }

    @Test
    @DisplayName("onFull(DROP_OLDEST) overrides the per-kind default on an inter-procedure channel")
    void onFullOverridesDefault() {
        StreamOperativeTopology topology =
                StreamOperativeTopologyBuilder.build(new InterProcedureDropOldestStreamOperative());
        assertThat(topology.channels().get(1).overflowPolicy()).isEqualTo(OverflowPolicy.DROP_OLDEST);
    }

    @Test
    @DisplayName("onFull(DROP_NEWEST) on an entry channel overrides the default: shedding is declared")
    void onFullOnEntryChannelOverrides() {
        StreamOperativeTopology topology = StreamOperativeTopologyBuilder.build(new EntryDropNewestStreamOperative());
        assertThat(topology.channels().get(0).overflowPolicy()).isEqualTo(OverflowPolicy.DROP_NEWEST);
    }

    @Test
    @DisplayName("Procedure.mailboxCapacity() override flows into the descriptor when overridden flag is set")
    void procedureOverrideHonored() {
        StreamOperativeTopology topology = StreamOperativeTopologyBuilder.build(new BigMailboxStreamOperative());
        int procedureId = topology.procedureNameToId().get("big_mailbox_procedure");
        assertThat(topology.procedures().get(procedureId).mailboxCapacity()).isEqualTo(8192);
        assertThat(topology.procedures().get(procedureId).mailboxCapacityOverridden())
                .isTrue();
    }

    @Test
    @DisplayName("default mailbox capacity is 1024 when no override and no env var present")
    void defaultMailboxCapacity() {
        StreamOperativeTopology topology = StreamOperativeTopologyBuilder.build(new TwoStageStreamOperative());
        for (ProcedureDescriptor desc : topology.procedures()) {
            assertThat(desc.mailboxCapacity()).isEqualTo(1024);
            assertThat(desc.mailboxCapacityOverridden()).isFalse();
        }
    }

    @Test
    @DisplayName("mailboxCapacity = 0 fails registration, named by the core")
    void zeroCapacityRejected() {
        assertThatThrownBy(() -> StreamOperativeTopologyBuilder.build(new ZeroCapacityStreamOperative()))
                .isInstanceOf(StreamOperativeConfigurationException.class)
                .hasMessageContaining("MailboxCapacityZero")
                .hasMessageContaining("procedure `zero_capacity_procedure` declared mailbox_capacity = 0");
    }

    @Test
    @DisplayName("mailboxCapacity above MAX is rejected, named by the core with the value")
    void tooLargeRejected() {
        assertThatThrownBy(() -> StreamOperativeTopologyBuilder.build(new HugeCapacityStreamOperative()))
                .isInstanceOf(StreamOperativeConfigurationException.class)
                .hasMessageContaining("MailboxCapacityTooLarge")
                .hasMessageContaining("huge_capacity_procedure")
                .hasMessageContaining("9999999");
    }

    // ---- Test procedures / operatives ----------------------------------------------------

    public static final class A extends StatelessTestProcedure<String, Object, Integer> {
        @Override
        public String name() {
            return "a";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class B extends StatelessTestProcedure<Integer, Object, Void> {
        @Override
        public String name() {
            return "b";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(Integer input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class TwoStageStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "two_stage";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sA = c.input(String.class, "input").into(A.class);
            sA.named("mid").into(B.class);
        }
    }

    public static final class InterProcedureDropOldestStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "interprocedure_drop_oldest";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sA = c.input(String.class, "input").into(A.class);
            sA.named("mid").onFull(OverflowPolicy.DROP_OLDEST).into(B.class);
        }
    }

    /**
     * Promising effectively-once and asking to shed is a contradiction, not a
     * trade-off: a dropped input's offset is still advanced past by the tier's
     * watermark, so the loss is permanent under a contract that says nothing
     * is lost. Refused at declare time, where the author can still see it.
     */
    @Test
    @DisplayName("a dropping policy under effectivelyOnce() is refused, naming the channel")
    void droppingPolicyUnderTheTierIsRefused() {
        assertThatThrownBy(() -> StreamOperativeTopologyBuilder.build(new TierShedsStreamOperative()))
                .isInstanceOf(StreamOperativeConfigurationException.class)
                .hasMessageContaining("DroppingPolicyUnderEffectivelyOnce")
                .hasMessageContaining("channel `input` declares .on_full(drop_newest)")
                .hasMessageContaining("effectively_once()");
    }

    @Test
    @DisplayName("shedding stays available to a class that declares neither the tier nor a correction")
    void droppingPolicyOffTheTierStillBuilds() {
        StreamOperativeTopology topology = StreamOperativeTopologyBuilder.build(new EntryDropNewestStreamOperative());
        assertThat(topology.channels().get(0).overflowPolicy()).isEqualTo(OverflowPolicy.DROP_NEWEST);
    }

    public static final class TierShedsStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "tier_sheds";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.input(String.class, "input")
                    .onFull(OverflowPolicy.DROP_NEWEST)
                    .into(A.class)
                    .named("out")
                    .keyed()
                    .sink("kafka");
        }
    }

    public static final class EntryDropNewestStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "entry_drop_newest";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sA = c.input(String.class, "input")
                    .onFull(OverflowPolicy.DROP_NEWEST)
                    .into(A.class);
            sA.named("mid").into(B.class);
        }
    }

    // Custom Procedure with an explicit mailboxCapacity() override.
    public static final class BigMailboxProcedure extends StatelessTestProcedure<String, Object, Integer> {
        @Override
        public String name() {
            return "big_mailbox_procedure";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
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
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class BigMailboxStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "big_mailbox";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(BigMailboxProcedure.class);
        }
    }

    public static final class ZeroCapacityProcedure extends StatelessTestProcedure<String, Object, Integer> {
        @Override
        public String name() {
            return "zero_capacity_procedure";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
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
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class ZeroCapacityStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "zero_cap";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(ZeroCapacityProcedure.class);
        }
    }

    public static final class HugeCapacityProcedure extends StatelessTestProcedure<String, Object, Integer> {
        @Override
        public String name() {
            return "huge_capacity_procedure";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
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
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class HugeCapacityStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "huge_cap";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(HugeCapacityProcedure.class);
        }
    }
}
