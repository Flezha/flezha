package com.flezha.sdk.runtime.operative;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.runtime.operative.builder.UnboundedOperativeTopologyBuilder;
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
 * resolution + the two range-validation exception factories.
 */
class BoundedMailboxTopologyTest {

    @BeforeEach
    void reset() {
        UnboundedOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("default policy: entry channels DROP_NEWEST, inter-procedure BLOCK")
    void defaultPoliciesByKind() {
        UnboundedOperativeTopology topology = UnboundedOperativeTopologyBuilder.build(new TwoStageUnboundedOperative());
        // Channel index 0 is the entry channel "input"; index 1 is the
        // inter-procedure channel "mid".
        assertThat(topology.channels().get(0).overflowPolicy()).isEqualTo(OverflowPolicy.DROP_NEWEST);
        assertThat(topology.channels().get(1).overflowPolicy()).isEqualTo(OverflowPolicy.BLOCK);
    }

    @Test
    @DisplayName("onFull(DROP_OLDEST) overrides the per-kind default on an inter-procedure channel")
    void onFullOverridesDefault() {
        UnboundedOperativeTopology topology =
                UnboundedOperativeTopologyBuilder.build(new InterProcedureDropOldestUnboundedOperative());
        assertThat(topology.channels().get(1).overflowPolicy()).isEqualTo(OverflowPolicy.DROP_OLDEST);
    }

    @Test
    @DisplayName("onFull on an entry channel overrides the per-kind default")
    void onFullOnEntryChannelOverrides() {
        UnboundedOperativeTopology topology =
                UnboundedOperativeTopologyBuilder.build(new EntryBlockUnboundedOperative());
        assertThat(topology.channels().get(0).overflowPolicy()).isEqualTo(OverflowPolicy.BLOCK);
    }

    @Test
    @DisplayName("Procedure.mailboxCapacity() override flows into the descriptor when overridden flag is set")
    void procedureOverrideHonored() {
        UnboundedOperativeTopology topology =
                UnboundedOperativeTopologyBuilder.build(new BigMailboxUnboundedOperative());
        int procedureId = topology.procedureNameToId().get("big_mailbox_procedure");
        assertThat(topology.procedures().get(procedureId).mailboxCapacity()).isEqualTo(8192);
        assertThat(topology.procedures().get(procedureId).mailboxCapacityOverridden())
                .isTrue();
    }

    @Test
    @DisplayName("default mailbox capacity is 1024 when no override and no env var present")
    void defaultMailboxCapacity() {
        UnboundedOperativeTopology topology = UnboundedOperativeTopologyBuilder.build(new TwoStageUnboundedOperative());
        for (ProcedureDescriptor desc : topology.procedures()) {
            assertThat(desc.mailboxCapacity()).isEqualTo(1024);
            assertThat(desc.mailboxCapacityOverridden()).isFalse();
        }
    }

    @Test
    @DisplayName("mailboxCapacity = 0 fails registration with named factory")
    void zeroCapacityRejected() {
        assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build(new ZeroCapacityUnboundedOperative()))
                .isInstanceOf(UnboundedOperativeConfigurationException.class)
                .hasMessageContaining("zero_capacity_procedure")
                .hasMessageContaining("mailboxCapacity = 0");
    }

    @Test
    @DisplayName("mailboxCapacity above MAX is rejected with named factory + value")
    void tooLargeRejected() {
        assertThatThrownBy(() -> UnboundedOperativeTopologyBuilder.build(new HugeCapacityUnboundedOperative()))
                .isInstanceOf(UnboundedOperativeConfigurationException.class)
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
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
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
        public Class<Integer> inputType() {
            return Integer.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(Integer input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {}
    }

    public static final class TwoStageUnboundedOperative implements UnboundedOperative {
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

    public static final class InterProcedureDropOldestUnboundedOperative implements UnboundedOperative {
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

    public static final class EntryBlockUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "entry_block";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sA = c.input(String.class, "input").onFull(OverflowPolicy.BLOCK).into(A.class);
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
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
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

    public static final class BigMailboxUnboundedOperative implements UnboundedOperative {
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
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
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

    public static final class ZeroCapacityUnboundedOperative implements UnboundedOperative {
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
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Integer> outputType() {
            return Integer.class;
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

    public static final class HugeCapacityUnboundedOperative implements UnboundedOperative {
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
