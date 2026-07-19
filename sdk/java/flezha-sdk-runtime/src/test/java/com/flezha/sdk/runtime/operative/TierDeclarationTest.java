package com.flezha.sdk.runtime.operative;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.proto.AddOperativeResponse;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The effectively-once tier is <em>declaration-gated</em>: the controller runs
 * it for a class only because the class said so in
 * {@code AddOperativeResponse.metadata}. These cases pin that declaration and
 * the one refusal that protects it.
 */
class TierDeclarationTest {

    @BeforeEach
    void reset() {
        UnboundedOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("a tier operative declares effectively_once to the controller")
    void tierOperativeDeclaresEffectivelyOnce() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(TierOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("tier-ok");

        assertThat(topology.effectivelyOnce()).isTrue();

        AddOperativeResponse response = AddOperativeResponse.parseFrom(Declarations.forUnbounded(topology));
        assertThat(response.getMetadataMap()).containsEntry("effectively_once", "true");
    }

    @Test
    @DisplayName("a non-tier operative still declares nothing (zero bytes)")
    void nonTierOperativeDeclaresNothing() {
        UnboundedOperativeClassRegistry.registerClass(PlainOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("tier-plain");

        assertThat(topology.effectivelyOnce()).isFalse();
        assertThat(Declarations.forUnbounded(topology)).isEmpty();
    }

    @Test
    @DisplayName("a tier operative with a non-keyed plugin channel is refused")
    void tierWithNonKeyedPluginChannelIsRefused() {
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(TierUnkeyedOperative.class))
                .isInstanceOf(UnboundedOperativeConfigurationException.class)
                .hasMessageContaining("effectivelyOnce")
                .hasMessageContaining("keyed");
    }

    // --- fixtures -----------------------------------------------------------

    public static final class TierOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "tier-ok";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.input(String.class, "in").into(Op.class);
            c.channel(String.class, "out").from(Op.class).keyed().toPlugin("sink");
        }
    }

    public static final class PlainOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "tier-plain";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(Op.class);
            c.channel(String.class, "out").from(Op.class).toPlugin("sink");
        }
    }

    /** Tier on, but the plugin channel is not keyed — a replayed emit would duplicate. */
    public static final class TierUnkeyedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "tier-unkeyed";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.input(String.class, "in").into(Op.class);
            c.channel(String.class, "out").from(Op.class).toPlugin("sink");
        }
    }

    public record Unit() {}

    public static final class Op implements Operation<String, Unit, String, NoChange, WallClock, Void> {
        @Override
        public String name() {
            return "tier_op";
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
        public Unit initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new Unit();
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {}
    }
}
