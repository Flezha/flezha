package com.flezha.sdk.runtime.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeCtx;
import com.flezha.sdk.bounded.BoundedOperativeException;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry;
import com.flezha.sdk.runtime.bounded.BoundedSpec;
import com.flezha.sdk.runtime.operative.StatelessTestProcedure;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The {@code HostClassDecl} JSON builder — the successor of the retired
 * {@code AddOperativeResponse}-metadata declarations: what the poll core is
 * told about a registered class, with ids equal to declaration order.
 */
class HostClassDeclarationTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @AfterEach
    void reset() {
        UnboundedOperativeClassRegistry.reset();
        BoundedOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("an unbounded declaration emits channels and procedures in id order with tier facts")
    void unboundedDeclaration() throws Exception {
        UnboundedOperativeClassRegistry.registerClass(TierOperative.class);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology("decl_tier_op");
        JsonNode decl = JSON.readTree(HostClassDeclaration.forUnbounded(topology));

        assertThat(decl.get("factory_id").asText()).isEqualTo("decl_tier_op");
        assertThat(decl.get("sdk_lang").asInt()).isEqualTo(2);
        assertThat(decl.get("change_format_id").asInt()).isEqualTo(4);
        assertThat(decl.get("effectively_once").asBoolean()).isTrue();
        assertThat(decl.get("ack_granularity").asText()).isEqualTo("checkpoint");
        assertThat(decl.has("correction_window_ms")).isFalse();
        assertThat(decl.has("persistent"))
                .as("every procedure declares NoChange")
                .isFalse();
        assertThat(decl.has("bounded")).isFalse();

        JsonNode channels = decl.get("channels");
        assertThat(channels).hasSize(2);
        assertThat(channels.get(0).get("name").asText()).isEqualTo("in");
        assertThat(channels.get(0).get("kind").asText()).isEqualTo("entry");
        assertThat(channels.get(0).get("consumers").get(0).asInt()).isEqualTo(0);
        assertThat(channels.get(0).has("producer")).isFalse();
        assertThat(channels.get(1).get("kind").asText()).isEqualTo("plugin");
        assertThat(channels.get(1).get("producer").asInt()).isEqualTo(0);
        assertThat(channels.get(1).get("keyed").asBoolean()).isTrue();
        assertThat(channels.get(0).get("overflow").asText()).isEqualTo("drop_newest");

        JsonNode procedures = decl.get("procedures");
        assertThat(procedures).hasSize(1);
        assertThat(procedures.get(0).get("name").asText()).isEqualTo("decl_proc");
        assertThat(procedures.get(0).has("entity_mode"))
                .as("single mode is the default")
                .isFalse();
        assertThat(procedures.get(0).get("clock").asText()).isEqualTo("wall");
        assertThat(procedures.get(0).get("mailbox_capacity").asInt()).isPositive();
    }

    @Test
    @DisplayName("a run-sugar bounded declaration is empty channels + procedures with the bounded block")
    void runSugarBoundedDeclaration() throws Exception {
        BoundedOperativeClassRegistry.registerClass(DeclBounded.class);
        BoundedSpec spec = BoundedOperativeClassRegistry.getSpec("decl_bounded");
        JsonNode decl = JSON.readTree(HostClassDeclaration.forBounded(spec));

        assertThat(decl.get("factory_id").asText()).isEqualTo("decl_bounded");
        assertThat(decl.get("channels")).isEmpty();
        assertThat(decl.get("procedures")).isEmpty();
        assertThat(decl.get("persistent").asBoolean()).isTrue();

        JsonNode bounded = decl.get("bounded");
        assertThat(bounded.get("loop_mode").asText()).isEqualTo("once");
        assertThat(bounded.get("rollback_mode").asText()).isEqualTo("all_or_nothing");
        assertThat(bounded.get("retry_attempts").asInt()).isEqualTo(3);
        assertThat(bounded.get("retry_backoff_ms").asLong()).isEqualTo(500);
        assertThat(bounded.get("max_duration_ms").asLong()).isEqualTo(30_000);
        assertThat(bounded.has("wake_after_ms")).isFalse();
    }

    @Test
    @DisplayName("forFactory resolves both registries and refuses an unknown id")
    void forFactoryResolvesRegistries() {
        UnboundedOperativeClassRegistry.registerClass(TierOperative.class);
        BoundedOperativeClassRegistry.registerClass(DeclBounded.class);
        assertThat(HostClassDeclaration.forFactory("decl_tier_op")).isNotEmpty();
        assertThat(HostClassDeclaration.forFactory("decl_bounded")).isNotEmpty();
        assertThatThrownBy(() -> HostClassDeclaration.forFactory("missing"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing");
    }

    public static final class TierOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "decl_tier_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.checkpointAck();
            c.input(String.class, "in")
                    .into(DeclProc.class)
                    .keyed()
                    .output("sink", s -> s.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class DeclProc extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "decl_proc";
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
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx)
                throws com.flezha.sdk.procedure.ProcedureException {
            ctx.emitKeyed("k", input);
        }
    }

    public static final class DeclBounded implements BoundedOperative {
        @Override
        public String factoryId() {
            return "decl_bounded";
        }

        @Override
        public Optional<Duration> maxDuration() {
            return Optional.of(Duration.ofSeconds(30));
        }

        @Override
        public void run(BoundedOperativeCtx ctx, String args) throws BoundedOperativeException {
            ctx.complete(args);
        }
    }
}
