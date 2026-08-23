package com.flezha.sdk.runtime.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.runtime.operative.StatelessTestProcedure;
import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.runtime.workflow.WorkflowOperativeClassRegistry;
import com.flezha.sdk.runtime.workflow.WorkflowSpec;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.workflow.WorkflowOperative;
import com.flezha.sdk.workflow.WorkflowOperativeCtx;
import com.flezha.sdk.workflow.WorkflowOperativeException;
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
        StreamOperativeClassRegistry.reset();
        WorkflowOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("a stream declaration emits channels and procedures in id order with tier facts")
    void streamDeclaration() throws Exception {
        StreamOperativeClassRegistry.registerClass(TierOperative.class);
        StreamOperativeTopology topology = StreamOperativeClassRegistry.getTopology("decl_tier_op");
        JsonNode decl = JSON.readTree(HostClassDeclaration.forStream(topology));

        assertThat(decl.get("factory_id").asText()).isEqualTo("decl_tier_op");
        assertThat(decl.get("sdk_lang").asInt()).isEqualTo(2);
        assertThat(decl.get("change_format_id").asInt()).isEqualTo(4);
        assertThat(decl.get("effectively_once").asBoolean()).isTrue();
        assertThat(decl.get("ack_granularity").asText()).isEqualTo("checkpoint");
        assertThat(decl.has("checkpoint_every"))
                .as("a bare checkpointAck() declares no interval options")
                .isFalse();
        assertThat(decl.has("checkpoint_outputs")).isFalse();
        assertThat(decl.has("checkpoint_max_delay_ms")).isFalse();
        assertThat(decl.has("correction_window_ms")).isFalse();
        assertThat(decl.has("persistent"))
                .as("every procedure declares NoChange")
                .isFalse();
        assertThat(decl.has("workflow")).isFalse();

        JsonNode channels = decl.get("channels");
        assertThat(channels).hasSize(2);
        assertThat(channels.get(0).get("name").asText()).isEqualTo("in");
        assertThat(channels.get(0).get("kind").asText()).isEqualTo("entry");
        assertThat(channels.get(0).get("consumers").get(0).asInt()).isEqualTo(0);
        assertThat(channels.get(0).has("producer")).isFalse();
        assertThat(channels.get(1).get("kind").asText()).isEqualTo("plugin");
        assertThat(channels.get(1).get("producer").asInt()).isEqualTo(0);
        assertThat(channels.get(1).get("keyed").asBoolean()).isTrue();
        assertThat(channels.get(0).get("overflow").asText())
                .as("nothing drops by default: an entry channel blocks")
                .isEqualTo("block");

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
    @DisplayName("a run-sugar workflow declaration is empty channels + procedures with the workflow block")
    void runSugarWorkflowDeclaration() throws Exception {
        WorkflowOperativeClassRegistry.registerClass(DeclWorkflow.class);
        WorkflowSpec spec = WorkflowOperativeClassRegistry.getSpec("decl_workflow");
        JsonNode decl = JSON.readTree(HostClassDeclaration.forWorkflow(spec));

        assertThat(decl.get("factory_id").asText()).isEqualTo("decl_workflow");
        assertThat(decl.get("channels")).isEmpty();
        assertThat(decl.get("procedures")).isEmpty();
        assertThat(decl.get("persistent").asBoolean()).isTrue();

        JsonNode workflow = decl.get("workflow");
        assertThat(workflow.get("loop_mode").asText()).isEqualTo("once");
        assertThat(workflow.get("rollback_mode").asText()).isEqualTo("all_or_nothing");
        assertThat(workflow.get("retry_attempts").asInt()).isEqualTo(3);
        assertThat(workflow.get("retry_backoff_ms").asLong()).isEqualTo(500);
        assertThat(workflow.get("barrier_retry_attempts").asInt()).isEqualTo(3);
        assertThat(workflow.get("max_duration_ms").asLong()).isEqualTo(30_000);
        assertThat(workflow.has("wake_after_ms")).isFalse();
        assertThat(decl.has("sync_durability"))
                .as("asynchronous remote durability is the cost-free default")
                .isFalse();
    }

    @Test
    @DisplayName("a workflow class declaring synchronous durability ships the flag")
    void syncDurabilityDeclaration() throws Exception {
        WorkflowOperativeClassRegistry.registerClass(SyncDurableWorkflow.class);
        WorkflowSpec spec = WorkflowOperativeClassRegistry.getSpec("decl_sync_workflow");
        assertThat(spec.syncDurability()).isTrue();

        JsonNode decl = JSON.readTree(HostClassDeclaration.forWorkflow(spec));
        assertThat(decl.get("sync_durability").asBoolean()).isTrue();
    }

    @Test
    @DisplayName("forFactory resolves both registries and refuses an unknown id")
    void forFactoryResolvesRegistries() {
        StreamOperativeClassRegistry.registerClass(TierOperative.class);
        WorkflowOperativeClassRegistry.registerClass(DeclWorkflow.class);
        assertThat(HostClassDeclaration.forFactory("decl_tier_op")).isNotEmpty();
        assertThat(HostClassDeclaration.forFactory("decl_workflow")).isNotEmpty();
        assertThatThrownBy(() -> HostClassDeclaration.forFactory("missing"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing");
    }

    @Test
    @DisplayName("the checkpoint interval options ride the declaration only when declared")
    void checkpointIntervalOptions() throws Exception {
        StreamOperativeClassRegistry.registerClass(BatchedOutputsOperative.class);
        StreamOperativeTopology topology = StreamOperativeClassRegistry.getTopology("decl_batched_op");
        JsonNode decl = JSON.readTree(HostClassDeclaration.forStream(topology));

        assertThat(decl.get("ack_granularity").asText()).isEqualTo("checkpoint");
        assertThat(decl.get("checkpoint_every").asInt()).isEqualTo(500);
        assertThat(decl.get("checkpoint_outputs").asText()).isEqualTo("at_checkpoint");
        assertThat(decl.get("checkpoint_max_delay_ms").asLong()).isEqualTo(50L);
    }

    @Test
    @DisplayName("an interval option without the tier is refused, naming the missing tier")
    void checkpointOptionsWithoutTier() {
        assertThatThrownBy(() -> StreamOperativeClassRegistry.registerClass(NoTierBatchedOperative.class))
                .hasMessageContaining("CheckpointAckWithoutTier");
    }

    public static final class BatchedOutputsOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "decl_batched_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.checkpointEvery(500);
            c.outputsAtCheckpoint(50);
            c.input(String.class, "in")
                    .into(DeclProc.class)
                    .keyed()
                    .sink("sink", s -> s.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class NoTierBatchedOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "decl_no_tier_batched_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.outputsAtCheckpoint(50);
            c.input(String.class, "in")
                    .into(DeclProc.class)
                    .keyed()
                    .sink("sink", s -> s.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class TierOperative implements StreamOperative {
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
                    .sink("sink", s -> s.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class SyncDurableWorkflow implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "decl_sync_workflow";
        }

        @Override
        public boolean syncDurability() {
            return true;
        }

        @Override
        public void run(WorkflowOperativeCtx ctx, String args) throws WorkflowOperativeException {
            ctx.complete(args);
        }
    }

    public static final class DeclProc extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "decl_proc";
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

    public static final class DeclWorkflow implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "decl_workflow";
        }

        @Override
        public Optional<Duration> maxDuration() {
            return Optional.of(Duration.ofSeconds(30));
        }

        @Override
        public void run(WorkflowOperativeCtx ctx, String args) throws WorkflowOperativeException {
            ctx.complete(args);
        }
    }
}
