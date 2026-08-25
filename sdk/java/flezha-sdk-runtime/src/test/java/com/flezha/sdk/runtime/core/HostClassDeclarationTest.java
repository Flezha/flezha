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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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
                .as("nothing drops by default: an entry channel discards nothing")
                .isEqualTo("drop_nothing");

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
    @DisplayName("each Procedure declares its own schema version, never the class maximum")
    void perProcedureSchemaVersions() throws Exception {
        StreamOperativeClassRegistry.registerClass(MixedVersionOperative.class);
        JsonNode decl = JSON.readTree(
                HostClassDeclaration.forStream(StreamOperativeClassRegistry.getTopology("decl_mixed_version_op")));

        assertThat(decl.has("schema_version"))
                .as("a class-wide version would re-stamp a Procedure with a version it never declared")
                .isFalse();

        JsonNode procedures = decl.get("procedures");
        assertThat(procedures).hasSize(2);
        Map<String, JsonNode> byName = new LinkedHashMap<>();
        procedures.forEach(p -> byName.put(p.get("name").asText(), p));

        assertThat(byName.get("v2_proc").get("schema_version").asInt()).isEqualTo(2);
        assertThat(byName.get("v2_proc").get("min_readable_schema_version").asInt())
                .isEqualTo(1);
        assertThat(byName.get("v1_proc").get("schema_version").asInt())
                .as("the sibling keeps its own version when v2_proc is bumped")
                .isEqualTo(1);
        assertThat(byName.get("v1_proc").get("min_readable_schema_version").asInt())
                .isEqualTo(1);
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
    @DisplayName("the builder can spell every key the shared fixture pins")
    void everyPinnedDeclarationKeyHasAJavaSpelling() throws Exception {
        StreamOperativeClassRegistry.registerClass(FullyDeclaredOperative.class);
        StreamOperativeClassRegistry.registerClass(FullyDeclaredDynamicOperative.class);
        WorkflowOperativeClassRegistry.registerClass(FullyDeclaredWorkflow.class);

        JsonNode stream =
                JSON.readTree(HostClassDeclaration.forStream(StreamOperativeClassRegistry.getTopology("decl_full_op")));
        JsonNode dynamic = JSON.readTree(
                HostClassDeclaration.forStream(StreamOperativeClassRegistry.getTopology("decl_full_dynamic_op")));
        JsonNode workflow = JSON.readTree(
                HostClassDeclaration.forWorkflow(WorkflowOperativeClassRegistry.getSpec("decl_full_workflow")));

        // A key may live on any node of its section: the entry channel has no
        // `producer`, the plugin-bound one has the `keyed` flag.
        assertEverySpellingExists("class", List.of(stream), Set.of("sync_durability", "workflow"));
        assertEverySpellingExists("channel", nodes(stream.get("channels")), Set.of());
        // Entity mode splits the procedure keys: a static procedure declares its
        // key set, a dynamic one its TTL and cap. Both modes together are the
        // section.
        List<JsonNode> procedures = new ArrayList<>(nodes(stream.get("procedures")));
        procedures.addAll(nodes(dynamic.get("procedures")));
        assertEverySpellingExists("procedure", procedures, Set.of());
        assertEverySpellingExists("workflow", List.of(workflow.get("workflow")), Set.of("wake_after_ms"));
        assertThat(workflow.has("sync_durability")).isTrue();
    }

    private static List<JsonNode> nodes(JsonNode array) {
        List<JsonNode> out = new ArrayList<>();
        array.forEach(out::add);
        return out;
    }

    /**
     * Every key the fixture pins for {@code section} that no node of that
     * section carries, minus the ones this declaration legitimately omits (a
     * static procedure has no {@code idle_ttl_ms}, a stream class no
     * {@code sync_durability}). A key with no Java spelling at all shows up
     * here and nowhere else.
     */
    private static void assertEverySpellingExists(
            String section, List<JsonNode> sectionNodes, Set<String> legitimatelyAbsent) throws Exception {
        List<String> missing = new ArrayList<>();
        for (String key : pinnedKeys(section)) {
            boolean present = sectionNodes.stream().anyMatch(node -> node.has(key));
            if (!present && !legitimatelyAbsent.contains(key)) {
                missing.add(key);
            }
        }
        assertThat(missing)
                .as(
                        "%s keys pinned by %s that the Java builder never emits — teach "
                                + "HostClassDeclaration and the authoring surface, or the core and Java "
                                + "have drifted apart",
                        section, KEY_FIXTURE)
                .isEmpty();
    }

    private static final String KEY_FIXTURE = "test/fixtures/host_class_decl_keys.txt";

    private static List<String> pinnedKeys(String section) throws Exception {
        // The test runs from the module directory; the fixture is repo-relative.
        Path fixture = Path.of("..", "..", "..").resolve(KEY_FIXTURE).normalize();
        if (!Files.exists(fixture)) {
            fixture = Path.of(KEY_FIXTURE);
        }
        String prefix = section + ".";
        return Files.readAllLines(fixture).stream()
                .filter(line -> !line.startsWith("#") && !line.isBlank())
                .filter(line -> line.startsWith(prefix))
                .map(line -> line.substring(prefix.length()))
                .collect(Collectors.toCollection(java.util.ArrayList::new));
    }

    /** Declares every stream-side option the Java surface has a spelling for. */
    public static final class FullyDeclaredOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "decl_full_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.checkpointEvery(500);
            c.outputsAtCheckpoint(50);
            c.correctionWindow(600_000);
            c.beyondWindowPolicy(com.flezha.sdk.channel.BeyondWindowPolicy.DROP);
            c.input(String.class, "in")
                    .onFull(com.flezha.sdk.channel.OverflowPolicy.DROP_NOTHING)
                    .retryMax(3)
                    .retryBackoffMs(300)
                    .into(FullyDeclaredProc.class)
                    .keyed()
                    .sink("sink", s -> s.getBytes(StandardCharsets.UTF_8));
        }
    }

    /** The dynamic half of the procedure surface: a lazy Entity with a TTL and a cap. */
    public static final class FullyDeclaredDynamicOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "decl_full_dynamic_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in")
                    .keyBy(input -> input.split(":")[0])
                    .into(FullyDeclaredDynamicProc.class)
                    .named("out")
                    .sink("sink", s -> s.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class FullyDeclaredDynamicProc extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "decl_full_dynamic_proc";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public Optional<Duration> entityIdleTtl() {
            return Optional.of(Duration.ofMillis(200));
        }

        @Override
        public long maxEntities() {
            return 4096;
        }

        @Override
        public boolean maxEntitiesOverridden() {
            return true;
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx)
                throws com.flezha.sdk.procedure.ProcedureException {
            ctx.emit(input);
        }
    }

    /** A static, compensating, persistent procedure: the widest procedure declaration. */
    public static final class FullyDeclaredProc
            implements com.flezha.sdk.procedure.Procedure<
                    String, FullyDeclaredProc.State, String, DeclChange, com.flezha.sdk.state.EventTimeFrom, Void> {

        public static final class State {
            public int seen;
        }

        @Override
        public String name() {
            return "decl_full_proc";
        }

        @Override
        public State initState(
                Optional<String> entityKey, Optional<com.flezha.sdk.state.RestoreInitPayload> initPayload) {
            return new State();
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(State.class, DeclChange.class);
        }

        @Override
        public void applyChange(State state, DeclChange change) {
            state.seen++;
        }

        @Override
        public List<String> entityKeys() {
            return List.of("a", "b");
        }

        @Override
        public List<String> entitiesFor(String input) {
            return List.of("a");
        }

        @Override
        public boolean entitiesForOverridden() {
            return true;
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public long compactAfterBytes() {
            return 4096;
        }

        // `correctionWindow` needs an event-time clock: a correction rewinds to a
        // baseline before the late input's event time.
        @Override
        public Class<com.flezha.sdk.state.EventTimeFrom> clockType() {
            return com.flezha.sdk.state.EventTimeFrom.class;
        }

        @Override
        public long eventTime(String input) {
            return Long.parseLong(input.split(":")[0]);
        }

        @Override
        public boolean eventTimeOverridden() {
            return true;
        }

        @Override
        public void run(String input, StateHandle<State, DeclChange> state, ProcedureCtx ctx)
                throws com.flezha.sdk.procedure.ProcedureException {
            state.apply(new DeclChange(input));
            ctx.emitKeyed("a", input);
        }
    }

    public record DeclChange(String value) implements com.flezha.sdk.state.Change {}

    /** Declares every workflow-side option the Java surface has a spelling for. */
    public static final class FullyDeclaredWorkflow implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "decl_full_workflow";
        }

        @Override
        public boolean syncDurability() {
            return true;
        }

        @Override
        public Optional<Duration> maxDuration() {
            return Optional.of(Duration.ofSeconds(30));
        }

        @Override
        public void run(WorkflowOperativeCtx ctx, String args) throws WorkflowOperativeException {
            ctx.complete(args.getBytes(StandardCharsets.UTF_8));
        }
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

    /** Two procedures in one class, each at its own schema version. */
    public static final class MixedVersionOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "decl_mixed_version_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in")
                    .into(VersionTwoProc.class)
                    .named("mid")
                    .into(VersionOneProc.class)
                    .sink("sink", s -> s.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class VersionTwoProc extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "v2_proc";
        }

        @Override
        public int schemaVersion() {
            return 2;
        }

        @Override
        public int minReadableSchemaVersion() {
            return 1;
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx)
                throws com.flezha.sdk.procedure.ProcedureException {
            ctx.emit(input);
        }
    }

    public static final class VersionOneProc extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "v1_proc";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx)
                throws com.flezha.sdk.procedure.ProcedureException {
            ctx.emit(input);
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
