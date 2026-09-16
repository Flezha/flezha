package com.flezha.sdk.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.LocalOperative;
import com.flezha.sdk.SinkOutput;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.operative.StatelessTestProcedure;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.TimerKind;
import com.flezha.sdk.state.WallClock;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Local mode over the five capabilities the LANGUAGE LAYER owns, each driven through the fixture
 * class {@code test/apps/java_runner/conformance.json} names for it, so what passes here and what
 * passes end to end are not two different graphs.
 *
 * <p>The other ten capabilities in the vocabulary are the controller's: persistence,
 * effectively-once, correction, partitioning, workflows, liveness, restarts. Local mode refuses them
 * by name rather than approximating them.
 *
 * <p>What each test can assert is what a run with no controller can see. A durable write reaches a
 * null sink, a parked payload has nowhere to park, and a partition member's key range arrives with
 * {@code AddOperative}, so the assertions here are the output side of each capability, which is the
 * half the language layer produces.
 *
 * <p>The classes are DECLARED HERE rather than imported, which the other SDKs' local-mode suites do
 * not have to do. {@code test/apps/java_runner} is a Maven project of its own that depends on the
 * published SDK, so no SDK module can reach its sources without inverting that dependency. Each
 * declaration therefore carries the test app's factory id and channel names verbatim, and
 * {@link #declares} holds the id against the manifest, so a class the end-to-end renames or drops
 * fails here instead of leaving a second graph nobody else runs. They are stateless where the test
 * app's twins journal a Change per input: the Change is there so the controller writes a WAL record,
 * and a run with no controller has nothing to write it to. The one exception is the tier class,
 * whose declaration the core refuses without it.
 *
 * <p>Release order is asserted per key, not across keys. Java dispatches activations on a worker
 * pool and keeps one Entity on one worker at a time, so the order two keys reach the sink in is not
 * something the language layer promises.
 */
class LocalModeCapabilitiesTest {

    private static final String MANIFEST = "test/apps/java_runner/conformance.json";

    private static final Duration BUDGET = Duration.ofSeconds(20);

    /** Short enough to keep the run brief, long enough not to race the feed that arms it. */
    private static final Duration SESSION_TIMEOUT = Duration.ofMillis(200);

    @Test
    @DisplayName("graph_builder: a declared graph is built and run with no controller")
    void aDeclaredGraphIsBuiltAndRunWithNoController() {
        declares("graph_builder", "sugar_do_nothing");

        List<SinkOutput> released = release(
                SugarDoNothingStreamOperative.class, "input", "sugar_do_nothing_echo", List.of("alpha", "beta"));

        assertThat(payloads(released))
                .as("the builder produced a graph the core ran, in order")
                .containsExactly("alpha", "beta");
    }

    @Test
    @DisplayName("keyed_channels: a keyed sink stamps the key the class resolved")
    void aKeyedSinkStampsTheKeyTheClassResolved() {
        // `partition_relay` is the manifest's only keyed-channel class. Partitioning itself is the
        // controller's (a member's key-group range arrives with `AddOperative`), so what is asserted
        // here is the half the language layer owns: the key it resolves and the stamps it puts on
        // the released value.
        declares("keyed_channels", "partition_relay");

        List<SinkOutput> released =
                release(PartitionRelayStreamOperative.class, "in", "input", List.of("a:1", "b:1", "a:2"));

        assertThat(payloads(released)).containsExactlyInAnyOrder("a:1", "b:1", "a:2");
        assertThat(payloadsOfKey(released, "a")).containsExactly("a:1", "a:2");
        assertThat(released)
                .as("each release carries the key its own value resolved to")
                .allSatisfy(output -> assertThat(output.metadata()).containsEntry("flz.key", key(payload(output))));
        assertThat(released.get(0).metadata()).containsKeys("flz.entity", "flz.seq");
    }

    @Test
    @DisplayName("dynamic_entity: the declared filter runs before keying and nothing else is lost")
    void theDeclaredFilterRunsBeforeKeyingAndNothingElseIsLost() {
        // The class's sink is not keyed, so a released value carries no entity stamp and the per-key
        // minting is not visible on this channel. It is asserted above, on `partition_relay`, whose
        // procedure is dynamic too and whose sink IS keyed. What this class shows that that one does
        // not is the declared filter: a value it rejects answers no key, so the core is never asked
        // to mint an Entity for it.
        declares("dynamic_entity", "dynamic_entity");

        List<SinkOutput> released =
                release(DynamicEntityStreamOperative.class, "input", "kafka", List.of("a:1", "b:1", "drop", "a:2"));

        assertThat(payloads(released))
                .as("the filter dropped `drop` before it was keyed, and every other value reached its Entity")
                .containsExactlyInAnyOrder("a:1", "b:1", "a:2");
        assertThat(payloadsOfKey(released, "a")).containsExactly("a:1", "a:2");
        assertThat(released)
                .as("an unkeyed sink carries its channel and no identity stamps")
                .allSatisfy(output -> assertThat(output.channel()).isEqualTo("kafka"));
    }

    @Test
    @DisplayName("retry_ladder: a bounded mailbox that drops nothing loses no input")
    void aBoundedMailboxThatDropsNothingLosesNoInput() {
        // `poison_probe` is the manifest's other class for this capability and its poison path ends
        // in the dead-letter queue, which is the controller's. The ladder's other arm, a full
        // mailbox under `drop_nothing` where the producer is held rather than the value discarded,
        // is entirely the language layer's, and is what a run with no controller can see.
        declares("retry_ladder", "bounded_mailbox_drop_nothing");

        List<String> inputs = IntStream.range(0, 24).mapToObj(at -> "n" + at).toList();
        List<SinkOutput> released = release(
                BoundedMailboxDropNothingStreamOperative.class, "drop_nothing_input", "drop_nothing_echo", inputs);

        assertThat(payloads(released))
                .as("the mailbox holds 4 and 24 were fed: under `drop_nothing` the feed waits, in order")
                .containsExactlyElementsOf(inputs);
    }

    @Test
    @DisplayName("entity_timers: a declared timer fires locally with its scheduled time")
    void aDeclaredTimerFiresLocallyWithItsScheduledTime() {
        declares("entity_timers", "session_timeout");

        List<SinkOutput> released = release(SessionTimeoutStreamOperative.class, "input", "kafka", List.of("open"));

        assertThat(payloads(released))
                .as("the timer fired exactly once, carrying the timestamp it was scheduled for")
                .singleElement()
                .matches(closed -> closed.matches("closed@\\d+"));
    }

    @Test
    @DisplayName("a controller capability is refused by name, not approximated")
    void aControllerCapabilityIsRefusedByNameNotApproximated() {
        // The line local mode draws: the ten the controller owns stay refused. `tier_store` is
        // effectively-once, which needs a durable store to release from, so a local run says so
        // instead of running the graph.
        declares("effectively_once", "tier_store");

        assertThatThrownBy(() -> LocalOperative.start(TierStoreStreamOperative.class))
                .hasMessageContaining("effectively_once")
                .hasMessageContaining("requires a controller");
    }

    /**
     * Run one class over {@code inputs} on {@code channel}, and return what {@code sink} released.
     *
     * <p>Read after the run is closed, not at the moment it settles: the callback is invoked from the
     * run's own delivery thread, which can be behind the graph, and closing is what hands the last of
     * the released values over.
     */
    private static List<SinkOutput> release(
            Class<? extends StreamOperative> operative, String channel, String sink, List<String> inputs) {
        ConcurrentLinkedQueue<SinkOutput> released = new ConcurrentLinkedQueue<>();
        try (LocalOperative run = LocalOperative.start(operative)) {
            run.onSink(sink, released::add);
            inputs.forEach(input -> run.feed(channel, input));
            run.runUntilIdle(BUDGET);
        }
        return List.copyOf(released);
    }

    /** The class under test is one the manifest names for the capability. */
    private static void declares(String capability, String factoryId) {
        JsonNode declared;
        try {
            // `lines()` drops comment lines and trims; JSON carries neither a comment nor a meaning
            // for the whitespace, so the joined lines are the file.
            declared = new ObjectMapper()
                    .readTree(String.join("\n", RepositoryFixture.lines(MANIFEST)))
                    .path("fixtures")
                    .path(capability);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        List<String> ids = new ArrayList<>();
        declared.forEach(id -> ids.add(id.asText()));
        assertThat(ids)
                .as("the local-mode run must drive a class the e2e drives for %s", capability)
                .contains(factoryId);
    }

    private static List<String> payloads(List<SinkOutput> released) {
        return released.stream().map(LocalModeCapabilitiesTest::payload).toList();
    }

    /** What one key released, in release order: the order a single Entity does promise. */
    private static List<String> payloadsOfKey(List<SinkOutput> released, String key) {
        return payloads(released).stream()
                .filter(value -> key(value).equals(key))
                .toList();
    }

    private static String payload(SinkOutput output) {
        return new String(output.payload(), StandardCharsets.UTF_8);
    }

    /** The key every keyed fixture here resolves: the text before the first {@code :}. */
    private static String key(String input) {
        int at = input.indexOf(':');
        return at < 0 ? input : input.substring(0, at);
    }

    /** Releases the input's bytes on the operative's sink. */
    abstract static class EchoProcedure extends StatelessTestProcedure<String, Object, byte[]> {

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emit(input.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class SugarDoNothingProcedure extends EchoProcedure {
        @Override
        public String name() {
            return "sugar_do_nothing";
        }
    }

    /** The graph the test app's {@code @Procedure}-annotated {@code SugarDoNothing} generates. */
    public static final class SugarDoNothingStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "sugar_do_nothing";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(SugarDoNothingProcedure.class).sink("sugar_do_nothing_echo");
        }
    }

    /** Releases the input under the key it resolves to, on the class's keyed sink. */
    public static final class PartitionRelayProcedure extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "partition_relay_procedure";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emitKeyed(key(input), input);
        }
    }

    public static final class PartitionRelayStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "partition_relay";
        }

        @Override
        public void channels(ChannelBuilder c) {
            // The entry channel is `in` and the sink `input` because a forwarded frame keeps its
            // producing channel's name, and that is what selects the receiving operative's channel.
            c.input(String.class, "in")
                    .keyBy(LocalModeCapabilitiesTest::key)
                    .into(PartitionRelayProcedure.class)
                    .keyed()
                    .sink("input");
        }
    }

    public static final class DynamicEntityProcedure extends EchoProcedure {
        @Override
        public String name() {
            return "dynamic_entity_procedure";
        }
    }

    public static final class DynamicEntityStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "dynamic_entity";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input")
                    .onFull(OverflowPolicy.DROP_NOTHING)
                    .filter(input -> !input.equals("drop"))
                    .keyBy(LocalModeCapabilitiesTest::key)
                    .into(DynamicEntityProcedure.class)
                    .sink("kafka");
        }
    }

    public static final class DropNothingProcedure extends EchoProcedure {
        @Override
        public String name() {
            return "drop_nothing_test_procedure";
        }

        @Override
        public int mailboxCapacity() {
            return 4;
        }

        @Override
        public boolean mailboxCapacityOverridden() {
            return true;
        }
    }

    public static final class BoundedMailboxDropNothingStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "bounded_mailbox_drop_nothing";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "drop_nothing_input")
                    .onFull(OverflowPolicy.DROP_NOTHING)
                    .into(DropNothingProcedure.class)
                    .sink("drop_nothing_echo");
        }
    }

    /**
     * A processing-time timer, because the answer to "nothing has arrived" cannot be an event-time
     * one. The test app reads the deadline from {@code TIMER_TEST_TIMEOUT_MS} so its e2e can shorten
     * it; a class declared here names the short one outright.
     */
    public static final class SessionTimeoutProcedure extends StatelessTestProcedure<String, Object, byte[]> {
        @Override
        public String name() {
            return "session_timeout_procedure";
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.timerAfter(SESSION_TIMEOUT);
        }

        @Override
        public void onTimer(long firedAt, TimerKind kind, StateHandle<Object, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit(("closed@" + firedAt).getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class SessionTimeoutStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "session_timeout";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(SessionTimeoutProcedure.class).sink("kafka");
        }
    }

    /**
     * The tier IS durable state, so a class whose every procedure declares {@code NoChange} is
     * refused at registration before the local run reaches the capability. This one journals a
     * Change so the refusal under test is the one local mode owns.
     */
    public static final class TierStoreProcedure
            implements Procedure<String, TierStoreProcedure.Stored, String, TierStoreProcedure.Store, WallClock, Void> {

        public static final class Stored {
            public String value = "";
        }

        public record Store(String value) implements Change {}

        @Override
        public String name() {
            return "tier_store_procedure";
        }

        @Override
        public Stored initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Stored();
        }

        @Override
        public void applyChange(Stored state, Store change) {
            state.value = change.value();
        }

        @Override
        public void run(String input, StateHandle<Stored, Store> state, ProcedureCtx ctx) throws ProcedureException {
            state.apply(new Store(input));
            ctx.emitKeyed(key(input), input);
        }
    }

    public static final class TierStoreStreamOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "tier_store";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.input(String.class, "input")
                    .onFull(OverflowPolicy.DROP_NOTHING)
                    .into(TierStoreProcedure.class)
                    .keyed()
                    .sink("sink");
        }
    }
}
