package com.flezha.sdk.runtime.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeCtx;
import com.flezha.sdk.bounded.BoundedOperativeException;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.StatelessTestProcedure;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.TimerOp;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.TimerKind;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The pure host layer, driven entirely through
 * {@link ActivationDispatcher#dispatch}: constructed Activations in, asserted
 * ActivationResults out — no native core, no wire. Consolidates the
 * behavioral assertions of the pre-core dispatcher suites (stream transforms,
 * merged input, dynamic/static entities, per-entity instances, timers,
 * checkpoint ack, verdict classification, output identity, state
 * serialization, restore fold, bounded run-sugar and compensation).
 */
class ActivationDispatcherTest {

    private static final String OP = "op-inst";

    private static final List<String> RECORDED = new CopyOnWriteArrayList<>();
    private static final AtomicInteger TRAVERSALS = new AtomicInteger();
    private static final List<String> TIMER_EVENTS = new CopyOnWriteArrayList<>();

    private FakeSession session;
    private ActivationDispatcher dispatcher;

    @BeforeEach
    void reset() {
        RECORDED.clear();
        TRAVERSALS.set(0);
        TIMER_EVENTS.clear();
        UnboundedOperativeClassRegistry.reset();
        BoundedOperativeClassRegistry.reset();
        session = new FakeSession();
        dispatcher = new ActivationDispatcher(id -> session);
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static Activation work(
            String factory, int procedure, Optional<String> key, long changeSeq, WorkItem... items) {
        return new Activation(
                1, new EntityRef(OP, procedure, key), new ActivationKind.Work(List.of(items)), changeSeq, factory);
    }

    private static WorkItem.Input input(int channel, String payload, long now) {
        return input(channel, payload, now, OptionalLong.empty(), false);
    }

    private static WorkItem.Input input(int channel, String payload, long now, OptionalLong offset, boolean replay) {
        return new WorkItem.Input(channel, payload.getBytes(StandardCharsets.UTF_8), List.of(), offset, replay, now);
    }

    private static Activation command(String factory, int procedure, Optional<String> key, ActivationKind kind) {
        return new Activation(1, new EntityRef(OP, procedure, key), kind, 0, factory);
    }

    private ActivationOutcome outcome(Activation activation) {
        ActivationResult result = dispatcher.dispatch(activation);
        assertThat(result.token()).isEqualTo(activation.token());
        return result.outcome();
    }

    private ItemEffect soleEffect(Activation activation) {
        ActivationOutcome out = outcome(activation);
        assertThat(out).isInstanceOf(ActivationOutcome.Work.class);
        List<ItemEffect> effects = ((ActivationOutcome.Work) out).effects();
        assertThat(effects).hasSize(1);
        return effects.get(0);
    }

    // ------------------------------------------------------------------
    // work dispatch: state, changes, plugin output, identity stamps
    // ------------------------------------------------------------------

    @Test
    @DisplayName("an input applies its Change, ships decodable change bytes, and stages the plugin output")
    void workAppliesChangesAndCollectsOutputs() {
        UnboundedOperativeClassRegistry.registerClass(CounterOperative.class);
        ItemEffect effect =
                soleEffect(work("counter_op", 0, Optional.empty(), 7, input(0, "x", 1_000, OptionalLong.of(9), false)));

        assertThat(effect.verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(effect.consumedOffset()).hasValue(9);
        assertThat(effect.eventTime()).isEqualTo(1_000);
        assertThat(effect.checkpoint()).isFalse();
        assertThat(effect.changes()).hasSize(1);
        CountChange change = new KryoCodec(List.of(CountChange.class))
                .deserialize(effect.changes().get(0), CountChange.class);
        assertThat(change.delta).isEqualTo(1);

        assertThat(effect.outputs()).hasSize(1);
        Output output = effect.outputs().get(0);
        assertThat(output.target()).isEqualTo(new OutputTarget.Plugin("out"));
        assertThat(output.key()).isEmpty();
        assertThat(output.metadata())
                .as("non-keyed channel carries no identity stamps")
                .isEmpty();
        assertThat(new String(output.payload(), StandardCharsets.UTF_8)).isEqualTo("seen:x");
    }

    @Test
    @DisplayName("a keyed plugin emit carries flz.entity / flz.seq / flz.key and the schema key")
    void keyedEmitCarriesIdentityStamps() {
        UnboundedOperativeClassRegistry.registerClass(KeyedEmitOperative.class);
        ItemEffect effect = soleEffect(work("keyed_op", 0, Optional.empty(), 5, input(0, "v", 1_000)));

        assertThat(effect.outputs()).hasSize(1);
        Output output = effect.outputs().get(0);
        assertThat(output.key()).contains("k-v");
        Map<String, String> meta = metaMap(output);
        assertThat(meta)
                .containsEntry("flz.entity", OP + "/keyed_proc/")
                .containsEntry("flz.seq", "5.0")
                .containsEntry("flz.key", "k-v")
                .containsEntry("user", "meta");
    }

    // ------------------------------------------------------------------
    // transforms + dynamic entities
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a transform chain is one fused closure; KeyRequest answers the key of the MAPPED value")
    void transformChainAndKeyRequest() {
        UnboundedOperativeClassRegistry.registerClass(DynamicChainOperative.class);

        ActivationOutcome prepared = outcome(command(
                "dyn_chain",
                0,
                Optional.empty(),
                new ActivationKind.KeyRequest(
                        0,
                        List.of("AAPL:3".getBytes(StandardCharsets.UTF_8), "#c".getBytes(StandardCharsets.UTF_8)),
                        true,
                        false)));
        List<PrepareResult> answers = ((ActivationOutcome.Prepared) prepared).answers();
        assertThat(answers).hasSize(2);
        assertThat(((PrepareResult.Ok) answers.get(0)).answer().keys()).containsExactly("AAPL");
        assertThat(((PrepareResult.Ok) answers.get(1)).answer().keys())
                .as("the filtered value yields no key")
                .isEmpty();

        ItemEffect effect = soleEffect(work("dyn_chain", 0, Optional.of("AAPL"), 0, input(0, "AAPL:3", 1)));
        assertThat(effect.verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(RECORDED).containsExactly("AAPL=AAPLx3");
        assertThat(TRAVERSALS)
                .as("prepare + work each traverse the ONE fused closure once per payload")
                .hasValue(3);

        soleEffect(work("dyn_chain", 0, Optional.of("MSFT"), 0, input(0, "MSFT:7", 2)));
        assertThat(RECORDED).containsExactly("AAPL=AAPLx3", "MSFT=MSFTx7");
    }

    @Test
    @DisplayName("a filtered-out payload consumes its offset with no invocation")
    void filteredPayloadStillAcks() {
        UnboundedOperativeClassRegistry.registerClass(DynamicChainOperative.class);
        ItemEffect effect = soleEffect(
                work("dyn_chain", 0, Optional.of("AAPL"), 0, input(0, "#comment", 1, OptionalLong.of(4), false)));
        assertThat(effect.verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(effect.consumedOffset()).hasValue(4);
        assertThat(RECORDED).isEmpty();
    }

    @Test
    @DisplayName("flatMap fans one payload across keys; each Entity receives only its own slot")
    void fanOutKeysEveryYieldedValue() {
        UnboundedOperativeClassRegistry.registerClass(FanOutOperative.class);

        ActivationOutcome prepared = outcome(command(
                "fan_out",
                0,
                Optional.empty(),
                new ActivationKind.KeyRequest(0, List.of("AAPL:3".getBytes(StandardCharsets.UTF_8)), true, false)));
        assertThat(((PrepareResult.Ok) ((ActivationOutcome.Prepared) prepared)
                                .answers()
                                .get(0))
                        .answer()
                        .keys())
                .containsExactly("AAPL@1m", "AAPL@5m");

        soleEffect(work("fan_out", 0, Optional.of("AAPL@5m"), 0, input(0, "AAPL:3", 1)));
        assertThat(RECORDED).containsExactly("AAPL@5m=AAPL@5m");
    }

    @Test
    @DisplayName("static mode: entitiesFor routes, and each Entity runs its OWN Procedure instance")
    void staticEntitiesUsePerEntityInstances() {
        UnboundedOperativeClassRegistry.registerClass(StaticOperative.class);

        soleEffect(work("static_op", 0, Optional.of("a"), 0, input(0, "a:1", 1)));
        soleEffect(work("static_op", 0, Optional.of("a"), 0, input(0, "a:2", 2)));
        soleEffect(work("static_op", 0, Optional.of("b"), 0, input(0, "b:1", 3)));

        assertThat(RECORDED).containsExactly("a#1=a:1", "a#2=a:2", "b#1=b:1");

        ActivationOutcome prepared = outcome(command(
                "static_op",
                0,
                Optional.empty(),
                new ActivationKind.KeyRequest(0, List.of("b:9".getBytes(StandardCharsets.UTF_8)), true, false)));
        assertThat(((PrepareResult.Ok) ((ActivationOutcome.Prepared) prepared)
                                .answers()
                                .get(0))
                        .answer()
                        .keys())
                .containsExactly("b");
    }

    @Test
    @DisplayName("merged entry channels each apply their own mapping at delivery")
    void mergedInputAppliesEdgeMapping() {
        UnboundedOperativeClassRegistry.registerClass(MergeOperative.class);
        soleEffect(work("merge_op", 0, Optional.empty(), 0, input(0, "x", 1)));
        soleEffect(work("merge_op", 0, Optional.empty(), 0, input(1, "y", 2)));
        assertThat(RECORDED).containsExactly("=A:x", "=B:y");
    }

    // ------------------------------------------------------------------
    // inter-proc edge outputs
    // ------------------------------------------------------------------

    @Test
    @DisplayName("an inter-proc emit becomes per-consumer Edge outputs carrying the post-transform value")
    void interProcEmitBecomesEdgeOutputs() {
        UnboundedOperativeClassRegistry.registerClass(PipelineOperative.class);
        ItemEffect effect = soleEffect(work("pipeline_op", 0, Optional.empty(), 0, input(0, "v", 1)));

        assertThat(effect.outputs()).hasSize(1);
        Output edge = effect.outputs().get(0);
        assertThat(edge.target()).isEqualTo(new OutputTarget.Edge(1, 1));
        assertThat(edge.key()).isEmpty();
        assertThat(edge.eventTime()).isEmpty();

        // The Edge payload round-trips through the consumer's Work dispatch.
        ItemEffect consumed = soleEffect(work(
                "pipeline_op",
                1,
                Optional.empty(),
                0,
                new WorkItem.Input(1, edge.payload(), List.of(), OptionalLong.empty(), false, 2)));
        assertThat(consumed.verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(RECORDED).containsExactly("=mid:v");
    }

    // ------------------------------------------------------------------
    // verdicts
    // ------------------------------------------------------------------

    @Test
    @DisplayName("verdict classification: NonRetryable vs retryable, stopping at the first failed item")
    void verdictClassificationStopsAtFirstFailure() {
        UnboundedOperativeClassRegistry.registerClass(PoisonOperative.class);
        ActivationOutcome out = outcome(work(
                "poison_op", 0, Optional.empty(), 0, input(0, "ok-1", 1), input(0, "bad", 2), input(0, "ok-2", 3)));
        List<ItemEffect> effects = ((ActivationOutcome.Work) out).effects();
        assertThat(effects)
                .as("processing stopped at the failed item, which is included")
                .hasSize(2);
        assertThat(effects.get(0).verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(effects.get(1).verdict()).isInstanceOf(Verdict.RetryableError.class);
        assertThat(RECORDED).containsExactly("ok-1");

        ItemEffect fatal = soleEffect(work("poison_op", 0, Optional.empty(), 0, input(0, "bad-fatal", 4)));
        assertThat(fatal.verdict()).isInstanceOf(Verdict.NonRetryableError.class);
        assertThat(((Verdict.NonRetryableError) fatal.verdict()).message()).contains("fatal");
    }

    // ------------------------------------------------------------------
    // timers
    // ------------------------------------------------------------------

    @Test
    @DisplayName("registrations ride the effect; a firing's removal leads its ops; now() equals firedAt")
    void timerOpsRideTheEffect() {
        UnboundedOperativeClassRegistry.registerClass(TimerOperative.class);
        ItemEffect armed = soleEffect(work("timer_op", 0, Optional.empty(), 0, input(0, "1000", 1_000)));
        assertThat(armed.timers())
                .containsExactly(
                        TimerOp.register(TimerKind.EVENT_TIME, 1_010),
                        TimerOp.register(TimerKind.PROCESSING_TIME, 1_005));

        ItemEffect fired =
                soleEffect(work("timer_op", 0, Optional.empty(), 0, new WorkItem.Timer(TimerKind.EVENT_TIME, 1_010)));
        assertThat(fired.eventTime()).isEqualTo(1_010);
        assertThat(fired.timers().get(0)).isEqualTo(TimerOp.cancel(TimerKind.EVENT_TIME, 1_010));
        assertThat(fired.timers())
                .as("the chained timerAfter anchored on firedAt")
                .contains(TimerOp.register(TimerKind.PROCESSING_TIME, 1_017));
        assertThat(TIMER_EVENTS).containsExactly("EVENT_TIME@1010 now=1010");
    }

    @Test
    @DisplayName("an event-time timer under a wall clock is refused inside the invocation")
    void eventTimeTimerWithoutEventTimeClockIsRefused() {
        UnboundedOperativeClassRegistry.registerClass(WallTimerOperative.class);
        ItemEffect effect = soleEffect(work("wall_timer_op", 0, Optional.empty(), 0, input(0, "x", 1)));
        assertThat(effect.verdict()).isInstanceOf(Verdict.RetryableError.class);
        assertThat(((Verdict.RetryableError) effect.verdict()).message()).contains("event");
    }

    // ------------------------------------------------------------------
    // checkpoint ack + tier outputs
    // ------------------------------------------------------------------

    @Test
    @DisplayName("ctx.checkpoint() marks the effect; tier outputs carry no host-side identity stamps")
    void checkpointFlagAndTierOutputs() {
        UnboundedOperativeClassRegistry.registerClass(CheckpointOperative.class);
        ItemEffect open = soleEffect(work("ckpt_op", 0, Optional.empty(), 0, input(0, "accumulate", 1)));
        assertThat(open.checkpoint()).isFalse();
        assertThat(open.outputs()).hasSize(1);
        assertThat(metaMap(open.outputs().get(0)))
                .as("under the tier the controller derives the stamps at flush")
                .doesNotContainKeys("flz.entity", "flz.seq", "flz.key");
        assertThat(open.outputs().get(0).key()).contains("ck");

        ItemEffect closing = soleEffect(work("ckpt_op", 0, Optional.empty(), 0, input(0, "commit", 2)));
        assertThat(closing.checkpoint()).isTrue();
    }

    // ------------------------------------------------------------------
    // restore / snapshot / live state / apply state
    // ------------------------------------------------------------------

    @Test
    @DisplayName("RestoreFold rebuilds snapshot + changes; SnapshotRequest and LiveStateRead read the result")
    void restoreSnapshotAndLiveRead() {
        UnboundedOperativeClassRegistry.registerClass(CounterOperative.class);
        ItemEffect first = soleEffect(work("counter_op", 0, Optional.empty(), 0, input(0, "a", 1)));
        soleEffect(work("counter_op", 0, Optional.empty(), 1, input(0, "b", 2)));

        ActivationOutcome json =
                outcome(command("counter_op", 0, Optional.empty(), new ActivationKind.LiveStateRead()));
        assertThat(new String(((ActivationOutcome.StateJson) json).json(), StandardCharsets.UTF_8))
                .isEqualTo("{\"n\":2}");

        ActivationOutcome snap =
                outcome(command("counter_op", 0, Optional.empty(), new ActivationKind.SnapshotRequest()));
        byte[] snapshot = ((ActivationOutcome.Snapshot) snap).state();

        // A fresh dispatcher (a rebind) folds snapshot(n=2) + one change → 3.
        ActivationDispatcher rebound = new ActivationDispatcher(id -> session);
        ActivationOutcome restored = rebound.dispatch(command(
                        "counter_op",
                        0,
                        Optional.empty(),
                        new ActivationKind.RestoreFold(Optional.of(snapshot), first.changes())))
                .outcome();
        assertThat(restored).isInstanceOf(ActivationOutcome.Restored.class);
        ActivationOutcome reboundJson = rebound.dispatch(
                        command("counter_op", 0, Optional.empty(), new ActivationKind.LiveStateRead()))
                .outcome();
        assertThat(new String(((ActivationOutcome.StateJson) reboundJson).json(), StandardCharsets.UTF_8))
                .isEqualTo("{\"n\":3}");
    }

    @Test
    @DisplayName("ApplyState replaces the state from its JSON form; MigrationSerialize reads it back")
    void applyStateRoundTrips() {
        UnboundedOperativeClassRegistry.registerClass(CounterOperative.class);
        ActivationOutcome applied = outcome(command(
                "counter_op",
                0,
                Optional.empty(),
                new ActivationKind.ApplyState("{\"n\":42}".getBytes(StandardCharsets.UTF_8))));
        assertThat(applied).isInstanceOf(ActivationOutcome.StateApplied.class);

        ActivationOutcome migrated =
                outcome(command("counter_op", 0, Optional.empty(), new ActivationKind.MigrationSerialize()));
        assertThat(new String(((ActivationOutcome.MigrationState) migrated).json(), StandardCharsets.UTF_8))
                .isEqualTo("{\"n\":42}");
    }

    // ------------------------------------------------------------------
    // prepare: event time + per-payload failure
    // ------------------------------------------------------------------

    @Test
    @DisplayName("KeyRequest answers the extractor's event time; a failing parse is a per-payload Err")
    void prepareEventTimeAndPerPayloadFailure() {
        UnboundedOperativeClassRegistry.registerClass(TimerOperative.class);
        ActivationOutcome prepared = outcome(command(
                "timer_op",
                0,
                Optional.empty(),
                new ActivationKind.KeyRequest(
                        0,
                        List.of("1234".getBytes(StandardCharsets.UTF_8), "nope".getBytes(StandardCharsets.UTF_8)),
                        false,
                        true)));
        List<PrepareResult> answers = ((ActivationOutcome.Prepared) prepared).answers();
        assertThat(((PrepareResult.Ok) answers.get(0)).answer().eventTime()).hasValue(1234);
        assertThat(answers.get(1)).isInstanceOf(PrepareResult.Err.class);
    }

    // ------------------------------------------------------------------
    // bounded
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a run-sugar body emits and recvs through the session and reports BoundedDone")
    void boundedRunSugar() {
        BoundedOperativeClassRegistry.registerClass(GreetBounded.class);
        session.inbox.add("ping".getBytes(StandardCharsets.UTF_8));

        ActivationOutcome out = outcome(command(
                "greet_bounded",
                0,
                Optional.empty(),
                new ActivationKind.BoundedRun("bob".getBytes(StandardCharsets.UTF_8))));
        assertThat(out).isInstanceOf(ActivationOutcome.BoundedDone.class);
        ActivationOutcome.BoundedDone done = (ActivationOutcome.BoundedDone) out;
        assertThat(done.completed()).isTrue();
        assertThat(new String(done.result(), StandardCharsets.UTF_8)).isEqualTo("done:bob");
        assertThat(session.emitted.stream()
                        .map(b -> new String(b, StandardCharsets.UTF_8))
                        .toList())
                .containsExactly("hi bob", "ping");
    }

    @Test
    @DisplayName("a throwing run-sugar body fails the activation with its classification")
    void boundedRunSugarFailure() {
        BoundedOperativeClassRegistry.registerClass(ThrowingBounded.class);
        ActivationOutcome out =
                outcome(command("throwing_bounded", 0, Optional.empty(), new ActivationKind.BoundedRun(new byte[0])));
        assertThat(out).isInstanceOf(ActivationOutcome.Failed.class);
        WorkError error = ((ActivationOutcome.Failed) out).error();
        assertThat(error.message()).contains("nope");
        assertThat(error.nonRetryable()).isTrue();
    }

    @Test
    @DisplayName("CompensateRun decodes the recorded outputs and collects the undo emits")
    void compensateRunUndoesWithDecodedOutputs() {
        BoundedOperativeClassRegistry.registerClass(SagaBounded.class);
        ItemEffect forward = soleEffect(work("saga_bounded", 0, Optional.empty(), 0, input(0, "job", 1)));
        assertThat(forward.outputs()).hasSize(1);

        ActivationOutcome out = outcome(command(
                "saga_bounded",
                0,
                Optional.empty(),
                new ActivationKind.CompensateRun(
                        List.of(forward.outputs().get(0).payload()))));
        List<ItemEffect> effects = ((ActivationOutcome.Work) out).effects();
        assertThat(effects).hasSize(1);
        assertThat(effects.get(0).verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(effects.get(0).outputs())
                .as("the undo emit rides the effect")
                .hasSize(1);
        assertThat(RECORDED).containsExactly("compensated:fwd:job");
    }

    // ------------------------------------------------------------------
    // failures + replay
    // ------------------------------------------------------------------

    @Test
    @DisplayName("an unknown factory fails the whole activation as structural")
    void unknownFactoryIsStructural() {
        ActivationOutcome out = outcome(work("no_such_factory", 0, Optional.empty(), 0, input(0, "x", 1)));
        assertThat(out).isInstanceOf(ActivationOutcome.Failed.class);
        WorkError error = ((ActivationOutcome.Failed) out).error();
        assertThat(error.nonRetryable()).isTrue();
        assertThat(error.message()).contains("no_such_factory");
    }

    @Test
    @DisplayName("a replayed item serves ctx.replaying() = true")
    void replayFlagReachesTheContext() {
        UnboundedOperativeClassRegistry.registerClass(ReplayProbeOperative.class);
        soleEffect(work("replay_probe", 0, Optional.empty(), 0, input(0, "x", 1, OptionalLong.empty(), true)));
        assertThat(RECORDED).containsExactly("replaying=true");
    }

    private static Map<String, String> metaMap(Output output) {
        return output.metadata().stream()
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // ==================================================================
    // fixtures
    // ==================================================================

    /** A bounded session fake: scripted inbox, captured emits. */
    static final class FakeSession implements BoundedSession {
        final Deque<byte[]> inbox = new ArrayDeque<>();
        final List<byte[]> emitted = new ArrayList<>();
        boolean cancelled;

        @Override
        public void emit(byte[] payload) {
            emitted.add(payload);
        }

        @Override
        public byte[] recv(long timeoutMs) {
            return inbox.poll();
        }

        @Override
        public boolean cancelled() {
            return cancelled;
        }
    }

    public static class CounterState {
        public int n;
    }

    public static class CountChange implements Change {
        public int delta;

        public CountChange() {}

        CountChange(int delta) {
            this.delta = delta;
        }
    }

    public static final class CounterOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "counter_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(CountProc.class).named("out").output("out", s -> ("seen:" + s)
                    .getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class CountProc
            implements Procedure<String, CounterState, String, CountChange, WallClock, Void> {
        @Override
        public String name() {
            return "count_proc";
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
        public Class<CountChange> changeType() {
            return CountChange.class;
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
        public List<Class<?>> kryoTypes() {
            return List.of(CountChange.class, CounterState.class);
        }

        @Override
        public CounterState initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new CounterState();
        }

        @Override
        public void applyChange(CounterState state, CountChange change) {
            state.n += change.delta;
        }

        @Override
        public void run(String input, StateHandle<CounterState, CountChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            state.apply(new CountChange(1));
            ctx.emit(input);
        }
    }

    public static final class KeyedEmitOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "keyed_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in")
                    .into(KeyedProc.class)
                    .keyed()
                    .output("sink", s -> s.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class KeyedProc extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "keyed_proc";
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
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emitKeyedWithMeta("k-" + input, input, Map.of("user", "meta"));
        }
    }

    public static final class DynamicChainOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "dyn_chain";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in")
                    .filter(raw -> {
                        TRAVERSALS.incrementAndGet();
                        return !raw.startsWith("#");
                    })
                    .map(String.class, raw -> raw.split(":")[0] + "x" + raw.split(":")[1])
                    .keyBy(shaped -> shaped.split("x")[0])
                    .into(KeyRecorder.class);
        }
    }

    public static final class FanOutOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "fan_out";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in")
                    .flatMap(String.class, raw -> List.of(raw.split(":")[0] + "@1m", raw.split(":")[0] + "@5m"))
                    .keyBy(slot -> slot)
                    .into(KeyRecorder.class);
        }
    }

    /** Records {@code entityKey=value}; one instance per Entity by contract. */
    public static final class KeyRecorder extends StatelessTestProcedure<String, Object, Void> {
        private String key = "";

        @Override
        public String name() {
            return "key_recorder";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            this.key = entityKey.orElse("");
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {
            RECORDED.add(key + "=" + input);
        }
    }

    public static final class StaticOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "static_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(StaticProc.class);
        }
    }

    /** Static keys a/b; counts runs per INSTANCE to pin per-Entity instances. */
    public static final class StaticProc extends StatelessTestProcedure<String, Object, Void> {
        private String key = "";
        private int instanceRuns;

        @Override
        public String name() {
            return "static_proc";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        public List<String> entityKeys() {
            return List.of("a", "b");
        }

        @Override
        public List<String> entitiesFor(String input) {
            return List.of(input.split(":")[0]);
        }

        @Override
        public boolean entitiesForOverridden() {
            return true;
        }

        @Override
        protected Object initState(Optional<String> entityKey) {
            this.key = entityKey.orElse("");
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {
            instanceRuns++;
            RECORDED.add(key + "#" + instanceRuns + "=" + input);
        }
    }

    public static final class MergeOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "merge_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.merge(String.class)
                    .from(c.input(String.class, "a"), s -> "A:" + s)
                    .from(c.input(String.class, "b"), s -> "B:" + s)
                    .into(KeyRecorder.class);
        }
    }

    public static final class PipelineOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "pipeline_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(MidProc.class).into(KeyRecorder.class);
        }
    }

    public static final class MidProc extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "mid_proc";
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
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emit("mid:" + input);
        }
    }

    public static final class PoisonOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "poison_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(PoisonProc.class);
        }
    }

    public static final class PoisonProc extends StatelessTestProcedure<String, Object, Void> {
        @Override
        public String name() {
            return "poison_proc";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
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
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            if (input.equals("bad-fatal")) {
                throw new ProcedureException.NonRetryable("fatal poison");
            }
            if (input.startsWith("bad")) {
                throw new IllegalStateException("transient poison");
            }
            RECORDED.add(input);
        }
    }

    public static final class TimerOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "timer_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(TimerProc.class);
        }
    }

    public static final class TimerProc implements Procedure<String, Object, Void, NoChange, EventTimeFrom, Void> {
        @Override
        public String name() {
            return "timer_proc";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        public Class<NoChange> changeType() {
            return NoChange.class;
        }

        @Override
        public Class<EventTimeFrom> clockType() {
            return EventTimeFrom.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public long eventTime(String input) {
            return Long.parseLong(input);
        }

        @Override
        public boolean eventTimeOverridden() {
            return true;
        }

        @Override
        public Object initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.timerAtEventTime(ctx.now() + 10);
            ctx.timerAfter(Duration.ofMillis(5));
        }

        @Override
        public void onTimer(long firedAt, TimerKind kind, StateHandle<Object, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            TIMER_EVENTS.add(kind + "@" + firedAt + " now=" + ctx.now());
            if (kind == TimerKind.EVENT_TIME) {
                ctx.timerAfter(Duration.ofMillis(7));
            }
        }
    }

    public static final class WallTimerOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "wall_timer_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(WallTimerProc.class);
        }
    }

    public static final class WallTimerProc extends StatelessTestProcedure<String, Object, Void> {
        @Override
        public String name() {
            return "wall_timer_proc";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
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
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.timerAtEventTime(1);
        }
    }

    public static final class CheckpointOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "ckpt_op";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.checkpointAck();
            c.input(String.class, "in")
                    .into(CkptProc.class)
                    .keyed()
                    .output("sink", s -> s.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class CkptProc extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "ckpt_proc";
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
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emitKeyed("ck", input);
            if (input.equals("commit")) {
                ctx.checkpoint();
            }
        }
    }

    public static final class ReplayProbeOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "replay_probe";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(ReplayProbeProc.class);
        }
    }

    public static final class ReplayProbeProc extends StatelessTestProcedure<String, Object, Void> {
        @Override
        public String name() {
            return "replay_probe_proc";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
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
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {
            RECORDED.add("replaying=" + ctx.replaying());
        }
    }

    public static final class GreetBounded implements BoundedOperative {
        @Override
        public String factoryId() {
            return "greet_bounded";
        }

        @Override
        public void run(BoundedOperativeCtx ctx, String args) throws BoundedOperativeException {
            ctx.emit("hi " + args);
            ctx.recv().ifPresent(item -> {
                try {
                    ctx.emit(item);
                } catch (BoundedOperativeException e) {
                    throw new IllegalStateException(e);
                }
            });
            ctx.complete("done:" + args);
        }
    }

    public static final class ThrowingBounded implements BoundedOperative {
        @Override
        public String factoryId() {
            return "throwing_bounded";
        }

        @Override
        public void run(BoundedOperativeCtx ctx, String args) throws BoundedOperativeException {
            throw BoundedOperativeException.nonRetryable("nope");
        }
    }

    public static final class SagaBounded implements BoundedOperative {
        @Override
        public String factoryId() {
            return "saga_bounded";
        }

        @Override
        public boolean persistenceEnabled() {
            return false;
        }

        @Override
        public void procedures(ChannelBuilder c) {
            c.input(String.class, "seed").into(UndoableProc.class).into(KeyRecorder.class);
        }
    }

    public static final class UndoableProc extends StatelessTestProcedure<String, Object, String> {
        @Override
        public String name() {
            return "undoable_proc";
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
        public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emit("fwd:" + input);
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<Object, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            for (String output : completedOutput) {
                RECORDED.add("compensated:" + output);
                ctx.emit("undo:" + output);
            }
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }
    }
}
