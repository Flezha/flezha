package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.BeyondWindowPolicy;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.segment.SegmentSlot;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unbounded late-input correction: a slice of re-forwarded inputs is dispatched
 * in <b>event-time order</b>, so a late arrival is folded into the state at the
 * position it should have occupied.
 */
class CorrectionTest {

    /** Event times, in the order the Operation actually saw them. */
    private static final List<Long> SEEN = new CopyOnWriteArrayList<>();

    private RunnerContext ctx;

    @BeforeEach
    void reset() {
        SEEN.clear();
        UnboundedOperativeClassRegistry.reset();
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ctx = new RunnerContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));
        ctx.streamRegistry().register(0, 100L, 200L);
    }

    @AfterEach
    void close() {
        if (ctx != null && ctx.workerPool() != null) {
            ctx.workerPool().shutdownNow();
        }
    }

    private UnboundedOperativeInstance instantiate(Class<? extends UnboundedOperative> cls, String factoryId) {
        UnboundedOperativeClassRegistry.registerClass(cls);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology(factoryId);
        return UnboundedOperativeInstance.create(topology, factoryId + "-inst", ctx, ctx.workerPool());
    }

    private void awaitDrain() throws Exception {
        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("a replayed slice dispatches in event-time order, and live inputs behind it wait")
    void replayedSliceDispatchesInEventTimeOrder() throws Exception {
        UnboundedOperativeInstance instance = instantiate(CorrectionOperative.class, "corr");
        OperationDispatcher dispatcher = instance.dispatcher(0);

        // The slice arrives out of order (30, 10, 20) — that is the whole point:
        // 10 is the late input the correction exists to re-fold.
        dispatcher.fanoutSegments("30", 100L, OptionalLong.of(0), true);
        dispatcher.fanoutSegments("10", 100L, OptionalLong.of(1), true);
        dispatcher.fanoutSegments("20", 100L, OptionalLong.of(2), true);

        // A LIVE input arrives while the slice is still open. It must NOT jump
        // ahead of the inputs being re-ordered.
        dispatcher.fanoutSegments("40", 100L, OptionalLong.of(3), false);

        // Nothing has run yet: the slice is buffered.
        assertThat(SEEN).isEmpty();

        dispatcher.endReplaySliceAllSegments();
        awaitDrain();

        // Slice in event-time order, THEN the live input behind it.
        assertThat(SEEN).containsExactly(10L, 20L, 30L, 40L);
    }

    @Test
    @DisplayName("a replay does not lower the event-time high-water")
    void replayDoesNotLowerTheHighWater() throws Exception {
        UnboundedOperativeInstance instance = instantiate(CorrectionOperative.class, "corr");
        OperationDispatcher dispatcher = instance.dispatcher(0);
        SegmentSlot slot = instance.segments().get(0).get(0);

        // A live input at 500 sets the high-water.
        dispatcher.fanoutSegments("500", 100L, OptionalLong.of(0), false);
        // A replay whose max event time is only 300.
        dispatcher.fanoutSegments("300", 100L, OptionalLong.of(1), true);
        dispatcher.endReplaySliceAllSegments();
        awaitDrain();

        // Had the replay lowered the mark to 300, a later input at 320 would read
        // as on-time and silently skip the correction it needed.
        assertThat(slot.eventTimeHighWater()).isEqualTo(500L);
    }

    @Test
    @DisplayName("beyond-window default is process-as-arrival, not drop")
    void beyondWindowDefaultProcessesAsArrival() throws Exception {
        UnboundedOperativeInstance instance = instantiate(CorrectionOperative.class, "corr");
        OperationDispatcher dispatcher = instance.dispatcher(0);

        dispatcher.fanoutSegments("100000", 100L, OptionalLong.of(0), false);
        // Far older than the 1000 ms window — but the default must still process
        // it. Declaring a window must never silently start dropping data.
        dispatcher.fanoutSegments("1", 100L, OptionalLong.of(1), false);
        awaitDrain();

        assertThat(SEEN).containsExactly(100000L, 1L);
    }

    @Test
    @DisplayName("beyondWindowPolicy(DROP) drops an input older than the window")
    void dropPolicyDropsBeyondWindow() throws Exception {
        UnboundedOperativeInstance instance = instantiate(DropOperative.class, "corr-drop");
        OperationDispatcher dispatcher = instance.dispatcher(0);

        dispatcher.fanoutSegments("100000", 100L, OptionalLong.of(0), false);
        dispatcher.fanoutSegments("1", 100L, OptionalLong.of(1), false);
        awaitDrain();

        assertThat(SEEN).containsExactly(100000L);
    }

    @Test
    @DisplayName("a correction window without the tier is refused")
    void correctionWithoutTierIsRefused() {
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(NoTierCorrectionOperative.class))
                .isInstanceOf(UnboundedOperativeConfigurationException.class)
                .hasMessageContaining("effectivelyOnce");
    }

    @Test
    @DisplayName("a correction window without an EventTimeFrom clock is refused")
    void correctionWithoutEventTimeClockIsRefused() {
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(NoEventTimeOperative.class))
                .isInstanceOf(UnboundedOperativeConfigurationException.class)
                .hasMessageContaining("EventTimeFrom");
    }

    // --- fixtures -----------------------------------------------------------

    public static final class CorrectionOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "corr";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.correctionWindow(1000);
            c.input(String.class, "in").into(CorrectionOp.class);
        }
    }

    public static final class DropOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "corr-drop";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.correctionWindow(1000);
            c.beyondWindowPolicy(BeyondWindowPolicy.DROP);
            c.input(String.class, "in").into(CorrectionOp.class);
        }
    }

    public static final class NoTierCorrectionOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "corr-no-tier";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.correctionWindow(1000);
            c.input(String.class, "in").into(CorrectionOp.class);
        }
    }

    /** Declares a window but uses the default WallClock — there is no event time to compare. */
    public static final class NoEventTimeOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "corr-no-event-time";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.correctionWindow(1000);
            c.input(String.class, "in").into(WallClockOp.class);
        }
    }

    public static final class CountState {
        public int n;
    }

    public record Inc() implements Change {
        @Override
        public Optional<CoalesceKey> coalesceKey() {
            return Optional.empty();
        }
    }

    /** Input IS its event time, as a decimal string — keeps the fixtures honest and readable. */
    public static final class CorrectionOp implements Operation<String, CountState, String, Inc, EventTimeFrom, Void> {
        @Override
        public String name() {
            return "corr_op";
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
        public Class<Inc> changeType() {
            return Inc.class;
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
        public List<Class<?>> kryoTypes() {
            return List.of(Inc.class, CountState.class);
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
        public void applyChange(CountState state, Inc change) {
            state.n++;
        }

        @Override
        public CountState initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new CountState();
        }

        @Override
        public void run(String input, StateHandle<CountState, Inc> state, OperationCtx ctx) throws OperationException {
            SEEN.add(ctx.now());
            state.apply(new Inc());
        }
    }

    public static final class WallClockOp
            implements Operation<String, CountState, String, Inc, com.flezha.sdk.state.WallClock, Void> {
        @Override
        public String name() {
            return "wallclock_op";
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
        public Class<Inc> changeType() {
            return Inc.class;
        }

        @Override
        public Class<com.flezha.sdk.state.WallClock> clockType() {
            return com.flezha.sdk.state.WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(Inc.class, CountState.class);
        }

        @Override
        public void applyChange(CountState state, Inc change) {
            state.n++;
        }

        @Override
        public CountState initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new CountState();
        }

        @Override
        public void run(String input, StateHandle<CountState, Inc> state, OperationCtx ctx) throws OperationException {}
    }
}
