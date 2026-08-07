package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.entity.EntitySlot;
import com.flezha.sdk.runtime.entity.TimerCounters;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.state.EffectRecord;
import com.flezha.sdk.runtime.state.EntityStateRestore;
import com.flezha.sdk.runtime.state.HeaderTemplate;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.PendingTimers;
import com.flezha.sdk.runtime.state.TimerOp;
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.TimerKind;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Per-Entity timers (`entity-timers`), Java side: the registration surface, both
 * trigger paths, the durability of the pending set, and the refusals.
 *
 * <p>The Rust twin is {@code sdk/rust/tests/entity_timers.rs}; the two assert the
 * same contract, so a divergence shows up as one suite passing and the other not.
 */
class EntityTimersTest {

    /** What each fixture invocation observed, in order. */
    private static final List<String> ORDER = new CopyOnWriteArrayList<>();

    private RunnerContext ctx;

    @BeforeEach
    void reset() {
        ORDER.clear();
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

    // ------------------------------------------------- surface and refusals

    @Test
    @DisplayName("registration returns the absolute time, is idempotent, and cancels")
    void registrationSurfaceIsAbsoluteIdempotentAndCancellable() throws Exception {
        UnboundedOperativeInstance instance = instantiate(SurfaceOperative.class, "timers-surface");
        ProcedureDispatcher dispatcher = instance.dispatcher(0);
        EntitySlot slot = instance.entities().get(0).get(0);

        dispatcher.fanoutEntities("register-twice", 100L);
        dispatcher.fanoutEntities("cancel", 100L);
        awaitDrain();

        assertThat(ORDER).containsExactly("register-twice", "cancel:true");
        assertThat(slot.timers().all(TimerKind.PROCESSING_TIME))
                .as("one registration survived the duplicate, and the cancel removed it")
                .isEmpty();
    }

    @Test
    @DisplayName("registration past the cap is refused naming the procedure and the bound")
    void registrationPastTheCapIsRefused() throws Exception {
        UnboundedOperativeInstance instance = instantiate(SurfaceOperative.class, "timers-surface");
        instance.dispatcher(0).fanoutEntities("overflow", 100L);
        awaitDrain();

        assertThat(ORDER).hasSize(1);
        assertThat(ORDER.get(0)).contains("timers_surface_procedure").contains("3");
    }

    @Test
    @DisplayName("an event-time timer under a wall clock is refused naming the clock")
    void eventTimeTimerUnderAWallClockIsRefused() throws Exception {
        UnboundedOperativeInstance instance = instantiate(SurfaceOperative.class, "timers-surface");
        instance.dispatcher(0).fanoutEntities("event-time", 100L);
        awaitDrain();

        assertThat(ORDER).hasSize(1);
        assertThat(ORDER.get(0)).contains("timers_surface_procedure").contains("WallClock");
    }

    @Test
    @DisplayName("a context with no timer session refuses rather than silently dropping")
    void aSessionLessContextRefuses() {
        ProcedureCtx bare = new ProcedureCtx() {
            @Override
            public long now() {
                return 0;
            }

            @Override
            public com.flezha.sdk.procedure.StableId stableId() {
                return com.flezha.sdk.procedure.StableId.derive(new byte[0], 0, 0);
            }

            @Override
            public void checkpoint() {}

            @Override
            public <C> void emitKeyed(String key, C value) {}

            @Override
            public <C> void emitKeyedWithMeta(String key, C value, Map<String, String> meta) {}

            @Override
            public <C> void emit(C value) {}

            @Override
            public <C> void emit(String channel, C value) {}

            @Override
            public <C> void emitWithMeta(C value, Map<String, String> meta) {}

            @Override
            public <C> void emitWithMeta(String channel, C value, Map<String, String> meta) {}
        };

        assertThatThrownBy(() -> bare.timerAfter(Duration.ofSeconds(1)))
                .isInstanceOf(ProcedureException.TimersUnavailable.class)
                .hasMessageContaining("not available");
        assertThatThrownBy(() -> bare.timerAtEventTime(1)).isInstanceOf(ProcedureException.TimersUnavailable.class);
        assertThatThrownBy(() -> bare.cancelEventTimeTimer(1)).isInstanceOf(ProcedureException.TimersUnavailable.class);
        assertThatThrownBy(() -> bare.cancelProcessingTimeTimer(1))
                .isInstanceOf(ProcedureException.TimersUnavailable.class);
    }

    // -------------------------------------------------------- event time

    @Test
    @DisplayName("crossing the high-water fires in order, before the input that crossed it")
    void crossingTheHighWaterFiresInOrderBeforeTheInput() throws Exception {
        UnboundedOperativeInstance instance = instantiate(WindowOperative.class, "timers-window");
        ProcedureDispatcher dispatcher = instance.dispatcher(0);
        EntitySlot slot = instance.entities().get(0).get(0);

        dispatcher.fanoutEntities(50L, 100L);
        dispatcher.fanoutEntities(250L, 100L);
        awaitDrain();

        assertThat(ORDER).containsExactly("run@50", "timer@100", "timer@200", "run@250");
        assertThat(slot.timers().all(TimerKind.EVENT_TIME))
                .as("300 is beyond the new high-water and stays pending")
                .containsExactly(300L);
    }

    @Test
    @DisplayName("a quiet Entity never fires its event-time timer")
    void aQuietEntityNeverFires() throws Exception {
        UnboundedOperativeInstance instance = instantiate(WindowOperative.class, "timers-window");
        instance.dispatcher(0).fanoutEntities(50L, 100L);
        awaitDrain();

        Thread.sleep(150);
        assertThat(ORDER).as("no firing, however much wall-clock time passes").containsExactly("run@50");
    }

    // --------------------------------------------------- processing time

    @Test
    @DisplayName("a processing-time timer fires from the wheel with its SCHEDULED time")
    void aProcessingTimeTimerFiresWithItsScheduledTime() throws Exception {
        UnboundedOperativeInstance instance = instantiate(SessionOperative.class, "timers-session");
        ProcedureDispatcher dispatcher = instance.dispatcher(0);
        EntitySlot slot = instance.entities().get(0).get(0);

        long before = System.currentTimeMillis();
        dispatcher.fanoutEntities("alice", 100L);

        long deadline = System.currentTimeMillis() + 5_000;
        while (ORDER.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertThat(ORDER).as("the wheel must fire the deadline").hasSize(2);
        assertThat(ORDER.get(0)).isEqualTo("run:alice");

        long firedAt = Long.parseLong(ORDER.get(1).substring("timer:".length()));
        assertThat(firedAt)
                .as("fired_at is the scheduled instant, not the wall clock at firing")
                .isBetween(before + SessionProcedure.TIMEOUT_MS, System.currentTimeMillis());
        assertThat(slot.timers().isEmpty())
                .as("the firing removed the timer from the pending set")
                .isTrue();
        assertThat(instance.timerWheel().occupancy()).isZero();
    }

    @Test
    @DisplayName("onTimer never runs concurrently with run on one Entity")
    void onTimerNeverRunsConcurrentlyWithRun() throws Exception {
        UnboundedOperativeInstance instance = instantiate(SessionOperative.class, "timers-session");
        ProcedureDispatcher dispatcher = instance.dispatcher(0);

        for (int i = 0; i < 8; i++) {
            dispatcher.fanoutEntities("alice", 100L);
        }
        long deadline = System.currentTimeMillis() + 10_000;
        while (SessionProcedure.FIRED.size() < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        Thread.sleep(100);
        assertThat(SessionProcedure.OVERLAPS)
                .as("run and onTimer must never be inside the same Entity at once")
                .isEmpty();
    }

    // ------------------------------------------------------- durability

    @Test
    @DisplayName("a crash before firing leaves the timer pending; after firing it does not re-fire")
    void theChangeLogFoldRebuildsExactlyWhatIsOwed() {
        PendingTimers pending = restore(timerOpsRecord(0, TimerOp.register(TimerKind.PROCESSING_TIME, 31_000)));
        assertThat(pending.all(TimerKind.PROCESSING_TIME)).containsExactly(31_000L);

        byte[] log = concat(
                timerOpsRecord(0, TimerOp.register(TimerKind.PROCESSING_TIME, 31_000)),
                timerOpsRecord(1, TimerOp.cancel(TimerKind.PROCESSING_TIME, 31_000)));
        assertThat(restore(log).isEmpty())
                .as("the fired timer is gone; nothing re-fires")
                .isTrue();
    }

    @Test
    @DisplayName("a firing's effects and its removal fold from ONE effect record")
    void aFiringIsAtomicWithItsEffects() {
        byte[] registered = timerOpsRecord(0, TimerOp.register(TimerKind.PROCESSING_TIME, 31_000));
        assertThat(restore(registered).all(TimerKind.PROCESSING_TIME))
                .as("a crash before the firing's record is durable leaves the timer owed")
                .containsExactly(31_000L);

        EffectRecord firing = new EffectRecord(
                List.of(),
                List.of(),
                OptionalLong.empty(),
                Optional.empty(),
                31_000L,
                List.of(TimerOp.cancel(TimerKind.PROCESSING_TIME, 31_000)));
        byte[] both = concat(registered, walRecord(1, HeaderTemplate.FORMAT_ID_EFFECT_RECORD, firing.encode()));
        assertThat(restore(both).isEmpty())
                .as("with the record durable, the removal landed with the effects")
                .isTrue();
        assertThat(firing.consumedOffset())
                .as("a firing consumes no input offset, so the watermark cannot advance on it")
                .isEmpty();
    }

    @Test
    @DisplayName("the per-Procedure counters follow the committed ops")
    void perProcedureCountersFollowTheCommittedOps() throws Exception {
        UnboundedOperativeInstance instance = instantiate(SurfaceOperative.class, "timers-surface");
        instance.dispatcher(0).fanoutEntities("register-twice", 100L);
        instance.dispatcher(0).fanoutEntities("cancel", 100L);
        instance.dispatcher(0).fanoutEntities("overflow", 100L);
        awaitDrain();

        TimerCounters counters = com.flezha.sdk.runtime.entity.TimerCounters.snapshot().stream()
                .filter(c -> c.procedure().equals("timers_surface_procedure"))
                .findFirst()
                .orElseThrow();
        assertThat(counters.registered().get()).isPositive();
        assertThat(counters.cancelled().get()).isPositive();
        assertThat(counters.refused().get()).as("the cap refusal is counted").isPositive();
    }

    @Test
    @DisplayName("timer ops round-trip through the postcard record body")
    void timerOpsRoundTripThroughTheirRecordBody() {
        List<TimerOp> ops =
                List.of(TimerOp.register(TimerKind.PROCESSING_TIME, 31_000), TimerOp.cancel(TimerKind.EVENT_TIME, 100));
        assertThat(TimerOp.decodeList(TimerOp.encodeList(ops))).isEqualTo(ops);
    }

    @Test
    @DisplayName("timer-op postcard bytes are the cross-language fixture")
    void timerOpBytesAreTheCrossLanguageFixture() {
        byte[] bytes = TimerOp.encodeList(List.of(
                TimerOp.register(TimerKind.PROCESSING_TIME, 31_000), TimerOp.cancel(TimerKind.EVENT_TIME, 100)));
        assertThat(bytes)
                .containsExactly(
                        0x02, // 2 ops
                        0x01, 0x98, 0xF2, 0x01, 0x01, // ProcessingTime, 31000, register
                        0x00, 0x64, 0x00); // EventTime, 100, cancel
    }

    // ---------------------------------------------------------- fixtures

    /** Registration surface + both refusals; cap of 3 so the bound is reachable. */
    public static final class SurfaceProcedure
            implements Procedure<String, Void, Void, NoChange, com.flezha.sdk.state.WallClock, Void> {

        static final java.util.concurrent.atomic.AtomicLong REGISTERED_AT =
                new java.util.concurrent.atomic.AtomicLong();

        @Override
        public String name() {
            return "timers_surface_procedure";
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
        public Class<com.flezha.sdk.state.WallClock> clockType() {
            return com.flezha.sdk.state.WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public int maxTimersPerEntity() {
            return 3;
        }

        @Override
        public Void initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return null;
        }

        @Override
        public void run(String input, StateHandle<Void, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            switch (input) {
                case "register-twice" -> {
                    // Both calls in ONE invocation, so both anchor on the same
                    // ctx.now(): pending timers are a SET, so the second is a
                    // no-op — which is only observable at one anchor.
                    long first = ctx.timerAfter(Duration.ofSeconds(30));
                    long second = ctx.timerAfter(Duration.ofSeconds(30));
                    ORDER.add(
                            first == ctx.now() + 30_000 && first == second
                                    ? "register-twice"
                                    : "register:UNEXPECTED " + first + "/" + second);
                    // The wall clock moves between invocations, so the cancel
                    // has to use the timestamp registration RETURNED — which is
                    // exactly the handle the surface promises.
                    REGISTERED_AT.set(first);
                }
                case "cancel" -> ORDER.add("cancel:" + ctx.cancelProcessingTimeTimer(REGISTERED_AT.get()));
                case "overflow" -> {
                    try {
                        for (int i = 0; i < 4; i++) {
                            ctx.timerAfter(Duration.ofSeconds(30 + i));
                        }
                        ORDER.add("NO REFUSAL");
                    } catch (ProcedureException.TimerCapExceeded e) {
                        ORDER.add(e.getMessage());
                    }
                }
                case "event-time" -> {
                    try {
                        ctx.timerAtEventTime(5_000);
                        ORDER.add("NO REFUSAL");
                    } catch (ProcedureException.EventTimeTimerWithoutEventTimeClock e) {
                        ORDER.add(e.getMessage());
                    }
                }
                default -> throw new ProcedureException("unknown command " + input);
            }
        }

        @Override
        public void onTimer(long firedAt, TimerKind kind, StateHandle<Void, NoChange> state, ProcedureCtx ctx) {
            ORDER.add("timer@" + firedAt);
        }
    }

    public static final class SurfaceOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "timers-surface";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(SurfaceProcedure.class);
        }
    }

    /** Event-time fixture: one input arms three timers ahead of itself. */
    public static final class WindowProcedure implements Procedure<Long, Void, Void, NoChange, EventTimeFrom, Void> {
        @Override
        public String name() {
            return "timers_window_procedure";
        }

        @Override
        public Class<Long> inputType() {
            return Long.class;
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
        public long eventTime(Long input) {
            return input;
        }

        @Override
        public boolean eventTimeOverridden() {
            return true;
        }

        @Override
        public Void initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return null;
        }

        @Override
        public void run(Long input, StateHandle<Void, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ORDER.add("run@" + input);
            if (input == 50L) {
                for (long at : new long[] {300L, 100L, 200L}) {
                    ctx.timerAtEventTime(at);
                }
            }
        }

        @Override
        public void onTimer(long firedAt, TimerKind kind, StateHandle<Void, NoChange> state, ProcedureCtx ctx) {
            ORDER.add("timer@" + firedAt);
        }
    }

    public static final class WindowOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "timers-window";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(Long.class, "in").into(WindowProcedure.class);
        }
    }

    /** The worked session-timeout shape: each input pushes the idle deadline out. */
    public static final class SessionProcedure
            implements Procedure<String, Void, Void, NoChange, com.flezha.sdk.state.WallClock, Void> {

        static final long TIMEOUT_MS = 60L;
        static final List<Long> FIRED = new CopyOnWriteArrayList<>();
        static final List<String> OVERLAPS = new CopyOnWriteArrayList<>();
        private static final java.util.concurrent.atomic.AtomicInteger INSIDE =
                new java.util.concurrent.atomic.AtomicInteger();

        private static void enter(String what) {
            if (INSIDE.getAndIncrement() != 0) {
                OVERLAPS.add(what);
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private static void leave() {
            INSIDE.decrementAndGet();
        }

        @Override
        public String name() {
            return "timers_session_procedure";
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
        public Class<com.flezha.sdk.state.WallClock> clockType() {
            return com.flezha.sdk.state.WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public Void initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return null;
        }

        @Override
        public void run(String input, StateHandle<Void, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            enter("run");
            try {
                ORDER.add("run:" + input);
                ctx.timerAfter(Duration.ofMillis(TIMEOUT_MS));
            } finally {
                leave();
            }
        }

        @Override
        public void onTimer(long firedAt, TimerKind kind, StateHandle<Void, NoChange> state, ProcedureCtx ctx) {
            enter("onTimer");
            try {
                FIRED.add(firedAt);
                ORDER.add("timer:" + firedAt);
            } finally {
                leave();
            }
        }
    }

    public static final class SessionOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "timers-session";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(SessionProcedure.class);
        }
    }

    // ----------------------------------------------------------- helpers

    private static PendingTimers restore(byte[] changeLog) {
        EntityStateRestore.Rebuilt rebuilt = EntityStateRestore.rebuild(
                new SurfaceProcedure(),
                new KryoCodec(List.of()),
                Optional.empty(),
                new byte[0],
                changeLog,
                new HeaderTemplate());
        return rebuilt.timers();
    }

    private static byte[] timerOpsRecord(long seq, TimerOp... ops) {
        return walRecord(seq, HeaderTemplate.FORMAT_ID_TIMER_OPS, TimerOp.encodeList(List.of(ops)));
    }

    /** Controller on-disk WAL framing: {@code [seq][len][header][body][crc]}. */
    private static byte[] walRecord(long seq, byte formatId, byte[] body) {
        byte[] header = new HeaderTemplate().stamp(1, formatId, body);
        CRC32 crc = new CRC32();
        crc.update(header);
        crc.update(body);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bos)) {
            out.writeLong(seq);
            out.writeInt(16 + body.length);
            out.write(header);
            out.write(body);
            out.writeInt((int) crc.getValue());
            return bos.toByteArray();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }
}
