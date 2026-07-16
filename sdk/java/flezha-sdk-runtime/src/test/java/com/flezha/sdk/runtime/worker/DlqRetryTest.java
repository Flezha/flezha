package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.RetryPolicy;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeRegistry;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.operative.StatelessTestOperation;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.state.DlqPark;
import com.flezha.sdk.runtime.state.SegmentStateSink;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.StateHandle;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Java mirror of {@code sdk/rust/tests/dlq_retry.rs}: retry-then-park
 * dispatcher semantics ({@code platform-dead-letter-queue}) —
 * transient-recovers, non-retryable-parks-immediately, panic-parity.
 */
class DlqRetryTest {

    private static final AtomicInteger TRANSIENT_ATTEMPTS = new AtomicInteger();
    private static final AtomicInteger TRANSIENT_OK = new AtomicInteger();
    private static final AtomicInteger NON_RETRYABLE_RUNS = new AtomicInteger();
    private static final AtomicInteger NON_RETRYABLE_OK = new AtomicInteger();
    private static final AtomicInteger PANIC_RUNS = new AtomicInteger();
    private static final AtomicInteger PANIC_OK = new AtomicInteger();

    /** Park bodies captured by the auto-acking sink, decoded from the wire. */
    private static final List<ParkedEntry> PARKED = new CopyOnWriteArrayList<>();

    private RunnerContext ctx;

    record ParkedEntry(
            String parkId, String channel, OptionalLong originOffset, String error, int attempts, byte[] payload) {}

    @BeforeEach
    void reset() {
        TRANSIENT_ATTEMPTS.set(0);
        TRANSIENT_OK.set(0);
        NON_RETRYABLE_RUNS.set(0);
        NON_RETRYABLE_OK.set(0);
        PANIC_RUNS.set(0);
        PANIC_OK.set(0);
        PARKED.clear();
        UnboundedOperativeRegistry.reset();

        // Auto-acking sink: capture the park body and ack from another
        // thread — the dispatch thread is blocked in parkBlocking while
        // this write returns (the controller's fsync-then-ack, minus the
        // controller).
        SegmentStateSink.installGlobalSink((payloadId, frame) -> {
            if (DlqPark.PAYLOAD_ID_PARK.equals(payloadId)) {
                ParkedEntry entry = decodeParkFrame(frame);
                if (PARKED.stream().noneMatch(p -> p.parkId().equals(entry.parkId()))) {
                    PARKED.add(entry);
                }
                new Thread(() -> DlqPark.completeAck(entry.parkId())).start();
            }
            return true;
        });

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "2"));
        ctx = new RunnerContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(2));
        ctx.streamRegistry().register(0, 100L, 200L);
    }

    @AfterEach
    void close() {
        if (ctx != null && ctx.workerPool() != null) {
            ctx.workerPool().shutdownNow();
        }
    }

    private UnboundedOperativeInstance instantiate(
            Class<? extends UnboundedOperative> operativeClass, String factoryId) {
        UnboundedOperativeRegistry.registerUnbounded(operativeClass);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology(factoryId);
        return UnboundedOperativeInstance.create(topology, factoryId + "-instance", ctx, ctx.workerPool());
    }

    private void feed(UnboundedOperativeInstance instance, String operationName, Object input) {
        int operationId = instance.topology().operationNameToId().get(operationName);
        instance.dispatcher(operationId).fanoutSegments(input, 100L);
    }

    private static boolean waitFor(java.util.function.BooleanSupplier predicate, long millis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            if (predicate.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return predicate.getAsBoolean();
    }

    // ---------- transient failure recovers within the retry budget ----------

    @Test
    @DisplayName("transient failure recovers within the retry budget; nothing parked")
    void transientFailureRecoversWithinRetryBudget() {
        long retriesBefore = DlqPark.RETRIES_TOTAL.get();
        long parkedBefore = DlqPark.PARKED_TOTAL.get();
        UnboundedOperativeInstance instance = instantiate(TransientOperative.class, "dlq_transient");

        feed(instance, "dlq_transient_operation", "p");

        assertThat(waitFor(() -> TRANSIENT_OK.get() == 1, 5_000))
                .as("third attempt should succeed")
                .isTrue();
        assertThat(TRANSIENT_ATTEMPTS.get()).isEqualTo(3);
        assertThat(DlqPark.RETRIES_TOTAL.get() - retriesBefore)
                .as("attempt telemetry records the two retries")
                .isEqualTo(2);
        assertThat(DlqPark.PARKED_TOTAL.get()).as("no payload parked").isEqualTo(parkedBefore);
        assertThat(PARKED).isEmpty();
    }

    // ---------- non-retryable parks immediately ----------

    @Test
    @DisplayName("NonRetryable parks immediately and the segment continues")
    void nonRetryableParksImmediately() {
        UnboundedOperativeInstance instance = instantiate(NonRetryableOperative.class, "dlq_nonretry");

        feed(instance, "dlq_nonretry_operation", "poison");
        feed(instance, "dlq_nonretry_operation", "healthy");

        assertThat(waitFor(() -> NON_RETRYABLE_OK.get() == 1, 5_000))
                .as("segment must continue past the parked payload")
                .isTrue();
        assertThat(PARKED).hasSize(1);
        ParkedEntry entry = PARKED.get(0);
        assertThat(entry.attempts()).as("no further attempts").isEqualTo(1);
        assertThat(entry.channel()).isEqualTo("input");
        assertThat(entry.payload()).isEqualTo("poison".getBytes(StandardCharsets.UTF_8));
        assertThat(entry.error()).contains("bad record");
        assertThat(NON_RETRYABLE_RUNS.get())
                .as("poison ran once, healthy ran once")
                .isEqualTo(2);
    }

    // ---------- panic parity ----------

    @Test
    @DisplayName("RuntimeException exhausts the budget, parks, and the segment continues")
    void panicParityExhaustsBudgetThenParksAndContinues() {
        UnboundedOperativeInstance instance = instantiate(PanickyOperative.class, "dlq_panicky");

        feed(instance, "dlq_panicky_operation", "poison");
        feed(instance, "dlq_panicky_operation", "healthy");

        assertThat(waitFor(() -> PANIC_OK.get() == 1, 5_000))
                .as("segment must continue past the parked payload — never a silent drop")
                .isTrue();
        assertThat(PARKED)
                .as("parked exactly as a thrown OperationException would be")
                .hasSize(1);
        ParkedEntry entry = PARKED.get(0);
        assertThat(entry.attempts())
                .as("RuntimeException is retryable by default")
                .isEqualTo(3);
        assertThat(entry.error()).contains("kaboom");
        assertThat(entry.payload()).isEqualTo("poison".getBytes(StandardCharsets.UTF_8));
        assertThat(PANIC_RUNS.get())
                .as("poison ran 3 attempts, healthy ran once")
                .isEqualTo(4);
    }

    // ---------- backoff shape ----------

    @Test
    @DisplayName("backoff grows exponentially with jitter and respects the cap")
    void backoffGrowsExponentiallyWithJitterAndCap() {
        for (int i = 0; i < 100; i++) {
            long first = new RetryPolicy(3, 50).backoffFor(1);
            assertThat(first).isBetween(25L, 75L);
            long second = new RetryPolicy(3, 50).backoffFor(2);
            assertThat(second).isBetween(50L, 150L);
            long capped = new RetryPolicy(3, 5_000).backoffFor(10);
            assertThat(capped).isLessThanOrEqualTo(5_000L);
        }
    }

    // ---------- cross-language postcard fixture ----------

    @Test
    @DisplayName("ParkBody postcard bytes match the Rust/controller fixture")
    void parkBodyPostcardBytesAreTheCrossLanguageFixture() {
        // Shared fixture with sdk/rust/src/segment/park.rs
        // `park_body_postcard_bytes_are_the_cross_language_fixture` and the
        // controller's decode; an encoding change breaks loudly here.
        byte[] bytes = DlqPark.encodeParkBody(new DlqPark.ParkBody(
                "s#0", OptionalLong.of(7), new DlqPark.DlqEntry("in", "boom", 3, Optional.of(new byte[] {1, 2}))));
        assertThat(bytes).isEqualTo(new byte[] {
            0x03, 's', '#', '0', // park_id "s#0"
            0x01, 0x07, // Some(7)
            0x02, 'i', 'n', // entry.channel "in"
            0x04, 'b', 'o', 'o', 'm', // entry.error "boom"
            0x03, // entry.attempts 3
            0x01, 0x02, 0x01, 0x02, // entry.payload Some([1,2])
        });
    }

    // ---------- wire decode helpers ----------

    /**
     * Parse the outer segment frame
     * ({@code [pid_len:u16][pid][seq:u64][header:16][body_len:u32][body][crc:u32]})
     * and postcard-decode the {@code ParkBody}.
     */
    private static ParkedEntry decodeParkFrame(byte[] frame) {
        ByteBuffer buf = ByteBuffer.wrap(frame);
        int pidLen = Short.toUnsignedInt(buf.getShort());
        buf.position(buf.position() + pidLen + 8 + 16);
        int bodyLen = buf.getInt();
        byte[] body = new byte[bodyLen];
        buf.get(body);

        ByteBuffer b = ByteBuffer.wrap(body);
        String parkId = readString(b);
        OptionalLong originOffset = b.get() == 0 ? OptionalLong.empty() : OptionalLong.of(readVarint(b));
        String channel = readString(b);
        String error = readString(b);
        int attempts = (int) readVarint(b);
        byte[] payload = b.get() == 0 ? null : readBytes(b);
        return new ParkedEntry(parkId, channel, originOffset, error, attempts, payload);
    }

    private static long readVarint(ByteBuffer b) {
        long value = 0;
        int shift = 0;
        while (true) {
            byte x = b.get();
            value |= (long) (x & 0x7F) << shift;
            if ((x & 0x80) == 0) {
                return value;
            }
            shift += 7;
        }
    }

    private static byte[] readBytes(ByteBuffer b) {
        int len = (int) readVarint(b);
        byte[] out = new byte[len];
        b.get(out);
        return out;
    }

    private static String readString(ByteBuffer b) {
        return new String(readBytes(b), StandardCharsets.UTF_8);
    }

    // =========================================================================
    // Test topologies. Operations are re-instantiated reflectively, so they
    // close over the static counters above.
    // =========================================================================

    public static final class TransientOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "dlq_transient";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").retryBackoffMs(1).into(TransientOperation.class);
        }
    }

    public static final class TransientOperation extends StatelessTestOperation<String, Object, Void> {
        @Override
        public String name() {
            return "dlq_transient_operation";
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
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) throws OperationException {
            if (TRANSIENT_ATTEMPTS.getAndIncrement() < 2) {
                throw new OperationException("transient");
            }
            TRANSIENT_OK.incrementAndGet();
        }
    }

    public static final class NonRetryableOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "dlq_nonretry";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").retryBackoffMs(1).into(NonRetryableOperation.class);
        }
    }

    public static final class NonRetryableOperation extends StatelessTestOperation<String, Object, Void> {
        @Override
        public String name() {
            return "dlq_nonretry_operation";
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
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) throws OperationException {
            NON_RETRYABLE_RUNS.incrementAndGet();
            if ("poison".equals(input)) {
                throw new OperationException.NonRetryable("bad record");
            }
            NON_RETRYABLE_OK.incrementAndGet();
        }
    }

    public static final class PanickyOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "dlq_panicky";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").retryBackoffMs(1).into(PanickyOperation.class);
        }
    }

    public static final class PanickyOperation extends StatelessTestOperation<String, Object, Void> {
        @Override
        public String name() {
            return "dlq_panicky_operation";
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
        protected Object initState(Optional<String> operationKey) {
            return new Object();
        }

        @Override
        public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) {
            PANIC_RUNS.incrementAndGet();
            if ("poison".equals(input)) {
                throw new RuntimeException("kaboom");
            }
            PANIC_OK.incrementAndGet();
        }
    }
}
