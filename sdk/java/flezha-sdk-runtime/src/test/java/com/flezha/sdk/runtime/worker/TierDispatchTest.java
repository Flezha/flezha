package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeRegistry;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.state.DlqPark;
import com.flezha.sdk.runtime.state.EffectRecord;
import com.flezha.sdk.runtime.state.EffectRecordCapture;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
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
 * The effectively-once tier's dispatch contract: one invocation produces ONE
 * effect record carrying its changes, its outputs and the offset of the input it
 * consumed — and, when that input is poison, its dead-letter entry too.
 *
 * <p>The atomicity is the point. A crash must never be able to advance the
 * consumed offset while losing the state or the output it was advanced for, so
 * all of it has to reach the controller in a single record.
 */
class TierDispatchTest {

    private static final List<byte[]> PLUGIN_WRITES = new CopyOnWriteArrayList<>();

    private final EffectRecordCapture shipped = new EffectRecordCapture();
    private ExecutorContext ctx;

    @BeforeEach
    void reset() {
        PLUGIN_WRITES.clear();
        UnboundedOperativeRegistry.reset();
        shipped.install();
        OperationDispatcher.setWireWriter((handle, streamId, frame) -> {
            PLUGIN_WRITES.add(frame);
            return 0;
        });
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ctx = new ExecutorContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));
        ctx.streamRegistry().register(0, 100L, 200L);
    }

    @AfterEach
    void close() {
        OperationDispatcher.resetWireWriter();
        if (ctx != null && ctx.workerPool() != null) {
            ctx.workerPool().shutdownNow();
        }
    }

    /** Dispatch one input, then wait for the background flusher to ship its record. */
    private void feed(Class<? extends UnboundedOperative> cls, String factoryId, String input, long offset)
            throws Exception {
        UnboundedOperativeRegistry.registerUnbounded(cls);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology(factoryId);
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, factoryId + "-inst", ctx, ctx.workerPool());
        instance.dispatcher(0).fanoutSegments(input, 100L, OptionalLong.of(offset));
        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        shipped.awaitRecords(1);
    }

    @Test
    @DisplayName("one invocation ships ONE effect record: its changes, its output, its consumed offset")
    void oneInvocationShipsOneEffectRecord() throws Exception {
        feed(TierOperative.class, "tier-dispatch", "BTC", 7L);

        EffectRecord record = shipped.only();

        assertThat(record.changes()).hasSize(2);
        assertThat(record.outputs()).hasSize(1);
        assertThat(record.outputs().get(0).channel()).isEqualTo("out");
        assertThat(record.outputs().get(0).key()).isEqualTo("BTC");
        assertThat(record.consumedOffset()).hasValue(7L);
        assertThat(record.dlq()).isEmpty();

        // The output was STAGED into the record, not shipped as its own plugin
        // frame — that is what makes state, output and offset one atomic record.
        assertThat(PLUGIN_WRITES).isEmpty();
    }

    @Test
    @DisplayName("a poison input parks INSIDE its effect record, alongside the offset advance")
    void poisonParksInsideTheEffectRecord() throws Exception {
        feed(PoisonOperative.class, "tier-poison", "bad", 9L);

        EffectRecord record = shipped.only();

        // The park and the consumed-offset advance are ONE record: a crash
        // cannot acknowledge the poison input while losing the evidence of why.
        assertThat(record.dlq()).isPresent();
        assertThat(record.dlq().get().error()).contains("boom");
        assertThat(record.consumedOffset()).hasValue(9L);

        // No standalone park_payload frame under the tier.
        assertThat(shipped.shipped(DlqPark.PAYLOAD_ID_PARK)).isFalse();
    }
    // --- fixtures -----------------------------------------------------------

    public static final class TierOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "tier-dispatch";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.input(String.class, "in").into(TierOp.class);
            c.channel(String.class, "out").from(TierOp.class).keyed().toPlugin("sink");
        }
    }

    public static final class PoisonOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "tier-poison";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.input(String.class, "in").retryMax(1).into(PoisonOp.class);
            c.channel(String.class, "out").from(PoisonOp.class).keyed().toPlugin("sink");
        }
    }

    /** A counter, so the invocation has real journaled state to carry. */
    public static final class CountState {
        public int n;
    }

    public record Inc(int by) implements Change {
        @Override
        public Optional<CoalesceKey> coalesceKey() {
            return Optional.empty();
        }
    }

    abstract static class Base implements Operation<String, CountState, String, Inc, WallClock, Void> {
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
        public Class<WallClock> clockType() {
            return WallClock.class;
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
            state.n += change.by();
        }

        @Override
        public CountState initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new CountState();
        }
    }

    public static final class TierOp extends Base {
        @Override
        public String name() {
            return "tier_op";
        }

        @Override
        public void run(String input, StateHandle<CountState, Inc> state, OperationCtx ctx) throws OperationException {
            state.apply(new Inc(1));
            state.apply(new Inc(2));
            ctx.emitKeyed(input, "payload");
        }
    }

    public static final class PoisonOp extends Base {
        @Override
        public String name() {
            return "poison_op";
        }

        @Override
        public void run(String input, StateHandle<CountState, Inc> state, OperationCtx ctx) throws OperationException {
            throw new OperationException("boom");
        }
    }
}
