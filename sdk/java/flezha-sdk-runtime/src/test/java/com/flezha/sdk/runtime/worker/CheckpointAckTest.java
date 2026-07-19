package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Checkpoint ack-granularity: the consumed offset is withheld until
 * {@code ctx.checkpoint()} closes the interval.
 *
 * <p>The interval is what a crash replays. Holding it open means the Segment
 * never persists a half-built view of state that is only coherent at a boundary;
 * the cost is that the controller must retain the interval's inputs.
 */
class CheckpointAckTest {

    private RunnerContext ctx;
    private final EffectRecordCapture shipped = new EffectRecordCapture();

    @BeforeEach
    void reset() {
        UnboundedOperativeClassRegistry.reset();
        shipped.install();
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

    /** Dispatch inputs 0..n-1 and return the Segment, WITHOUT letting the flusher drain. */
    private SegmentSlot feedAll(Class<? extends UnboundedOperative> cls, String factoryId, List<String> inputs)
            throws Exception {
        UnboundedOperativeClassRegistry.registerClass(cls);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology(factoryId);
        UnboundedOperativeInstance instance =
                UnboundedOperativeInstance.create(topology, factoryId + "-inst", ctx, ctx.workerPool());
        for (int i = 0; i < inputs.size(); i++) {
            instance.dispatcher(0).fanoutSegments(inputs.get(i), 100L, OptionalLong.of(i));
        }
        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        return instance.segments().get(0).get(0);
    }

    @Test
    @DisplayName("an open interval holds the consumed offset and stages its changes")
    void openIntervalHoldsTheOffset() throws Exception {
        // Three plain inputs; none says "checkpoint".
        SegmentSlot slot = feedAll(CheckpointOperative.class, "ckpt", List.of("a", "b", "c"));

        // Nothing acknowledgeable was produced: no shipped record carries an offset.
        assertThat(shipped.records()).noneMatch(r -> r.consumedOffset().isPresent());
        // The interval's changes are staged, not flushed. (stagedChanges is safe
        // to read: the flusher deliberately never drains it — an open interval is
        // not flushable.)
        assertThat(slot.stagedChanges()).hasSize(3);
    }

    @Test
    @DisplayName("checkpoint() closes the interval: one record carries the whole interval and the offset")
    void checkpointClosesTheInterval() throws Exception {
        // The 4th input (offset 3) says "commit" → close.
        SegmentSlot slot = feedAll(CheckpointOperative.class, "ckpt", List.of("a", "b", "c", "commit"));

        assertThat(slot.stagedChanges())
                .as("the interval closed, so nothing stays staged")
                .isEmpty();

        shipped.awaitRecords(1);
        List<EffectRecord> closing = shipped.records().stream()
                .filter(r -> r.consumedOffset().isPresent())
                .toList();
        assertThat(closing).hasSize(1);
        // All four invocations' changes ride the ONE closing record...
        assertThat(closing.get(0).changes()).hasSize(4);
        // ...and it acknowledges the offset of the input that closed it.
        assertThat(closing.get(0).consumedOffset()).hasValue(3L);
    }

    @Test
    @DisplayName("a park closes the interval, so the poison offset is never acked ahead of its state")
    void parkClosesTheInterval() throws Exception {
        SegmentSlot slot = feedAll(PoisonCheckpointOperative.class, "ckpt-poison", List.of("a", "b", "poison"));

        shipped.awaitRecords(1);
        List<EffectRecord> closing =
                shipped.records().stream().filter(r -> r.dlq().isPresent()).toList();
        assertThat(closing).hasSize(1);

        // The park record carries the interval's staged changes AND the poison
        // offset. Were the interval left open, the poison offset would be
        // acknowledged while the state built before it stayed non-durable.
        assertThat(closing.get(0).changes()).hasSize(2);
        assertThat(closing.get(0).consumedOffset()).hasValue(2L);
        assertThat(slot.stagedChanges()).isEmpty();
    }

    @Test
    @DisplayName("checkpointAck without the tier is refused")
    void checkpointAckWithoutTierIsRefused() {
        assertThatThrownBy(() -> UnboundedOperativeClassRegistry.registerClass(NoTierOperative.class))
                .isInstanceOf(UnboundedOperativeConfigurationException.class)
                .hasMessageContaining("CheckpointAckWithoutTier");
    }

    // --- fixtures -----------------------------------------------------------

    public static final class CheckpointOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "ckpt";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.checkpointAck();
            c.input(String.class, "in").into(CheckpointOp.class);
        }
    }

    public static final class PoisonCheckpointOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "ckpt-poison";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.checkpointAck();
            c.input(String.class, "in").retryMax(1).into(PoisonCheckpointOp.class);
        }
    }

    /** checkpointAck() without effectivelyOnce(): no offset exists to withhold. */
    public static final class NoTierOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "ckpt-no-tier";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.checkpointAck();
            c.input(String.class, "in").into(CheckpointOp.class);
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
            state.n++;
        }

        @Override
        public CountState initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new CountState();
        }
    }

    public static final class CheckpointOp extends Base {
        @Override
        public String name() {
            return "ckpt_op";
        }

        @Override
        public void run(String input, StateHandle<CountState, Inc> state, OperationCtx ctx) throws OperationException {
            state.apply(new Inc());
            if ("commit".equals(input)) {
                ctx.checkpoint();
            }
        }
    }

    public static final class PoisonCheckpointOp extends Base {
        @Override
        public String name() {
            return "ckpt_poison_op";
        }

        @Override
        public void run(String input, StateHandle<CountState, Inc> state, OperationCtx ctx) throws OperationException {
            if ("poison".equals(input)) {
                throw new OperationException("boom");
            }
            state.apply(new Inc());
        }
    }
}
