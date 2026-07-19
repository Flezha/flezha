package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Output identity: the {@code flz.segment} / {@code flz.seq} / {@code flz.key}
 * stamps a keyed emit carries, the byte-identical layout a non-keyed emit keeps,
 * and the determinism surface ({@code ctx.now()} / {@code ctx.stableId()}).
 *
 * <p>Java port of the Rust {@code operative_dispatch.rs} identity-stamp cases.
 */
class OutputIdentityTest {

    private static final List<String> NOW_SEEN = new CopyOnWriteArrayList<>();
    private static final List<String> IDS_SEEN = new CopyOnWriteArrayList<>();
    private static final List<String> EMIT_OUTCOMES = new CopyOnWriteArrayList<>();

    private final List<byte[]> capturedWrites = new CopyOnWriteArrayList<>();
    private RunnerContext ctx;

    @BeforeEach
    void reset() {
        NOW_SEEN.clear();
        IDS_SEEN.clear();
        EMIT_OUTCOMES.clear();
        capturedWrites.clear();
        UnboundedOperativeClassRegistry.reset();

        OperationDispatcher.setWireWriter((handle, streamId, frame) -> {
            capturedWrites.add(frame);
            return 0;
        });

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ctx = new RunnerContext("ns", cfg, 0L);
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

    private UnboundedOperativeInstance instantiate(Class<? extends UnboundedOperative> cls, String factoryId) {
        UnboundedOperativeClassRegistry.registerClass(cls);
        UnboundedOperativeTopology topology = UnboundedOperativeClassRegistry.getTopology(factoryId);
        return UnboundedOperativeInstance.create(topology, factoryId + "-instance", ctx, ctx.workerPool());
    }

    private void feed(UnboundedOperativeInstance instance, Object input) throws Exception {
        instance.dispatcher(0).fanoutSegments(input, 100L);
        ctx.workerPool().shutdown();
        assertThat(ctx.workerPool().awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    private List<Map<String, String>> capturedMetadata() {
        FrameParser parser = new FrameParser();
        List<Map<String, String>> out = new ArrayList<>();
        for (byte[] write : capturedWrites) {
            for (Frame f : parser.feed(write)) {
                out.add(f.metadata());
            }
        }
        return out;
    }

    @Test
    @DisplayName("a keyed emit stamps flz.segment, flz.seq and flz.key")
    void keyedEmitCarriesIdentityStamps() throws Exception {
        UnboundedOperativeInstance instance = instantiate(KeyedOperative.class, "identity-keyed");
        feed(instance, "BTC");

        List<Map<String, String>> metas = capturedMetadata();
        assertThat(metas).hasSize(1);
        Map<String, String> meta = metas.get(0);

        assertThat(meta).containsEntry("channel", "out");
        assertThat(meta).containsEntry("flz.key", "BTC");
        // instance / operation / segmentKey — the segment key is empty for a
        // single-mode operation, matching the controller's identity encoding.
        assertThat(meta.get("flz.segment")).isEqualTo("identity-keyed-instance/keyed_op/");
        // <changeSeq>.<emitOrdinal> — first emit of a stateless op.
        assertThat(meta).containsEntry("flz.seq", "0.0");
    }

    @Test
    @DisplayName("a non-keyed emit's frame stays byte-identical to the pre-change layout")
    void nonKeyedEmitIsUnstamped() throws Exception {
        UnboundedOperativeInstance instance = instantiate(PlainOperative.class, "identity-plain");
        feed(instance, "hello");

        List<Map<String, String>> metas = capturedMetadata();
        assertThat(metas).hasSize(1);
        // Exactly the pre-change metadata: the channel name and nothing else.
        // A non-keyed channel gains no stamps, so existing wire consumers see
        // byte-identical frames.
        assertThat(metas.get(0)).containsExactlyEntriesOf(Map.of("channel", "out"));
    }

    @Test
    @DisplayName("ctx.now() is constant within an invocation and ctx.stableId() is ordinal-distinct")
    void determinismSurface() throws Exception {
        UnboundedOperativeInstance instance = instantiate(DeterminismOperative.class, "identity-determinism");
        feed(instance, "x");

        // now() served three times from one evaluation.
        assertThat(NOW_SEEN).hasSize(3);
        assertThat(NOW_SEEN).containsOnly(NOW_SEEN.get(0));

        // stableId() differs per call within the invocation...
        assertThat(IDS_SEEN).hasSize(2);
        assertThat(IDS_SEEN.get(0)).isNotEqualTo(IDS_SEEN.get(1));
        // ...and is a 32-hex-digit rendering, matching Rust's {:032x}.
        assertThat(IDS_SEEN.get(0)).matches("[0-9a-f]{32}");
    }

    @Test
    @DisplayName("a user metadata key in the reserved flz. namespace is rejected, not dropped")
    void reservedMetadataKeyIsRejected() throws Exception {
        UnboundedOperativeInstance instance = instantiate(ReservedMetaOperative.class, "identity-reserved");
        feed(instance, "x");

        // The emit was refused, so no frame reached the wire — a stamp can be
        // neither spoofed nor clobbered.
        assertThat(EMIT_OUTCOMES).containsExactly("ReservedMetadataKey:flz.key");
        assertThat(capturedWrites).isEmpty();
    }

    @Test
    @DisplayName("a plain emit on a keyed channel is refused")
    void plainEmitOnKeyedChannelIsRefused() throws Exception {
        UnboundedOperativeInstance instance = instantiate(PlainEmitOnKeyedOperative.class, "identity-plain-on-keyed");
        feed(instance, "x");

        assertThat(EMIT_OUTCOMES).containsExactly("KeyRequiredOnKeyedChannel:out");
        assertThat(capturedWrites).isEmpty();
    }

    // --- fixtures -----------------------------------------------------------

    public static final class KeyedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "identity-keyed";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(KeyedOp.class);
            c.channel(String.class, "out").from(KeyedOp.class).keyed().toPlugin("sink");
        }
    }

    public static final class PlainOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "identity-plain";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(PlainOp.class);
            c.channel(String.class, "out").from(PlainOp.class).toPlugin("sink");
        }
    }

    public static final class DeterminismOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "identity-determinism";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(DeterminismOp.class);
        }
    }

    public static final class ReservedMetaOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "identity-reserved";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(ReservedMetaOp.class);
            c.channel(String.class, "out").from(ReservedMetaOp.class).toPlugin("sink");
        }
    }

    public static final class PlainEmitOnKeyedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "identity-plain-on-keyed";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(PlainEmitOnKeyedOp.class);
            c.channel(String.class, "out")
                    .from(PlainEmitOnKeyedOp.class)
                    .keyed()
                    .toPlugin("sink");
        }
    }

    public record Unit() {}

    abstract static class Base implements Operation<String, Unit, String, NoChange, WallClock, Void> {
        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public Class<NoChange> changeType() {
            return NoChange.class;
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
        public Unit initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new Unit();
        }
    }

    public static final class KeyedOp extends Base {
        @Override
        public String name() {
            return "keyed_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emitKeyed(input, "payload");
        }
    }

    public static final class PlainOp extends Base {
        @Override
        public String name() {
            return "plain_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit("payload");
        }
    }

    public static final class DeterminismOp extends Base {
        @Override
        public String name() {
            return "determinism_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) {
            NOW_SEEN.add(String.valueOf(ctx.now()));
            NOW_SEEN.add(String.valueOf(ctx.now()));
            NOW_SEEN.add(String.valueOf(ctx.now()));
            IDS_SEEN.add(ctx.stableId().toString());
            IDS_SEEN.add(ctx.stableId().toString());
        }
    }

    public static final class ReservedMetaOp extends Base {
        @Override
        public String name() {
            return "reserved_meta_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) {
            try {
                ctx.emitWithMeta("payload", Map.of("flz.key", "spoofed"));
                EMIT_OUTCOMES.add("ok");
            } catch (OperationException.ReservedMetadataKey e) {
                EMIT_OUTCOMES.add("ReservedMetadataKey:" + e.key());
            } catch (OperationException e) {
                EMIT_OUTCOMES.add("other:" + e);
            }
        }
    }

    public static final class PlainEmitOnKeyedOp extends Base {
        @Override
        public String name() {
            return "plain_on_keyed_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) {
            try {
                ctx.emit("payload");
                EMIT_OUTCOMES.add("ok");
            } catch (OperationException.KeyRequiredOnKeyedChannel e) {
                EMIT_OUTCOMES.add("KeyRequiredOnKeyedChannel:" + e.channel());
            } catch (OperationException e) {
                EMIT_OUTCOMES.add("other:" + e);
            }
        }
    }
}
