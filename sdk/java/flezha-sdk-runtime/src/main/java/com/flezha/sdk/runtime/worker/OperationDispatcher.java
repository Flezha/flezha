package com.flezha.sdk.runtime.worker;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.OperationDescriptor;
import com.flezha.sdk.runtime.operative.PluginSink;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.segment.SegmentSlot;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.StateHandle;
import java.lang.ref.WeakReference;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-Operation dispatcher. Caches the producer-edge
 * {@code outputChannelId} at build time so {@code ctx.emit(...)} pays no
 * channel-name lookup on the hot path.
 *
 * <p>One {@link OperationDispatcher} per Operation×Segment (held by
 * {@link SegmentSlot#dispatcher()}). The Operation instance is held inside
 * this dispatcher to keep mutable per-Operation scratch (if any) per-segment.
 */
public final class OperationDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(OperationDispatcher.class);

    /**
     * Indirection over {@link com.flezha.sdk.native_.NativeTransport#streamWrite(long, long, byte[])}
     * so dispatcher integration tests can observe plugin-wire emissions
     * without mocking the {@code native} static (mockito-inline cannot
     * intercept native methods).
     */
    @FunctionalInterface
    public interface WireWriter {
        int write(long handle, long streamId, byte[] frame);
    }

    private static volatile WireWriter WIRE_WRITER = com.flezha.sdk.native_.NativeTransport::streamWrite;

    /** Replace the wire-writer hook. Tests only; production code does not call this. */
    public static void setWireWriter(@Nonnull WireWriter writer) {
        WIRE_WRITER = writer;
    }

    /** Restore the default {@link com.flezha.sdk.native_.NativeTransport#streamWrite} writer. */
    public static void resetWireWriter() {
        WIRE_WRITER = com.flezha.sdk.native_.NativeTransport::streamWrite;
    }

    private final ExecutorContext ctx;
    private final ExecutorService pool;
    private final WeakReference<UnboundedOperativeInstance> instance;
    private final UnboundedOperativeTopology topology;
    private final int operationId;
    private final OperationDescriptor operationDescriptor;
    private final Optional<ChannelDescriptor> outputChannelDescriptor;
    private final OptionalInt outputChannelId;

    @SuppressWarnings("rawtypes")
    private final Operation operationInstance;

    @SuppressWarnings("unchecked")
    public OperationDispatcher(
            @Nonnull ExecutorContext ctx,
            @Nonnull ExecutorService pool,
            @Nonnull UnboundedOperativeInstance instance,
            @Nonnull UnboundedOperativeTopology topology,
            int operationId,
            @Nonnull Operation<?, ?, ?, ?, ?, ?> operationInstance) {
        this.ctx = ctx;
        this.pool = pool;
        this.instance = new WeakReference<>(instance);
        this.topology = topology;
        this.operationId = operationId;
        this.operationDescriptor = topology.operation(operationId);
        this.outputChannelId = operationDescriptor.outputChannelId();
        this.outputChannelDescriptor = outputChannelId.isPresent()
                ? Optional.ofNullable(topology.channel(outputChannelId.getAsInt()))
                : Optional.empty();
        this.operationInstance = operationInstance;
    }

    public int operationId() {
        return operationId;
    }

    /**
     * The per-Segment {@link Operation} instance held by this dispatcher. Used by
     * the warm-restore path to call {@code initState} / {@code applyChange} when
     * rebuilding a Segment's state from the controller's change-log.
     */
    @Nonnull
    @SuppressWarnings("rawtypes")
    public Operation operationInstance() {
        return operationInstance;
    }

    public OperationDescriptor descriptor() {
        return operationDescriptor;
    }

    /**
     * Enqueue an input on the segments targeted by this Operation. Single-mode
     * operations skip the {@link Operation#segmentFor(Object) segmentFor} call
     * and route to segment index 0; static-mode operations call
     * {@code segmentFor} and fan out across the matching segments.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void fanoutSegments(@Nonnull Object value, long primaryStreamId) {
        UnboundedOperativeInstance ri = instance.get();
        if (ri == null) {
            return;
        }
        List<SegmentSlot> segments = ri.segments().get(operationId);
        if (operationDescriptor.segmentKeys().isEmpty()) {
            SegmentSlot slot = segments.get(0);
            enqueue(slot, value, primaryStreamId);
            return;
        }
        // Operation.Input is erased at this boundary; topology wiring guarantees value's runtime type matches.
        List<String> keys = operationInstance.segmentFor(value);
        Map<String, Integer> keyToIdx = ri.segmentKeyToIdx().get(operationId);
        for (String key : keys) {
            Integer idx = keyToIdx.get(key);
            if (idx != null) {
                enqueue(segments.get(idx), value, primaryStreamId);
            }
        }
    }

    private void enqueue(SegmentSlot slot, Object value, long primaryStreamId) {
        boolean shouldDrain = slot.mailbox().push(new InputEnvelope(value, primaryStreamId));
        if (shouldDrain) {
            pool.execute(() -> drain(slot));
        }
    }

    private void drain(SegmentSlot slot) {
        try {
            while (true) {
                Object item = slot.mailbox().pop();
                while (item != null) {
                    InputEnvelope env = (InputEnvelope) item;
                    runOnce(slot, env.input, env.primaryStreamId);
                    item = slot.mailbox().pop();
                }
                slot.mailbox().releaseInflight();
                if (slot.mailbox().isEmpty()) {
                    return;
                }
                if (!slot.mailbox().tryMarkInflight()) {
                    return;
                }
            }
        } catch (Throwable t) {
            // Defense-in-depth: drain loop must release inflight even on
            // unexpected failure so subsequent pushes can resubmit.
            slot.mailbox().releaseInflight();
            LOG.error("dispatcher drain failed for operation `{}`: {}", operationDescriptor.name(), t.toString());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void runOnce(SegmentSlot slot, Object input, long primaryStreamId) {
        slot.stateLock().lock();
        try {
            Object state = slot.state().get();
            EmitContext ctxImpl = new EmitContext(primaryStreamId, slot);
            // Build a StateHandle that records Changes onto the Segment's
            // flush buffer under the bufferLock. The runtime's flusher owns
            // the buffer's drain cadence; here we just append.
            BiConsumer<Object, Change> applyChangeFn = (s, c) -> {
                operationInstance.applyChange(s, c);
            };
            List<Change> bufferView;
            synchronized (slot.changeBufferLock()) {
                bufferView = slot.changeBuffer();
            }
            StateHandle<Object, Change> handle = StateHandle.create(state, bufferView, applyChangeFn);
            try {
                operationInstance.run(input, handle, ctxImpl);
            } catch (OperationException e) {
                LOG.warn(
                        "operation `{}` segment `{}` threw OperationException: {}",
                        operationDescriptor.name(),
                        slot.operationKey().orElse("<single>"),
                        e.getMessage());
            } catch (RuntimeException e) {
                LOG.error(
                        "operation `{}` segment `{}` threw RuntimeException: {}",
                        operationDescriptor.name(),
                        slot.operationKey().orElse("<single>"),
                        e.toString());
            }
        } finally {
            slot.stateLock().unlock();
        }
    }

    private record InputEnvelope(Object input, long primaryStreamId) {}

    /**
     * Per-invocation {@link OperationCtx}. Single allocation per Operation.run call.
     * Re-using the descriptor cached on the dispatcher avoids any per-emit
     * lookup.
     */
    private final class EmitContext implements OperationCtx {
        private final long primaryStreamId;
        private final SegmentSlot slot;

        EmitContext(long primaryStreamId, SegmentSlot slot) {
            this.primaryStreamId = primaryStreamId;
            this.slot = slot;
        }

        @Override
        public <C> void emit(@Nonnull C value) throws OperationException {
            doEmit(outputChannelDescriptor.orElse(null), value, null);
        }

        @Override
        public <C> void emit(@Nonnull String channelName, @Nonnull C value) throws OperationException {
            doEmit(resolveChannel(channelName), value, null);
        }

        @Override
        public <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) throws OperationException {
            doEmit(outputChannelDescriptor.orElse(null), value, meta);
        }

        @Override
        public <C> void emitWithMeta(@Nonnull String channelName, @Nonnull C value, @Nonnull Map<String, String> meta)
                throws OperationException {
            doEmit(resolveChannel(channelName), value, meta);
        }

        private ChannelDescriptor resolveChannel(String name) throws OperationException {
            Integer id = topology.lookupChannelId(name);
            if (id == null) {
                throw new OperationException.UnknownChannel(name);
            }
            return topology.channel(id);
        }

        private void doEmit(ChannelDescriptor descriptor, Object value, Map<String, String> meta)
                throws OperationException {
            if (descriptor == null) {
                throw new OperationException.UnknownChannel("<no output channel declared on this operation>");
            }
            switch (descriptor.kind()) {
                case INTER_OPERATION -> {
                    Optional<PluginSink> sink = descriptor.pluginSink();
                    if (meta != null && sink.isEmpty()) {
                        throw new OperationException.MetadataNotAllowedOnInterOperationChannel(descriptor.name());
                    }
                    fanoutInterOperation(descriptor, value, primaryStreamId);
                    if (sink.isPresent()) {
                        emitPluginSink(descriptor, sink.get(), value, meta, primaryStreamId);
                    }
                }
                case PLUGIN_BOUND -> emitPlugin(descriptor, value, meta, primaryStreamId);
                case ENTRY -> throw new OperationException(
                        "cannot emit on entry channel `" + descriptor.name() + "` from a operation");
            }
        }
    }

    private void fanoutInterOperation(ChannelDescriptor descriptor, Object value, long primaryStreamId) {
        UnboundedOperativeInstance ri = instance.get();
        if (ri == null) {
            return;
        }
        for (int consumerOperationId : descriptor.consumers()) {
            OperationDispatcher consumer = ri.dispatcher(consumerOperationId);
            if (consumer != null) {
                consumer.fanoutSegments(value, primaryStreamId);
            }
        }
    }

    private void emitPlugin(
            ChannelDescriptor descriptor, Object value, Map<String, String> userMeta, long primaryStreamId) {
        UnboundedOperativeInstance ri = instance.get();
        if (ri == null) {
            return;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        java.util.function.Function<Object, byte[]> serializer =
                (java.util.function.Function) descriptor.pluginSerializer().orElse(null);
        if (serializer == null) {
            LOG.error("plugin channel `{}` is missing a serializer", descriptor.name());
            return;
        }

        byte[] bytes;
        try {
            bytes = serializer.apply(value);
        } catch (RuntimeException e) {
            LOG.error("plugin channel `{}` serializer failed: {}", descriptor.name(), e.toString());
            return;
        }

        writePluginFrame(ri, descriptor, bytes, userMeta, primaryStreamId);
    }

    private void emitPluginSink(
            ChannelDescriptor descriptor,
            PluginSink sink,
            Object value,
            Map<String, String> userMeta,
            long primaryStreamId)
            throws OperationException {
        UnboundedOperativeInstance ri = instance.get();
        if (ri == null) {
            return;
        }
        byte[] bytes;
        try {
            bytes = sink.serializer().apply(value);
        } catch (RuntimeException | Error e) {
            throw new OperationException.PluginSerializerFailure(descriptor.name(), e.toString(), e);
        }
        writePluginFrame(ri, descriptor, bytes, userMeta, primaryStreamId);
    }

    private void writePluginFrame(
            UnboundedOperativeInstance ri,
            ChannelDescriptor descriptor,
            byte[] bytes,
            Map<String, String> userMeta,
            long primaryStreamId) {
        Map<String, String> metadata = new LinkedHashMap<>();
        if (userMeta != null) {
            for (Map.Entry<String, String> e : userMeta.entrySet()) {
                if (!"channel".equals(e.getKey())) {
                    metadata.put(e.getKey(), e.getValue());
                }
            }
        }
        metadata.put("channel", descriptor.name());

        long secondaryStreamId = ctx.streamRegistry().secondaryFor(primaryStreamId);
        if (secondaryStreamId == 0L) {
            LOG.warn(
                    "plugin emit on `{}`: no secondary stream paired with primary {}",
                    descriptor.name(),
                    primaryStreamId);
            return;
        }

        byte[] frame = FrameEncoder.build(ri.instanceId(), bytes, metadata);
        int writeStatus = WIRE_WRITER.write(ctx.nativeHandle(), secondaryStreamId, frame);
        if (writeStatus != 0) {
            LOG.error("plugin emit on `{}`: stream write failed (status {})", descriptor.name(), writeStatus);
        }
    }
}
