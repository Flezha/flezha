package com.flezha.sdk.runtime.worker;

import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.partition.PartitionSlot;
import com.flezha.sdk.runtime.relay.ChannelDescriptor;
import com.flezha.sdk.runtime.relay.RelayInstance;
import com.flezha.sdk.runtime.relay.RelayTopology;
import com.flezha.sdk.runtime.relay.ShardDescriptor;
import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.StateHandle;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
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
 * Per-Shard dispatcher (replacement for the legacy
 * {@code DefaultRelayDispatcher}). Caches the producer-edge
 * {@code outputChannelId} at build time so {@code ctx.emit(...)} pays no
 * channel-name lookup on the hot path.
 *
 * <p>One {@link ShardDispatcher} per Shard×Partition (held by
 * {@link PartitionSlot#dispatcher()}). The Shard instance is held inside
 * this dispatcher to keep mutable per-Shard scratch (if any) per-partition.
 */
public final class ShardDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(ShardDispatcher.class);

    private final ExecutorContext ctx;
    private final ExecutorService pool;
    private final WeakReference<RelayInstance> instance;
    private final RelayTopology topology;
    private final int shardId;
    private final ShardDescriptor shardDescriptor;
    private final Optional<ChannelDescriptor> outputChannelDescriptor;
    private final OptionalInt outputChannelId;
    @SuppressWarnings("rawtypes")
    private final Shard shardInstance;

    @SuppressWarnings("unchecked")
    public ShardDispatcher(
            @Nonnull ExecutorContext ctx,
            @Nonnull ExecutorService pool,
            @Nonnull RelayInstance instance,
            @Nonnull RelayTopology topology,
            int shardId,
            @Nonnull Shard<?, ?, ?, ?, ?, ?> shardInstance) {
        this.ctx = ctx;
        this.pool = pool;
        this.instance = new WeakReference<>(instance);
        this.topology = topology;
        this.shardId = shardId;
        this.shardDescriptor = topology.shard(shardId);
        this.outputChannelId = shardDescriptor.outputChannelId();
        this.outputChannelDescriptor = outputChannelId.isPresent()
                ? Optional.ofNullable(topology.channel(outputChannelId.getAsInt()))
                : Optional.empty();
        this.shardInstance = shardInstance;
    }

    public int shardId() {
        return shardId;
    }

    public ShardDescriptor descriptor() {
        return shardDescriptor;
    }

    /**
     * Enqueue an input on the partitions targeted by this Shard. Single-mode
     * shards skip the {@link Shard#partitionFor(Object) partitionFor} call
     * and route to partition index 0; static-mode shards call
     * {@code partitionFor} and fan out across the matching partitions.
     */
    public void fanoutPartitions(@Nonnull Object value, long primaryStreamId) {
        RelayInstance ri = instance.get();
        if (ri == null) {
            return;
        }
        List<PartitionSlot> partitions = ri.partitions().get(shardId);
        if (shardDescriptor.partitionKeys().isEmpty()) {
            PartitionSlot slot = partitions.get(0);
            enqueue(slot, value, primaryStreamId);
            return;
        }
        List<String> keys = shardInstance.partitionFor(value);
        Map<String, Integer> keyToIdx = ri.partitionKeyToIdx().get(shardId);
        for (String key : keys) {
            Integer idx = keyToIdx.get(key);
            if (idx != null) {
                enqueue(partitions.get(idx), value, primaryStreamId);
            }
        }
    }

    private void enqueue(PartitionSlot slot, Object value, long primaryStreamId) {
        boolean shouldDrain = slot.mailbox().push(new InputEnvelope(value, primaryStreamId));
        if (shouldDrain) {
            pool.execute(() -> drain(slot));
        }
    }

    private void drain(PartitionSlot slot) {
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
            LOG.error("dispatcher drain failed for shard `{}`: {}",
                    shardDescriptor.name(), t.toString());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void runOnce(PartitionSlot slot, Object input, long primaryStreamId) {
        slot.stateLock().lock();
        try {
            Object state = slot.state().get();
            EmitContext ctxImpl = new EmitContext(primaryStreamId, slot);
            // Build a StateHandle that records Changes onto the Partition's
            // flush buffer under the bufferLock. The runtime's flusher owns
            // the buffer's drain cadence; here we just append.
            BiConsumer<Object, Change> applyChangeFn = (s, c) -> {
                ((Shard) shardInstance).applyChange(s, c);
            };
            List<Change> bufferView;
            synchronized (slot.changeBufferLock()) {
                bufferView = slot.changeBuffer();
            }
            StateHandle<Object, Change> handle = StateHandle.create(state, bufferView, applyChangeFn);
            try {
                shardInstance.run(input, handle, ctxImpl);
            } catch (ShardException e) {
                LOG.warn("shard `{}` partition `{}` threw ShardException: {}",
                        shardDescriptor.name(), slot.shardKey().orElse("<single>"), e.getMessage());
            } catch (RuntimeException e) {
                LOG.error("shard `{}` partition `{}` threw RuntimeException: {}",
                        shardDescriptor.name(), slot.shardKey().orElse("<single>"), e.toString());
            }
        } finally {
            slot.stateLock().unlock();
        }
    }

    private record InputEnvelope(Object input, long primaryStreamId) {}

    /**
     * Per-invocation {@link ShardCtx}. Single allocation per Shard.run call.
     * Re-using the descriptor cached on the dispatcher avoids any per-emit
     * lookup.
     */
    private final class EmitContext implements ShardCtx {
        private final long primaryStreamId;
        private final PartitionSlot slot;

        EmitContext(long primaryStreamId, PartitionSlot slot) {
            this.primaryStreamId = primaryStreamId;
            this.slot = slot;
        }

        @Override
        public <C> void emit(@Nonnull C value) throws ShardException {
            doEmit(outputChannelDescriptor.orElse(null), value, null);
        }

        @Override
        public <C> void emit(@Nonnull String channelName, @Nonnull C value) throws ShardException {
            doEmit(resolveChannel(channelName), value, null);
        }

        @Override
        public <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) throws ShardException {
            doEmit(outputChannelDescriptor.orElse(null), value, meta);
        }

        @Override
        public <C> void emitWithMeta(
                @Nonnull String channelName,
                @Nonnull C value,
                @Nonnull Map<String, String> meta) throws ShardException {
            doEmit(resolveChannel(channelName), value, meta);
        }

        private ChannelDescriptor resolveChannel(String name) throws ShardException {
            Integer id = topology.lookupChannelId(name);
            if (id == null) {
                throw new ShardException.UnknownChannel(name);
            }
            return topology.channel(id);
        }

        private void doEmit(ChannelDescriptor descriptor, Object value, Map<String, String> meta)
                throws ShardException {
            if (descriptor == null) {
                throw new ShardException.UnknownChannel("<no output channel declared on this shard>");
            }
            switch (descriptor.kind()) {
                case INTER_SHARD -> {
                    if (meta != null) {
                        throw new ShardException.MetadataNotAllowedOnInterShardChannel(descriptor.name());
                    }
                    fanoutInterShard(descriptor, value, primaryStreamId);
                }
                case PLUGIN_BOUND -> emitPlugin(descriptor, value, meta, primaryStreamId);
                case ENTRY -> throw new ShardException(
                        "cannot emit on entry channel `" + descriptor.name() + "` from a shard");
            }
        }
    }

    private void fanoutInterShard(ChannelDescriptor descriptor, Object value, long primaryStreamId) {
        RelayInstance ri = instance.get();
        if (ri == null) {
            return;
        }
        for (int consumerShardId : descriptor.consumers()) {
            ShardDispatcher consumer = ri.dispatcher(consumerShardId);
            if (consumer != null) {
                consumer.fanoutPartitions(value, primaryStreamId);
            }
        }
    }

    private void emitPlugin(
            ChannelDescriptor descriptor,
            Object value,
            Map<String, String> userMeta,
            long primaryStreamId) {
        RelayInstance ri = instance.get();
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
            LOG.warn("plugin emit on `{}`: no secondary stream paired with primary {}",
                    descriptor.name(), primaryStreamId);
            return;
        }

        byte[] frame = FrameEncoder.build(ri.instanceId(), bytes, metadata);
        int writeStatus = com.flezha.sdk.native_.NativeTransport.streamWrite(
                ctx.nativeHandle(), secondaryStreamId, frame);
        if (writeStatus != 0) {
            LOG.error("plugin emit on `{}`: stream write failed (status {})",
                    descriptor.name(), writeStatus);
        }
    }
}
