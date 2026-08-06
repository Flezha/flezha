package com.flezha.sdk.runtime.bounded;

import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.procedure.StableId;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.state.EntityId;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * {@link ProcedureCtx} for the bounded sequential driver. Inter-procedure output
 * is <strong>collected</strong> for handoff to the next stage at the barrier;
 * plugin-bound output is serialized and routed to the wire via the
 * {@link BoundedCollectSink}. Constructed fresh per procedure run, so it never
 * threads through the unbounded push-dispatch mailbox path (the unbounded
 * {@code ProcedureDispatcher.EmitContext} is the reference this forks).
 *
 * <p>Bounded output carries no per-channel metadata (it rides the instance-id
 * frame, like {@code BoundedOperativeCtx.emit}), so the {@code emitWithMeta}
 * variants drop the metadata, mirroring the Rust SDK's {@code BoundedCollectAdapter}.
 */
final class BoundedCollectAdapter implements ProcedureCtx {

    private final UnboundedOperativeTopology topology;
    private final Optional<ChannelDescriptor> outputChannel;
    private final BoundedCollectSink sink;
    private final EntityId identity;

    /** The wall clock, sampled once — the adapter is constructed fresh per procedure run. */
    private final long now = System.currentTimeMillis();

    private int stableIdOrdinal;

    BoundedCollectAdapter(
            @Nonnull UnboundedOperativeTopology topology,
            @Nonnull Optional<ChannelDescriptor> outputChannel,
            @Nonnull BoundedCollectSink sink,
            @Nonnull EntityId identity) {
        this.topology = topology;
        this.outputChannel = outputChannel;
        this.sink = sink;
        this.identity = identity;
    }

    @Override
    public long now() {
        return now;
    }

    /** No-op: a bounded operative's durability boundary is the proc commit, not a checkpoint. */
    @Override
    public void checkpoint() {}

    /**
     * Derived at invocation seq 0: a bounded operative that re-runs from the top
     * re-derives the same ids, which is exactly what makes its re-emitted output
     * idempotent under at-least-once.
     */
    @Override
    @Nonnull
    public StableId stableId() {
        return StableId.derive(identity.encodedBytes(), 0L, stableIdOrdinal++);
    }

    /**
     * Bounded plugin output rides the instance-id frame and carries no
     * per-channel metadata, so a {@code flz.key} stamp has nowhere to go —
     * a keyed emit is refused rather than silently unkeyed. Matches the Rust
     * SDK, which rejects keyed emits on bounded operatives for the same reason.
     */
    @Override
    public <C> void emitKeyed(@Nonnull String key, @Nonnull C value) throws ProcedureException {
        throw new ProcedureException.KeyNotAllowedOnNonKeyedChannel(
                outputChannel.map(ChannelDescriptor::name).orElse("<no output channel>"));
    }

    @Override
    public <C> void emitKeyedWithMeta(@Nonnull String key, @Nonnull C value, @Nonnull Map<String, String> meta)
            throws ProcedureException {
        emitKeyed(key, value);
    }

    @Override
    public <C> void emit(@Nonnull C value) throws ProcedureException {
        doEmit(outputChannel.orElse(null), value);
    }

    @Override
    public <C> void emit(@Nonnull String channelName, @Nonnull C value) throws ProcedureException {
        doEmit(resolveChannel(channelName), value);
    }

    @Override
    public <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) throws ProcedureException {
        doEmit(outputChannel.orElse(null), value);
    }

    @Override
    public <C> void emitWithMeta(@Nonnull String channelName, @Nonnull C value, @Nonnull Map<String, String> meta)
            throws ProcedureException {
        doEmit(resolveChannel(channelName), value);
    }

    private ChannelDescriptor resolveChannel(String name) throws ProcedureException {
        Integer id = topology.lookupChannelId(name);
        if (id == null) {
            throw new ProcedureException.UnknownChannel(name);
        }
        return topology.channel(id);
    }

    private void doEmit(ChannelDescriptor descriptor, Object value) throws ProcedureException {
        if (descriptor == null) {
            throw new ProcedureException.UnknownChannel("<no output channel declared on this procedure>");
        }
        switch (descriptor.kind()) {
            case INTER_PROCEDURE -> {
                // Collect for the next stage; a hybrid alsoOutput(...) sink
                // also routes a serialized copy to the plugin wire.
                sink.collect(value);
                if (descriptor.pluginSink().isPresent()) {
                    routePlugin(descriptor.pluginSink().get().serializer(), value, descriptor.name());
                }
            }
            case PLUGIN_BOUND -> {
                Function<Object, byte[]> serializer =
                        descriptor.pluginSerializer().orElse(null);
                if (serializer == null) {
                    throw new ProcedureException(
                            "bounded plugin channel `" + descriptor.name() + "` is missing a serializer");
                }
                routePlugin(serializer, value, descriptor.name());
            }
            case ENTRY -> throw new ProcedureException(
                    "cannot emit on entry channel `" + descriptor.name() + "` from a procedure");
        }
    }

    private void routePlugin(Function<Object, byte[]> serializer, Object value, String channelName)
            throws ProcedureException {
        byte[] bytes;
        try {
            bytes = serializer.apply(value);
        } catch (RuntimeException | Error e) {
            throw new ProcedureException.PluginSerializerFailure(channelName, e.toString(), e);
        }
        sink.emitPluginBytes(channelName, bytes);
    }
}
