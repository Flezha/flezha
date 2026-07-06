package com.flezha.sdk.runtime.bounded;

import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * {@link OperationCtx} for the bounded sequential driver. Inter-operation output
 * is <strong>collected</strong> for handoff to the next stage at the barrier;
 * plugin-bound output is serialized and routed to the wire via the
 * {@link BoundedCollectSink}. Constructed fresh per operation run, so it never
 * threads through the unbounded push-dispatch mailbox path (the unbounded
 * {@code OperationDispatcher.EmitContext} is the reference this forks).
 *
 * <p>Bounded output carries no per-channel metadata (it rides the instance-id
 * frame, like {@code BoundedOperativeCtx.emit}), so the {@code emitWithMeta}
 * variants drop the metadata, mirroring the Rust SDK's {@code BoundedCollectAdapter}.
 */
final class BoundedCollectAdapter implements OperationCtx {

    private final UnboundedOperativeTopology topology;
    private final Optional<ChannelDescriptor> outputChannel;
    private final BoundedCollectSink sink;

    BoundedCollectAdapter(
            @Nonnull UnboundedOperativeTopology topology,
            @Nonnull Optional<ChannelDescriptor> outputChannel,
            @Nonnull BoundedCollectSink sink) {
        this.topology = topology;
        this.outputChannel = outputChannel;
        this.sink = sink;
    }

    @Override
    public <C> void emit(@Nonnull C value) throws OperationException {
        doEmit(outputChannel.orElse(null), value);
    }

    @Override
    public <C> void emit(@Nonnull String channelName, @Nonnull C value) throws OperationException {
        doEmit(resolveChannel(channelName), value);
    }

    @Override
    public <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) throws OperationException {
        doEmit(outputChannel.orElse(null), value);
    }

    @Override
    public <C> void emitWithMeta(@Nonnull String channelName, @Nonnull C value, @Nonnull Map<String, String> meta)
            throws OperationException {
        doEmit(resolveChannel(channelName), value);
    }

    private ChannelDescriptor resolveChannel(String name) throws OperationException {
        Integer id = topology.lookupChannelId(name);
        if (id == null) {
            throw new OperationException.UnknownChannel(name);
        }
        return topology.channel(id);
    }

    private void doEmit(ChannelDescriptor descriptor, Object value) throws OperationException {
        if (descriptor == null) {
            throw new OperationException.UnknownChannel("<no output channel declared on this operation>");
        }
        switch (descriptor.kind()) {
            case INTER_OPERATION -> {
                // Collect for the next stage; a hybrid alsoToPluginWith(...) sink
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
                    throw new OperationException(
                            "bounded plugin channel `" + descriptor.name() + "` is missing a serializer");
                }
                routePlugin(serializer, value, descriptor.name());
            }
            case ENTRY -> throw new OperationException(
                    "cannot emit on entry channel `" + descriptor.name() + "` from an operation");
        }
    }

    private void routePlugin(Function<Object, byte[]> serializer, Object value, String channelName)
            throws OperationException {
        byte[] bytes;
        try {
            bytes = serializer.apply(value);
        } catch (RuntimeException | Error e) {
            throw new OperationException.PluginSerializerFailure(channelName, e.toString(), e);
        }
        sink.emitPluginBytes(bytes);
    }
}
