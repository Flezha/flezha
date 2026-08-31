package com.flezha.sdk.runtime.operative.builder;

import com.flezha.sdk.channel.EdgeStream;
import com.flezha.sdk.channel.Stream;

/**
 * The producer side of the shared surface: its channel is the node's output
 * channel, created lazily. {@link StreamImpl} holds one of these rather than
 * extending it, because {@link Stream} must stay an abstract class (for
 * {@code @SafeVarargs branch}) and Java has no second superclass to spend.
 */
final class NodeEdge<T> extends EdgeStreamImpl<T> {

    private final int node;

    NodeEdge(StreamOperativeTopologyBuilder owner, int node, Integer pending, Class<?> payloadType) {
        super(owner, pending, payloadType != null ? payloadType : owner.procedure(node).outputType);
        this.node = node;
    }

    @Override
    public int siteChannelId() {
        return owner.ensureOutputChannel(node);
    }

    @Override
    <U> EdgeStream<U> relink(Integer chain, Class<?> newPayloadType) {
        return new NodeEdge<U>(owner, node, chain, newPayloadType);
    }
}
