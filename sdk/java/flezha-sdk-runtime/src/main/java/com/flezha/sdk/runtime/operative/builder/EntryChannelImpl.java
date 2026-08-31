package com.flezha.sdk.runtime.operative.builder;

import com.flezha.sdk.channel.EdgeStream;
import com.flezha.sdk.channel.EntryChannel;

/** An entry channel handle: its site is the entry channel it was declared as. */
final class EntryChannelImpl<T> extends EdgeStreamImpl<T> implements EntryChannel<T> {

    private final int channelId;
    private final String name;

    EntryChannelImpl(
            StreamOperativeTopologyBuilder owner, int channelId, String name, Integer pending, Class<?> payloadType) {
        super(owner, pending, payloadType);
        this.channelId = channelId;
        this.name = name;
    }

    @Override
    public int siteChannelId() {
        return channelId;
    }

    @Override
    <U> EdgeStream<U> relink(Integer pending, Class<?> payloadType) {
        return new EntryChannelImpl<>(owner, channelId, name, pending, payloadType);
    }

    @Override
    public String toString() {
        return "EntryChannel(" + name + ")";
    }
}
