package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Java equivalent of the Rust SDK's {@code StatefulCounterStreamOperative}.
 * One Procedure ({@link CounterProcedure}); each input recorded as a
 * {@link CounterChange}.
 */
public final class StatefulCounterStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "stateful_counter";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(IntInputAdapterProcedure.class);
    }
}
