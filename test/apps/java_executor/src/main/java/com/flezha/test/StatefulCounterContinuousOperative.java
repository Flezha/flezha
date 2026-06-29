package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.ContinuousOperative;

/**
 * Java equivalent of the Rust SDK's {@code StatefulCounterContinuousOperative}.
 * One Operation ({@link CounterOperation}); each input recorded as a
 * {@link CounterChange}.
 */
public final class StatefulCounterContinuousOperative implements ContinuousOperative {

    @Override
    public String factoryId() {
        return "stateful_counter";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(IntInputAdapterOperation.class);
    }
}
