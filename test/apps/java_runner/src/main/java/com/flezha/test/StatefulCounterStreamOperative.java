package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Java equivalent of the Rust SDK's {@code StatefulCounterStreamOperative}:
 * {@link CounterEntryProcedure} parses the decimal input and the {@code ticks}
 * channel carries the delta to {@link CounterProcedure}, which records it as a
 * {@link CounterChange}.
 */
public final class StatefulCounterStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "stateful_counter";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(CounterEntryProcedure.class).named("ticks").into(CounterProcedure.class);
    }
}
