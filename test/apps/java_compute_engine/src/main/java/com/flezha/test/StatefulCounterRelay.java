package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.relay.Relay;

/**
 * Java equivalent of the Rust SDK's {@code StatefulCounterRelay}. One Shard
 * ({@link CounterShard}); each input recorded as a {@link CounterChange}.
 */
public final class StatefulCounterRelay implements Relay {

    @Override
    public String factoryId() {
        return "stateful_counter";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(IntInputAdapterShard.class);
    }
}
