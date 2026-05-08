package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.relay.Relay;

/**
 * Mirror of the Rust SDK's {@code CompareFilterRelay}: filters by comparing
 * the input against a stored value held in the Shard's State.
 */
public final class CompareFilterRelay implements Relay {

    @Override
    public String factoryId() {
        return "compare_filter";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(CompareFilterShard.class);
    }
}
