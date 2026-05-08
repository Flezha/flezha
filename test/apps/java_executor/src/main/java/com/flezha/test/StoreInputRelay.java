package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.relay.Relay;

/**
 * Mirror of the Rust SDK's {@code StoreInputRelay}: appends each input to the
 * Shard's State. No emit — the State is observable via {@code PartitionState}.
 */
public final class StoreInputRelay implements Relay {

    @Override
    public String factoryId() {
        return "store_input";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(StoreInputShard.class);
    }
}
