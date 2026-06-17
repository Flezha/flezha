package com.flezha.test;

import com.flezha.sdk.authoring.Relay;
import com.flezha.sdk.channel.ChannelBuilder;

/**
 * Multishard reference relay authored with explicit topology, wiring the
 * generated {@code SugarEntryShard} -> {@code SugarDownstreamShard} -> plugin.
 * {@code @Relay} collects it into {@code GeneratedRelays.registerAll()}.
 */
@Relay
public class SugarStaticPartitionRelay implements com.flezha.sdk.relay.Relay {

    @Override
    public String factoryId() {
        return "java_sugar_static_partition";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(SugarEntryShard.class);
        c.channel(String.class, "hop").from(SugarEntryShard.class).into(SugarDownstreamShard.class);
        c.channel(byte[].class, "kafka").from(SugarDownstreamShard.class).toPlugin("test_kafka");
    }
}
