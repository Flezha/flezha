package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Mirror of the Rust runner's {@code DynamicEntityOperative}: the Entity key is
 * declared on the WIRING, so one input per fresh key lazily creates an Entity
 * and the literal {@code "drop"} is discarded by a {@code filter} rather than by
 * a keyless return.
 */
public final class DynamicEntityStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "dynamic_entity";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input")
                .onFull(OverflowPolicy.BLOCK)
                .filter(input -> !input.equals("drop"))
                .keyBy(DynamicEntityStreamOperative::key)
                .into(DynamicEntityProcedure.class)
                .named("kafka")
                .sink("test_kafka");
    }

    /** Key = text before the first {@code :}; {@code partition_relay} shares it. */
    static String key(String input) {
        int at = input.indexOf(':');
        return at < 0 ? input : input.substring(0, at);
    }
}
