package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
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
        return "java_dynamic_entity";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input")
                .filter(input -> !input.equals("drop"))
                .keyBy(input -> input.split(":")[0])
                .into(DynamicEntityProcedure.class);
    }
}
