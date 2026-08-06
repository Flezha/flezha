package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;

/**
 * Mirror of the Rust SDK's {@code StoreInputUnboundedOperative}: appends each
 * input to the Procedure's State. No emit — the State is observable via
 * {@code EntityState}.
 */
public final class StoreInputUnboundedOperative implements UnboundedOperative {

    @Override
    public String factoryId() {
        return "store_input";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(StoreInputProcedure.class);
    }
}
