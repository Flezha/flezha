package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.ContinuousOperative;

/**
 * Mirror of the Rust SDK's {@code StoreInputContinuousOperative}: appends each
 * input to the Operation's State. No emit — the State is observable via
 * {@code SegmentState}.
 */
public final class StoreInputContinuousOperative implements ContinuousOperative {

    @Override
    public String factoryId() {
        return "store_input";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(StoreInputOperation.class);
    }
}
