package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Mirror of the Rust SDK's {@code CompareFilterStreamOperative}: filters by
 * comparing the input against a stored value held in the Procedure's State.
 */
public final class CompareFilterStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "compare_filter";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(CompareFilterProcedure.class).named("sink").sink("compare_filter_sink");
    }
}
