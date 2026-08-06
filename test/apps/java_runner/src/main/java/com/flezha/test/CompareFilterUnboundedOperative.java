package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;

/**
 * Mirror of the Rust SDK's {@code CompareFilterUnboundedOperative}: filters by
 * comparing the input against a stored value held in the Procedure's State.
 */
public final class CompareFilterUnboundedOperative implements UnboundedOperative {

    @Override
    public String factoryId() {
        return "compare_filter";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(CompareFilterProcedure.class);
    }
}
