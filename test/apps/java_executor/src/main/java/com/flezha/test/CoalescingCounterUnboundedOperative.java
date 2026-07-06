package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;

/**
 * Demonstrates the per-Segment flusher's coalescing path: {@link TickChange}
 * carries a {@link com.flezha.sdk.state.CoalesceKey} so Changes with equal
 * {@code (interval, ts)} collapse to the latest within a flush window.
 */
public final class CoalescingCounterUnboundedOperative implements UnboundedOperative {

    @Override
    public String factoryId() {
        return "coalescing_counter";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(TickOperation.class);
    }
}
