package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Mirror of the Rust test app's {@code CandleOperative}: the
 * stream-late-input-correction fixture — an event-time clock plus a declared
 * correction window, so a late trade rewinds the Entity and replays the
 * affected slice in event-time order.
 */
public final class CandleStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "candle";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.effectivelyOnce();
        c.correctionWindow(600_000);
        c.input(String.class, "input").into(CandleProcedure.class).named("sink").keyed().sink("tier_sink");
    }
}
