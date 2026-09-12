package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Mirror of the Rust test app's {@code BenchEchoOperative}: {@code do_nothing}
 * with a declared {@code DROP_NOTHING} on its entry mailbox, so the round-trip bench's
 * back-pressure posture stays independent of the {@code do_nothing} factory.
 */
public final class BenchEchoStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "bench_echo";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input")
                .onFull(OverflowPolicy.DROP_NOTHING)
                .into(DoNothingProcedure.class)
                .sink("echo");
    }
}
