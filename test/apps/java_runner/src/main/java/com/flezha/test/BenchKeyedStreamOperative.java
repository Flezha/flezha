package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Mirror of the Rust test app's {@code BenchKeyedOperative}: {@code tier_store}
 * minus the effectively-once declaration, so the round-trip bench can measure the
 * same keyed upsert under both durability postures.
 */
public final class BenchKeyedStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "bench_keyed";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input")
                .onFull(OverflowPolicy.BLOCK)
                .into(TierStoreProcedure.class)
                .named("sink")
                .keyed()
                .sink("tier_sink");
    }
}
