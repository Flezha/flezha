package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Mirror of the Rust test app's {@code BenchKeyedNoPersistOperative}:
 * {@link BenchKeyedStreamOperative} with local state persistence off, isolating
 * the per-Entity WAL cost from the keyed upsert itself.
 */
public final class BenchKeyedNoPersistStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "bench_keyed_nopersist";
    }

    @Override
    public boolean persistenceEnabled() {
        return false;
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
