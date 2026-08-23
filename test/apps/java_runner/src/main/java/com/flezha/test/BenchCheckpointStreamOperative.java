package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Mirror of the Rust test app's {@code BenchCheckpointOperative}
 * ({@code mix bench.roundtrip --durability checkpoint}): the
 * {@link CheckpointStoreStreamOperative} keyed upsert driven by a DECLARED
 * cadence instead of a {@code commit:} input.
 */
public final class BenchCheckpointStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "bench_checkpoint";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.effectivelyOnce();
        c.checkpointEvery((int) TestEnv.longVar("FLEZHA_BENCH_CHECKPOINT_EVERY", 500));
        if ("at_checkpoint".equals(System.getenv("FLEZHA_BENCH_CHECKPOINT_OUTPUTS"))) {
            c.outputsAtCheckpoint(TestEnv.longVar("FLEZHA_BENCH_CHECKPOINT_DELAY_MS", 50));
        }
        c.input(String.class, "input")
                .onFull(OverflowPolicy.BLOCK)
                .into(BenchCheckpointProcedure.class)
                .named("sink")
                .keyed()
                .sink("tier_sink");
    }
}
