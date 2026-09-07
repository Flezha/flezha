package com.flezha.examples.effectivelyonce;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import javax.annotation.Nonnull;

/**
 * The two declarations at the top are what turn the guarantee on, and neither is optional.
 *
 * <p>{@code effectivelyOnce} makes state, output and consumed offset become durable together in one
 * record, so a crash cannot advance the offset while losing the output. {@code checkpointAck} is
 * what withholds the offset until your code says the state is coherent.
 *
 * <p>Registration refuses this combination without keyed sinks, because without a key the sink has
 * no way to recognise a repeat.
 */
public final class RollupOperative implements StreamOperative {

    @Override
    @Nonnull
    public String factoryId() {
        return "rollup";
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.effectivelyOnce();
        c.checkpointAck();

        c.input(String.class, "ticks")
                .into(RollupProcedure.class)
                .named("rollups")
                .keyed()
                .sink("postgres_sink");
    }
}
