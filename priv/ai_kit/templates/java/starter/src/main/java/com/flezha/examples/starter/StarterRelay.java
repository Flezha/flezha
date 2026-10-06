package com.flezha.examples.starter;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.relay.Relay;
import javax.annotation.Nonnull;

/**
 * The relay: a visit arrives on {@code hit}, goes to the cell of its page, and the count leaves
 * on the output {@code counts}, which also answers a caller.
 */
public final class StarterRelay implements Relay {

    @Override
    @Nonnull
    public String factoryId() {
        return "starter"; // the name `flz relay start` takes
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(CountHits.Hit.class, "hit")
                .describe("Counts visits per page") // shown by `flz relay info` and to AI agents
                .keyBy(CountHits.Hit::page) // one cell, and one count, per page
                .into(CountHits.class)
                .output("counts");
    }
}
