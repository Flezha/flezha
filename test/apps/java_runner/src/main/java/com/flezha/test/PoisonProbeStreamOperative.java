package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Mirror of the Rust test app's {@code PoisonProbeOperative}: tier-off
 * retry-then-park probe for the dead-letter-queue e2e.
 */
public final class PoisonProbeStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "poison_probe";
    }

    @Override
    public void channels(ChannelBuilder c) {
        var sPoisonProbeProcedure = c.input(String.class, "input").retryBackoffMs(25).into(PoisonProbeProcedure.class);
        sPoisonProbeProcedure.named("out").sink("probe_sink");
    }
}
