package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;

/**
 * Mirror of the Rust test app's {@code PoisonProbeOperative}: tier-off
 * retry-then-park probe for the dead-letter-queue e2e.
 */
public final class PoisonProbeUnboundedOperative implements UnboundedOperative {

    @Override
    public String factoryId() {
        return "java_poison_probe";
    }

    @Override
    public void channels(ChannelBuilder c) {
        var sPoisonProbeProcedure = c.input(String.class, "input").retryBackoffMs(25).into(PoisonProbeProcedure.class);
        sPoisonProbeProcedure.named("out").output("probe_sink");
    }
}
