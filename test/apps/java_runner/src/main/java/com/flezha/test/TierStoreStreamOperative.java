package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Mirror of the Rust test app's {@code TierStoreOperative}: the effectively-once
 * fixture. A 300 ms backoff base keeps the min-watermark freeze observable in the
 * dead-letter e2e, and the entry channel is declared {@code BLOCK} because a
 * durable-input-log operative that shed at its mailbox would hold the
 * min-watermark on the shed offset forever.
 */
public final class TierStoreStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "tier_store";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.effectivelyOnce();
        c.input(String.class, "input")
                .onFull(OverflowPolicy.BLOCK)
                .retryBackoffMs(300)
                .into(TierStoreProcedure.class)
                .named("sink")
                .keyed()
                .sink("tier_sink");
    }
}
