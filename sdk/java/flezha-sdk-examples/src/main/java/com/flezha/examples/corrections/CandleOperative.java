package com.flezha.examples.corrections;

import com.flezha.sdk.channel.BeyondWindowPolicy;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import javax.annotation.Nonnull;

/** Accepts corrections up to five minutes late; anything older is treated as a new arrival. */
public final class CandleOperative implements StreamOperative {

    private static final long FIVE_MINUTES_MS = 5 * 60_000L;

    @Override
    @Nonnull
    public String factoryId() {
        return "late_candles";
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.effectivelyOnce();
        c.correctionWindow(FIVE_MINUTES_MS);
        c.beyondWindowPolicy(BeyondWindowPolicy.PROCESS_AS_ARRIVAL);

        c.input(String.class, "trades")
                .into(CandleProcedure.class)
                .named("candles")
                .keyed()
                .sink("postgres_sink");
    }
}
