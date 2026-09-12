package com.flezha.examples.spawn;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import javax.annotation.Nonnull;

/** One instance, running forever over the event stream. */
public final class AlertDetectorOperative implements StreamOperative {

    @Override
    @Nonnull
    public String factoryId() {
        return "alert_detector";
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(String.class, "events").into(DetectProcedure.class).sink("alerts");
    }
}
