package com.flezha.examples.timers;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import javax.annotation.Nonnull;

/** One entity per user, so every session owns its own idle deadline. */
public final class SessionOperative implements StreamOperative {

    @Override
    @Nonnull
    public String factoryId() {
        return "session_window";
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(String.class, "clicks")
                .keyBy(click -> click.split(":")[0])
                .into(SessionWindow.class)
                .named("closed_sessions")
                .sink("kafka");
    }
}
