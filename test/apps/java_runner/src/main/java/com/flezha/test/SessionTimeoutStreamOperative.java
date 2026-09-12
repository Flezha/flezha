package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;

/** Mirror of the Rust runner's {@code SessionTimeoutOperative}. */
public final class SessionTimeoutStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "session_timeout";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input")
                .into(SessionTimeoutProcedure.class)
                .sink("kafka");
    }
}
