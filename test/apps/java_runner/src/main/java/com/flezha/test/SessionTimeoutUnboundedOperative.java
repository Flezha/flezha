package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;

/** Mirror of the Rust runner's {@code SessionTimeoutOperative}. */
public final class SessionTimeoutUnboundedOperative implements UnboundedOperative {

    @Override
    public String factoryId() {
        return "java_session_timeout";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input")
                .into(SessionTimeoutProcedure.class)
                .named("kafka")
                .output("test_kafka");
    }
}
