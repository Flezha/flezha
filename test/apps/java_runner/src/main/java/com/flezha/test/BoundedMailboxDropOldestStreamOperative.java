package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperative;

/** Mirror of the Rust test app's {@code BoundedMailboxDropOldestOperative}. */
public final class BoundedMailboxDropOldestStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "bounded_mailbox_drop_oldest";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "drop_oldest_input")
                .onFull(OverflowPolicy.DROP_OLDEST)
                .into(DropOldestTestProcedure.class)
                .named("drop_oldest_echo")
                .sink("bounded_mailbox_echo");
    }

    public static final class DropOldestTestProcedure extends BoundedMailboxProcedure {
        @Override
        public String name() {
            return "drop_oldest_test_procedure";
        }
    }
}
