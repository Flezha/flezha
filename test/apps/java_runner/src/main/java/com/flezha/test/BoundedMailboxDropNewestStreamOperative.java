package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperative;

/** Mirror of the Rust test app's {@code BoundedMailboxDropNewestOperative}. */
public final class BoundedMailboxDropNewestStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "bounded_mailbox_drop_newest";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "drop_newest_input")
                .onFull(OverflowPolicy.DROP_NEWEST)
                .into(DropNewestTestProcedure.class)
                .named("drop_newest_echo")
                .sink("bounded_mailbox_echo");
    }

    public static final class DropNewestTestProcedure extends BoundedMailboxProcedure {
        @Override
        public String name() {
            return "drop_newest_test_procedure";
        }
    }
}
