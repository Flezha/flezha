package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperative;

/** Mirror of the Rust test app's {@code BoundedMailboxBlockOperative}. */
public final class BoundedMailboxBlockStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "bounded_mailbox_block";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "block_input")
                .onFull(OverflowPolicy.BLOCK)
                .into(BlockTestProcedure.class)
                .named("block_echo")
                .sink("bounded_mailbox_echo");
    }

    public static final class BlockTestProcedure extends BoundedMailboxProcedure {
        @Override
        public String name() {
            return "block_test_procedure";
        }
    }
}
