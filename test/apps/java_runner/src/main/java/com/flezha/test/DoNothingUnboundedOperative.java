package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;

/**
 * Mirror of the Rust SDK's {@code DoNothingUnboundedOperative}: echoes the input
 * bytes on the plugin-bound {@code echo} channel so the controller's
 * {@code :on_output} extensions see one event per input. The integration tests
 * using {@code attach_output_msg(do_nothing)} depend on this echo.
 */
public final class DoNothingUnboundedOperative implements UnboundedOperative {

    @Override
    public String factoryId() {
        return "do_nothing";
    }

    @Override
    public void channels(ChannelBuilder c) {
        var sDoNothingOperation = c.input(String.class, "input").into(DoNothingOperation.class);
        sDoNothingOperation.named("echo").output("do_nothing_echo");
    }
}
