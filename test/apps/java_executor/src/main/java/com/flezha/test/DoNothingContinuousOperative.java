package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.ContinuousOperative;

/**
 * Mirror of the Rust SDK's {@code DoNothingContinuousOperative}: echoes the input
 * bytes on the plugin-bound {@code echo} channel so the controller's
 * {@code :on_output} extensions see one event per input. The integration tests
 * using {@code attach_output_msg(do_nothing)} depend on this echo.
 */
public final class DoNothingContinuousOperative implements ContinuousOperative {

    @Override
    public String factoryId() {
        return "do_nothing";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(DoNothingOperation.class);
        c.channel(byte[].class, "echo")
                .from(DoNothingOperation.class)
                .toPlugin("do_nothing_echo");
    }
}
