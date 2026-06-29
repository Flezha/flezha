package com.flezha.test;

import com.flezha.sdk.authoring.ContinuousOperative;
import com.flezha.sdk.channel.ChannelBuilder;

/**
 * Multioperation reference operative authored with explicit topology, wiring the
 * generated {@code SugarEntryOperation} -> {@code SugarDownstreamOperation} -> plugin.
 * {@code @ContinuousOperative} collects it into
 * {@code GeneratedContinuousOperatives.registerAll()}.
 */
@ContinuousOperative
public class SugarStaticSegmentContinuousOperative implements com.flezha.sdk.operative.ContinuousOperative {

    @Override
    public String factoryId() {
        return "java_sugar_static_segment";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(SugarEntryOperation.class);
        c.channel(String.class, "hop").from(SugarEntryOperation.class).into(SugarDownstreamOperation.class);
        c.channel(byte[].class, "kafka").from(SugarDownstreamOperation.class).toPlugin("test_kafka");
    }
}
