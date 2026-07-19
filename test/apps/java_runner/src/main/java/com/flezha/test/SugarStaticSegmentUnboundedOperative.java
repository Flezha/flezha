package com.flezha.test;

import com.flezha.sdk.authoring.UnboundedOperative;
import com.flezha.sdk.channel.ChannelBuilder;

/**
 * Multioperation reference operative authored with explicit topology, wiring the
 * generated {@code SugarEntryOperation} -> {@code SugarDownstreamOperation} -> plugin.
 * {@code @UnboundedOperative} collects it into
 * {@code GeneratedUnboundedOperatives.defineAll(FlezhaRunner)}.
 */
@UnboundedOperative
public class SugarStaticSegmentUnboundedOperative implements com.flezha.sdk.operative.UnboundedOperative {

    @Override
    public String factoryId() {
        return "java_sugar_static_segment";
    }

    @Override
    public void channels(ChannelBuilder c) {
        var sSugarEntryOperation = c.input(String.class, "input").into(SugarEntryOperation.class);
        var sSugarDownstreamOperation = sSugarEntryOperation.named("hop").into(SugarDownstreamOperation.class);
        sSugarDownstreamOperation.named("kafka").output("test_kafka");
    }
}
