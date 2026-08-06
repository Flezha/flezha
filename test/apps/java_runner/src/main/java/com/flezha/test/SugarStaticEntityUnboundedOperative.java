package com.flezha.test;

import com.flezha.sdk.authoring.UnboundedOperative;
import com.flezha.sdk.channel.ChannelBuilder;

/**
 * Multiprocedure reference operative authored with explicit topology, wiring the
 * generated {@code SugarEntryProcedure} -> {@code SugarDownstreamProcedure} -> plugin.
 * {@code @UnboundedOperative} collects it into
 * {@code GeneratedUnboundedOperatives.defineAll(FlezhaRunner)}.
 */
@UnboundedOperative
public class SugarStaticEntityUnboundedOperative implements com.flezha.sdk.operative.UnboundedOperative {

    @Override
    public String factoryId() {
        return "java_sugar_static_entity";
    }

    @Override
    public void channels(ChannelBuilder c) {
        var sSugarEntryProcedure = c.input(String.class, "input").into(SugarEntryProcedure.class);
        var sSugarDownstreamProcedure = sSugarEntryProcedure.named("hop").into(SugarDownstreamProcedure.class);
        sSugarDownstreamProcedure.named("kafka").output("test_kafka");
    }
}
