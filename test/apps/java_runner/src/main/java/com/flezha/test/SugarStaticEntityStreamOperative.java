package com.flezha.test;

import com.flezha.sdk.authoring.StreamOperative;
import com.flezha.sdk.channel.ChannelBuilder;

/**
 * Multiprocedure reference operative authored with explicit topology, wiring the
 * generated {@code SugarEntryProcedure} -> {@code SugarDownstreamProcedure} -> plugin.
 * {@code @StreamOperative} collects it into
 * {@code GeneratedStreamOperatives.defineAll(FlezhaRunner)}.
 */
@StreamOperative
public class SugarStaticEntityStreamOperative implements com.flezha.sdk.operative.StreamOperative {

    @Override
    public String factoryId() {
        return "java_sugar_static_entity";
    }

    @Override
    public void channels(ChannelBuilder c) {
        var sSugarEntryProcedure = c.input(String.class, "input").into(SugarEntryProcedure.class);
        var sSugarDownstreamProcedure = sSugarEntryProcedure.named("hop").into(SugarDownstreamProcedure.class);
        sSugarDownstreamProcedure.named("kafka").sink("test_kafka");
    }
}
