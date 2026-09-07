package com.flezha.examples.pipeline;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import javax.annotation.Nonnull;

/**
 * A two-stage graph: one procedure's output is the next procedure's input.
 *
 * <p>The preceding examples each declare a single procedure, which can suggest that chaining is a
 * distinct feature. It is not. A procedure's output travels on an ordinary channel, and a channel
 * can be routed to another procedure as readily as to a sink.
 */
public final class PipelineOperative implements StreamOperative {

    @Override
    @Nonnull
    public String factoryId() {
        return "pipeline";
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        // Read top to bottom as the path a message takes: it arrives on "in", Normalize handles
        // it, Normalize's output travels on a channel labelled "normalized", Shout consumes that,
        // and Shout's output reaches a plugin sink.
        //
        // Each stage is an independent procedure with its own state, scheduled and restarted
        // separately, so a slow or failing stage does not halt the others. The chain extends to any
        // number of stages; the only coupling is the type, since a stage's output type must match
        // the next stage's input type.
        //
        // The labels are identities rather than documentation: "normalized" and "shouted" are what
        // a sink registers against and what a test feeds or reads.
        c.input(String.class, "in")
                .into(Normalize.class)
                .named("normalized")
                .into(Shout.class)
                .named("shouted")
                .sink("my_sink");
    }
}
