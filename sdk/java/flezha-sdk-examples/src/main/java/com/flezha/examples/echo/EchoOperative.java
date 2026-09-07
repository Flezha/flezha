package com.flezha.examples.echo;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import javax.annotation.Nonnull;

/**
 * The deployable unit. An operative declares a graph and holds no logic of its own.
 *
 * <p>A procedure cannot be deployed alone, because nothing in the system knows where its messages
 * originate or where its output belongs. The operative supplies that binding, which makes it the
 * unit the controller schedules onto a host, restarts after a failure, relocates when a host is
 * withdrawn, and scales across several hosts.
 */
public final class EchoOperative implements StreamOperative {

    /**
     * The operative's stable identity, durable in the same way the procedure's name is. The
     * controller's registry and the durable state both key on this value, so changing it declares a
     * new operative rather than renaming an existing one.
     */
    @Override
    @Nonnull
    public String factoryId() {
        return "echo";
    }

    /**
     * Invoked once at registration to declare the graph. Channels are never constructed directly;
     * each call on the builder returns the next handle in the chain, and the completed declaration
     * is validated as a whole before any message is processed.
     *
     * <p>The expression below declares an entry channel named {@code in} carrying strings, routes
     * it to the echo procedure, labels that procedure's output {@code out}, and delivers the
     * labelled channel to a plugin sink named {@code my_sink}. The label is an identity rather than
     * documentation: it is what a sink registers against and what a test feeds or reads.
     */
    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(String.class, "in").into(EchoProcedure.class).named("out").sink("my_sink");
    }
}
