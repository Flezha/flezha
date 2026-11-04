package com.flezha.examples.echo;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.relay.Relay;
import javax.annotation.Nonnull;

/**
 * The deployable unit. A relay declares the graph a message travels: the entry channel it
 * arrives on, the steps it passes through, and the channel its result leaves on.
 *
 * <p>The relay is what the controller schedules onto a host, restarts after a failure,
 * relocates when a host is withdrawn, and scales across several hosts. A tasklet is one stage
 * inside that graph.
 */
public final class EchoRelay implements Relay {

    /**
     * The relay's stable identity, durable in the same way the tasklet's name is. The
     * controller's registry and the durable state both key on this value, so changing it declares a
     * new relay rather than renaming an existing one.
     */
    @Override
    @Nonnull
    public String factoryId() {
        return "echo";                       // the name `flz relay start` takes
    }

    /**
     * Invoked once at registration to declare the graph. Channels are never constructed directly;
     * each call on the builder returns the next handle in the chain, and the completed declaration
     * is validated as a whole before any message is processed.
     *
     * <p>The expression below declares an entry channel named {@code in} carrying strings, routes
     * it to the echo tasklet, and terminates that tasklet's output on the plugin layer under
     * the name {@code out}. The name an output carries is the channel's own: an identity rather than
     * documentation, and what a plugin filter matches or a test reads.
     */
    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(String.class, "in")          // messages arrive on the entry channel "in"
                .into(Echo.class)            // each one runs through the echo tasklet
                .output("out");              // the result leaves on "out", for a plugin to pick up
    }
}
