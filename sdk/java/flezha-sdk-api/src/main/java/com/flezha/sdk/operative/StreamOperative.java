package com.flezha.sdk.operative;

import com.flezha.sdk.channel.ChannelBuilder;

/**
 * Declares a graph of channels and procedures that runs continuously over a stream of messages.
 *
 * <p>This is the operative kind to reach for when work is expressed as messages flowing through one
 * or more procedures: readings arriving from a sensor, trades from a market feed, events from an
 * application. It runs until stopped, holds per-entity state between messages, and scales by
 * splitting that state across entities. For work consisting of ordered steps that must not be
 * re-executed as a unit after a partial failure, use {@link WorkflowOperative} instead.
 *
 * <p>An implementation declares two things and nothing else:
 *
 * <pre>{@code
 * public final class WordCountOperative implements StreamOperative {
 *
 *     @Override
 *     public String factoryId() {
 *         return "word_count";
 *     }
 *
 *     @Override
 *     public void channels(ChannelBuilder c) {
 *         c.input(String.class, "lines")
 *                 .into(Split.class)
 *                 .keyBy(word -> word)
 *                 .into(Tally.class)
 *                 .named("totals")
 *                 .sink("word_totals");
 *     }
 * }
 * }</pre>
 *
 * <p>The class is then declared on a runner, which is what starts serving it:
 *
 * <pre>{@code
 * Flezha.createRunner("my_app").defineStream(WordCountOperative.class).run();
 * }</pre>
 *
 * <p>Implementations must provide a public no-argument constructor. The runtime instantiates the
 * class once at registration in order to read its graph, and that instance is not the one that
 * handles messages.
 */
public interface StreamOperative extends Operative {

    /**
     * Declares the graph. Called exactly once per class, at
     * {@link com.flezha.sdk.FlezhaRunner#defineStream(Class) defineStream} time, and the resulting
     * graph is shared by every instance of this operative that the controller later starts.
     *
     * <p>Channels are never constructed directly. Each call on the builder returns the next handle
     * in the chain, so the whole graph is one fluent expression, and the completed declaration is
     * validated as a whole before any message is processed. An inconsistent declaration, or one
     * requesting a capability the deployment cannot supply, raises
     * {@link StreamOperativeConfigurationException} naming the declaration at fault.
     *
     * <p>Because this method runs once at registration rather than per message, it must not depend
     * on anything that varies at run time.
     */
    void channels(ChannelBuilder c);

    /**
     * Whether per-entity state is persisted for this operative class. Defaults to {@code true}.
     *
     * <p>Override to {@code false} to opt out for the whole class: changes are still applied in
     * memory, so procedures behave normally, but nothing is written to disk and an entity begins
     * with no history after a restart. This is a throughput trade for work whose state can be
     * recomputed or safely lost, and it is not the same as declaring
     * {@link com.flezha.sdk.state.NoChange}, which removes the change machinery from the procedure
     * altogether.
     *
     * <p>An operator can override this per runner; the effective value is the runner's override
     * where present, otherwise this method's, otherwise {@code true}.
     */
    default boolean persistenceEnabled() {
        return true;
    }
}
