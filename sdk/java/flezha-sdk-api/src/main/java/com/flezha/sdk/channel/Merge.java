package com.flezha.sdk.channel;

import com.flezha.sdk.procedure.Procedure;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * A <b>merged</b> subscription under construction: one procedure fed by two or
 * more source channels, each supplying a mapping from its payload into the
 * procedure's declared {@code Input} sum type
 * ({@code multi-input-procedures}).
 *
 * <pre>{@code
 * c.merge(SignalInput.class)
 *     .from(tickerStream, SignalInput.Ticker::new)
 *     .from(candleStream, SignalInput.Candle::new)
 *     .into(DetectorProcedure.class);
 * }</pre>
 *
 * <p>Sources may be entry channels (sealed payloads) or another procedure's
 * output stream, freely mixed — on an <b>stream</b> operative. A workflow
 * operative is seeded from its spawn args (and, for a {@code PER_INPUT} loop,
 * its inbox items) through one entry channel, and nothing on that path says
 * which channel a payload arrived on, so a workflow merge over an entry channel
 * is refused at registration ({@code MergeFromEntryChannel}); its sources are
 * the output streams of two or more of its procedures.
 *
 * <p>{@code entitiesFor} and the merge's own {@link #keyBy} are evaluated on
 * the <b>mapped</b> value, so per-source entity mode is encoded in the sum
 * type. Per-source FIFO order is preserved;
 * nothing is guaranteed across sources — correlate in the procedure's own state.
 *
 * <p>The mappings MUST be pure: they run on live delivery and again on WAL
 * replay, and a replay that disagreed with the live run would restore a state
 * the live run never had. They must also be total — a mapping returns
 * {@code I}, not a result, so unlike an entry parser it has no error to hand
 * the channel's retry→DLQ ladder. Do the fallible work in the procedure, on the
 * sum type.
 *
 * <p>A mapping that throws is contained everywhere it can be: on an
 * inter-procedure source it is attributed to the producing procedure
 * (stream) or turned into a terminal chain error the saga compensates
 * (workflow). The one unguarded seam is a stream entry-channel source, where
 * the mapping runs on the wire-reader path — the same exposure the entry parser
 * there already carries. {@code docs/sdk/multi_input_procedures.md} tabulates it.
 *
 * <p>Fewer than two sources, the same channel subscribed twice, a source whose
 * edge into this procedure is already wired, or a merged type that is not the
 * consumer's {@code inputType()} all fail registration with a named error.
 *
 * <p>Nothing is recorded on the topology until {@code into}, so a merge left
 * unclosed declares no edges at all. Rust marks the equivalent builder
 * {@code #[must_use]} and the compiler warns; this module configures no
 * return-value lint (no ErrorProne/SpotBugs), so here it is a convention: every
 * {@code merge(...)} chain ends in an {@code into(...)}.
 */
public interface Merge<I> {

    /**
     * Add one source — an entry channel or another procedure's output stream —
     * with the mapping that lifts its payload into the merged input. Any
     * transform declared on the source handle runs <b>before</b> that mapping,
     * on the source's own payload type.
     */
    @Nonnull
    <S> Merge<I> from(@Nonnull EdgeStream<S> source, @Nonnull Function<S, I> map);

    /**
     * Declare the Entity key for the merged procedure — once, on the merge,
     * because by the time keying runs every source has been mapped into the
     * one input type. Declaring one on a <i>source</i> handle instead is
     * refused: the key would be handed the merged value, not that source's.
     */
    @Nonnull
    Merge<I> keyBy(@Nonnull Function<I, String> key);

    /**
     * Close the merge on its consumer and return <b>the consumer's</b> output
     * stream handle, so a merged node chains like any other.
     */
    @Nonnull
    <O> Stream<O> into(@Nonnull Class<? extends Procedure<I, ?, O, ?, ?, ?>> procedure);

    /**
     * Like {@link #into(Class)}, but the consumer is a distinct named node —
     * see {@link Stream#into(Class, String)}.
     */
    @Nonnull
    <O> Stream<O> into(@Nonnull Class<? extends Procedure<I, ?, O, ?, ?, ?>> procedure, @Nonnull String nodeName);
}
