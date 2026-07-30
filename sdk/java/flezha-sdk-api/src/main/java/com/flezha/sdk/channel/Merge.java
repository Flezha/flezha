package com.flezha.sdk.channel;

import com.flezha.sdk.operation.Operation;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * A <b>merged</b> subscription under construction: one operation fed by two or
 * more source channels, each supplying a mapping from its payload into the
 * operation's declared {@code Input} sum type
 * ({@code multi-input-operations}).
 *
 * <pre>{@code
 * c.merge(SignalInput.class)
 *     .from(tickerStream, SignalInput.Ticker::new)
 *     .from(candleStream, SignalInput.Candle::new)
 *     .into(DetectorOperation.class);
 * }</pre>
 *
 * <p>Sources may be entry channels (sealed payloads) or another operation's
 * output stream, freely mixed — on an <b>unbounded</b> operative. A bounded
 * operative is seeded from its spawn args (and, for a {@code PER_INPUT} loop,
 * its inbox items) through one entry channel, and nothing on that path says
 * which channel a payload arrived on, so a bounded merge over an entry channel
 * is refused at registration ({@code MergeFromEntryChannel}); its sources are
 * the output streams of two or more of its operations.
 *
 * <p>{@code segmentFor} is evaluated on the <b>mapped</b> value, so per-source
 * segmentation is encoded in the sum type. Per-source FIFO order is preserved;
 * nothing is guaranteed across sources — correlate in the operation's own state.
 *
 * <p>The mappings MUST be pure: they run on live delivery and again on WAL
 * replay, and a replay that disagreed with the live run would restore a state
 * the live run never had. They must also be total — a mapping returns
 * {@code I}, not a result, so unlike an entry parser it has no error to hand
 * the channel's retry→DLQ ladder. Do the fallible work in the operation, on the
 * sum type.
 *
 * <p>A mapping that throws is contained everywhere it can be: on an
 * inter-operation source it is attributed to the producing operation
 * (unbounded) or turned into a terminal chain error the saga compensates
 * (bounded). The one unguarded seam is an unbounded entry-channel source, where
 * the mapping runs on the wire-reader path — the same exposure the entry parser
 * there already carries. {@code docs/multi_input_operations.md} tabulates it.
 *
 * <p>Fewer than two sources, the same channel subscribed twice, a source whose
 * edge into this operation is already wired, or a merged type that is not the
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
     * Add an entry channel as a merge source, with the mapping that lifts its
     * payload into the merged input.
     */
    @Nonnull
    <S> Merge<I> from(@Nonnull EntryChannel<S> source, @Nonnull Function<S, I> map);

    /**
     * Add another operation's output stream as a merge source, with the
     * mapping that lifts its payload into the merged input.
     */
    @Nonnull
    <S> Merge<I> from(@Nonnull Stream<S> source, @Nonnull Function<S, I> map);

    /**
     * Close the merge on its consumer and return <b>the consumer's</b> output
     * stream handle, so a merged node chains like any other.
     */
    @Nonnull
    <O> Stream<O> into(@Nonnull Class<? extends Operation<I, ?, O, ?, ?, ?>> operation);

    /**
     * Like {@link #into(Class)}, but the consumer is a distinct named node —
     * see {@link Stream#into(Class, String)}.
     */
    @Nonnull
    <O> Stream<O> into(@Nonnull Class<? extends Operation<I, ?, O, ?, ?, ?>> operation, @Nonnull String nodeName);
}
