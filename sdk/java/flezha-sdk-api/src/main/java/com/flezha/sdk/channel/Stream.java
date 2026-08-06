package com.flezha.sdk.channel;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * A procedure's output stream handle, returned by {@code .into(Proc.class)}:
 * a lightweight reference into the operative's topology builder.
 *
 * <ul>
 *   <li>{@code stream.into(Next.class)} wires Next as a consumer of this
 *       stream's channel and returns <b>Next's own</b> output stream handle,
 *       so a linear pipeline is one chained expression reading in flow order.
 *   <li>Using one handle twice — two {@code .into} calls on the same held
 *       variable — is <b>fan-out</b>: every consumer receives every value
 *       released on the channel. {@link #branch(Consumer[])} is equivalent
 *       sugar.
 *   <li>Two <i>different</i> streams wiring the same unnamed class fan
 *       <b>in</b>: the mentions unify into ONE node, which accumulates both
 *       producers' contributions.
 *   <li>Per-channel configuration ({@link #named}, {@link #keyed},
 *       {@link #codec}, {@link #onFull}, {@link #retryMax},
 *       {@link #retryBackoffMs}, {@link #alsoOutput}) chains in any order
 *       relative to consumer wiring.
 * </ul>
 *
 * <p>The channel out of a node is created lazily on first use of the handle
 * and named by the node's identity string (the procedure's {@code name()}, or
 * the explicit node name given at wiring time) unless {@link #named(String)}
 * overrides it.
 *
 * <p>Handles are only usable inside {@code channels()} / {@code procedures()}:
 * the topology freezes at registration and a retained handle used afterwards
 * throws {@link IllegalStateException} naming the operative.
 *
 * <p>This is an abstract class rather than an interface so {@link #branch}
 * can be a {@code final} {@link SafeVarargs} method (interfaces cannot carry
 * {@code @SafeVarargs} on abstract or default methods, and callers would get
 * unchecked-generic-array warnings).
 */
public abstract class Stream<T> implements EdgeStream<T> {

    /**
     * Override the auto-derived channel name (the producer node's identity)
     * everywhere the channel name appears: wire metadata, journals, errors.
     * Derived, overridden, and entry names share one per-operative namespace
     * with a duplicate check at registration.
     */
    @Nonnull
    public abstract Stream<T> named(@Nonnull String name);

    /**
     * Declare that emits on this channel carry a natural key, supplied via
     * {@code ctx.emitKeyed(key, value)}. The runtime stamps {@code flz.key}
     * on the plugin frame. Requires a plugin wire ({@link #output} or
     * {@link #alsoOutput}); a keyed channel without one fails registration.
     */
    @Nonnull
    public abstract Stream<T> keyed();

    /**
     * Declare a codec for this channel's payload, using the SDK's default
     * JSON serialization. <b>Required</b> on every inter-procedure channel of
     * a durable structured bounded operative — the handoff between two
     * procedures is journaled at the proc barrier and re-seeded from that blob
     * on crash-resume.
     */
    @Nonnull
    public abstract Stream<T> codec();

    /** {@link #codec()} with an explicit encoder/decoder pair. */
    @Nonnull
    public abstract Stream<T> codecWith(@Nonnull Function<T, byte[]> encoder, @Nonnull Function<byte[], T> decoder);

    /**
     * {@inheritDoc}
     *
     * <p>Refined to {@link Stream} so the producer-side surface stays
     * chainable in any order relative to channel configuration.
     */
    @Override
    @Nonnull
    public abstract Stream<T> onFull(@Nonnull OverflowPolicy policy);

    /** {@inheritDoc} Refined to {@link Stream} — see {@link #onFull}. */
    @Override
    @Nonnull
    public abstract Stream<T> retryMax(int maxAttempts);

    /** {@inheritDoc} Refined to {@link Stream} — see {@link #onFull}. */
    @Override
    @Nonnull
    public abstract Stream<T> retryBackoffMs(long baseMs);

    /**
     * Attach a plugin sink alongside inter-procedure fan-out: typed values
     * still reach {@code .into(...)} consumers, and a serialized copy is
     * published to the named plugin. The serializer is invoked once per emit
     * regardless of consumer count. Declaring a second sink on the same
     * channel fails registration.
     */
    @Nonnull
    public abstract Stream<T> alsoOutput(@Nonnull String pluginName, @Nonnull Function<T, byte[]> serializer);

    /**
     * {@link #alsoOutput(String, Function)} with the default serialization
     * rules of {@link #output(String)}: passthrough for {@code byte[]},
     * UTF-8 for {@code String}, Jackson JSON otherwise.
     */
    @Nonnull
    public abstract Stream<T> alsoOutput(@Nonnull String pluginName);

    /**
     * Close this stream plugin-bound: the operative declares a named exit and
     * the controller's plugin layer receives the values, serialized with the
     * default rules — passthrough for {@code byte[]}, UTF-8 for
     * {@code String}, Jackson JSON otherwise. Terminal: the returned
     * {@link Channel} exposes no further builder methods.
     */
    @Nonnull
    public abstract Channel<T> output(@Nonnull String pluginName);

    /** {@link #output(String)} with an explicit serializer override. */
    @Nonnull
    public abstract Channel<T> output(@Nonnull String pluginName, @Nonnull Function<T, byte[]> serializer);

    /**
     * Branching sugar: each arm receives <b>this</b> handle, so
     * {@code s.branch(b -> b.into(A.class), b -> b.into(B.class))} builds
     * exactly the topology of two {@code s.into} statements — nothing more,
     * and the two spellings are freely mixable. Fewer than two arms is a
     * registration error (accumulated and thrown at build, like every other
     * declaration error).
     */
    @SafeVarargs
    @Nonnull
    public final Stream<T> branch(@Nonnull Consumer<Stream<T>>... arms) {
        return branchArms(List.of(arms));
    }

    /** Implementation seam for {@link #branch(Consumer[])}. */
    @Nonnull
    protected abstract Stream<T> branchArms(@Nonnull List<Consumer<Stream<T>>> arms);
}
