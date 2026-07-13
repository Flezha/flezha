package com.flezha.sdk.channel;

import javax.annotation.Nonnull;

/**
 * Fluent builder consumed by {@link com.flezha.sdk.operative.UnboundedOperative#channels(ChannelBuilder)}.
 * Declarations land in a per-UnboundedOperative topology that the runtime freezes at
 * {@code registerUnbounded} time.
 */
public interface ChannelBuilder {

    /**
     * Declare an entry channel fed by the controller. {@code T} must be
     * {@code byte[]} or {@code String}; other types throw
     * {@link com.flezha.sdk.operative.UnboundedOperativeConfigurationException} at
     * {@code registerUnbounded} time (umbrella Decision 12).
     *
     * <p>The returned {@link EntryChannel} only exposes {@code .into(...)}
     * for wiring an entry-operation consumer.
     */
    @Nonnull
    <T> EntryChannel<T> input(@Nonnull Class<T> type, @Nonnull String name);

    /**
     * Declare an inter-operation or plugin-bound channel. The returned
     * {@link ChannelDecl} requires {@code .from(...)} to identify a producer
     * Operation; after {@code .from}, it requires either {@code .into(...)}
     * (inter-operation) or {@code .toPlugin(...)} (plugin-bound). A channel
     * with neither is rejected at build-topology time with
     * {@link com.flezha.sdk.operative.UnboundedOperativeConfigurationException}.
     */
    @Nonnull
    <T> ChannelDecl<T> channel(@Nonnull Class<T> type, @Nonnull String name);

    /**
     * Opt this Operative into the <b>effectively-once output tier</b>: each
     * invocation's state changes, plugin outputs and consumed input offset ship
     * as ONE {@code segment_effect_record}, which the controller persists as one
     * WAL record under one fsync. A crash can then never advance the consumed
     * offset while losing the state or the output it was advanced for.
     *
     * <p>Every plugin-bound channel of a tier-enabled Operative MUST be declared
     * {@code .keyed()} — the tier's replay re-emits outputs, and only a keyed
     * sink can absorb that as an idempotent upsert. A non-keyed plugin channel
     * fails registration.
     */
    void effectivelyOnce();

    /**
     * Acknowledge the consumed input offset only at an explicit
     * {@code ctx.checkpoint()}, rather than after every invocation.
     *
     * <p>The interval between checkpoints is the work a crash replays and the
     * work the controller's input log must retain. In exchange, a Segment whose
     * state is only coherent at a boundary (a closed window, a completed batch)
     * never persists a half-built view of it.
     *
     * <p>Requires {@link #effectivelyOnce()} — there is no offset to withhold
     * without the tier — and declaring it alone fails registration with
     * {@code CheckpointAckWithoutTier}.
     */
    void checkpointAck();

    /**
     * Declare that this Operative corrects itself when an input arrives late:
     * an input whose event time falls behind the Segment's event-time high-water
     * mark, but by less than {@code windowMs}, triggers the controller to rewind
     * the Segment to a baseline before that event time and replay the affected
     * slice in event-time order.
     *
     * <p>The window is a promise the controller must be able to keep: it sizes
     * the input log's hold window and the snapshot age floor, because the past
     * being repaired has to still exist. That is why the window is declared to
     * the controller rather than handled purely SDK-side.
     *
     * <p>Requires {@link #effectivelyOnce()} (a correction rewinds a consumed
     * offset), an {@code EventTimeFrom} clock (there is no event time to compare
     * without one), and every channel keyed (a corrected re-emit must overwrite,
     * not duplicate). Each is checked at registration.
     */
    void correctionWindow(long windowMs);

    /**
     * How to treat an input that arrives beyond the declared
     * {@link #correctionWindow(long)}. Defaults to
     * {@link BeyondWindowPolicy#PROCESS_AS_ARRIVAL}.
     */
    void beyondWindowPolicy(@Nonnull BeyondWindowPolicy policy);
}
