package com.flezha.sdk.channel;

import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Producer-first fluent graph builder consumed by
 * {@link com.flezha.sdk.operative.StreamOperative#channels(ChannelBuilder)}
 * (and by workflow {@code procedures(...)} authoring).
 *
 * <p>{@code c.input(T.class, "name").into(A.class).into(B.class).sink("plugin")}
 * — {@code .into} wires a consumer and returns that procedure's <b>output
 * stream handle</b> ({@link Stream}), so a linear pipeline is one chained
 * expression. Branching is handle reuse (hold the handle in a variable and
 * wire it twice) or the equivalent {@link Stream#branch} sugar. Fan-in is two
 * streams naming the same consumer class.
 *
 * <p>Entry channel names stay explicit and mandatory — they are the
 * operative's external input address. Inter-procedure channel names
 * auto-derive from the producer node's identity, with {@link Stream#named}
 * as the override. Declarations land in a per-operative topology the runtime
 * freezes at registration.
 */
public interface ChannelBuilder {

    /**
     * Declare the entry channel fed by the controller. Any payload type is
     * accepted: {@code byte[]} is delivered unchanged and {@code String}
     * UTF-8-decoded, byte-identical to the pre-typed-entry contract; any
     * other type is deserialized at the entry edge from the wire bytes with
     * the SDK's default JSON codec (Jackson). A failing parse is classified
     * like a failing invocation and rides this channel's retry→DLQ ladder.
     * For a non-JSON wire format, use
     * {@link #input(Class, String, Function)}.
     */
    @Nonnull
    <T> EntryChannel<T> input(Class<T> type, String name);

    /**
     * Declare the entry channel with a custom {@code bytes → T} parser,
     * replacing the default JSON codec of {@link #input(Class, String)}.
     *
     * <p>The parser MUST be deterministic — the same bytes always produce the
     * same value, with no ambient reads (time, randomness, I/O) — the same
     * contract output serializers carry. A thrown exception is classified
     * like a failing invocation and rides this channel's retry→DLQ ladder.
     */
    @Nonnull
    <T> EntryChannel<T> input(Class<T> type, String name, Function<byte[], T> parser);

    /**
     * Open a <b>merged</b> subscription: one procedure whose {@code Input} is
     * the sum type {@code type}, fed by two or more source channels that each
     * supply a mapping into it. See {@link Merge}.
     */
    @Nonnull
    <I> Merge<I> merge(Class<I> type);

    /**
     * Turn an operative-wide document stream into per-key deliveries: every key
     * seen on {@code traffic} receives each distinct document exactly once, on
     * that key's own next traffic item, as a {@link RoutedDocument} the consumer
     * keys by.
     *
     * <pre>{@code
     * EntryChannel<Thresholds> thresholds = c.input(Thresholds.class, "thresholds");
     * EntryChannel<Trade> trades = c.input(Trade.class, "trades");
     * Stream<RoutedDocument<Thresholds>> routed =
     *         c.routeLatest(Thresholds.class, thresholds, trades, Trade::symbol);
     *
     * c.merge(DetectorInput.class)
     *     .from(trades, DetectorInput::ofTrade)
     *     .from(routed, DetectorInput::ofThresholds)
     *     .keyBy(DetectorInput::key)
     *     .into(Detector.class);
     * }</pre>
     *
     * <p>Why this shape rather than a broadcast: a dynamically-keyed procedure
     * cannot receive one — every key function is total, and inside a partitioned
     * member a computed key is admitted against that member's key-group range.
     * Delivering by fanning out over a <i>remembered</i> key set would emit keys
     * this member does not own (a rescale moved them, or one out-of-range item
     * was seen during a reconcile window), and every such delivery parks
     * against the partition guard. Delivering on the key's own traffic makes
     * over-delivery impossible by construction: the only keys emitted are keys
     * whose traffic just proved this member owns them.
     *
     * <p>Contract:
     *
     * <ul>
     *   <li>a document {@link Object#equals equal} to the retained one is
     *       <b>inert</b> — a re-poll after a reconnect re-delivers the same
     *       document and nothing downstream sees it;
     *   <li>a differing document replaces it and bumps the version, so every key
     *       is owed one delivery again;
     *   <li>a key never seen in traffic receives <b>nothing</b>;
     *   <li><b>latency is one tick per key</b>: a key learns of a new document
     *       on its next traffic item, not when the document arrives. That is the
     *       price of the ownership-safety construction above.
     * </ul>
     *
     * <p>The document is kept <b>opaque</b> — retained and compared, never
     * parsed — so parsing (and its failure mode) stays with the consumer exactly
     * as when the consumer read the document stream directly. Declare
     * {@code equals} on it accordingly: a document type without value equality
     * makes every arrival a new version.
     *
     * <p>The block expands to a merge into ONE SDK-packaged single-entity
     * procedure whose {@code {document, version, delivered}} state rides the
     * standard change journal: authoring sugar over existing primitives, not a
     * runtime feature. The node is named {@code route_latest} like any procedure
     * node, so a second router in one operative needs
     * {@link #routeLatest(Class, EdgeStream, EdgeStream, Function, String)} to
     * name it.
     *
     * @param documentType the document's runtime type — the routed payload's
     *     {@code Class} token, and the registration the retained document's
     *     journaled state needs
     */
    @Nonnull
    <D, T> Stream<RoutedDocument<D>> routeLatest(
            Class<D> documentType, EdgeStream<D> documents, EdgeStream<T> traffic, Function<T, String> keyFn);

    /**
     * {@link #routeLatest(Class, EdgeStream, EdgeStream, Function)} with an
     * explicit node identity, so one operative may carry several routers (the
     * node-identity rule makes unnamed mentions of one class one node).
     */
    @Nonnull
    <D, T> Stream<RoutedDocument<D>> routeLatest(
            Class<D> documentType,
            EdgeStream<D> documents,
            EdgeStream<T> traffic,
            Function<T, String> keyFn,
            String nodeName);

    /**
     * Opt this Operative into the <b>effectively-once output tier</b>: each
     * invocation's state changes, plugin outputs and consumed input offset ship
     * as ONE {@code entity_effect_record}, which the controller persists as one
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
     * work the controller's input log must retain. In exchange, an Entity whose
     * state is only coherent at a boundary (a closed window, a completed batch)
     * never persists a half-built view of it.
     *
     * <p>Requires {@link #effectivelyOnce()} — there is no offset to withhold
     * without the tier — and declaring it alone fails registration with
     * {@code CheckpointAckWithoutTier}.
     *
     * <p>Emits made before a checkpoint are still flushed as they happen —
     * checkpointing moves the <em>acknowledgement</em>, not the output. To hold
     * them for the interval too, see {@link #outputsAtCheckpoint(long)}.
     */
    void checkpointAck();

    /**
     * Close the checkpoint interval once {@code inputs} inputs have folded into
     * it, instead of waiting for an explicit {@code ctx.checkpoint()}.
     *
     * <p>An explicit checkpoint still closes the interval early: this is the
     * ceiling, not the only closer. Implies {@link #checkpointAck()}.
     */
    void checkpointEvery(int inputs);

    /**
     * Hold the interval's outputs with its changes and ship them inside its
     * checkpoint record, which the controller flushes as one batch after that
     * record is fsynced — one WAL append per interval instead of one per
     * invocation.
     *
     * <p>The trade is output latency: an emit is not visible downstream until
     * its interval closes. {@code maxDelayMs} bounds that — the interval closes
     * that long after the input that opened it even if nothing else arrives, so
     * a quiet Entity never holds an output indefinitely. It is a parameter
     * rather than an option because an unbounded hold is not a posture anyone
     * should be able to declare by omission.
     *
     * <p>Replay is exactly the mode's existing contract: an interval that did
     * not checkpoint re-produces its outputs when it replays, a checkpointed
     * record re-drives with identical identity, and the keyed-sink dedup the
     * tier already requires absorbs both. Implies {@link #checkpointAck()}.
     */
    void outputsAtCheckpoint(long maxDelayMs);

    /**
     * Declare that this Operative corrects itself when an input arrives late:
     * an input whose event time falls behind the Entity's event-time high-water
     * mark, but by less than {@code windowMs}, triggers the controller to rewind
     * the Entity to a baseline before that event time and replay the affected
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
    void beyondWindowPolicy(BeyondWindowPolicy policy);
}
