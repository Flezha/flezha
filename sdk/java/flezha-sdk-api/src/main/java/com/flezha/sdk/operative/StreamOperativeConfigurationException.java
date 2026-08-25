package com.flezha.sdk.operative;

import java.util.List;
import java.util.Objects;

/**
 * Thrown by {@link com.flezha.sdk.FlezhaRunner#defineStream(Class)} when a StreamOperative's
 * declared topology violates a wiring constraint (type-equality between a
 * stream's payload and its consumer Procedures' input, node-identity and
 * channel-name uniqueness, graph shape — one entry, reachable, acyclic —
 * static-mode default-impl detection).
 *
 * <p>Carries channel name and expected/actual type info so the runner
 * can diagnose the misconfiguration without re-running with a debugger.
 */
public class StreamOperativeConfigurationException extends RuntimeException {

    private final String channel;
    private final Class<?> expected;
    private final Class<?> actual;

    public StreamOperativeConfigurationException(String message) {
        super(message);
        this.channel = null;
        this.expected = null;
        this.actual = null;
    }

    public StreamOperativeConfigurationException(String channel, Class<?> expected, Class<?> actual, String message) {
        super(message);
        this.channel = channel;
        this.expected = expected;
        this.actual = actual;
    }

    public StreamOperativeConfigurationException(List<String> errors) {
        super("operative topology validation failed: " + Objects.requireNonNull(errors));
        this.channel = null;
        this.expected = null;
        this.actual = null;
    }

    /**
     * Raised when a Procedure's type argument cannot be recovered from its
     * generic supertype chain and the corresponding type-token method is not
     * overridden — the argument is still generic at the concrete class. Naming
     * the class and the parameter is the whole contract: a guess would surface
     * later as a type mismatch far from its cause.
     */
    public static StreamOperativeConfigurationException unresolvedTypeToken(
            String className, String parameter, String method) {
        return new StreamOperativeConfigurationException("procedure class `" + className + "` leaves the `"
                + parameter + "` type argument of Procedure<…> unresolved; declare it concretely on the "
                + "implements/extends clause, or override `" + method + "()`");
    }

    /**
     * Raised when a Procedure declares {@code EventTimeFrom} but does not
     * override {@code eventTime(Input)}. Rust enforces the extractor with a
     * compile-time {@code TimeExtractor<Input>} bound; Java's erasure precludes
     * that, so the same defect is caught at registration — before any input is
     * served and before anything durable exists.
     */
    public static StreamOperativeConfigurationException eventTimeExtractorMissing(String procedureName) {
        return new StreamOperativeConfigurationException("procedure `" + procedureName
                + "` declares the EventTimeFrom clock but does not override `eventTime(Input)`; "
                + "an event-time clock has no value to serve without an extractor");
    }

    /**
     * Raised when a Procedure declares {@code LogicalClock} with
     * {@code Change = NoChange}. The logical clock serves the durable change
     * seq, and a stateless Procedure has none. Carries the Rust error name
     * {@code UnservableClock}.
     */
    public static StreamOperativeConfigurationException unservableClock(String procedureName) {
        return new StreamOperativeConfigurationException("UnservableClock: procedure `" + procedureName
                + "` declares LogicalClock with Change = NoChange; there is no durable change seq to serve");
    }

    /**
     * Raised when {@code checkpointAck()} is declared without
     * {@code effectivelyOnce()}. Checkpointing withholds the consumed input
     * offset until a boundary — and without the tier there is no offset to
     * withhold.
     */
    public static StreamOperativeConfigurationException checkpointAckWithoutTier() {
        return new StreamOperativeConfigurationException(
                "CheckpointAckWithoutTier: `checkpointAck()` requires `effectivelyOnce()`; "
                        + "without the tier there is no consumed offset to withhold until a checkpoint");
    }

    /**
     * Raised when {@code checkpointEvery(0)} is declared. An interval that
     * closes every zero inputs is not an interval — omitting the call is how
     * an Operative says "close only on an explicit {@code ctx.checkpoint()}".
     */
    public static StreamOperativeConfigurationException checkpointEveryZero() {
        return new StreamOperativeConfigurationException(
                "CheckpointEveryZero: `checkpointEvery(0)` is not an interval; omit it to close "
                        + "only on an explicit ctx.checkpoint()");
    }

    /**
     * Raised when {@code outputsAtCheckpoint(0)} is declared. The delay is what
     * bounds a quiet Entity's output latency; zero would make the bound
     * meaningless rather than tight.
     */
    public static StreamOperativeConfigurationException checkpointDelayZero() {
        return new StreamOperativeConfigurationException(
                "CheckpointDelayZero: `outputsAtCheckpoint(0)` gives the interval no time to "
                        + "accumulate; declare the delay you are willing to hold an output for");
    }

    /**
     * Raised when {@code correctionWindow(...)} is declared without a
     * prerequisite it cannot work without: the tier (a correction rewinds a
     * consumed offset), an {@code EventTimeFrom} clock (there is no event time
     * to compare), or keyed channels (a corrected re-emit must overwrite rather
     * than duplicate).
     */
    public static StreamOperativeConfigurationException correctionWindowRequires(String missing) {
        return new StreamOperativeConfigurationException("`correctionWindow(...)` requires " + missing
                + "; a correction rewinds the Entity to a baseline before the late input's event time "
                + "and replays the slice, which is impossible without it");
    }

    /**
     * Raised when an effectively-once Operative has a plugin-bound channel that
     * is not {@code .keyed()}. The tier's replay re-emits outputs, and only a
     * keyed sink can absorb that as an idempotent upsert.
     */
    public static StreamOperativeConfigurationException tierChannelNotKeyed(String channel) {
        return new StreamOperativeConfigurationException("channel `" + channel
                + "` is plugin-bound on an `effectivelyOnce()` operative but is not declared `.keyed()`; "
                + "the tier re-emits outputs on replay, which only a keyed sink can absorb idempotently");
    }

    /**
     * Raised when a channel declares a dropping overflow policy on an
     * {@code effectivelyOnce()} Operative. A shed input's offset is still
     * advanced past by the tier's watermark, so the loss is permanent under a
     * contract that promises nothing is lost. A correction window requires the
     * tier, so it is refused by the same rule.
     */
    public static StreamOperativeConfigurationException tierChannelSheds(String channel, String policy) {
        return new StreamOperativeConfigurationException("channel `" + channel
                + "` declares `.onFull(" + policy + ")` on an `effectivelyOnce()` operative; "
                + "a shed input's offset is still advanced past by the tier's watermark, so the loss "
                + "is permanent and reported nowhere — declare one or the other, not both");
    }

    /**
     * Raised when a channel is declared {@code .keyed()} without a plugin wire.
     * A key rides as a {@code flz.key} stamp on a plugin frame, so a pure
     * inter-procedure channel has nowhere to carry it.
     */
    public static StreamOperativeConfigurationException keyedChannelWithoutPluginWire(String channel) {
        return new StreamOperativeConfigurationException("channel `" + channel
                + "` is declared `.keyed()` but has no plugin wire; a key rides on a plugin frame's "
                + "`flz.key` stamp, so it has nowhere to go on a pure inter-procedure channel");
    }

    /**
     * Factory raised at {@code registerStream} time when a channel is declared
     * with {@code sink(...)} more than once. Only one plugin sink
     * per channel is allowed; the first call registers it, the second fails.
     */
    public static StreamOperativeConfigurationException duplicatePluginSink(String channel) {
        return new StreamOperativeConfigurationException("channel `" + channel + "` was declared with `sink(...)` "
                + "more than once; only one plugin sink per channel is allowed");
    }

    /**
     * Raised when a node identity string (an explicit node name or an
     * procedure's {@code name()}) is claimed by two distinct nodes. Carries
     * the Rust error name {@code NodeIdentityCollision}.
     */
    public static StreamOperativeConfigurationException nodeIdentityCollision(String name) {
        return new StreamOperativeConfigurationException("NodeIdentityCollision: node identity `" + name
                + "` is declared by two distinct nodes; a node name must be unique per operative"
                + " and distinct from every procedure name()");
    }

    /** Raised when {@code branch(...)} is called with fewer than two arms. */
    public static StreamOperativeConfigurationException branchTooFewArms(int arms) {
        return new StreamOperativeConfigurationException("branch(...) requires at least two arms; " + arms + " given");
    }

    /**
     * Raised when a {@code merge(...)} closes on fewer than two sources.
     * Carries the Rust error name {@code MergeTooFewSources}.
     */
    public static StreamOperativeConfigurationException mergeTooFewSources(String procedure, int sources) {
        return new StreamOperativeConfigurationException("MergeTooFewSources: merge into procedure `" + procedure
                + "` declares " + sources + " source channel(s); a merge consumes two or more — wire a single"
                + " source with `.into(Proc.class)` instead");
    }

    /**
     * Raised when one {@code merge(...)} subscribes the same channel twice.
     * Carries the Rust error name {@code DuplicateMergeSource}.
     */
    public static StreamOperativeConfigurationException duplicateMergeSource(String channel) {
        return new StreamOperativeConfigurationException("DuplicateMergeSource: merge subscribes channel `"
                + channel + "` twice; each merge source must be a distinct channel (one mapping per channel"
                + " into the merged input)");
    }

    /**
     * Raised when a merge source would re-wire a channel→procedure edge that
     * {@code into(...)} or another merge already declared. Carries the Rust
     * error name {@code EdgeAlreadyWired}.
     */
    public static StreamOperativeConfigurationException edgeAlreadyWired(String channel, String procedure) {
        return new StreamOperativeConfigurationException("EdgeAlreadyWired: channel `" + channel
                + "` already feeds procedure `" + procedure + "`; an edge carries exactly one transform and one"
                + " key or neither, so a re-declaration where either side carries one is refused rather than"
                + " silently keeping the first");
    }

    /**
     * Raised when a {@code keyBy(...)} edge wires a procedure that declares a
     * non-empty {@code entityKeys()}. Carries the Rust error name
     * {@code KeyByOnUnkeyableTarget}.
     */
    public static StreamOperativeConfigurationException keyByOnUnkeyableTarget(String channel, String procedure) {
        return new StreamOperativeConfigurationException("KeyByOnUnkeyableTarget: channel `" + channel
                + "` declares .keyBy(...) into procedure `" + procedure + "`, which declares a non-empty"
                + " entityKeys() (static mode); static keying routes by entitiesFor and may name several"
                + " Entities, edge keying names exactly one — declare one OR the other, not both");
    }

    /**
     * Raised when one consumer edge declares two key functions. Carries the
     * Rust error name {@code DuplicateKeyBy}.
     */
    public static StreamOperativeConfigurationException duplicateKeyBy(String channel) {
        return new StreamOperativeConfigurationException(
                "DuplicateKeyBy: channel `" + channel + "` declares .keyBy(...) twice before one `.into(...)`");
    }

    /**
     * Raised when a transform or key is declared on a handle already closed by
     * a plugin sink. Carries the Rust error name {@code TransformAfterTerminal}.
     */
    public static StreamOperativeConfigurationException transformAfterTerminal(String channel) {
        return new StreamOperativeConfigurationException("TransformAfterTerminal: channel `" + channel
                + "` declares a transform or .keyBy(...) after a plugin sink; the sink serializes the channel's"
                + " own value, so nothing downstream of it can be reshaped");
    }

    /**
     * Raised when a transform is declared after the key it would feed. Carries
     * the Rust error name {@code TransformAfterKeyBy}.
     */
    public static StreamOperativeConfigurationException transformAfterKeyBy(String channel) {
        return new StreamOperativeConfigurationException("TransformAfterKeyBy: channel `" + channel
                + "` declares a transform after .keyBy(...); the key is evaluated on the final post-transform"
                + " value, so it must be the last link before `.into(...)`");
    }

    /**
     * Raised when a transform chain is never closed by an {@code into(...)}.
     * Carries the Rust error name {@code DanglingTransformChain}.
     */
    public static StreamOperativeConfigurationException danglingTransformChain(String channel) {
        return new StreamOperativeConfigurationException("DanglingTransformChain: channel `" + channel
                + "` declares a transform or .keyBy(...) that is never closed by `.into(...)`; a transform chain"
                + " is stored on the consumer edge it terminates in, so a dangling one would silently do nothing");
    }

    /**
     * Raised when a keyless edge feeds a procedure another edge keyed into
     * dynamic mode. Carries the Rust error name
     * {@code UnkeyedEdgeIntoDynamicProcedure}.
     */
    public static StreamOperativeConfigurationException unkeyedEdgeIntoDynamicProcedure(
            String channel, String procedure) {
        return new StreamOperativeConfigurationException("UnkeyedEdgeIntoDynamicProcedure: channel `" + channel
                + "` feeds procedure `" + procedure + "` without a .keyBy(...), but another edge keys it into"
                + " dynamic mode; every edge into a dynamic-mode procedure must name the Entity its inputs reach");
    }

    /**
     * Raised when a node's output (transitively) reaches its own input.
     * Carries the Rust error name {@code Cycle}.
     */
    public static StreamOperativeConfigurationException cycle(String procedure) {
        return new StreamOperativeConfigurationException("Cycle: procedure `" + procedure
                + "` is on a cycle; an operative topology is a DAG — a node's output may never"
                + " reach its own input");
    }

    /**
     * Raised when a procedure is not reachable from any entry channel.
     * Carries the Rust error name {@code UnreachableProcedure}.
     */
    public static StreamOperativeConfigurationException unreachableProcedure(String procedure) {
        return new StreamOperativeConfigurationException(
                "UnreachableProcedure: procedure `" + procedure + "` is not reachable from the entry channel");
    }

    /**
     * Factory raised when a Procedure's resolved {@code mailboxCapacity()} (after
     * applying the {@code FLEZHA_MAILBOX_CAPACITY} env fallback) is zero.
     * Capacity must be in {@code 1..=1_048_576}.
     */
    public static StreamOperativeConfigurationException mailboxCapacityZero(String procedure) {
        return new StreamOperativeConfigurationException(
                "procedure `" + procedure + "` declared mailboxCapacity = 0; capacity must be in 1..=1_048_576");
    }

    /**
     * Factory raised when a Procedure's resolved {@code mailboxCapacity()} is
     * above {@code 1_048_576}. Capacity must be in {@code 1..=1_048_576}.
     */
    public static StreamOperativeConfigurationException mailboxCapacityTooLarge(String procedure, long value) {
        return new StreamOperativeConfigurationException("procedure `" + procedure + "` declared mailboxCapacity = "
                + value + "; capacity must be in 1..=1_048_576");
    }

    public String channel() {
        return channel;
    }

    public Class<?> expected() {
        return expected;
    }

    public Class<?> actual() {
        return actual;
    }
}
