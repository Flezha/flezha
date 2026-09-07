package com.flezha.sdk.procedure;

import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * The unit of user code: handles one message at a time for one entity.
 *
 * <p>A procedure is where your logic goes. The runtime holds the per-entity state and threads it
 * into {@link #run} through a {@link StateHandle}, so the class itself carries no mutable fields of
 * its own that matter across messages.
 *
 * <p>A stateless procedure declares {@link NoChange} and is a plain function of its input:
 *
 * <pre>{@code
 * public final class EchoProcedure
 *         implements Procedure<String, EchoProcedure.None, String, NoChange, WallClock, Void> {
 *
 *     public record None() {}
 *
 *     @Override
 *     public String name() {
 *         return "echo";
 *     }
 *
 *     @Override
 *     public None initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
 *         return new None();
 *     }
 *
 *     @Override
 *     public void run(String input, StateHandle<None, NoChange> state, ProcedureCtx ctx) {
 *         ctx.emit("echo: " + input);
 *     }
 * }
 * }</pre>
 *
 * <p>A procedure that remembers something declares a change type instead of {@code NoChange}, and
 * mutates state only through {@link StateHandle#apply}, never by assigning a field:
 *
 * <pre>{@code
 * public record Increment(long atMs) implements Change {}
 *
 * @Override
 * public void applyChange(Counter state, Increment change) {
 *     state.count++;              // the ONLY place a field is written
 * }
 *
 * @Override
 * public void run(String input, StateHandle<Counter, Increment> state, ProcedureCtx ctx) {
 *     state.apply(new Increment(ctx.now()));
 *     ctx.emit("count=" + state.get().count);
 * }
 * }</pre>
 *
 * <p>The reason for that indirection is that state is persisted as the ordered log of applied
 * changes and reconstructed by replaying it, so a directly assigned field is absent from the log
 * and absent from the reconstructed object. The failure is silent under normal operation.
 *
 * <h2>Ordering and concurrency</h2>
 *
 * <p>Invocation is serialised per entity: two messages carrying the same key are never handled
 * concurrently, so no locking is needed around per-entity state. Messages for distinct keys are
 * handled concurrently, on this host or another, and the runtime holds one procedure instance per
 * entity rather than sharing one across them.
 *
 * <h2>Determinism</h2>
 *
 * <p>Recovery re-executes {@link #run}, so it must be a deterministic function of its declared
 * inputs. Reading {@code System.currentTimeMillis()} or generating a random identifier yields a
 * different result on replay and the reconstructed state then diverges from what was observed. Use
 * {@code ctx.now()} for time and {@link StableId} for identifiers.
 *
 * <h2>The six type arguments</h2>
 *
 * <p>The runtime needs runtime {@link Class} handles for all six, and recovers them by default from
 * the implementing class's generic supertype chain: a direct {@code implements Procedure<...>} with
 * concrete arguments, or a concrete subclass of a generic intermediate class. An argument still
 * generic at the concrete class is never guessed. Override the single corresponding accessor in
 * that case ({@link #inputType()}, {@link #outputType()}, {@link #changeType()},
 * {@link #clockType()}, {@link #configType()}) or registration fails naming the class and the
 * parameter. An explicit override always wins.
 *
 * <p>Implementations must provide a public no-argument constructor.
 *
 * @param <Input> the value handed to {@link #run}, decoded from the channel that feeds this
 *     procedure
 * @param <State> the per-entity state, held by the runtime and Kryo-serialisable
 * @param <Output> the value emitted through {@link ProcedureCtx}
 * @param <ChangeT> how a state mutation is recorded; {@link NoChange} opts out of persistence
 *     entirely
 * @param <ClockT> the clock {@code ctx.now()} reads; {@link WallClock} is processing time, and
 *     {@link com.flezha.sdk.state.EventTimeFrom} takes the time from the message itself
 * @param <ConfigT> the Jackson-deserialised configuration type; {@link Void} when the procedure
 *     takes none
 */
public interface Procedure<
        Input, State, Output, ChangeT extends Change, ClockT extends com.flezha.sdk.state.Clock, ConfigT> {

    /**
     * This procedure's name, unique within its operative.
     *
     * <p>The name identifies the procedure in durable state and in the channel labels derived from
     * it, so changing it means the runtime no longer recognises what it already recorded.
     *
     * @return the stable name, unique among the procedures of one operative
     */
    @Nonnull
    String name();

    /**
     * The input type, recovered from the {@code implements} clause. Override only when the type
     * argument is still generic at your concrete class, which the runtime will not guess.
     *
     * @return the class of values this procedure receives
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Class<Input> inputType() {
        return (Class<Input>) TypeTokens.require(getClass(), TypeTokens.Token.INPUT);
    }

    /**
     * The output type, recovered from the {@code implements} clause. Override only when the type
     * argument is still generic at your concrete class.
     *
     * @return the class of values this procedure emits
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Class<Output> outputType() {
        return (Class<Output>) TypeTokens.require(getClass(), TypeTokens.Token.OUTPUT);
    }

    /**
     * The change type, recovered from the {@code implements} clause. {@link NoChange} here means
     * the procedure persists nothing.
     *
     * @return the class of recorded state changes
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Class<ChangeT> changeType() {
        return (Class<ChangeT>) TypeTokens.require(getClass(), TypeTokens.Token.CHANGE);
    }

    /**
     * The clock type, recovered from the {@code implements} clause. Determines what
     * {@code ctx.now()} returns.
     *
     * @return the declared clock class
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Class<ClockT> clockType() {
        return (Class<ClockT>) TypeTokens.require(getClass(), TypeTokens.Token.CLOCK);
    }

    /**
     * The configuration type, recovered from the {@code implements} clause. {@code Void.class} when
     * the procedure takes no configuration.
     *
     * @return the declared configuration class
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Class<ConfigT> configType() {
        return (Class<ConfigT>) TypeTokens.require(getClass(), TypeTokens.Token.CONFIG);
    }

    /**
     * Builds the starting state for one entity, called once on first sight of that entity.
     *
     * <p>On a resume the runtime restores the most recent snapshot and folds the recorded changes
     * over whatever this returns, so it only has to be correct for a cold start.
     *
     * @param entityKey the key this entity is for. Empty when the procedure runs as a single
     *     entity, present when the stream is keyed or the procedure declares static keys
     * @param initPayload state handed over when this entity is being relocated from another host
     *     rather than started cold. Empty on a cold start. The runtime applies the payload itself
     *     after this method returns, so an implementation normally ignores it; it is exposed for
     *     instrumentation, such as recording the transferred byte size
     * @return the initial state for this entity
     */
    @Nonnull
    State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload);

    /**
     * Applies one recorded change to the state. This is the only method permitted to write state
     * fields.
     *
     * <p>The runtime calls it when {@link StateHandle#apply} is invoked, and again for every
     * recorded change when rebuilding an entity after a restart. It must therefore apply the change
     * and do nothing else: no emitting, no clock reads, no external calls, since anything done here
     * happens again on every replay.
     *
     * <p>A procedure declaring a change type must override this. The default throws, rather than
     * silently discarding state, so the omission is found at the first change instead of at the
     * first restart.
     *
     * @param state the entity's current state, mutated in place
     * @param change the change to apply, as passed to {@link StateHandle#apply}
     */
    default void applyChange(State state, ChangeT change) {
        if (changeType() == NoChange.class) {
            return;
        }
        throw new UnsupportedOperationException(
                "Procedure '" + name() + "' declares Change=" + changeType().getSimpleName()
                        + " but does not override applyChange. Override Procedure.applyChange to implement state mutation.");
    }

    /**
     * Handle a timer this Procedure registered via
     * {@link ProcedureCtx#timerAtEventTime} / {@link ProcedureCtx#timerAfter}.
     * Receives the same {@link StateHandle} and {@link ProcedureCtx} as
     * {@link #run}, so it may apply Changes, emit, and register further timers.
     *
     * <p>{@code firedAt} is the timestamp the timer was <b>scheduled at</b>,
     * never the wall clock at which it actually fired: a processing-time timer
     * set for T that fires at T+δ still reports T. Passing T+δ would make a
     * replay observe a different value and break the determinism contract in
     * exactly the way {@code System.currentTimeMillis()} inside {@code run}
     * does. For the same reason {@link ProcedureCtx#now} equals {@code firedAt}
     * inside this call, so a chained {@code ctx.timerAfter(..)} schedules off
     * the timer that woke it.
     *
     * <p>A failure here climbs the input channel's retry ladder and parks to the
     * dead-letter queue exactly as a failing {@code run} does. The fired timer's
     * removal is durable either way, so a poison timer parks once instead of
     * firing forever.
     *
     * <p>The default throws naming the Procedure: registering a timer without
     * handling it is an authoring error, and registration is a runtime call, so
     * no build-time check can catch it.
     */
    default void onTimer(
            long firedAt, com.flezha.sdk.state.TimerKind kind, StateHandle<State, ChangeT> state, ProcedureCtx ctx)
            throws ProcedureException {
        throw new UnsupportedOperationException("Procedure '" + name()
                + "' registered a timer but does not override onTimer. Override"
                + " Procedure.onTimer(firedAt, kind, state, ctx) to handle the firing.");
    }

    /**
     * Per-Entity bound on pending timers. Registering past it throws
     * {@link ProcedureException.TimerCapExceeded} naming this Procedure and the
     * bound, rather than allocating — the same posture as the soft cap the
     * dynamic-entity registry puts on Entities per Procedure.
     */
    default int maxTimersPerEntity() {
        return 10_000;
    }

    /** Optional coalesce key. Default: dispatches to the Change's own key. */
    @Nonnull
    default Optional<CoalesceKey> coalesceKey(ChangeT change) {
        return change.coalesceKey();
    }

    /**
     * Per-Entity WAL size threshold before the controller solicits a snapshot.
     * Default 8 MiB, in lockstep with the Rust SDK's {@code compact_after_bytes}
     * and the controller's {@code :default_compact_after_bytes}.
     */
    default long compactAfterBytes() {
        return 8L * 1024 * 1024;
    }

    /**
     * Per-Procedure schema version stamped into every persisted record this
     * Procedure writes — change records and state snapshots alike. Default: 1.
     *
     * <p>Bump it on any non-additive edit to the State <b>or</b> the Change
     * type. Kryo encodes them positionally, so reordering fields, removing one,
     * or changing a field's type is not a compatibility question the codec can
     * answer. The version turns that into a decision instead of a silent
     * corruption.
     *
     * <p>A bump alone refuses the old records. Declare
     * {@link #minReadableSchemaVersion()} and the migration hooks to read them.
     */
    default int schemaVersion() {
        return 1;
    }

    /**
     * The oldest {@link #schemaVersion()} whose persisted records this
     * Procedure can still read. Defaults to its own version — "reads only what
     * it wrote".
     *
     * <p>Lowering it is a promise that {@link #migrateState} and
     * {@link #migrateChange} handle every version in between. A record below
     * the floor, and any record stamped above {@link #schemaVersion()} (older
     * code meeting newer state), is refused rather than guessed at.
     */
    default int minReadableSchemaVersion() {
        return schemaVersion();
    }

    /**
     * Rebuild the State from a snapshot written at an older schema version.
     * Invoked only for records between {@link #minReadableSchemaVersion()} and
     * {@link #schemaVersion()}; {@code bytes} is the snapshot body exactly as
     * that version wrote it.
     *
     * <p>Keep the old type in your own source and decode it here — the runtime
     * knows only the types this build declares. Must be deterministic:
     * crash-resume, retry-replay and rewind all re-enter the restore fold.
     */
    @Nonnull
    default State migrateState(int fromVersion, byte[] bytes) {
        throw new IllegalStateException(
                "procedure `" + name() + "` declares no state migration from schema_version " + fromVersion);
    }

    /**
     * Rebuild a Change from a record written at an older schema version. Same
     * contract as {@link #migrateState}; a WAL legitimately holds records from
     * more than one version, so this is invoked per record.
     */
    @Nonnull
    default ChangeT migrateChange(int fromVersion, byte[] bytes) {
        throw new IllegalStateException(
                "procedure `" + name() + "` declares no change migration from schema_version " + fromVersion);
    }

    /**
     * Stable Kryo type IDs for user types. Ordering of returned classes
     * determines their stable IDs (id N = list[N]). Append-only: adding a new
     * variant appends a new class to the END of the list, preserving prior IDs
     * across SDK versions.
     *
     * <p>Defaults to the resolved Change and State classes — the two types the
     * per-Entity codec always needs. A {@link NoChange} Procedure persists
     * nothing and defaults to the empty list, as does one whose Change or State
     * argument is unresolvable.
     */
    @Nonnull
    default List<Class<?>> kryoTypes() {
        Class<?> change = TypeTokens.find(getClass(), TypeTokens.Token.CHANGE);
        Class<?> state = TypeTokens.find(getClass(), TypeTokens.Token.STATE);
        if (change == null || change == NoChange.class || state == null) {
            return List.of();
        }
        return List.of(change, state);
    }

    /**
     * Construct an instance from typed Config. Default for {@code Config = Void}
     * invokes the no-arg constructor via reflection. Procedures with non-Void
     * Config types must override.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Procedure<Input, State, Output, ChangeT, ClockT, ConfigT> fromConfig(ConfigT cfg) {
        if (cfg == null && configType() == Void.class) {
            try {
                return (Procedure<Input, State, Output, ChangeT, ClockT, ConfigT>)
                        getClass().getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Procedure '" + name() + "' has no public no-arg constructor", e);
            }
        }
        throw new UnsupportedOperationException(
                "Procedure '" + name() + "' has Config=" + configType().getSimpleName()
                        + " but does not override fromConfig. Override Procedure.fromConfig to implement typed config.");
    }

    /**
     * Static entity declaration. Default returns an empty list which the
     * runtime interprets as "single-entity mode".
     */
    @Nonnull
    default List<String> entityKeys() {
        return List.of();
    }

    /**
     * For static mode: which entities does this input belong to? Returning
     * multiple keys causes the SDK to enqueue the input on each named
     * entity's mailbox (fan-out). Returning an empty list drops the input.
     * Static-mode procedures MUST override this method AND return {@code true}
     * from {@link #entitiesForOverridden()}.
     */
    @Nonnull
    default List<String> entitiesFor(Input input) {
        return List.of();
    }

    /** Sentinel for static-mode-default detection. */
    default boolean entitiesForOverridden() {
        return false;
    }

    /**
     * Per-Entity mailbox capacity. The consumer Entity allocates one
     * bounded ring buffer of this size at operative startup. Static-mode Procedures
     * allocate one mailbox per declared entity key, all sharing the same
     * capacity.
     *
     * <p>Override precedence at registration time:
     * <ol>
     *   <li>This method's value, if {@link #mailboxCapacityOverridden()}
     *       returns {@code true}.</li>
     *   <li>{@code FLEZHA_MAILBOX_CAPACITY} env var (parsed as a positive
     *       integer).</li>
     *   <li>The built-in default ({@code 1024}).</li>
     * </ol>
     *
     * <p>Implementors overriding the default MUST also override
     * {@link #mailboxCapacityOverridden()} to return {@code true} so the
     * runtime applies the Procedure value over any {@code FLEZHA_MAILBOX_CAPACITY}
     * env value. Capacity must be in {@code 1..=1_048_576}; out-of-range
     * values fail {@code FlezhaRunner.defineStream(...)} with the
     * offending Procedure's name.
     */
    default int mailboxCapacity() {
        return 1024;
    }

    /**
     * Override-detection sentinel for {@link #mailboxCapacity()}. Default
     * {@code false}; implementors that override {@code mailboxCapacity()}
     * MUST also override this to return {@code true} so the runtime applies
     * the Procedure value over any {@code FLEZHA_MAILBOX_CAPACITY} env override.
     */
    default boolean mailboxCapacityOverridden() {
        return false;
    }

    /**
     * Handles one message. This is where your logic goes.
     *
     * <p>Called once per message, for one entity, and never concurrently for the same entity. It is
     * also re-executed during recovery, so it must produce the same result given the same input:
     * read time through {@code ctx.now()} and generate identifiers through
     * {@code ctx.stableId()} rather than reaching for the system clock or a random source.
     *
     * <pre>{@code
     * public void run(String input, StateHandle<Counter, Increment> state, ProcedureCtx ctx) {
     *     state.apply(new Increment(ctx.now()));
     *     ctx.emit("count=" + state.get().count);
     * }
     * }</pre>
     *
     * @param input the message, already decoded from the channel that feeds this procedure
     * @param state this entity's state. Read it with {@code state.get()}, and mutate it only with
     *     {@code state.apply(change)}; a field assigned directly is not recorded and does not
     *     survive a restart
     * @param ctx the runtime handle for emitting output, reading the clock, obtaining a
     *     replay-stable identifier, and registering timers
     * @throws ProcedureException to fail this invocation. The failure is recorded against the
     *     operative, procedure and entity, and any output emitted earlier in this invocation is
     *     discarded rather than released. Any other runtime exception is treated the same way
     */
    void run(Input input, StateHandle<State, ChangeT> state, ProcedureCtx ctx) throws ProcedureException;

    /**
     * Optional saga <strong>compensation</strong> hook (workflow operatives only).
     * A procedure whose {@link #run} commits a reversible side effect overrides
     * this to undo it. The workflow driver fires it ONLY during abnormal-termination
     * rollback — never on a clean completion, never for a stream operative —
     * once per entity that ran, in reverse completion order, handing back that
     * entity's own collected outputs and its {@link StateHandle} so it can
     * reverse precisely. Compensation MUST be idempotent by author contract: a
     * crash mid-rollback may re-invoke it.
     *
     * <p>The default is a no-op; a procedure that overrides this MUST also
     * override {@link #compensateOverridden()} to return {@code true} so the
     * workflow driver registers it as compensable (hookless procedures are
     * skipped during rollback).
     */
    default void compensate(List<Output> completedOutput, StateHandle<State, ChangeT> state, ProcedureCtx ctx)
            throws ProcedureException {
        // No-op: a procedure with no reversible effect needs no compensation.
    }

    /** Override-detection sentinel for {@link #compensate}. Default {@code false}. */
    default boolean compensateOverridden() {
        return false;
    }

    /**
     * Extract the event time (epoch millis) carried by an input. Required when
     * the Procedure declares {@code Clock = EventTimeFrom}: that clock has no
     * value to serve without an extractor, and {@code ctx.now()} returns what
     * this method reports.
     *
     * <p>Rust binds the extractor at compile time via
     * {@code EventTimeFrom<F: TimeExtractor<Input>>}. Java's erasure precludes
     * that, so declaring {@code EventTimeFrom} without overriding this fails
     * Operative registration — the same defect, caught before any input is
     * served.
     *
     * <p>A procedure that overrides this MUST also override
     * {@link #eventTimeOverridden()} to return {@code true}.
     */
    default long eventTime(Input input) {
        throw new UnsupportedOperationException(
                "procedure `" + name() + "` declares EventTimeFrom but does not override eventTime(Input)");
    }

    /**
     * How long a dynamic Entity may sit idle before it is evicted. Empty (the
     * default) means eviction is OFF: a dynamic Procedure keeps every Entity it
     * ever created.
     *
     * <p>Eviction is <b>destructive</b>: it ships {@code delete_entity_state} and
     * a later input for the same key cold-starts a fresh Entity rather than
     * restoring the old one. Declare a TTL only where re-accumulating from scratch
     * is acceptable — a key that reappears does not get its history back.
     */
    @Nonnull
    default java.util.Optional<java.time.Duration> entityIdleTtl() {
        return java.util.Optional.empty();
    }

    /** Override-detection sentinel for {@link #eventTime}. Default {@code false}. */
    default boolean eventTimeOverridden() {
        return false;
    }

    /**
     * Dynamic-mode soft cap on live Entities for this Procedure. At the cap,
     * inputs for a new key are dropped; existing keys are unaffected. This
     * bounds memory against a runaway {@code keyBy}, not admission — there is no
     * error path back to the controller.
     */
    default long maxEntities() {
        return 100_000L;
    }

    /** Override-detection sentinel for {@link #maxEntities}. Default {@code false}. */
    default boolean maxEntitiesOverridden() {
        return false;
    }
}
