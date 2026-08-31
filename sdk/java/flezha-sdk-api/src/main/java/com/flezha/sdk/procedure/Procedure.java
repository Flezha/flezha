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
 * One Procedure in a StreamOperative topology. Stateless dispatcher; per-Entity
 * {@code State} is held by the runtime and threaded through {@link #run} via
 * a {@link StateHandle}.
 *
 * <p>Implementations MUST provide a public no-arg constructor — the runtime
 * instantiates one Procedure per Entity at operative-instance startup.
 *
 * <p>The topology builder, the Kryo codec and the per-Entity flusher need
 * runtime {@link Class} handles for the type parameters. {@link #inputType()},
 * {@link #outputType()}, {@link #changeType()}, {@link #clockType()} and
 * {@link #configType()} recover them by default from the implementing class's
 * generic supertype chain — a direct {@code implements Procedure<…>} with
 * concrete arguments, or a concrete subclass of a generic intermediate class.
 * An argument still generic at the concrete class is never guessed: the
 * implementation overrides that one method, or registration fails naming the
 * class and the parameter. An explicit override always wins.
 *
 * @param <Input>  per-entity input value
 * @param <State>  per-entity mutable state (Kryo-serializable)
 * @param <Output> per-entity output value emitted via {@link ProcedureCtx}
 * @param <ChangeT> per-Entity state-change type; default {@link NoChange}
 *     opts out of state persistence
 * @param <ClockT>  clock declaration; v1 fully wires {@link WallClock} only
 * @param <ConfigT> Jackson-deserialized config; {@link Void} for operative-typed
 *     configs
 */
public interface Procedure<
        Input, State, Output, ChangeT extends Change, ClockT extends com.flezha.sdk.state.Clock, ConfigT> {

    /** Stable identifier within the StreamOperative class. Must be unique per StreamOperative. */
    @Nonnull
    String name();

    /** Compile-time typed Input class. Used at registerUnbounded time for wiring checks. */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Class<Input> inputType() {
        return (Class<Input>) TypeTokens.require(getClass(), TypeTokens.Token.INPUT);
    }

    /** Compile-time typed Output class. Used at registerUnbounded time for wiring checks. */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Class<Output> outputType() {
        return (Class<Output>) TypeTokens.require(getClass(), TypeTokens.Token.OUTPUT);
    }

    /** Compile-time typed Change class. Drives flusher registration and Kryo dispatch. */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Class<ChangeT> changeType() {
        return (Class<ChangeT>) TypeTokens.require(getClass(), TypeTokens.Token.CHANGE);
    }

    /** Compile-time typed Clock class. Drives v1 StreamOperativeConfigurationException for non-WallClock. */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Class<ClockT> clockType() {
        return (Class<ClockT>) TypeTokens.require(getClass(), TypeTokens.Token.CLOCK);
    }

    /** Compile-time typed Config class. {@code Void.class} for Procedures without typed config. */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Class<ConfigT> configType() {
        return (Class<ConfigT>) TypeTokens.require(getClass(), TypeTokens.Token.CONFIG);
    }

    /**
     * Initialize state for a new Entity. Called once per Entity at StreamOperative
     * startup. {@code entityKey} is empty for single-entity mode; present
     * for static mode. {@code initPayload} is empty on cold start; present
     * during a {@code RestoreInitState} round-trip — the user MAY inspect the
     * byte sizes for instrumentation but the runtime auto-applies the payload
     * (Kryo-deserialize the snapshot, fold the change log) AFTER {@code initState}
     * returns, so users typically ignore the parameter.
     */
    @Nonnull
    State initState(@Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload);

    /**
     * Apply a Change to the current state. Default: no-op for {@link NoChange}
     * (which is uninhabited and never reaches this method anyway), throws
     * {@link UnsupportedOperationException} for any other Change type to force
     * an explicit override.
     */
    default void applyChange(@Nonnull State state, @Nonnull ChangeT change) {
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
            long firedAt,
            @Nonnull com.flezha.sdk.state.TimerKind kind,
            @Nonnull StateHandle<State, ChangeT> state,
            @Nonnull ProcedureCtx ctx)
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
    default Optional<CoalesceKey> coalesceKey(@Nonnull ChangeT change) {
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
    default State migrateState(int fromVersion, @Nonnull byte[] bytes) {
        throw new IllegalStateException(
                "procedure `" + name() + "` declares no state migration from schema_version " + fromVersion);
    }

    /**
     * Rebuild a Change from a record written at an older schema version. Same
     * contract as {@link #migrateState}; a WAL legitimately holds records from
     * more than one version, so this is invoked per record.
     */
    @Nonnull
    default ChangeT migrateChange(int fromVersion, @Nonnull byte[] bytes) {
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
    default List<String> entitiesFor(@Nonnull Input input) {
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
     * Per-entity compute. Receives the typed input, the entity's state
     * via a {@link StateHandle} (read via {@code state.get()}, mutate via
     * {@code state.apply(change)}), and a context for emitting outputs.
     * Throwing {@link ProcedureException} (or any other {@link RuntimeException})
     * is logged against {@code (operativeInstance, procedure, entity)} and discards
     * any partial emits made during this invocation.
     */
    void run(@Nonnull Input input, @Nonnull StateHandle<State, ChangeT> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException;

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
    default void compensate(
            @Nonnull List<Output> completedOutput,
            @Nonnull StateHandle<State, ChangeT> state,
            @Nonnull ProcedureCtx ctx)
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
    default long eventTime(@Nonnull Input input) {
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
