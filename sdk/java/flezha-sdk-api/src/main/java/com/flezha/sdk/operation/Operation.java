package com.flezha.sdk.operation;

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
 * One Operation in an UnboundedOperative topology. Stateless dispatcher; per-Segment
 * {@code State} is held by the runtime and threaded through {@link #run} via
 * a {@link StateHandle}.
 *
 * <p>Implementations MUST provide a public no-arg constructor — the runtime
 * instantiates one Operation per Segment at operative-instance startup.
 *
 * <p>Java's type erasure means the type parameters are not reflectively
 * inspectable from {@code Class<? extends Operation<…>>}. Implementations MUST
 * therefore override {@link #inputType()}, {@link #outputType()},
 * {@link #changeType()}, {@link #clockType()}, and {@link #configType()} to
 * expose the runtime {@link Class} handles the topology builder, the Kryo
 * codec, and the per-Segment flusher need.
 *
 * @param <Input>  per-segment input value
 * @param <State>  per-segment mutable state (Kryo-serializable)
 * @param <Output> per-segment output value emitted via {@link OperationCtx}
 * @param <ChangeT> per-Segment state-change type; default {@link NoChange}
 *     opts out of state persistence
 * @param <ClockT>  clock declaration; v1 fully wires {@link WallClock} only
 * @param <ConfigT> Jackson-deserialized config; {@link Void} for operative-typed
 *     configs
 */
public interface Operation<
        Input, State, Output, ChangeT extends Change, ClockT extends com.flezha.sdk.state.Clock, ConfigT> {

    /** Stable identifier within the UnboundedOperative class. Must be unique per UnboundedOperative. */
    @Nonnull
    String name();

    /** Compile-time typed Input class. Used at registerUnbounded time for wiring checks. */
    @Nonnull
    Class<Input> inputType();

    /** Compile-time typed Output class. Used at registerUnbounded time for wiring checks. */
    @Nonnull
    Class<Output> outputType();

    /** Compile-time typed Change class. Drives flusher registration and Kryo dispatch. */
    @Nonnull
    Class<ChangeT> changeType();

    /** Compile-time typed Clock class. Drives v1 UnboundedOperativeConfigurationException for non-WallClock. */
    @Nonnull
    Class<ClockT> clockType();

    /** Compile-time typed Config class. {@code Void.class} for Operations without typed config. */
    @Nonnull
    Class<ConfigT> configType();

    /**
     * Initialize state for a new Segment. Called once per Segment at UnboundedOperative
     * startup. {@code operationKey} is empty for single-segment mode; present
     * for static mode. {@code initPayload} is empty on cold start; present
     * during a {@code RestoreInitState} round-trip — the user MAY inspect the
     * byte sizes for instrumentation but the runtime auto-applies the payload
     * (Kryo-deserialize the snapshot, fold the change log) AFTER {@code initState}
     * returns, so users typically ignore the parameter.
     */
    @Nonnull
    State initState(@Nonnull Optional<String> operationKey, @Nonnull Optional<RestoreInitPayload> initPayload);

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
                "Operation '" + name() + "' declares Change=" + changeType().getSimpleName()
                        + " but does not override applyChange. Override Operation.applyChange to implement state mutation.");
    }

    /** Optional coalesce key. Default: dispatches to the Change's own key. */
    @Nonnull
    default Optional<CoalesceKey> coalesceKey(@Nonnull ChangeT change) {
        return change.coalesceKey();
    }

    /**
     * Per-Segment WAL size threshold before the controller solicits a snapshot.
     * Default 8 MiB, in lockstep with the Rust SDK's {@code compact_after_bytes}
     * and the controller's {@code :default_compact_after_bytes}.
     */
    default long compactAfterBytes() {
        return 8L * 1024 * 1024;
    }

    /** Schema version for additive Change evolution. Default: 1. */
    default int schemaVersion() {
        return 1;
    }

    /**
     * Stable Kryo type IDs for user types. Default: empty. Ordering of returned
     * classes determines their stable IDs (id N = list[N]). Append-only: adding
     * a new variant appends a new class to the END of the list, preserving
     * prior IDs across SDK versions.
     */
    @Nonnull
    default List<Class<?>> kryoTypes() {
        return List.of();
    }

    /**
     * Construct an instance from typed Config. Default for {@code Config = Void}
     * invokes the no-arg constructor via reflection. Operations with non-Void
     * Config types must override.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Operation<Input, State, Output, ChangeT, ClockT, ConfigT> fromConfig(ConfigT cfg) {
        if (cfg == null && configType() == Void.class) {
            try {
                return (Operation<Input, State, Output, ChangeT, ClockT, ConfigT>)
                        getClass().getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Operation '" + name() + "' has no public no-arg constructor", e);
            }
        }
        throw new UnsupportedOperationException(
                "Operation '" + name() + "' has Config=" + configType().getSimpleName()
                        + " but does not override fromConfig. Override Operation.fromConfig to implement typed config.");
    }

    /**
     * Static segment declaration. Default returns an empty list which the
     * runtime interprets as "single-segment mode".
     */
    @Nonnull
    default List<String> operationKeys() {
        return List.of();
    }

    /**
     * For static mode: which segments does this input belong to? Returning
     * multiple keys causes the SDK to enqueue the input on each named
     * segment's mailbox (fan-out). Returning an empty list drops the input.
     * Static-mode operations MUST override this method AND return {@code true}
     * from {@link #segmentForOverridden()}.
     */
    @Nonnull
    default List<String> segmentFor(@Nonnull Input input) {
        return List.of();
    }

    /** Sentinel for static-mode-default detection. */
    default boolean segmentForOverridden() {
        return false;
    }

    /**
     * Dynamic-mode reservation. v1 runtime rejects Operation impls whose
     * {@link #operationKeyOverridden()} returns {@code true} unless
     * {@link #operationKeys()} is non-empty. Reserved for the future
     * {@code operative-dynamic-segmenting} change; v1 SHALL NOT call this
     * on the hot path.
     */
    @Nonnull
    default Optional<String> operationKey(@Nonnull Input input) {
        return Optional.empty();
    }

    /** Sentinel for dynamic-mode-default detection. */
    default boolean operationKeyOverridden() {
        return false;
    }

    /**
     * Per-Segment mailbox capacity. The consumer Segment allocates one
     * bounded ring buffer of this size at operative startup. Static-mode Operations
     * allocate one mailbox per declared segment key, all sharing the same
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
     * runtime applies the Operation value over any {@code FLEZHA_MAILBOX_CAPACITY}
     * env value. Capacity must be in {@code 1..=1_048_576}; out-of-range
     * values fail {@code FlezhaRunner.defineUnbounded(...)} with the
     * offending Operation's name.
     */
    default int mailboxCapacity() {
        return 1024;
    }

    /**
     * Override-detection sentinel for {@link #mailboxCapacity()}. Default
     * {@code false}; implementors that override {@code mailboxCapacity()}
     * MUST also override this to return {@code true} so the runtime applies
     * the Operation value over any {@code FLEZHA_MAILBOX_CAPACITY} env override.
     */
    default boolean mailboxCapacityOverridden() {
        return false;
    }

    /**
     * Per-segment compute. Receives the typed input, the segment's state
     * via a {@link StateHandle} (read via {@code state.get()}, mutate via
     * {@code state.apply(change)}), and a context for emitting outputs.
     * Throwing {@link OperationException} (or any other {@link RuntimeException})
     * is logged against {@code (operativeInstance, operation, segment)} and discards
     * any partial emits made during this invocation.
     */
    void run(@Nonnull Input input, @Nonnull StateHandle<State, ChangeT> state, @Nonnull OperationCtx ctx)
            throws OperationException;

    /**
     * Optional saga <strong>compensation</strong> hook (bounded operatives only).
     * An operation whose {@link #run} commits a reversible side effect overrides
     * this to undo it. The bounded driver fires it ONLY during abnormal-termination
     * rollback — never on a clean completion, never for an unbounded operative —
     * once per segment that ran, in reverse completion order, handing back that
     * segment's own collected outputs and its {@link StateHandle} so it can
     * reverse precisely. Compensation MUST be idempotent by author contract: a
     * crash mid-rollback may re-invoke it.
     *
     * <p>The default is a no-op; an operation that overrides this MUST also
     * override {@link #compensateOverridden()} to return {@code true} so the
     * bounded driver registers it as compensable (hookless operations are
     * skipped during rollback).
     */
    default void compensate(
            @Nonnull List<Output> completedOutput,
            @Nonnull StateHandle<State, ChangeT> state,
            @Nonnull OperationCtx ctx)
            throws OperationException {
        // No-op: an operation with no reversible effect needs no compensation.
    }

    /** Override-detection sentinel for {@link #compensate}. Default {@code false}. */
    default boolean compensateOverridden() {
        return false;
    }

    /**
     * Extract the event time (epoch millis) carried by an input. Required when
     * the Operation declares {@code Clock = EventTimeFrom}: that clock has no
     * value to serve without an extractor, and {@code ctx.now()} returns what
     * this method reports.
     *
     * <p>Rust binds the extractor at compile time via
     * {@code EventTimeFrom<F: TimeExtractor<Input>>}. Java's erasure precludes
     * that, so declaring {@code EventTimeFrom} without overriding this fails
     * Operative registration — the same defect, caught before any input is
     * served.
     *
     * <p>An operation that overrides this MUST also override
     * {@link #eventTimeOverridden()} to return {@code true}.
     */
    default long eventTime(@Nonnull Input input) {
        throw new UnsupportedOperationException(
                "operation `" + name() + "` declares EventTimeFrom but does not override eventTime(Input)");
    }

    /**
     * How long a dynamic Segment may sit idle before it is evicted. Empty (the
     * default) means eviction is OFF: a dynamic Operation keeps every Segment it
     * ever created.
     *
     * <p>Eviction is <b>destructive</b>: it ships {@code delete_segment_state} and
     * a later input for the same key cold-starts a fresh Segment rather than
     * restoring the old one. Declare a TTL only where re-accumulating from scratch
     * is acceptable — a key that reappears does not get its history back.
     */
    @Nonnull
    default java.util.Optional<java.time.Duration> segmentIdleTtl() {
        return java.util.Optional.empty();
    }

    /** Override-detection sentinel for {@link #eventTime}. Default {@code false}. */
    default boolean eventTimeOverridden() {
        return false;
    }
}
