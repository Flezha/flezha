package com.flezha.sdk.shard;

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
 * One Shard in a Relay topology. Stateless dispatcher; per-Partition
 * {@code State} is held by the runtime and threaded through {@link #run} via
 * a {@link StateHandle}.
 *
 * <p>Implementations MUST provide a public no-arg constructor — the runtime
 * instantiates one Shard per Partition at relay-instance startup.
 *
 * <p>Java's type erasure means the type parameters are not reflectively
 * inspectable from {@code Class<? extends Shard<…>>}. Implementations MUST
 * therefore override {@link #inputType()}, {@link #outputType()},
 * {@link #changeType()}, {@link #clockType()}, and {@link #configType()} to
 * expose the runtime {@link Class} handles the topology builder, the Kryo
 * codec, and the per-Partition flusher need.
 *
 * @param <Input>  per-partition input value
 * @param <State>  per-partition mutable state (Kryo-serializable)
 * @param <Output> per-partition output value emitted via {@link ShardCtx}
 * @param <ChangeT> per-Partition state-change type; default {@link NoChange}
 *     opts out of state persistence
 * @param <ClockT>  clock declaration; v1 fully wires {@link WallClock} only
 * @param <ConfigT> Jackson-deserialized config; {@link Void} for unit-typed
 *     configs
 */
public interface Shard<
        Input, State, Output, ChangeT extends Change, ClockT extends com.flezha.sdk.state.Clock, ConfigT> {

    /** Stable identifier within the Relay class. Must be unique per Relay. */
    @Nonnull
    String name();

    /** Compile-time typed Input class. Used at registerRelay time for wiring checks. */
    @Nonnull
    Class<Input> inputType();

    /** Compile-time typed Output class. Used at registerRelay time for wiring checks. */
    @Nonnull
    Class<Output> outputType();

    /** Compile-time typed Change class. Drives flusher registration and Kryo dispatch. */
    @Nonnull
    Class<ChangeT> changeType();

    /** Compile-time typed Clock class. Drives v1 RelayConfigurationException for non-WallClock. */
    @Nonnull
    Class<ClockT> clockType();

    /** Compile-time typed Config class. {@code Void.class} for Shards without typed config. */
    @Nonnull
    Class<ConfigT> configType();

    /**
     * Initialize state for a new Partition. Called once per Partition at Relay
     * startup. {@code shardKey} is empty for single-partition mode; present
     * for static mode. {@code initPayload} is empty on cold start; present
     * during a {@code RestoreInitState} round-trip — the user MAY inspect the
     * byte sizes for instrumentation but the runtime auto-applies the payload
     * (Kryo-deserialize the snapshot, fold the change log) AFTER {@code initState}
     * returns, so users typically ignore the parameter.
     */
    @Nonnull
    State initState(@Nonnull Optional<String> shardKey, @Nonnull Optional<RestoreInitPayload> initPayload);

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
                "Shard '" + name() + "' declares Change=" + changeType().getSimpleName()
                        + " but does not override applyChange. Override Shard.applyChange to implement state mutation.");
    }

    /** Optional coalesce key. Default: dispatches to the Change's own key. */
    @Nonnull
    default Optional<CoalesceKey> coalesceKey(@Nonnull ChangeT change) {
        return change.coalesceKey();
    }

    /** WAL size threshold before the controller solicits a snapshot. Default: 64 MiB per Partition. */
    default long compactAfterBytes() {
        return 64L * 1024 * 1024;
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
     * invokes the no-arg constructor via reflection. Shards with non-Void
     * Config types must override.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default Shard<Input, State, Output, ChangeT, ClockT, ConfigT> fromConfig(ConfigT cfg) {
        if (cfg == null && configType() == Void.class) {
            try {
                return (Shard<Input, State, Output, ChangeT, ClockT, ConfigT>)
                        getClass().getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Shard '" + name() + "' has no public no-arg constructor", e);
            }
        }
        throw new UnsupportedOperationException(
                "Shard '" + name() + "' has Config=" + configType().getSimpleName()
                        + " but does not override fromConfig. Override Shard.fromConfig to implement typed config.");
    }

    /**
     * Static partition declaration. Default returns an empty list which the
     * runtime interprets as "single-partition mode".
     */
    @Nonnull
    default List<String> shardKeys() {
        return List.of();
    }

    /**
     * For static mode: which partitions does this input belong to? Returning
     * multiple keys causes the SDK to enqueue the input on each named
     * partition's mailbox (fan-out). Returning an empty list drops the input.
     * Static-mode shards MUST override this method AND return {@code true}
     * from {@link #partitionForOverridden()}.
     */
    @Nonnull
    default List<String> partitionFor(@Nonnull Input input) {
        return List.of();
    }

    /** Sentinel for static-mode-default detection. */
    default boolean partitionForOverridden() {
        return false;
    }

    /**
     * Dynamic-mode reservation. v1 runtime rejects Shard impls whose
     * {@link #shardKeyOverridden()} returns {@code true} unless
     * {@link #shardKeys()} is non-empty. Reserved for the future
     * {@code relay-dynamic-partitioning} change; v1 SHALL NOT call this
     * on the hot path.
     */
    @Nonnull
    default Optional<String> shardKey(@Nonnull Input input) {
        return Optional.empty();
    }

    /** Sentinel for dynamic-mode-default detection. */
    default boolean shardKeyOverridden() {
        return false;
    }

    /**
     * Per-Partition mailbox capacity. The consumer Partition allocates one
     * bounded ring buffer of this size at relay startup. Static-mode Shards
     * allocate one mailbox per declared partition key, all sharing the same
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
     * runtime applies the Shard value over any {@code FLEZHA_MAILBOX_CAPACITY}
     * env value. Capacity must be in {@code 1..=1_048_576}; out-of-range
     * values fail {@code RelayRegistry.registerRelay(...)} with the
     * offending Shard's name.
     */
    default int mailboxCapacity() {
        return 1024;
    }

    /**
     * Override-detection sentinel for {@link #mailboxCapacity()}. Default
     * {@code false}; implementors that override {@code mailboxCapacity()}
     * MUST also override this to return {@code true} so the runtime applies
     * the Shard value over any {@code FLEZHA_MAILBOX_CAPACITY} env override.
     */
    default boolean mailboxCapacityOverridden() {
        return false;
    }

    /**
     * Per-partition compute. Receives the typed input, the partition's state
     * via a {@link StateHandle} (read via {@code state.get()}, mutate via
     * {@code state.apply(change)}), and a context for emitting outputs.
     * Throwing {@link ShardException} (or any other {@link RuntimeException})
     * is logged against {@code (relayInstance, shard, partition)} and discards
     * any partial emits made during this invocation.
     */
    void run(@Nonnull Input input, @Nonnull StateHandle<State, ChangeT> state, @Nonnull ShardCtx ctx)
            throws ShardException;
}
