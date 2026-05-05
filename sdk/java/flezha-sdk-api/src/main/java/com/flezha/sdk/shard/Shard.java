package com.flezha.sdk.shard;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * One Shard in a Relay topology. Stateless dispatcher; per-Partition
 * {@code State} is held by the runtime and threaded through {@link #run}.
 *
 * <p>Implementations MUST provide a public no-arg constructor — the runtime
 * instantiates one Shard per Partition at relay-instance startup.
 *
 * <p>Java's type erasure means {@code Input} / {@code State} / {@code Output}
 * are not reflectively inspectable from {@code Class<? extends Shard<…>>}.
 * Implementations MUST therefore override {@link #inputType()} and
 * {@link #outputType()} to expose the runtime {@link Class} handles the
 * topology builder needs for wiring checks at {@code registerRelay} time.
 *
 * @param <Input>  per-partition input value (immutable from the producer's
 *     side once emitted across an inter-shard channel — see
 *     {@code design.md} Decision 4).
 * @param <State>  per-partition mutable state.
 * @param <Output> per-partition output value emitted via {@link ShardCtx}.
 */
public interface Shard<Input, State, Output> {

    /** Stable identifier within the Relay class. Must be unique per Relay. */
    @Nonnull String name();

    /** Compile-time typed Input class. Used at registerRelay time for wiring checks. */
    @Nonnull Class<Input> inputType();

    /** Compile-time typed Output class. Used at registerRelay time for wiring checks. */
    @Nonnull Class<Output> outputType();

    /**
     * Initialize state for a new Partition. Called once per Partition at Relay
     * startup. {@code shardKey} is empty for single-partition mode; present
     * for static mode where its value is one of the keys returned by
     * {@link #shardKeys()}.
     */
    @Nonnull State initState(@Nonnull Optional<String> shardKey);

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
     * Per-partition compute. Receives the typed input, the partition's
     * mutable state, and a context for emitting outputs. Producing zero
     * outputs == no {@code ctx.emit} call. Multiple {@code ctx.emit} calls
     * fan out to multiple consumers (fan-out is the channel topology's job).
     * Throwing {@link ShardException} (or any other {@link RuntimeException})
     * is logged against {@code (relayInstance, shard, partition)} and
     * discards any partial emits made during this invocation.
     */
    void run(
            @Nonnull Input input,
            @Nonnull State state,
            @Nonnull ShardCtx ctx) throws ShardException;
}
