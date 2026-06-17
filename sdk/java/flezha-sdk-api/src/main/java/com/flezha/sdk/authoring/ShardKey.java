package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the dynamic-mode key method on a {@link Shard} class:
 * {@code Optional<String> name(Input input)}. The generated shard sets
 * {@code shardKeyOverridden()} to true, delegates {@code shardKey} to it, leaves
 * {@code shardKeys()} empty, and applies the optional TTL / cap overrides per
 * {@code relay-dynamic-partitioning}. Mutually exclusive with
 * {@link PartitionFor}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface ShardKey {

    /** Idle-eviction window in milliseconds. {@code -1} disables eviction. */
    long idleTtl() default -1L;

    /** Soft cap on live partitions. {@code 0} keeps the runtime default. */
    int maxPartitions() default 0;
}
