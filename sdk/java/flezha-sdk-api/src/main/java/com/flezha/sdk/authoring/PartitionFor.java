package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the static-mode partition-routing method on a {@link Shard} class:
 * {@code List<String> name(Input input)}. The generated shard delegates
 * {@code partitionFor} to it, sets {@code partitionForOverridden()} to true, and
 * exposes the {@link Shard#keys()} as {@code shardKeys()}. Mutually exclusive
 * with {@link ShardKey}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface PartitionFor {}
