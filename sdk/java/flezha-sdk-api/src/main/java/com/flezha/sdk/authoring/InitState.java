package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the per-partition state-init method on a {@link Shard} class:
 * {@code State name(java.util.Optional<String> shardKey)}. The generated shard
 * delegates {@code initState} to it, passing the partition's shard key (absent
 * in single-partition mode and the test-harness default), instead of returning
 * {@code new State()}.
 *
 * <p>Use it when a partition's starting state is a function of its key — e.g. a
 * statically-partitioned shard whose window length or bucket span is encoded in
 * the shard key. Requires a stateful {@code run} (a {@code @State} handle
 * parameter); declaring it on a stateless shard is a compile-time error. The
 * restore payload stays runtime-applied after {@code initState} returns, so the
 * method never sees it. Mirrors Rust's {@code #[flezha::shard(init = fn)]}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface InitState {}
