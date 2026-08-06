package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the per-entity state-init method on a {@link Procedure} class:
 * {@code State name(java.util.Optional<String> entityKey)}. The generated procedure
 * delegates {@code initState} to it, passing the entity's procedure key (absent
 * in single-entity mode and the test-harness default), instead of returning
 * {@code new State()}.
 *
 * <p>Use it when an entity's starting state is a function of its key — e.g. a
 * statically-entity procedure whose window length or bucket span is encoded in
 * the procedure key. Requires a stateful {@code run} (a {@code @State} handle
 * parameter); declaring it on a stateless procedure is a compile-time error. The
 * restore payload stays runtime-applied after {@code initState} returns, so the
 * method never sees it. Mirrors Rust's {@code #[flezha::procedure(init = fn)]}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface InitState {}
