package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the per-segment state-init method on a {@link Operation} class:
 * {@code State name(java.util.Optional<String> operationKey)}. The generated operation
 * delegates {@code initState} to it, passing the segment's operation key (absent
 * in single-segment mode and the test-harness default), instead of returning
 * {@code new State()}.
 *
 * <p>Use it when a segment's starting state is a function of its key — e.g. a
 * statically-segmented operation whose window length or bucket span is encoded in
 * the operation key. Requires a stateful {@code run} (a {@code @State} handle
 * parameter); declaring it on a stateless operation is a compile-time error. The
 * restore payload stays runtime-applied after {@code initState} returns, so the
 * method never sees it. Mirrors Rust's {@code #[flezha::operation(init = fn)]}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface InitState {}
