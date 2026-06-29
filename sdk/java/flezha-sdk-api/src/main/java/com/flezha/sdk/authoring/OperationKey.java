package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the dynamic-mode key method on a {@link Operation} class:
 * {@code Optional<String> name(Input input)}. The generated operation sets
 * {@code operationKeyOverridden()} to true, delegates {@code operationKey} to it, leaves
 * {@code operationKeys()} empty, and applies the optional TTL / cap overrides per
 * {@code operative-dynamic-segmenting}. Mutually exclusive with
 * {@link SegmentFor}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface OperationKey {

    /** Idle-eviction window in milliseconds. {@code -1} disables eviction. */
    long idleTtl() default -1L;

    /** Soft cap on live segments. {@code 0} keeps the runtime default. */
    int maxSegments() default 0;
}
