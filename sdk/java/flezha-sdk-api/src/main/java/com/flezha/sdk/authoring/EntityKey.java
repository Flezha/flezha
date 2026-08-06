package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the dynamic-mode key method on a {@link Procedure} class:
 * {@code Optional<String> name(Input input)}. The generated procedure sets
 * {@code entityKeyOverridden()} to true, delegates {@code entityKey} to it, leaves
 * {@code entityKeys()} empty, and applies the optional TTL / cap overrides per
 * dynamic entity mode. Mutually exclusive with
 * {@link EntitiesFor}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface EntityKey {

    /** Idle-eviction window in milliseconds. {@code -1} disables eviction. */
    long idleTtl() default -1L;

    /** Soft cap on live entities. {@code 0} keeps the runtime default. */
    int maxEntities() default 0;
}
