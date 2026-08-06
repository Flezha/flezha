package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the static-mode entity-routing method on a {@link Procedure} class:
 * {@code List<String> name(Input input)}. The generated procedure delegates
 * {@code entitiesFor} to it, sets {@code entitiesForOverridden()} to true, and
 * exposes the {@link Procedure#keys()} as {@code entityKeys()}. Mutually exclusive
 * with edge keying (`.keyBy(f)` on the wiring).
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface EntitiesFor {}
