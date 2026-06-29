package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the static-mode segment-routing method on a {@link Operation} class:
 * {@code List<String> name(Input input)}. The generated operation delegates
 * {@code segmentFor} to it, sets {@code segmentForOverridden()} to true, and
 * exposes the {@link Operation#keys()} as {@code operationKeys()}. Mutually exclusive
 * with {@link OperationKey}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface SegmentFor {}
