package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <b>Removed.</b> Dynamic Entity keying is declared on the wiring —
 * {@code .keyBy(f)} on the stream that feeds the procedure — so a reader of
 * {@code channels()} can see where work fans out. This annotation survives only
 * so that declaring it fails annotation processing with that instruction
 * instead of a bare unknown-symbol error; it generates nothing.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface EntityKey {}
