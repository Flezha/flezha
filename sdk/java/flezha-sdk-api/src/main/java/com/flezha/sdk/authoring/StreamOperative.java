package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a multiprocedure operative class (one that implements
 * {@link com.flezha.sdk.operative.StreamOperative} with an explicit {@code channels(...)}
 * body) for auto-registration. The annotation processor collects every
 * {@code @StreamOperative} class — and every single-procedure operative generated from a
 * {@link Procedure} with a {@code sink} — into a
 * {@code GeneratedStreamOperatives.defineAll(FlezhaRunner)} aggregator, so
 * the author makes one startup call instead of N
 * {@code runner.defineStream} calls. The author's topology is preserved
 * verbatim. See {@code docs/sdk/authoring_contract.md} §5.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface StreamOperative {}
