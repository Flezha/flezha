package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a multiprocedure operative class (one that implements
 * {@link com.flezha.sdk.operative.UnboundedOperative} with an explicit {@code channels(...)}
 * body) for auto-registration. The annotation processor collects every
 * {@code @UnboundedOperative} class — and every single-procedure operative generated from a
 * {@link Procedure} with a {@code sink} — into a
 * {@code GeneratedUnboundedOperatives.defineAll(FlezhaRunner)} aggregator, so
 * the author makes one startup call instead of N
 * {@code runner.defineUnbounded} calls. The author's topology is preserved
 * verbatim. See {@code docs/sdk_authoring_contract.md} §5.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface UnboundedOperative {}
