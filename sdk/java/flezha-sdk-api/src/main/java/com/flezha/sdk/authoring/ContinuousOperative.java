package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a multioperation operative class (one that implements
 * {@link com.flezha.sdk.operative.ContinuousOperative} with an explicit {@code channels(...)}
 * body) for auto-registration. The annotation processor collects every
 * {@code @ContinuousOperative} class — and every single-operation operative generated from a
 * {@link Operation} with a {@code sink} — into a {@code GeneratedContinuousOperatives.registerAll()}
 * aggregator, so the author makes one startup call instead of N
 * {@code ContinuousOperativeRegistry.registerContinuous} calls. The author's topology is preserved
 * verbatim. See {@code docs/sdk_authoring_contract.md} §5.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface ContinuousOperative {}
