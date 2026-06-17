package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a multishard relay class (one that implements
 * {@link com.flezha.sdk.relay.Relay} with an explicit {@code channels(...)}
 * body) for auto-registration. The annotation processor collects every
 * {@code @Relay} class — and every single-shard relay generated from a
 * {@link Shard} with a {@code sink} — into a {@code GeneratedRelays.registerAll()}
 * aggregator, so the author makes one startup call instead of N
 * {@code RelayRegistry.registerRelay} calls. The author's topology is preserved
 * verbatim. See {@code docs/sdk_authoring_contract.md} §5.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface Relay {}
