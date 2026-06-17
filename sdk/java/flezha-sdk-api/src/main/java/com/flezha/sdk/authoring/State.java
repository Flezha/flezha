package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a state class for field-granular change derivation. The annotation
 * processor generates {@code <ClassName>Change} with a {@code set<Field>} /
 * {@code push<Field>} factory per field, an {@code apply(state, change)} folder,
 * and the Kryo type list — journaled through the existing WAL. See
 * {@code docs/sdk_authoring_contract.md} §3.
 *
 * <p>Requires public, non-final fields and a public no-arg constructor; a
 * mutation that is conditional, multi-field-atomic, or computed is beyond the
 * derivable boundary and needs a hand-written {@link com.flezha.sdk.state.Change}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface State {}
