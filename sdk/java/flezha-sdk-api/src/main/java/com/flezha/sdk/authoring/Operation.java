package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an authoring class whose {@code run} method is expanded into a full
 * {@link com.flezha.sdk.operation.Operation} implementation by the Flezha annotation
 * processor. The annotation processor generates {@code <ClassName>Operation}
 * implementing the trait surface, inferring the associated types from the
 * {@code run} signature per {@code docs/sdk_authoring_contract.md} §2.
 *
 * <p>Supported {@code run} shapes:
 * <ul>
 *   <li>{@code Output run(Input input)} — stateless.</li>
 *   <li>{@code Output run(Input input, StateHandle<State, Change> state)} —
 *       stateful; {@code State}/{@code Change} are read from the handle's type
 *       arguments.</li>
 *   <li>A {@code void} return declares no output channel.</li>
 * </ul>
 *
 * <p>When {@link #sink()} is set and the class is a sole operation, the processor
 * also generates a single-operation {@code input -> operation -> plugin(sink)} operative and
 * registers it (see {@link UnboundedOperative}).
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface Operation {

    /** Stable name / factory id. Defaults to the snake_case class name. */
    String id() default "";

    /** Plugin sink name; set to opt into single-operation auto-wiring. */
    String sink() default "";

    /** Static-mode segment keys. Requires a {@link SegmentFor} method. */
    String[] keys() default {};

    /** Per-segment mailbox capacity override. {@code 0} keeps the default. */
    int capacity() default 0;

    /** Channel overflow policy: {@code "block"}, {@code "drop_newest"}, or {@code "drop_oldest"}. */
    String onFull() default "";
}
