package com.flezha.sdk.authoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an authoring class whose {@code run} method is expanded into a full
 * {@link com.flezha.sdk.procedure.Procedure} implementation by the Flezha annotation
 * processor. The annotation processor generates {@code <ClassName>Procedure}
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
 * <p>When {@link #sink()} is set and the class is a sole procedure, the processor
 * also generates a single-procedure {@code input -> procedure -> plugin(sink)} operative and
 * registers it (see {@link StreamOperative}).
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface Procedure {

    /** Stable name / factory id. Defaults to the snake_case class name. */
    String id() default "";

    /** Plugin sink name; set to opt into single-procedure auto-wiring. */
    String sink() default "";

    /**
     * Name of a {@code static String <name>(Output out)} method on this class supplying the
     * emit's natural key. Set it to feed a {@code .keyed()} channel: the generated run then
     * calls {@code ctx.emitKeyed(...)}, the only emit such a channel accepts. Requires a
     * non-void return type.
     */
    String key() default "";

    /** Static-mode entity keys. Requires a {@link EntitiesFor} method. */
    String[] keys() default {};

    /** Per-entity mailbox capacity override. {@code 0} keeps the default. */
    int capacity() default 0;

    /** Channel overflow policy: {@code "block"}, {@code "drop_newest"}, or {@code "drop_oldest"}. */
    String onFull() default "";
}
