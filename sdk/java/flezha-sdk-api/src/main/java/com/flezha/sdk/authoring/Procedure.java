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
 * {@code run} signature per {@code docs/sdk/authoring_contract.md} §2.
 *
 * <p>Supported {@code run} shapes:
 * <ul>
 *   <li>{@code Output run(Input input)} — stateless.</li>
 *   <li>{@code Output run(Input input, StateHandle<State, Change> state)} —
 *       stateful; {@code State}/{@code Change} are read from the handle's type
 *       arguments.</li>
 *   <li>A trailing {@code ProcedureCtx ctx} parameter with a {@code void}
 *       return — the body performs any number of emits itself and
 *       {@link #emits()} declares the output type.</li>
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
     * Name of the entry channel of the operative {@link #sink()} auto-wires — the address an
     * operator injects on and the name a forwarded frame has to match. Empty (the default)
     * names it {@code input}. Setting it without a {@code sink} is an error: there is no
     * generated operative for it to name a channel on.
     */
    String input() default "";

    /**
     * Name of a {@code static String <name>(Output out)} method on this class supplying the
     * emit's natural key. Set it to feed a {@code .keyed()} channel: the generated run then
     * calls {@code ctx.emitKeyed(...)}, the only emit such a channel accepts. Requires a
     * non-void return type.
     */
    String key() default "";

    /**
     * Output type of a {@code run} that takes a trailing {@code ProcedureCtx}.
     * That shape returns {@code void} — the body emits — so the return type has
     * no output left to declare and this does it instead. {@code void.class}
     * (the default) means the procedure declares no output channel; setting it
     * without a context parameter is an error.
     */
    Class<?> emits() default void.class;

    /** Static-mode entity keys. Requires a {@link EntitiesFor} method. */
    String[] keys() default {};

    /** Per-entity mailbox capacity override. {@code 0} keeps the default. */
    int capacity() default 0;

    /** Channel overflow policy: {@code "drop_nothing"}, {@code "drop_newest"}, or {@code "drop_oldest"}. */
    String onFull() default "";
}
