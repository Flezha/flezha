package com.flezha.sdk.operative;

import java.util.List;
import java.util.Objects;

/**
 * Thrown by {@link com.flezha.sdk.FlezhaRunner#defineStream(Class)} when a
 * StreamOperative's declared topology is refused. The verdict it carries is
 * the core's, rendered once by {@code flz_core::host::issue::render_issues}:
 *
 * <pre>
 * topology rejected [DuplicateKeyBy, DanglingTransformChain]:
 *   - channel `in` declares .key_by(...) twice before one `.into(...)`
 *   - channel `in` declares a transform or .key_by(...) that is never closed …
 * </pre>
 *
 * <p>The names in the bracketed list are the reference vocabulary every SDK
 * reports a refusal under; the core hands them out as data beside the rendered
 * verdict, and {@link #refusals()} answers them so a caller can branch on a
 * name instead of on a sentence.
 */
public class StreamOperativeConfigurationException extends RuntimeException {

    private final List<String> refusals;

    /** A refusal the core has no vocabulary for, so it lists no names. */
    public StreamOperativeConfigurationException(String verdict) {
        this(verdict, List.of());
    }

    public StreamOperativeConfigurationException(String verdict, List<String> refusals) {
        super(Objects.requireNonNull(verdict));
        this.refusals = List.copyOf(refusals);
    }

    /**
     * The refusal names the verdict lists, in the order the core rendered
     * them. Empty when the refusal is one the core has no name for.
     */
    public List<String> refusals() {
        return refusals;
    }

    /**
     * Raised when a Procedure's type argument cannot be recovered from its
     * generic supertype chain and the corresponding type-token method is not
     * overridden — the argument is still generic at the concrete class. Naming
     * the class and the parameter is the whole contract: a guess would surface
     * later as a type mismatch far from its cause.
     *
     * <p>One of the two refusals the core has no vocabulary for: erasure is a
     * Java fact and the record carries no type tokens, so nothing the builder
     * could submit would let the core answer this.
     */
    public static StreamOperativeConfigurationException unresolvedTypeToken(
            String className, String parameter, String method) {
        return new StreamOperativeConfigurationException("procedure class `" + className + "` leaves the `"
                + parameter + "` type argument of Procedure<…> unresolved; declare it concretely on the "
                + "implements/extends clause, or override `" + method + "()`");
    }

    /**
     * Raised when a Procedure declares {@code EventTimeFrom} but does not
     * override {@code eventTime(Input)}. Rust enforces the extractor with a
     * compile-time {@code TimeExtractor<Input>} bound; Java's erasure precludes
     * that, so the same defect is caught at registration — before any input is
     * served and before anything durable exists.
     *
     * <p>The second refusal the core has no vocabulary for: the record states
     * the clock, never whether the host overrode its extractor.
     */
    public static StreamOperativeConfigurationException eventTimeExtractorMissing(String procedureName) {
        return new StreamOperativeConfigurationException("procedure `" + procedureName
                + "` declares the EventTimeFrom clock but does not override `eventTime(Input)`; "
                + "an event-time clock has no value to serve without an extractor");
    }
}
