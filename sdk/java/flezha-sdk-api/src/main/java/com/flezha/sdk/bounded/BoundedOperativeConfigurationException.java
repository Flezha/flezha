package com.flezha.sdk.bounded;

/**
 * Thrown at {@link com.flezha.sdk.Flezha#registerBounded registration} time when a
 * {@link BoundedOperative}'s authoring is invalid: an external-stop-only
 * operative (no self-termination and no {@code maxDuration()}), or a structured
 * {@link BoundedOperative#operations} declaration that is not a single connected
 * linear chain (branching, a cycle, a dynamic-segment operation, a disconnected
 * operation, or no entry channel).
 *
 * <p>Unchecked, mirroring {@link com.flezha.sdk.operative.ContinuousOperativeConfigurationException}
 * — it surfaces a programming error in the authoring, caught at registration
 * rather than at run time.
 */
public class BoundedOperativeConfigurationException extends RuntimeException {

    public BoundedOperativeConfigurationException(String message) {
        super(message);
    }
}
