package com.flezha.sdk.state;

/**
 * Empty per-entity state for stateless sugared procedures. The annotation
 * processor uses this as the {@code State} type when a {@code @Procedure} class's
 * {@code run} method declares no state parameter, so a generated stateless
 * procedure still satisfies the {@link com.flezha.sdk.procedure.Procedure} type surface
 * with a no-op {@code initState}.
 */
public final class Stateless {

    public Stateless() {}

    @Override
    public boolean equals(Object o) {
        return o instanceof Stateless;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "Stateless";
    }
}
