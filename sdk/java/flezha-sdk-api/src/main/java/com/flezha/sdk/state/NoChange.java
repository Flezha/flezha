package com.flezha.sdk.state;

/**
 * Uninhabited Change type used as a type-tag by Procedures opting out of state
 * persistence. The runtime detects {@code Procedure.changeType() == NoChange.class}
 * via reference equality and skips per-Entity flusher registration.
 *
 * <p>Cannot be instantiated; the constructor throws unconditionally. Equivalent
 * to Rust's {@code enum NoChange {}}.
 */
public final class NoChange implements Change {

    private NoChange() {
        throw new UnsupportedOperationException("NoChange is uninhabited");
    }
}
