package com.flezha.sdk;

/**
 * Thrown by {@link FlezhaRunner#removeUnbounded(Class)} /
 * {@link FlezhaRunner#removeBounded(Class)} when live operative instances of
 * the factory exist on the started runner — the removal does not take effect
 * and the type stays defined. Stop the instances (controller-side) before
 * removing the type. Mirrors the Rust SDK's
 * {@code FlezhaError::OperativeTypeInUse}.
 */
public final class OperativeTypeInUseException extends IllegalStateException {

    public OperativeTypeInUseException(String message) {
        super(message);
    }
}
