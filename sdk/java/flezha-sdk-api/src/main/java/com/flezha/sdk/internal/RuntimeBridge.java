package com.flezha.sdk.internal;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The ONE reflective bridge from {@code flezha-sdk-api} into
 * {@code flezha-sdk-runtime}. The api module resolves runtime classes by name
 * so it compiles standalone; every caller (the class registries,
 * {@code RunnerBuilder.start()}) routes through here so the classpath
 * diagnosis and the cause-unwrapping live in one place.
 *
 * <p><b>Internal.</b> Not part of the SDK's public API; user code never calls
 * this.
 */
public final class RuntimeBridge {

    private RuntimeBridge() {}

    /**
     * Invoke the static method {@code fqn.method(sig)(args)}.
     *
     * @param failureContext prefix for the error when the runtime call fails
     *     with a checked/reflective error (a thrown {@link RuntimeException}
     *     is rethrown as-is).
     * @throws IllegalStateException when {@code fqn} is not on the classpath
     *     (naming it), or wrapping a non-runtime failure under
     *     {@code failureContext}.
     */
    @Nullable
    public static Object invoke(
            @Nonnull String fqn,
            @Nonnull String method,
            @Nonnull Class<?>[] sig,
            @Nonnull String failureContext,
            Object... args) {
        try {
            return Class.forName(fqn).getMethod(method, sig).invoke(null, args);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    fqn + " not on the classpath. Make sure flezha-sdk-runtime is on the runtime classpath.", e);
        } catch (ReflectiveOperationException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(failureContext + ": " + cause.getMessage(), cause);
        }
    }

    /**
     * Invoke the no-arg static method {@code fqn.method()}, silently doing
     * nothing when the runtime is not on the classpath or the call fails —
     * the best-effort shape test-reset hooks need.
     */
    public static void invokeIfPresent(@Nonnull String fqn, @Nonnull String method) {
        try {
            Class.forName(fqn).getMethod(method).invoke(null);
        } catch (ClassNotFoundException ignored) {
            // Runtime not loaded — nothing to do.
        } catch (ReflectiveOperationException ignored) {
            // Best-effort; tests will surface the failure another way.
        }
    }
}
