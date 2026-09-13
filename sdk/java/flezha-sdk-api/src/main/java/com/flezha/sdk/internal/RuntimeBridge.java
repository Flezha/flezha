package com.flezha.sdk.internal;

import java.lang.reflect.InvocationTargetException;
import javax.annotation.Nullable;

/**
 * The ONE reflective bridge from {@code flezha-sdk-api} into
 * {@code flezha-sdk-runtime}. The api module resolves runtime classes by name
 * so it compiles standalone; every caller (the registry cores,
 * {@code FlezhaRunner.start()}) routes through here so the classpath
 * diagnosis and the cause-unwrapping live in one place. Both modules construct
 * an author's classes through {@link #construct} for the same unwrapping.
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
    public static Object invoke(String fqn, String method, Class<?>[] sig, String failureContext, Object... args) {
        try {
            return Class.forName(fqn).getMethod(method, sig).invoke(null, args);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    fqn + " not on the classpath. Make sure flezha-sdk-runtime is on the runtime classpath.", e);
        } catch (ReflectiveOperationException e) {
            throw unwrap(e, failureContext);
        }
    }

    /**
     * Construct {@code cls} through its no-arg constructor. What the
     * constructor itself throws propagates under {@link #invoke}'s rule, so an
     * author's own error is never reported as a missing constructor; only a
     * constructor that cannot be called reaches the caller as a
     * {@link ReflectiveOperationException}.
     */
    public static <T> T construct(Class<T> cls) throws ReflectiveOperationException {
        try {
            return cls.getDeclaredConstructor().newInstance();
        } catch (InvocationTargetException e) {
            throw unwrap(e, "constructor of `" + cls.getName() + "` failed");
        }
    }

    private static RuntimeException unwrap(ReflectiveOperationException e, String failureContext) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        if (cause instanceof RuntimeException re) {
            return re;
        }
        return new IllegalStateException(failureContext + ": " + cause.getMessage(), cause);
    }

    /**
     * Invoke the no-arg static method {@code fqn.method()}, silently doing
     * nothing when the runtime is not on the classpath or the call fails —
     * the best-effort shape test-reset hooks need.
     */
    public static void invokeIfPresent(String fqn, String method) {
        try {
            Class.forName(fqn).getMethod(method).invoke(null);
        } catch (ClassNotFoundException ignored) {
            // Runtime not loaded — nothing to do.
        } catch (ReflectiveOperationException ignored) {
            // Best-effort; tests will surface the failure another way.
        }
    }
}
