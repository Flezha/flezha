package com.flezha.sdk.runtime.operative;

import java.util.Optional;

/**
 * Per-thread {@code init_values} JSON string staged by
 * {@link com.flezha.sdk.runtime.control.handlers.AddOperativeHandler} before
 * a {@link ContinuousOperativeInstance} is constructed; readable from
 * {@link com.flezha.sdk.operation.Operation#initState(java.util.Optional)} via
 * {@link #get()}.
 *
 * <p>Set on the spawning thread, cleared after the {@code ContinuousOperativeInstance}
 * constructor returns. Cross-thread propagation is intentionally NOT
 * supported in v1: each Operation×Segment is initialized synchronously on
 * the spawning thread before the ContinuousOperativeInstance is published.
 */
public final class InitValuesContext {

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private InitValuesContext() {}

    public static void set(String initValuesJson) {
        if (initValuesJson == null || initValuesJson.isEmpty()) {
            CURRENT.remove();
        } else {
            CURRENT.set(initValuesJson);
        }
    }

    public static void clear() {
        CURRENT.remove();
    }

    /**
     * Read the current thread's staged {@code init_values}. Returns empty
     * outside of an {@code AddOperative} window.
     */
    public static Optional<String> get() {
        return Optional.ofNullable(CURRENT.get());
    }
}
