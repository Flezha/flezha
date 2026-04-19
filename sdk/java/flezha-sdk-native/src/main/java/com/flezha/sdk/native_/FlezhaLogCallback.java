package com.flezha.sdk.native_;

/**
 * Functional interface called by the native layer when the shared library
 * emits a log entry. Levels follow {@code FlezhaLogLevel}
 * ({@code 0=ERROR, 1=WARN, 2=INFO, 3=DEBUG, 4=TRACE}).
 *
 * <p>Implementations should be fast and non-blocking — the callback is
 * invoked on a native-owned thread which is temporarily attached to the
 * JVM. The default {@link NativeTransport} wiring routes these calls to
 * SLF4J at the matching log level.
 */
@FunctionalInterface
public interface FlezhaLogCallback {
    int LEVEL_ERROR = 0;
    int LEVEL_WARN = 1;
    int LEVEL_INFO = 2;
    int LEVEL_DEBUG = 3;
    int LEVEL_TRACE = 4;

    void onLog(int level, String message);
}
