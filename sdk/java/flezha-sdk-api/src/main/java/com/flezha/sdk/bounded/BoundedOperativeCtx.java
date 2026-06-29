package com.flezha.sdk.bounded;

import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

/**
 * Output context handed to {@link BoundedOperative#run}. Reuses the operative output path:
 * {@code emit} writes a frame tagged with the bounded's instance id on a
 * secondary stream, which the controller fans out to the plugins bound to
 * this operative (e.g. Kafka, stdout) — best-effort and at-least-once, like
 * operative output.
 *
 * <p>The concrete implementation lives in {@code flezha-sdk-runtime}; this
 * interface is the surface a bounded author sees.
 */
public interface BoundedOperativeCtx {

    /** Emit one output payload to the operative's bound plugins. */
    void emit(@Nonnull byte[] value) throws BoundedOperativeException;

    /** Convenience: emit a UTF-8 string payload. */
    default void emit(@Nonnull String value) throws BoundedOperativeException {
        emit(value.getBytes(StandardCharsets.UTF_8));
    }
}
