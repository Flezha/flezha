package com.flezha.sdk.task;

import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

/**
 * Output context handed to {@link Task#run}. Reuses the relay output path:
 * {@code emit} writes a frame tagged with the task's instance id on a
 * secondary stream, which the controller fans out to the plugins bound to
 * this unit (e.g. Kafka, stdout) — best-effort and at-least-once, like
 * relay output.
 *
 * <p>The concrete implementation lives in {@code flezha-sdk-runtime}; this
 * interface is the surface a task author sees.
 */
public interface TaskCtx {

    /** Emit one output payload to the unit's bound plugins. */
    void emit(@Nonnull byte[] value) throws TaskException;

    /** Convenience: emit a UTF-8 string payload. */
    default void emit(@Nonnull String value) throws TaskException {
        emit(value.getBytes(StandardCharsets.UTF_8));
    }
}
