package com.flezha.sdk.native_;

import javax.annotation.Nullable;

/**
 * Runtime exception raised from the native binding when a call into the poll
 * core is refused. The message is the context's last-error string; the
 * {@code flz_core_ffi} JNI layer throws through the single-argument
 * constructor via {@code ThrowNew}.
 *
 * <p>An entry point that hands out an answer on its refusal status — the
 * topology check, whose answer is the verdict JSON either way — throws through
 * the two-argument constructor instead, so the caller reads the refusal as
 * data rather than out of the rendered message.
 */
public final class FlezhaNativeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    @Nullable
    private final byte[] body;

    public FlezhaNativeException(String message) {
        this(message, null);
    }

    public FlezhaNativeException(String message, @Nullable byte[] body) {
        super(message);
        this.body = body == null ? null : body.clone();
    }

    /** The answer the core handed out with this refusal, or {@code null}. */
    @Nullable
    public byte[] body() {
        return body == null ? null : body.clone();
    }
}
