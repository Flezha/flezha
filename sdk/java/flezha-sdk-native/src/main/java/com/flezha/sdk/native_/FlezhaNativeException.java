package com.flezha.sdk.native_;

/**
 * Runtime exception raised from the native binding when a call into the poll
 * core is refused. The message is the context's last-error string; the
 * {@code flz_core_ffi} JNI layer throws through the single-argument
 * constructor via {@code ThrowNew}.
 */
public final class FlezhaNativeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public FlezhaNativeException(String message) {
        super(message);
    }
}
