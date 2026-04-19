package com.flezha.sdk.native_;

/**
 * Runtime exception raised from the JNI bridge when a native call returns a
 * non-OK {@link FlezhaStatus}. The message is populated from the native
 * thread-local error string (via {@code flezha_last_error()}).
 */
public final class FlezhaNativeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final FlezhaStatus status;

    public FlezhaNativeException(int statusCode, String message) {
        super(formatMessage(statusCode, message));
        this.status = FlezhaStatus.fromCode(statusCode);
    }

    public FlezhaNativeException(FlezhaStatus status, String message) {
        super(formatMessage(status.code(), message));
        this.status = status;
    }

    public FlezhaStatus getStatus() {
        return status;
    }

    private static String formatMessage(int statusCode, String message) {
        return "[" + FlezhaStatus.fromCode(statusCode) + "] " + message;
    }
}
