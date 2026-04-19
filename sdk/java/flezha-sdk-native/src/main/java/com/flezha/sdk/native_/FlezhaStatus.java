package com.flezha.sdk.native_;

/**
 * Mirror of the native {@code FlezhaStatus} enum defined in
 * {@code flz_transport::ffi::types::FlezhaStatus}. Every value maps to the
 * exact {@code i32} emitted by the C FFI so native return codes can be
 * translated in O(1) via {@link #fromCode(int)}.
 */
public enum FlezhaStatus {
    OK(0),
    INVALID_ARGUMENT(1),
    INVALID_STATE(2),
    TRANSPORT_ERROR(3),
    IO_ERROR(4),
    ALREADY_RUNNING(5),
    NOT_RUNNING(6),
    INTERNAL_ERROR(7);

    private final int code;

    FlezhaStatus(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static FlezhaStatus fromCode(int code) {
        for (FlezhaStatus s : values()) {
            if (s.code == code) {
                return s;
            }
        }
        return INTERNAL_ERROR;
    }
}
