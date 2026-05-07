package com.flezha.sdk.runtime.state;

/**
 * Thrown when a snapshot or change-log payload cannot be loaded by the SDK.
 * Carries a typed reason so the runtime can distinguish "wrong language"
 * (cross-language load attempt; umbrella Decision 9 fail-loud) from "wrong
 * schema" (additive evolution mismatch reserved for a future migrators
 * change) from "wrong format" (e.g. Java SDK reading postcard bytes).
 */
public class StateLoadException extends RuntimeException {

    private final Reason reason;

    public StateLoadException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }

    public enum Reason {
        FORMAT_NOT_SUPPORTED,
        SDK_LANG_MISMATCH,
        SCHEMA_VERSION_MISMATCH,
        CRC_MISMATCH,
        TRUNCATED,
    }

    /** Cross-format detection: agent shipped postcard bytes to a Java SDK. */
    public static StateLoadException formatNotSupported(int formatId) {
        return new StateLoadException(Reason.FORMAT_NOT_SUPPORTED,
                "format_id=" + formatId + " is not supported by the Java SDK; expected Kryo (4)");
    }

    /** Cross-language detection: agent shipped Rust-written bytes to Java. */
    public static StateLoadException sdkLangMismatch(int sdkLang) {
        return new StateLoadException(Reason.SDK_LANG_MISMATCH,
                "sdk_lang=" + sdkLang + " mismatches this consumer; cross-language load is not supported");
    }
}
