package com.flezha.sdk.native_;

/**
 * Transport variant selector. The native side resolves {@code "uds"} vs
 * {@code "quic"} from the string form, matching {@code FLEZHA_TRANSPORT}.
 */
public enum FlezhaTransport {
    UDS("uds"),
    QUIC("quic");

    private final String nativeName;

    FlezhaTransport(String nativeName) {
        this.nativeName = nativeName;
    }

    public String nativeName() {
        return nativeName;
    }

    public static FlezhaTransport parse(String raw) {
        if (raw == null) {
            return UDS;
        }
        return switch (raw.trim().toLowerCase()) {
            case "quic" -> QUIC;
            case "uds", "" -> UDS;
            default -> throw new IllegalArgumentException("Unknown FLEZHA_TRANSPORT value: " + raw);
        };
    }
}
