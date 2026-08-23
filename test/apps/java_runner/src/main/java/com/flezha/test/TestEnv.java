package com.flezha.test;

import java.util.Optional;

/** Environment knobs the fixtures read, parsed the way the Rust test app parses them. */
final class TestEnv {

    private TestEnv() {}

    /** The variable as a long, or {@code fallback} when unset or unparseable. */
    static long longVar(String key, long fallback) {
        return optLong(key).orElse(fallback);
    }

    /** The variable as a long, or empty when unset or unparseable. */
    static Optional<Long> optLong(String key) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Long.parseLong(raw.trim()));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
