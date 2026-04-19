package com.flezha.sdk;

import javax.annotation.Nullable;

/**
 * Optional, human-readable metadata about a factory, surfaced to the agent
 * as part of the handshake init JSON so operator tooling can display a
 * description for each factory id.
 */
public record FactoryMetadata(@Nullable String description) {

    public static FactoryMetadata empty() {
        return new FactoryMetadata(null);
    }

    public static FactoryMetadata of(String description) {
        return new FactoryMetadata(description);
    }
}
