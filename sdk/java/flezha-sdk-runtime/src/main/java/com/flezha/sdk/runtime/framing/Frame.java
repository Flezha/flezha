package com.flezha.sdk.runtime.framing;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Decoded SDK protocol frame. Corresponds to the wire layout documented in
 * {@code docs/sdk_protocol_spec.md} §3:
 *
 * <pre>
 * [payload_id (32)] [payload_size (u32, BE)]
 *   [message_size (u32, BE)] [message ...]
 *   [metadata_size (u16, BE)] [metadata ...]
 * </pre>
 */
public record Frame(
        @Nonnull String payloadId,
        @Nonnull byte[] message,
        @Nonnull Map<String, String> metadata) {

    public Frame {
        if (payloadId == null) {
            throw new IllegalArgumentException("payloadId must not be null");
        }
        if (message == null) {
            throw new IllegalArgumentException("message must not be null");
        }
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
