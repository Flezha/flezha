package com.flezha.sdk.runtime.framing;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Pure-Java builder for the SDK protocol frame format.
 *
 * <p>Layout (big-endian integers):
 * <pre>
 * [payload_id: 32 bytes, space-padded]
 * [payload_size: u32]
 * [message_size: u32]
 * [message: message_size bytes]
 * [metadata_size: u16]
 * [metadata: metadata_size bytes]
 * </pre>
 *
 * <p>{@code payload_size = 4 + message.length + 2 + metadata.length}.
 *
 * <p>See {@code docs/sdk_protocol_spec.md} §3 and §9.1. Verified against
 * {@link com.flezha.sdk.native_.NativeTransport#buildFrame(String, byte[], String)}
 * byte-for-byte by the JNI bridge tests.
 */
public final class FrameEncoder {

    /** Fixed payload-id field width per the spec. */
    public static final int PAYLOAD_ID_WIDTH = 32;

    private FrameEncoder() {}

    @Nonnull
    public static byte[] build(
            @Nonnull String payloadId, @Nonnull byte[] message, @Nonnull Map<String, String> metadata) {
        if (payloadId == null) {
            throw new IllegalArgumentException("payloadId must not be null");
        }
        if (message == null) {
            throw new IllegalArgumentException("message must not be null");
        }

        byte[] metadataBytes = MetadataCodec.serialize(metadata == null ? Map.of() : metadata);

        int payloadSize = 4 + message.length + 2 + metadataBytes.length;
        int totalSize = PAYLOAD_ID_WIDTH + 4 + payloadSize;

        ByteBuffer buf = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);

        writePayloadId(buf, payloadId);
        buf.putInt(payloadSize);
        buf.putInt(message.length);
        buf.put(message);
        buf.putShort((short) metadataBytes.length);
        buf.put(metadataBytes);

        return buf.array();
    }

    @Nonnull
    public static byte[] buildText(
            @Nonnull String payloadId, @Nonnull String message, @Nonnull Map<String, String> metadata) {
        return build(payloadId, message.getBytes(StandardCharsets.UTF_8), metadata);
    }

    private static void writePayloadId(ByteBuffer buf, String payloadId) {
        byte[] idBytes = payloadId.getBytes(StandardCharsets.UTF_8);
        if (idBytes.length >= PAYLOAD_ID_WIDTH) {
            buf.put(idBytes, 0, PAYLOAD_ID_WIDTH);
        } else {
            buf.put(idBytes);
            // Pad with ASCII spaces (0x20) to 32 bytes, matching the Rust side.
            for (int i = idBytes.length; i < PAYLOAD_ID_WIDTH; i++) {
                buf.put((byte) 0x20);
            }
        }
    }
}
