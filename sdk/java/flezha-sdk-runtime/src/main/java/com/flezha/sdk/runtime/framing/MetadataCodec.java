package com.flezha.sdk.runtime.framing;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Flat metadata codec matching the Rust SDK's
 * {@code service::payloader::serialize_metadata}.
 *
 * <p>Wire layout: repeating records of
 * {@code [key_len(u8)] [val_len(u8)] [key bytes] [value bytes]}.
 * Keys and values MUST be ≤ 255 bytes each (UTF-8 length), otherwise the
 * codec rejects the entry.
 */
public final class MetadataCodec {

    private MetadataCodec() {
    }

    @Nonnull
    public static byte[] serialize(@Nonnull Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return new byte[0];
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Map.Entry<String, String> e : metadata.entrySet()) {
            byte[] key = e.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] value = e.getValue() == null
                    ? new byte[0]
                    : e.getValue().getBytes(StandardCharsets.UTF_8);

            if (key.length > 255) {
                throw new IllegalArgumentException(
                        "Metadata key exceeds 255 bytes: " + e.getKey());
            }
            if (value.length > 255) {
                throw new IllegalArgumentException(
                        "Metadata value exceeds 255 bytes for key: " + e.getKey());
            }

            out.write(key.length & 0xFF);
            out.write(value.length & 0xFF);
            out.write(key, 0, key.length);
            out.write(value, 0, value.length);
        }
        return out.toByteArray();
    }

    @Nonnull
    public static Map<String, String> deserialize(@Nonnull byte[] bytes) {
        Map<String, String> out = new LinkedHashMap<>();
        int offset = 0;
        while (offset + 2 <= bytes.length) {
            int keyLen = bytes[offset] & 0xFF;
            int valLen = bytes[offset + 1] & 0xFF;
            offset += 2;

            if (offset + keyLen + valLen > bytes.length) {
                throw new IllegalStateException(
                        "Invalid metadata frame: truncated key/value at offset " + offset);
            }

            String key = new String(bytes, offset, keyLen, StandardCharsets.UTF_8);
            offset += keyLen;
            String value = new String(bytes, offset, valLen, StandardCharsets.UTF_8);
            offset += valLen;

            out.put(key, value);
        }
        return out;
    }
}
