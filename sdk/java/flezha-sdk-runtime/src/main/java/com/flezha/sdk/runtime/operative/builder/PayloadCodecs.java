package com.flezha.sdk.runtime.operative.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * The SDK's default payload codecs: what an entry channel decodes with when the
 * author supplies no parser, and what {@code codec()} / {@code sink(...)} apply
 * when they are given no encoder. {@code byte[]} is passthrough, {@code String}
 * is UTF-8, everything else is Jackson JSON.
 */
final class PayloadCodecs {

    /** One shared Jackson mapper for every default codec (thread-safe once configured). */
    private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

    private PayloadCodecs() {}

    /**
     * The default entry decode for {@code T}: passthrough for {@code byte[]},
     * UTF-8 for {@code String} (both byte-identical to the pre-typed-entry
     * contract), the SDK's default JSON codec otherwise.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static Function<byte[], Object> defaultEntryParser(Class<?> payloadType) {
        if (payloadType == byte[].class) {
            return bytes -> bytes;
        }
        if (payloadType == String.class) {
            return bytes -> new String(bytes, StandardCharsets.UTF_8);
        }
        return bytes -> {
            try {
                return DEFAULT_MAPPER.readValue(bytes, (Class) payloadType);
            } catch (Exception e) {
                throw new RuntimeException("entry channel: JSON deserialization failed: " + e.getMessage(), e);
            }
        };
    }

    /** Mirror of {@link #defaultSerializer}: the decode half of a default {@code codec()}. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T> Function<byte[], T> defaultDeserializer(Class<T> payloadType) {
        if (payloadType == byte[].class) {
            return (Function) (Function<byte[], byte[]>) (b -> b);
        }
        if (payloadType == String.class) {
            return (Function) (Function<byte[], String>) (b -> new String(b, StandardCharsets.UTF_8));
        }
        return bytes -> {
            try {
                return DEFAULT_MAPPER.readValue(bytes, payloadType);
            } catch (Exception e) {
                throw new RuntimeException("inter-proc codec (Jackson) decode failed: " + e.getMessage(), e);
            }
        };
    }

    /**
     * Default plugin serializer — what {@code sink(...)} applies when the
     * author supplies none. {@code byte[]} is passthrough; {@code String} is
     * UTF-8 encoded; everything else goes through Jackson JSON.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T> Function<T, byte[]> defaultSerializer(Class<T> payloadType) {
        if (payloadType == byte[].class) {
            return (Function) (Function<byte[], byte[]>) (b -> b);
        }
        if (payloadType == String.class) {
            return (Function) (Function<String, byte[]>) (s -> s.getBytes(StandardCharsets.UTF_8));
        }
        return value -> {
            try {
                return DEFAULT_MAPPER.writeValueAsBytes(value);
            } catch (Exception e) {
                throw new RuntimeException("plugin serializer (Jackson) failed: " + e.getMessage(), e);
            }
        };
    }
}
