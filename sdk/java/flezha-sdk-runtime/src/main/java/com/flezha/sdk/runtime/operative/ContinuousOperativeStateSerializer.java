package com.flezha.sdk.runtime.operative;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.flezha.sdk.runtime.segment.SegmentSlot;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Serializes a {@link ContinuousOperativeInstance}'s per-Operation / per-Segment state to a
 * canonical JSON blob per {@code operative-mvp-rust/design.md} Decision 10:
 *
 * <pre>
 *   single-mode operation:  {"operationName": &lt;state&gt;}
 *   static-mode operation:  {"operationName": {"operationKey": &lt;state&gt;, ...}}
 * </pre>
 *
 * <p>Cross-language byte-equality with the Rust SDK is the eventual gate
 * (verified by an integration test under {@code --only java_sdk}). Jackson
 * is configured to sort keys and avoid pretty-printing so the byte-shape
 * is stable.
 */
public final class ContinuousOperativeStateSerializer {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .configure(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .build();

    private ContinuousOperativeStateSerializer() {}

    /**
     * Build the per-Operation / per-Segment JSON blob. Returns an empty
     * {@code byte[]} on a serialization failure (logged by the caller).
     */
    @Nonnull
    public static byte[] serialize(@Nonnull ContinuousOperativeInstance instance) throws Exception {
        Map<String, Object> root = new LinkedHashMap<>();
        for (int operationId = 0; operationId < instance.topology().operations().size(); operationId++) {
            OperationDescriptor desc = instance.topology().operations().get(operationId);
            List<SegmentSlot> segments = instance.segments().get(operationId);
            if (desc.segmentKeys().isEmpty()) {
                Object state = segments.get(0).state().get();
                root.put(desc.name(), state);
            } else {
                Map<String, Object> perKey = new LinkedHashMap<>();
                for (int idx = 0; idx < desc.segmentKeys().size(); idx++) {
                    String key = desc.segmentKeys().get(idx);
                    Object state = segments.get(idx).state().get();
                    perKey.put(key, state);
                }
                root.put(desc.name(), perKey);
            }
        }
        return MAPPER.writeValueAsBytes(root);
    }

    /**
     * Apply a previously-serialized state blob to a freshly-constructed
     * {@link ContinuousOperativeInstance}. Segments absent from the blob retain the
     * state set by their {@code initState} call (forward-compat with Operations
     * added after the blob was captured).
     */
    public static void deserialize(@Nonnull ContinuousOperativeInstance instance, @Nonnull byte[] blob)
            throws Exception {
        if (blob.length == 0) {
            return;
        }
        // Jackson erases the Map's value type at deserialize; the JSON blob is authored
        // by the matching serialize() above so the cast is round-trip safe.
        @SuppressWarnings("unchecked")
        Map<String, Object> root = MAPPER.readValue(blob, Map.class);
        for (int operationId = 0; operationId < instance.topology().operations().size(); operationId++) {
            OperationDescriptor desc = instance.topology().operations().get(operationId);
            Object operationEntry = root.get(desc.name());
            if (operationEntry == null) {
                continue;
            }
            List<SegmentSlot> segments = instance.segments().get(operationId);
            if (desc.segmentKeys().isEmpty()) {
                Object state =
                        decodeState(operationEntry, segments.get(0).state().get());
                segments.get(0).state().set(state);
            } else {
                if (!(operationEntry instanceof Map<?, ?> perKey)) {
                    continue;
                }
                for (int idx = 0; idx < desc.segmentKeys().size(); idx++) {
                    String key = desc.segmentKeys().get(idx);
                    Object encoded = perKey.get(key);
                    if (encoded == null) {
                        continue;
                    }
                    Object state =
                            decodeState(encoded, segments.get(idx).state().get());
                    segments.get(idx).state().set(state);
                }
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object decodeState(Object encoded, Object existing) throws Exception {
        if (existing == null) {
            return encoded;
        }
        Class<?> targetClass = existing.getClass();
        if (targetClass.isInstance(encoded)) {
            return encoded;
        }
        // Re-route through Jackson with the existing state's class as target;
        // covers records, POJOs, lists, maps.
        byte[] bytes = MAPPER.writeValueAsBytes(encoded);
        return MAPPER.readValue(bytes, targetClass);
    }
}
