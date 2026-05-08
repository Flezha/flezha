package com.flezha.sdk.runtime.relay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.flezha.sdk.runtime.partition.PartitionSlot;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Serializes a {@link RelayInstance}'s per-Shard / per-Partition state to a
 * canonical JSON blob per {@code relay-mvp-rust/design.md} Decision 10:
 *
 * <pre>
 *   single-mode shard:  {"shardName": &lt;state&gt;}
 *   static-mode shard:  {"shardName": {"shardKey": &lt;state&gt;, ...}}
 * </pre>
 *
 * <p>Cross-language byte-equality with the Rust SDK is the eventual gate
 * (verified by an integration test under {@code --only java_sdk}). Jackson
 * is configured to sort keys and avoid pretty-printing so the byte-shape
 * is stable.
 */
public final class RelayStateSerializer {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .configure(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .build();

    private RelayStateSerializer() {}

    /**
     * Build the per-Shard / per-Partition JSON blob. Returns an empty
     * {@code byte[]} on a serialization failure (logged by the caller).
     */
    @Nonnull
    public static byte[] serialize(@Nonnull RelayInstance instance) throws Exception {
        Map<String, Object> root = new LinkedHashMap<>();
        for (int shardId = 0; shardId < instance.topology().shards().size(); shardId++) {
            ShardDescriptor desc = instance.topology().shards().get(shardId);
            List<PartitionSlot> partitions = instance.partitions().get(shardId);
            if (desc.partitionKeys().isEmpty()) {
                Object state = partitions.get(0).state().get();
                root.put(desc.name(), state);
            } else {
                Map<String, Object> perKey = new LinkedHashMap<>();
                for (int idx = 0; idx < desc.partitionKeys().size(); idx++) {
                    String key = desc.partitionKeys().get(idx);
                    Object state = partitions.get(idx).state().get();
                    perKey.put(key, state);
                }
                root.put(desc.name(), perKey);
            }
        }
        return MAPPER.writeValueAsBytes(root);
    }

    /**
     * Apply a previously-serialized state blob to a freshly-constructed
     * {@link RelayInstance}. Partitions absent from the blob retain the
     * state set by their {@code initState} call (forward-compat with Shards
     * added after the blob was captured).
     */
    public static void deserialize(@Nonnull RelayInstance instance, @Nonnull byte[] blob) throws Exception {
        if (blob.length == 0) {
            return;
        }
        // Jackson erases the Map's value type at deserialize; the JSON blob is authored
        // by the matching serialize() above so the cast is round-trip safe.
        @SuppressWarnings("unchecked")
        Map<String, Object> root = MAPPER.readValue(blob, Map.class);
        for (int shardId = 0; shardId < instance.topology().shards().size(); shardId++) {
            ShardDescriptor desc = instance.topology().shards().get(shardId);
            Object shardEntry = root.get(desc.name());
            if (shardEntry == null) {
                continue;
            }
            List<PartitionSlot> partitions = instance.partitions().get(shardId);
            if (desc.partitionKeys().isEmpty()) {
                Object state = decodeState(shardEntry, partitions.get(0).state().get());
                partitions.get(0).state().set(state);
            } else {
                if (!(shardEntry instanceof Map<?, ?> perKey)) {
                    continue;
                }
                for (int idx = 0; idx < desc.partitionKeys().size(); idx++) {
                    String key = desc.partitionKeys().get(idx);
                    Object encoded = perKey.get(key);
                    if (encoded == null) {
                        continue;
                    }
                    Object state =
                            decodeState(encoded, partitions.get(idx).state().get());
                    partitions.get(idx).state().set(state);
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
