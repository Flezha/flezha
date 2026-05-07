package com.flezha.sdk.runtime.state;

import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Wire identity of a Partition. Combines the relay-instance id (= agent's
 * {@code processor_id}), the Shard's name within the Relay, and the partition
 * key (empty string for single-partition mode). Encoded on the wire as a
 * length-prefixed UTF-8 string by the per-Partition flusher.
 */
public record PartitionId(String instanceId, String shardName, String partitionKey) {

    public static PartitionId of(@Nonnull String instanceId, @Nonnull String shardName,
            @Nonnull Optional<String> partitionKey) {
        return new PartitionId(instanceId, shardName, partitionKey.orElse(""));
    }

    /** Wire-encoded form: {@code instanceId/shardName/partitionKey}. */
    @Nonnull
    public String encoded() {
        return instanceId + "/" + shardName + "/" + partitionKey;
    }
}
