package com.flezha.sdk.runtime.state;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Wire identity of a Partition. Combines the relay-instance id (= controller's
 * {@code relay_id}), the Shard's name within the Relay, and the partition
 * key (empty string for single-partition mode).
 *
 * <p>The controller keys each per-Partition WAL by this identity and echoes it
 * back verbatim as {@code RestoreInitState.partition_id}, so {@link #encodedBytes()}
 * MUST be byte-identical to the controller's {@code Flezha.State.Wire.encode_partition_id/3}.
 */
public record PartitionId(String instanceId, String shardName, String partitionKey) {

    public static PartitionId of(
            @Nonnull String instanceId, @Nonnull String shardName, @Nonnull Optional<String> partitionKey) {
        return new PartitionId(instanceId, shardName, partitionKey.orElse(""));
    }

    /** Slash-joined form used only as an in-process map key / log label. */
    @Nonnull
    public String encoded() {
        return instanceId + "/" + shardName + "/" + partitionKey;
    }

    /**
     * Structured binary identity consumed by the controller's
     * {@code decode_partition_id}:
     * {@code [rc_len:u8][rc][sh_len:u8][sh][pk_len:u16 BE][pk]}, where
     * {@code rc} = relay-instance id, {@code sh} = shard name, {@code pk} =
     * partition key (all UTF-8). Mirrors the Rust SDK's
     * {@code encode_partition_identity}.
     */
    @Nonnull
    public byte[] encodedBytes() {
        byte[] rc = instanceId.getBytes(StandardCharsets.UTF_8);
        byte[] sh = shardName.getBytes(StandardCharsets.UTF_8);
        byte[] pk = partitionKey.getBytes(StandardCharsets.UTF_8);
        if (rc.length > 255 || sh.length > 255) {
            throw new IllegalArgumentException("relay_class / shard_id must be <= 255 bytes");
        }
        if (pk.length > 0xFFFF) {
            throw new IllegalArgumentException("partition_key must be <= 65535 bytes");
        }
        ByteBuffer buf = ByteBuffer.allocate(1 + rc.length + 1 + sh.length + 2 + pk.length)
                .order(ByteOrder.BIG_ENDIAN);
        buf.put((byte) rc.length);
        buf.put(rc);
        buf.put((byte) sh.length);
        buf.put(sh);
        buf.putShort((short) pk.length);
        buf.put(pk);
        return buf.array();
    }
}
