package com.flezha.test;

import com.flezha.sdk.authoring.Shard;
import com.flezha.sdk.authoring.ShardKey;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Dynamic-mode authoring-sugar example. The processor generates the dynamic
 * trait surface ({@code shardKeyOverridden() == true}, {@code shardKey}
 * delegation, empty {@code shardKeys()}). No {@code sink} — it is not
 * auto-registered, because the Java runtime has not yet landed the
 * {@code relay-dynamic-partitioning} surface ({@code partitionIdleTtl} /
 * {@code maxPartitions} methods and the dynamic dispatch path), so the
 * {@code idleTtl} / {@code maxPartitions} attributes are not yet wired.
 */
@Shard(id = "sugar_dynamic_shard")
public class SugarDynamic {

    @ShardKey(idleTtl = 5000, maxPartitions = 10)
    public Optional<String> coin(String input) {
        return input.equals("drop") ? Optional.empty() : Optional.of(input.split(":")[0]);
    }

    public byte[] run(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }
}
