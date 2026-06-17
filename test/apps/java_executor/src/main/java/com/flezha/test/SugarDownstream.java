package com.flezha.test;

import com.flezha.sdk.authoring.Shard;
import java.nio.charset.StandardCharsets;

/** Stateless downstream shard of the 2-shard reference relay. */
@Shard(id = "sugar_static_downstream_shard")
public class SugarDownstream {
    public byte[] run(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }
}
