package com.flezha.test;

import com.flezha.sdk.authoring.Shard;
import java.nio.charset.StandardCharsets;

/**
 * Single-shard stateless authoring-sugar twin of {@link DoNothingShard}. The
 * annotation processor generates {@code SugarDoNothingShard} +
 * {@code SugarDoNothingRelay} and adds the relay to {@code GeneratedRelays}.
 */
@Shard(id = "java_sugar_do_nothing", sink = "java_sugar_echo")
public class SugarDoNothing {
    public byte[] run(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }
}
