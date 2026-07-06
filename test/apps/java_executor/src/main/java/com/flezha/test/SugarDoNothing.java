package com.flezha.test;

import com.flezha.sdk.authoring.Operation;
import java.nio.charset.StandardCharsets;

/**
 * Single-operation stateless authoring-sugar twin of {@link DoNothingOperation}.
 * The annotation processor generates {@code SugarDoNothingOperation} +
 * {@code SugarDoNothingUnboundedOperative} and adds the operative to
 * {@code GeneratedUnboundedOperatives}.
 */
@Operation(id = "java_sugar_do_nothing", sink = "java_sugar_echo")
public class SugarDoNothing {
    public byte[] run(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }
}
