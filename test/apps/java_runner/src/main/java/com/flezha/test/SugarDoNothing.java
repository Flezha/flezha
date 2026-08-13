package com.flezha.test;

import com.flezha.sdk.authoring.Procedure;
import java.nio.charset.StandardCharsets;

/**
 * Single-procedure stateless authoring-sugar twin of {@link DoNothingProcedure}.
 * The annotation processor generates {@code SugarDoNothingProcedure} +
 * {@code SugarDoNothingStreamOperative} and adds the operative to
 * {@code GeneratedStreamOperatives}.
 */
@Procedure(id = "java_sugar_do_nothing", sink = "java_sugar_echo")
public class SugarDoNothing {
    public byte[] run(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }
}
