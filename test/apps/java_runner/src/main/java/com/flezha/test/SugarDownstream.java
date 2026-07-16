package com.flezha.test;

import com.flezha.sdk.authoring.Operation;
import java.nio.charset.StandardCharsets;

/** Stateless downstream operation of the 2-operation reference operative. */
@Operation(id = "sugar_static_downstream_operation")
public class SugarDownstream {
    public byte[] run(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }
}
