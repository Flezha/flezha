package com.flezha.test;

import com.flezha.sdk.authoring.Procedure;
import java.nio.charset.StandardCharsets;

/** Stateless downstream procedure of the 2-procedure reference operative. */
@Procedure(id = "macro_static_downstream_procedure")
public class SugarDownstream {
    public byte[] run(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }
}
