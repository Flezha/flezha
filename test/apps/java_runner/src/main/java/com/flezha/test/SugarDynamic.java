package com.flezha.test;

import com.flezha.sdk.authoring.Procedure;
import com.flezha.sdk.authoring.EntityKey;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Dynamic-mode authoring-sugar example. The processor generates the dynamic
 * trait surface ({@code entityKeyOverridden() == true}, {@code entityKey}
 * delegation, empty {@code entityKeys()}). No {@code sink} — it is not
 * auto-registered, because the Java runtime has not yet landed the
 * dynamic entity mode surface ({@code entityIdleTtl} /
 * {@code maxEntities} methods and the dynamic dispatch path), so the
 * {@code idleTtl} / {@code maxEntities} attributes are not yet wired.
 */
@Procedure(id = "sugar_dynamic_procedure")
public class SugarDynamic {

    @EntityKey(idleTtl = 5000, maxEntities = 10)
    public Optional<String> coin(String input) {
        return input.equals("drop") ? Optional.empty() : Optional.of(input.split(":")[0]);
    }

    public byte[] run(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }
}
