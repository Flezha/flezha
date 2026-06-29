package com.flezha.test;

import com.flezha.sdk.authoring.Operation;
import com.flezha.sdk.authoring.OperationKey;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Dynamic-mode authoring-sugar example. The processor generates the dynamic
 * trait surface ({@code operationKeyOverridden() == true}, {@code operationKey}
 * delegation, empty {@code operationKeys()}). No {@code sink} — it is not
 * auto-registered, because the Java runtime has not yet landed the
 * {@code operative-dynamic-segmenting} surface ({@code segmentIdleTtl} /
 * {@code maxSegments} methods and the dynamic dispatch path), so the
 * {@code idleTtl} / {@code maxSegments} attributes are not yet wired.
 */
@Operation(id = "sugar_dynamic_operation")
public class SugarDynamic {

    @OperationKey(idleTtl = 5000, maxSegments = 10)
    public Optional<String> coin(String input) {
        return input.equals("drop") ? Optional.empty() : Optional.of(input.split(":")[0]);
    }

    public byte[] run(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }
}
