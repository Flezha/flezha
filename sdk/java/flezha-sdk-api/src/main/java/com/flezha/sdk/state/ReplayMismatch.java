package com.flezha.sdk.state;

/**
 * Diagnostic carried by {@code OperationTestHarness.replayEquals} when a replay's
 * Kryo bytes diverge from the original run's bytes. Users print
 * {@link #toString()} in their test failure messages.
 */
public record ReplayMismatch(byte[] expectedBytes, byte[] actualBytes) {

    @Override
    public String toString() {
        return "ReplayMismatch{\n  expected (" + expectedBytes.length + " bytes): "
                + hex(expectedBytes) + "\n  actual   (" + actualBytes.length + " bytes): "
                + hex(actualBytes) + "\n}";
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
