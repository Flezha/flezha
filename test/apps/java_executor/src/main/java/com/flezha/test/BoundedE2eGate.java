package com.flezha.test;

/**
 * Shared crash-window gate for the durable structured bounded crash-resume e2e
 * (§7c.3). An operation (or a compensation hook) parks a pool/driver thread
 * until {@code FLEZHA_BOUNDED_E2E_RELEASE} is present in the environment. The
 * first executor is launched WITHOUT it (so a SIGKILL lands while the operative
 * is mid-chain, with earlier operations' Segment state already durable); the
 * respawned executor is launched WITH it, so the resumed instance runs to
 * completion. Mirrors the Rust test app's {@code block_until_released}.
 */
final class BoundedE2eGate {

    static final String RELEASE_ENV = "FLEZHA_BOUNDED_E2E_RELEASE";

    private BoundedE2eGate() {}

    static boolean released() {
        return System.getenv(RELEASE_ENV) != null;
    }

    static void blockUntilReleased() {
        while (!released()) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
