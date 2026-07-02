package com.flezha.test;

import com.flezha.sdk.state.Change;

/**
 * Idempotent SET-style Change recorded by the durable structured bounded
 * operatives in the crash-resume e2e (§7c.3): inserts {@code key} into the
 * Segment's set. Re-applying the same key on a resume re-run is a no-op, so
 * Model A idempotent re-run over restored state is safe. Mirrors the Rust test
 * app's {@code Insert}.
 */
public record Insert(String key) implements Change {
}
