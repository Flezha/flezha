package com.flezha.test;

import java.util.Set;
import java.util.TreeSet;

/**
 * Idempotent Entity state for the crash-resume e2e operatives: a set of
 * inserted keys. {@link Insert} folds into it via {@code items.add}, so a
 * resume re-run over restored state converges to the same set. {@code TreeSet}
 * keeps iteration deterministic. Mirrors the Rust test app's {@code SeenState}.
 */
public final class SeenState {
    public final Set<String> items = new TreeSet<>();
}
