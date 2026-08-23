package com.flezha.test;

import java.util.Map;
import java.util.TreeMap;

/**
 * Latest value per key, folded by {@link TierStoreChange}. {@code TreeMap}
 * keeps snapshot bytes deterministic, so a crash-replay regenerates a
 * byte-identical state. Mirrors the Rust test app's {@code TierStoreState}.
 */
public final class TierStoreState {
    public final Map<String, String> values = new TreeMap<>();
}
