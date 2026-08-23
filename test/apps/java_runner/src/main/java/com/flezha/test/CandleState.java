package com.flezha.test;

import java.util.ArrayList;
import java.util.List;

/**
 * Event times in fold order — the assertion surface of the late-input
 * correction e2e: a late trade must end up folded in event-time order, not
 * arrival order. Mirrors the Rust test app's {@code CandleState}.
 */
public final class CandleState {
    public final List<Long> folded = new ArrayList<>();
}
