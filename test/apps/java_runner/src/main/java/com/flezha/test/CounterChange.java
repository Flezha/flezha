package com.flezha.test;

import com.flezha.sdk.state.Change;

/**
 * Per-input Change recorded by {@link CounterProcedure}: a delta to add to
 * {@code state.value}. Mirrors the Rust SDK's {@code CounterChange}.
 */
public record CounterChange(long delta) implements Change {
}
