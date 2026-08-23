package com.flezha.test;

import com.flezha.sdk.state.Change;

/** One trade folded into a {@link CandleState}, identified by its event time. */
public record CandleChange(long eventTime) implements Change {
}
