package com.flezha.test;

import com.flezha.sdk.state.Change;

/**
 * One mutation of a {@link SessionTimeoutState}. {@code closedAt >= 0} marks a
 * firing; anything else is a touch carrying the input.
 */
public final class SessionTimeoutChange implements Change {

    private String touched = "";
    private long closedAt = -1L;

    /** Kryo requires a no-arg constructor. */
    public SessionTimeoutChange() {}

    public static SessionTimeoutChange touched(String input) {
        SessionTimeoutChange c = new SessionTimeoutChange();
        c.touched = input;
        return c;
    }

    public static SessionTimeoutChange closed(long firedAt) {
        SessionTimeoutChange c = new SessionTimeoutChange();
        c.closedAt = firedAt;
        return c;
    }

    public String touched() {
        return touched;
    }

    public long closedAt() {
        return closedAt;
    }
}
