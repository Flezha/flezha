package com.flezha.examples.timers;

import com.flezha.sdk.state.Change;

/** One mutation of a session: a click, or the close its idle timer produced. */
public final class SessionChange implements Change {

    private String clicked = "";
    private long closedAt = -1L;

    public SessionChange() {}

    static SessionChange clicked(String page) {
        SessionChange c = new SessionChange();
        c.clicked = page;
        return c;
    }

    static SessionChange closed(long at) {
        SessionChange c = new SessionChange();
        c.closedAt = at;
        return c;
    }

    public String clicked() {
        return clicked;
    }

    public long closedAt() {
        return closedAt;
    }
}
