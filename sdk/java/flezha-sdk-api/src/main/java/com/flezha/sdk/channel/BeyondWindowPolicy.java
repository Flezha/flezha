package com.flezha.sdk.channel;

/**
 * What to do with an input whose event time falls outside the declared
 * correction window — i.e. so late that the state it would have corrected is no
 * longer reconstructible.
 */
public enum BeyondWindowPolicy {

    /**
     * Process it as though it had just arrived (the default). This is the
     * behaviour of an Operative that declares no window at all, which is why it
     * is the default: declaring a correction window must never silently start
     * dropping data.
     */
    PROCESS_AS_ARRIVAL,

    /** Drop it. Choose this only when a stale result is worse than a missing one. */
    DROP;

    /** The wire spelling carried in the declaration map. */
    public String wireName() {
        return this == DROP ? "drop" : "process_as_arrival";
    }
}
