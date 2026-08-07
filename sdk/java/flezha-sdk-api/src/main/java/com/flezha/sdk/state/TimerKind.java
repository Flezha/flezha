package com.flezha.sdk.state;

/**
 * Which clock a per-Entity timer is scheduled against. Serialized as a postcard
 * unit enum ({@code 0} / {@code 1}) inside every durable timer op, so the
 * ordinal order here is a wire contract shared with the Rust SDK's
 * {@code TimerKind}.
 */
public enum TimerKind {
    /**
     * Fires when the Entity's event-time high-water reaches the scheduled
     * timestamp. An Entity that stops receiving input never fires one — nothing
     * advances its clock. That is inherent to event time, and why
     * {@link #PROCESSING_TIME} exists.
     */
    EVENT_TIME,

    /**
     * Fires when wall-clock time reaches the scheduled timestamp, via the
     * Operative's timer wheel. The answer to "nothing has arrived".
     */
    PROCESSING_TIME;

    /** Stable lower-case name for log lines and assertions. Not a wire form — the wire carries the ordinal. */
    public String label() {
        return this == EVENT_TIME ? "event_time" : "processing_time";
    }
}
