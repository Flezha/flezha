package com.flezha.sdk.channel;

/**
 * Per-channel backpressure policy applied at the producer-side push site
 * when the consumer Entity's bounded mailbox is full. Declared via
 * {@code .onFull(...)} on the channel builder. The default is {@link #BLOCK}
 * for every kind, so nothing is discarded unless declared. Plugin-bound
 * channels carry no SDK-side mailbox so the value is never consulted on the
 * hot path.
 */
public enum OverflowPolicy {
    /**
     * Producer parks until the consumer drains an item, then retries.
     * Preserves ordering at the cost of producer-thread stall.
     */
    BLOCK,
    /**
     * Incoming item is discarded; the consumer's {@code dropsNewestTotal}
     * counter increments.
     */
    DROP_NEWEST,
    /**
     * Consumer's mailbox drops its oldest item and accepts the new one;
     * consumer's {@code dropsOldestTotal} counter increments. Best-effort
     * under contention.
     */
    DROP_OLDEST,
}
