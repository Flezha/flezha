package com.flezha.sdk.channel;

/**
 * What a channel discards when the consumer Entity's bounded mailbox is full,
 * applied at the producer-side push site. Declared via {@code .onFull(...)} on
 * the channel builder. The default is {@link #DROP_NOTHING} for every kind, so
 * nothing is discarded unless declared. Plugin-bound channels carry no
 * SDK-side mailbox so the value is never consulted on the hot path.
 */
public enum OverflowPolicy {
    /**
     * Discards nothing: the producer's rate is coupled to the consumer's
     * until room appears. Ordering is preserved.
     */
    DROP_NOTHING,
    /**
     * Discards the arriving item; the consumer's {@code dropsNewestTotal}
     * counter increments.
     */
    DROP_NEWEST,
    /**
     * Discards the queued head: the consumer's mailbox pops its oldest item
     * and accepts the new one; consumer's {@code dropsOldestTotal} counter
     * increments. Best-effort under contention.
     */
    DROP_OLDEST,
}
