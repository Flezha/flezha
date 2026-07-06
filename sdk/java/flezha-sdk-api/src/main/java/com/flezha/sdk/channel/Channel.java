package com.flezha.sdk.channel;

import javax.annotation.Nonnull;

/**
 * Phantom-typed channel handle. The {@code id} is the index into the UnboundedOperative's
 * channel list; the {@code T} parameter exists only for compile-time wiring
 * checks via the builder methods.
 */
public final class Channel<T> {

    private final int id;
    private final Class<T> payloadType;

    public Channel(int id, @Nonnull Class<T> payloadType) {
        this.id = id;
        this.payloadType = payloadType;
    }

    public int id() {
        return id;
    }

    @Nonnull
    public Class<T> payloadType() {
        return payloadType;
    }
}
