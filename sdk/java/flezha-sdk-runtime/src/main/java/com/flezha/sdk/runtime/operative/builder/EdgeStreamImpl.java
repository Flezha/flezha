package com.flezha.sdk.runtime.operative.builder;

import com.flezha.sdk.channel.EdgeStream;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.channel.Stream;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.runtime.operative.EdgeYield;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

/**
 * The transform / keying / consumer-side-configuration surface, implemented
 * ONCE. The entry handle and a procedure's output handle differ only in how
 * they resolve their channel ({@link #siteChannelId}) — everything else is
 * shared, which is what keeps the two from drifting.
 */
abstract class EdgeStreamImpl<T> implements EdgeStream<T>, EdgeSite {

    /** The builder this handle was declared on. */
    final StreamOperativeTopologyBuilder owner;
    /** The chain declared on this handle, {@code null} for a bare one. */
    final Integer pending;
    /** The payload type after this handle's transforms. */
    final Class<?> payloadType;

    EdgeStreamImpl(StreamOperativeTopologyBuilder owner, Integer pending, Class<?> payloadType) {
        this.owner = owner;
        this.pending = pending;
        this.payloadType = payloadType;
    }

    @Override
    public Integer sitePending() {
        return pending;
    }

    /** This handle with a new payload type and a new pending chain. */
    abstract <U> EdgeStream<U> relink(Integer pending, Class<?> payloadType);

    private <U> EdgeStream<U> link(
            Class<?> newPayloadType, Function<Object, EdgeYield> transform, Function<Object, String> key) {
        owner.checkMutable();
        int chain = owner.extendPending(siteChannelId(), pending, newPayloadType, transform, key);
        return relink(chain, newPayloadType);
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public <U> EdgeStream<U> map(@Nonnull Class<U> type, @Nonnull Function<T, U> mapper) {
        return link(type, value -> EdgeYield.one(mapper.apply((T) value)), null);
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public <U> EdgeStream<U> flatMap(@Nonnull Class<U> type, @Nonnull Function<T, ? extends Iterable<U>> mapper) {
        return link(
                type,
                value -> {
                    List<Object> out = new ArrayList<>();
                    mapper.apply((T) value).forEach(out::add);
                    return EdgeYield.many(out);
                },
                null);
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public EdgeStream<T> filter(@Nonnull Predicate<T> predicate) {
        return link(payloadType, value -> predicate.test((T) value) ? EdgeYield.one(value) : EdgeYield.DROP, null);
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public EdgeStream<T> keyBy(@Nonnull Function<T, String> key) {
        return link(payloadType, null, value -> key.apply((T) value));
    }

    @Override
    @Nonnull
    public <O> Stream<O> into(@Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedureClass) {
        return wireInto(procedureClass, null);
    }

    @Override
    @Nonnull
    public <O> Stream<O> into(
            @Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedureClass, @Nonnull String nodeName) {
        return wireInto(procedureClass, nodeName);
    }

    <O> Stream<O> wireInto(Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedureClass, String nodeName) {
        owner.checkMutable();
        return new StreamImpl<>(owner, owner.wireConsumer(siteChannelId(), procedureClass, nodeName, pending));
    }

    @Override
    @Nonnull
    public EdgeStream<T> onFull(@Nonnull OverflowPolicy policy) {
        owner.checkMutable();
        owner.channel(siteChannelId()).overflowPolicy = policy;
        return this;
    }

    @Override
    @Nonnull
    public EdgeStream<T> retryMax(int maxAttempts) {
        owner.checkMutable();
        owner.setRetryMax(siteChannelId(), maxAttempts);
        return this;
    }

    @Override
    @Nonnull
    public EdgeStream<T> retryBackoffMs(long baseMs) {
        owner.checkMutable();
        owner.channel(siteChannelId()).retryBackoffBaseMs = baseMs;
        return this;
    }
}
