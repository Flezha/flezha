package com.flezha.sdk.channel;

/**
 * The handle for a channel fed from outside the graph, returned by
 * {@link ChannelBuilder#input(Class, String)}. This is where a graph starts.
 *
 * <pre>{@code
 * c.input(String.class, "lines").into(Split.class).sink("words");
 * }</pre>
 *
 * <p>An entry channel is fed by the controller, so it has no producer side. Everything it offers is
 * the shared {@link EdgeStream} surface that {@link Stream} also carries, including the wiring
 * transforms:
 *
 * <pre>{@code
 * c.input(Trade.class, "trades")
 *         .filter(t -> t.qty() > 0)
 *         .keyBy(Trade::symbol)
 *         .into(Candle.class);
 * }</pre>
 *
 * <p>Sharing that surface is deliberate: it keeps the entry side from drifting away from
 * {@link Stream}, so a transform learned in one place works in the other.
 *
 * <p>Like {@link Stream}, this handle is usable only inside {@code channels()} or
 * {@code procedures()}. The graph freezes at registration, and a retained handle used afterwards
 * throws {@link IllegalStateException} naming the operative.
 */
public interface EntryChannel<T> extends EdgeStream<T> {}
