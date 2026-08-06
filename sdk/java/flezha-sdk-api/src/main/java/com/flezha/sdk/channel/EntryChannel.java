package com.flezha.sdk.channel;

/**
 * The entry channel's handle: wires the graph's root consumer and carries the
 * entry-side channel configuration. Entry channels are fed by the controller,
 * so there is no producer side — everything an entry handle offers is the
 * shared {@link EdgeStream} surface, which is what keeps it from drifting away
 * from {@link Stream}.
 *
 * <p>Like {@link Stream}, this handle is only usable inside
 * {@code channels()} / {@code procedures()}: using a retained handle after
 * the topology froze at registration throws {@link IllegalStateException}
 * naming the operative.
 */
public interface EntryChannel<T> extends EdgeStream<T> {}
