package com.flezha.sdk.runtime.operative;

import java.util.function.Function;

/**
 * Plugin sink attached to a channel via {@code sink(...)} — the one
 * representation of plugin egress. A channel that also has consumers keeps
 * {@link com.flezha.sdk.channel.ChannelKind#INTER_PROCEDURE}; one with none is
 * the {@link com.flezha.sdk.channel.ChannelKind#PLUGIN_BOUND} terminal, a kind
 * the builder derives from the wiring rather than from which method was called.
 */
public record PluginSink(Function<Object, byte[]> serializer) {}
