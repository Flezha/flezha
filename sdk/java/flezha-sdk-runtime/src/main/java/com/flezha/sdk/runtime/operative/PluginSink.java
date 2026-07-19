package com.flezha.sdk.runtime.operative;

import java.util.function.Function;

/**
 * Plugin sink attached to a channel via {@code alsoOutput(...)}.
 * Distinct from the {@code pluginName}/{@code pluginSerializer} fields used by
 * the closed-form {@code output(...)} path: a hybrid channel keeps
 * {@link com.flezha.sdk.channel.ChannelKind#INTER_OPERATION} and carries this
 * record in {@link ChannelDescriptor#pluginSink()}.
 */
public record PluginSink(String name, Function<Object, byte[]> serializer) {}
