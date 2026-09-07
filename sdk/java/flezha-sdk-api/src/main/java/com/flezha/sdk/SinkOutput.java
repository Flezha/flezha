package com.flezha.sdk;

import java.util.Map;

/**
 * One value a graph released on a plugin-bound sink, delivered to a local
 * run's callback. {@code payload} is what the channel's serializer produced
 * and {@code metadata} is what a deployed run would have put on the wire —
 * on a keyed sink that includes {@code flz.entity}, {@code flz.seq} and
 * {@code flz.key}, because local mode takes output where production frames
 * it rather than before.
 */
public record SinkOutput(String channel, byte[] payload, Map<String, String> metadata) {}
