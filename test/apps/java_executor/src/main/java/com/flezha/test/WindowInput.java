package com.flezha.test;

/** Input to the keyed-collection reference shards: a per-key value. */
public record WindowInput(String key, double value) {}
