package com.flezha.examples.keyed;

/** A trade as it arrives on the wire. */
public record Trade(String symbol, double price, double qty) {}
