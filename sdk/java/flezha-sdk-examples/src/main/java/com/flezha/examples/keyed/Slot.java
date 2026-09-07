package com.flezha.examples.keyed;

/** One trade, addressed to a single timeframe's candle. */
public record Slot(String symbol, String tf, double price) {}
