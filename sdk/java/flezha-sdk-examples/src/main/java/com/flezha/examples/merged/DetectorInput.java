package com.flezha.examples.merged;

import java.util.Optional;

/**
 * Everything the detector consumes, as a single type.
 *
 * <p>A procedure has one input type, so two channels have to become one. Java has no sum types, so
 * this record plays that role: one slot per source, exactly one of them populated, and a factory
 * method per variant.
 *
 * <p>Those factories are the mappings you hand to {@code merge}, referenced as
 * {@code DetectorInput::ofReading}. That is the whole trick.
 */
public record DetectorInput(Optional<Reading> reading, Optional<Threshold> threshold) {

    public static DetectorInput ofReading(Reading r) {
        return new DetectorInput(Optional.of(r), Optional.empty());
    }

    public static DetectorInput ofThreshold(Threshold t) {
        return new DetectorInput(Optional.empty(), Optional.of(t));
    }
}
