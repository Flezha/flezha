package com.flezha.sdk.state;

import java.time.Instant;
import java.util.function.Function;

/**
 * Clock that extracts an {@link Instant} from each input via a user-supplied
 * function. Type ships in v1 for forward compatibility; the runtime currently
 * rejects Operations declaring this Clock with
 * {@code ContinuousOperativeConfigurationException.clockNotImplementedYet}.
 */
public final class EventTimeFrom<Input> implements Clock {

    private final Function<Input, Instant> extractor;

    public EventTimeFrom(Function<Input, Instant> extractor) {
        this.extractor = extractor;
    }

    @Override
    public Instant now() {
        throw new UnsupportedOperationException("Clock not implemented yet");
    }

    public Instant nowFor(Input input) {
        throw new UnsupportedOperationException("Clock not implemented yet");
    }
}
