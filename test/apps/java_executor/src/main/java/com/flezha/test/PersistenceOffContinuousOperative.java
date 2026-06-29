package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.ContinuousOperative;

/**
 * Same Operation as {@link StatefulCounterContinuousOperative}, but with
 * persistence disabled at the operative-class level. Used by integration tests
 * to verify the toggle-off drain discards Changes without serialization or
 * wire I/O.
 */
public final class PersistenceOffContinuousOperative implements ContinuousOperative {

    @Override
    public String factoryId() {
        return "persistence_off";
    }

    @Override
    public boolean persistenceEnabled() {
        return false;
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(IntInputAdapterOperation.class);
    }
}
