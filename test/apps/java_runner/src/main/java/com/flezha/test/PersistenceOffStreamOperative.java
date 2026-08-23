package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Same Procedure topology as {@link StatefulCounterStreamOperative} but with
 * persistence disabled at the operative-class level. Used by integration tests
 * to verify the toggle-off drain discards Changes without serialization or
 * wire I/O.
 */
public final class PersistenceOffStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "persistence_off_counter";
    }

    @Override
    public boolean persistenceEnabled() {
        return false;
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input")
                .into(PersistenceOffEntryProcedure.class)
                .named("ticks")
                .into(PersistenceOffCounterProcedure.class);
    }

    /** The {@link CounterEntryProcedure} parse under this operative's own node identity. */
    public static final class PersistenceOffEntryProcedure extends CounterEntryProcedure {
        @Override
        public String name() {
            return "persistence_off_entry_procedure";
        }
    }

    /** The {@link CounterProcedure} fold under this operative's own node identity. */
    public static final class PersistenceOffCounterProcedure extends CounterProcedure {
        @Override
        public String name() {
            return "persistence_off_counter_procedure";
        }
    }
}
