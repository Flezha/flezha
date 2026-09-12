package com.flezha.examples.local;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import javax.annotation.Nonnull;

/** Splits lines into words, then counts each word in its own entity. */
public final class WordCountOperative implements StreamOperative {

    @Override
    @Nonnull
    public String factoryId() {
        return "word_count";
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(String.class, "lines")
                .into(Split.class)
                .keyBy(word -> word)
                .into(Tally.class)
                .keyed()
                .sink("totals");
    }
}
