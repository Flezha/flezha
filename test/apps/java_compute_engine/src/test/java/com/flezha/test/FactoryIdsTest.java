package com.flezha.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.flezha.sdk.FactoryMetadata;
import com.flezha.sdk.FactoryRegistry;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FactoryIdsTest {

    @BeforeEach
    void reset() {
        FactoryRegistry.reset();
    }

    @Test
    @DisplayName("three factories match the Rust test app ids")
    void sameIds() {
        FactoryRegistry.registerFactory(
                "do_nothing", DoNothingProcessor.settings(),
                new DoNothingProcessor.State(), DoNothingProcessor::new,
                FactoryMetadata.of("Passes input through unchanged"));
        FactoryRegistry.registerFactory(
                "store_input", StoreInputProcessor.settings(),
                new StoreInputProcessor.State(), StoreInputProcessor::new,
                FactoryMetadata.of("Accumulates all inputs in state for later retrieval"));
        FactoryRegistry.registerFactory(
                "compare_filter", CompareFilterProcessor.settings(),
                new CompareFilterProcessor.State(), CompareFilterProcessor::new,
                FactoryMetadata.of("Filters by comparing input against a stored value"));

        List<String> ids = FactoryRegistry.factoryIds();
        Set<String> expected = Set.of("do_nothing", "store_input", "compare_filter");
        assertEquals(expected, Set.copyOf(ids));
    }
}
