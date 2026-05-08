package com.flezha.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.flezha.sdk.relay.RelayRegistry;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FactoryIdsTest {

    @BeforeEach
    void reset() {
        RelayRegistry.reset();
    }

    @Test
    @DisplayName("three factories match the Rust test app ids")
    void sameIds() {
        RelayRegistry.registerRelay(DoNothingRelay.class);
        RelayRegistry.registerRelay(StoreInputRelay.class);
        RelayRegistry.registerRelay(CompareFilterRelay.class);

        List<String> ids = RelayRegistry.factoryIds();
        Set<String> expected = Set.of("do_nothing", "store_input", "compare_filter");
        assertEquals(expected, Set.copyOf(ids));
    }
}
