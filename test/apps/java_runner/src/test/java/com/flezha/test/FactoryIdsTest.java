package com.flezha.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.flezha.sdk.operative.UnboundedOperativeRegistry;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FactoryIdsTest {

    @BeforeEach
    void reset() {
        UnboundedOperativeRegistry.reset();
    }

    @Test
    @DisplayName("three factories match the Rust test app ids")
    void sameIds() {
        UnboundedOperativeRegistry.registerUnbounded(DoNothingUnboundedOperative.class);
        UnboundedOperativeRegistry.registerUnbounded(StoreInputUnboundedOperative.class);
        UnboundedOperativeRegistry.registerUnbounded(CompareFilterUnboundedOperative.class);

        List<String> ids = UnboundedOperativeRegistry.factoryIds();
        Set<String> expected = Set.of("do_nothing", "store_input", "compare_filter");
        assertEquals(expected, Set.copyOf(ids));
    }
}
