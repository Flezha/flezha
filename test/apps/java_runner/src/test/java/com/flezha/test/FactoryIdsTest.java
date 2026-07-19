package com.flezha.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FactoryIdsTest {

    @BeforeEach
    void reset() {
        UnboundedOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("three factories match the Rust test app ids")
    void sameIds() {
        UnboundedOperativeClassRegistry.registerClass(DoNothingUnboundedOperative.class);
        UnboundedOperativeClassRegistry.registerClass(StoreInputUnboundedOperative.class);
        UnboundedOperativeClassRegistry.registerClass(CompareFilterUnboundedOperative.class);

        List<String> ids = UnboundedOperativeClassRegistry.factoryIds();
        Set<String> expected = Set.of("do_nothing", "store_input", "compare_filter");
        assertEquals(expected, Set.copyOf(ids));
    }
}
