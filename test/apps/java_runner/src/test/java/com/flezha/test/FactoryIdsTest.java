package com.flezha.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FactoryIdsTest {

    @BeforeEach
    void reset() {
        StreamOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("three factories match the Rust test app ids")
    void sameIds() {
        StreamOperativeClassRegistry.registerClass(DoNothingStreamOperative.class);
        StreamOperativeClassRegistry.registerClass(StoreInputStreamOperative.class);
        StreamOperativeClassRegistry.registerClass(CompareFilterStreamOperative.class);

        List<String> ids = StreamOperativeClassRegistry.factoryIds();
        Set<String> expected = Set.of("do_nothing", "store_input", "compare_filter");
        assertEquals(expected, Set.copyOf(ids));
    }
}
