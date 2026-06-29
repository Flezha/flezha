package com.flezha.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.flezha.sdk.operative.ContinuousOperativeRegistry;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FactoryIdsTest {

    @BeforeEach
    void reset() {
        ContinuousOperativeRegistry.reset();
    }

    @Test
    @DisplayName("three factories match the Rust test app ids")
    void sameIds() {
        ContinuousOperativeRegistry.registerContinuous(DoNothingContinuousOperative.class);
        ContinuousOperativeRegistry.registerContinuous(StoreInputContinuousOperative.class);
        ContinuousOperativeRegistry.registerContinuous(CompareFilterContinuousOperative.class);

        List<String> ids = ContinuousOperativeRegistry.factoryIds();
        Set<String> expected = Set.of("do_nothing", "store_input", "compare_filter");
        assertEquals(expected, Set.copyOf(ids));
    }
}
