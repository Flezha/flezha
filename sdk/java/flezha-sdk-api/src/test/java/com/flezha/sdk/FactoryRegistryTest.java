package com.flezha.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FactoryRegistryTest {

    @BeforeEach
    void reset() {
        FactoryRegistry.reset();
    }

    private static ProcessorSettings<Object, String, String> simpleSettings() {
        return ProcessorSettings.<Object, String, String>builder()
                .inputTransformer(InputTransformers.text())
                .outputTransformer(OutputTransformers.returnText())
                .runner(ProcessorRunner.THREAD_POOL)
                .build();
    }

    private static ProcessorHandler<Object, String, String> passthrough() {
        return (input, state) -> input;
    }

    @Test
    @DisplayName("registering then looking up by id succeeds")
    void registerAndLookup() {
        FactoryRegistry.registerFactory("echo", simpleSettings(), new Object(),
                FactoryRegistryTest::passthrough);
        assertThat(FactoryRegistry.factoryIds()).containsExactly("echo");
        assertThat(FactoryRegistry.get("echo")).isNotNull();
    }

    @Test
    @DisplayName("duplicate factory id is rejected")
    void duplicateId() {
        FactoryRegistry.registerFactory("echo", simpleSettings(), new Object(),
                FactoryRegistryTest::passthrough);
        assertThatThrownBy(() -> FactoryRegistry.registerFactory(
                "echo", simpleSettings(), new Object(), FactoryRegistryTest::passthrough))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate factory id");
    }

    @Test
    @DisplayName("missing id lookup returns null")
    void missingId() {
        assertThat(FactoryRegistry.get("nope")).isNull();
    }

    @Test
    @DisplayName("registration after freeze is rejected")
    void postFreezeRejected() {
        FactoryRegistry.registerFactory("echo", simpleSettings(), new Object(),
                FactoryRegistryTest::passthrough);
        FactoryRegistry.freeze();
        assertThatThrownBy(() -> FactoryRegistry.registerFactory(
                "late", simpleSettings(), new Object(), FactoryRegistryTest::passthrough))
                .isInstanceOf(IllegalStateException.class);
    }
}
