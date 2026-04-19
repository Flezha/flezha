package com.flezha.sdk;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

/**
 * Global singleton of registered processor factories. Factories MUST be
 * registered <em>before</em> {@code Flezha.newInstance(...)} is called; once
 * the SDK is active the registry is frozen.
 *
 * <p>Mirrors the Rust SDK's {@code FACTORY_REGISTRY} + {@code DESCRIPTION_REGISTRY}
 * lazy statics. Thread-safe.
 */
public final class FactoryRegistry {

    private static final Object LOCK = new Object();
    private static final Map<String, FactoryEntry<?, ?, ?>> ENTRIES = new LinkedHashMap<>();
    private static final AtomicBoolean FROZEN = new AtomicBoolean(false);

    private FactoryRegistry() {
    }

    /**
     * Register a factory. Must be called before {@code Flezha.newInstance}.
     *
     * @throws IllegalStateException    if the registry is already frozen or
     *                                  the SDK is already initialized.
     * @throws IllegalArgumentException on duplicate {@code factoryId}.
     */
    public static <State, Input, Output> void registerFactory(
            @Nonnull String factoryId,
            @Nonnull ProcessorSettings<State, Input, Output> settings,
            @Nonnull State initialState,
            @Nonnull Supplier<ProcessorHandler<State, Input, Output>> handlerSupplier) {
        registerFactory(factoryId, settings, initialState, handlerSupplier, FactoryMetadata.empty());
    }

    public static <State, Input, Output> void registerFactory(
            @Nonnull String factoryId,
            @Nonnull ProcessorSettings<State, Input, Output> settings,
            @Nonnull State initialState,
            @Nonnull Supplier<ProcessorHandler<State, Input, Output>> handlerSupplier,
            @Nonnull FactoryMetadata metadata) {
        if (factoryId == null || factoryId.isEmpty()) {
            throw new IllegalArgumentException("factoryId must not be null or empty");
        }
        if (settings == null) {
            throw new IllegalArgumentException("settings must not be null");
        }
        if (initialState == null) {
            throw new IllegalArgumentException("initialState must not be null");
        }
        if (handlerSupplier == null) {
            throw new IllegalArgumentException("handlerSupplier must not be null");
        }

        synchronized (LOCK) {
            if (FROZEN.get()) {
                throw new IllegalStateException(
                        "Cannot register factory '" + factoryId + "' — the SDK is already initialized");
            }
            if (ENTRIES.containsKey(factoryId)) {
                throw new IllegalArgumentException(
                        "Duplicate factory id: '" + factoryId + "'");
            }
            ENTRIES.put(factoryId, new FactoryEntry<>(
                    factoryId, settings, initialState, handlerSupplier, metadata));
        }
    }

    /** Freeze the registry. Called by the SDK runtime when {@code newInstance} begins. */
    public static void freeze() {
        FROZEN.set(true);
    }

    /** Thaw the registry. Intended for tests that reset SDK state between runs. */
    public static void reset() {
        synchronized (LOCK) {
            ENTRIES.clear();
            FROZEN.set(false);
        }
    }

    @Nonnull
    public static List<String> factoryIds() {
        synchronized (LOCK) {
            return List.copyOf(ENTRIES.keySet());
        }
    }

    @Nonnull
    public static Map<String, FactoryMetadata> factoryMetadata() {
        synchronized (LOCK) {
            Map<String, FactoryMetadata> out = new LinkedHashMap<>();
            for (FactoryEntry<?, ?, ?> e : ENTRIES.values()) {
                out.put(e.factoryId(), e.metadata());
            }
            return Collections.unmodifiableMap(out);
        }
    }

    public static FactoryEntry<?, ?, ?> get(@Nonnull String factoryId) {
        synchronized (LOCK) {
            return ENTRIES.get(factoryId);
        }
    }

    public static boolean isEmpty() {
        synchronized (LOCK) {
            return ENTRIES.isEmpty();
        }
    }

    public record FactoryEntry<State, Input, Output>(
            String factoryId,
            ProcessorSettings<State, Input, Output> settings,
            State initialState,
            Supplier<ProcessorHandler<State, Input, Output>> handlerSupplier,
            FactoryMetadata metadata) {
    }
}
