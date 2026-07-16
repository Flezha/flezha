package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.operative.UnboundedOperative;
import java.util.ArrayList;
import java.util.List;

/**
 * Test double for the runtime class registry, resolved reflectively by
 * {@code com.flezha.sdk.operative.UnboundedOperativeRegistry} (the real one
 * lives in {@code flezha-sdk-runtime}, which is not on this module's test
 * classpath). Returns the factory id without topology validation — the
 * lifecycle tests exercise the guard and lifecycle, not wiring.
 */
public final class UnboundedOperativeClassRegistry {

    private static final List<String> REGISTERED = new ArrayList<>();

    private UnboundedOperativeClassRegistry() {}

    public static String registerClass(Class<? extends UnboundedOperative> operativeClass) throws Exception {
        String factoryId = operativeClass.getDeclaredConstructor().newInstance().factoryId();
        synchronized (REGISTERED) {
            REGISTERED.add(factoryId);
        }
        return factoryId;
    }

    public static void reset() {
        synchronized (REGISTERED) {
            REGISTERED.clear();
        }
    }
}
