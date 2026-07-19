package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.operative.UnboundedOperative;
import java.util.ArrayList;
import java.util.List;

/**
 * Test double for the runtime class registry, resolved reflectively by the
 * api module's {@code RegistryCore} (the real one lives in
 * {@code flezha-sdk-runtime}, which is not on this module's test classpath).
 * Returns the factory id without topology validation — the lifecycle tests
 * exercise the guard, lifecycle, and declared-set contracts, not wiring.
 * Overwrite-on-insert + {@code unregisterClass}, mirroring the real one.
 */
public final class UnboundedOperativeClassRegistry {

    private static final List<String> REGISTERED = new ArrayList<>();

    private UnboundedOperativeClassRegistry() {}

    public static String registerClass(Class<? extends UnboundedOperative> operativeClass) throws Exception {
        String factoryId = operativeClass.getDeclaredConstructor().newInstance().factoryId();
        synchronized (REGISTERED) {
            if (!REGISTERED.contains(factoryId)) {
                REGISTERED.add(factoryId);
            }
        }
        return factoryId;
    }

    public static Class<? extends UnboundedOperative> unregisterClass(String factoryId) {
        synchronized (REGISTERED) {
            REGISTERED.remove(factoryId);
        }
        return null;
    }

    /** The runtime-side registered ids — asserts the unregister path was reached. */
    public static List<String> registeredIds() {
        synchronized (REGISTERED) {
            return List.copyOf(REGISTERED);
        }
    }

    public static void reset() {
        synchronized (REGISTERED) {
            REGISTERED.clear();
        }
    }
}
