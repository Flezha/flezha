package com.flezha.test;

import com.flezha.sdk.Flezha;

/**
 * Mirror of {@code test/apps/rust_executor/src/main.rs}. Accepts the
 * namespace as its first command-line argument and keeps the process alive
 * until stdin closes — matching the Rust test app's {@code std_keeper}.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String namespace = args.length > 0 ? args[0] : "default";
        registerContinuousOperatives();
        registerBoundedOperatives();
        Flezha unused = Flezha.newInstance(namespace);
        // Intentionally unused — the SDK instance holds the connection open;
        // the JVM stays alive until stdin closes.
        stdKeeper();
    }

    private static void registerContinuousOperatives() {
        Flezha.registerContinuous(DoNothingContinuousOperative.class);
        Flezha.registerContinuous(StoreInputContinuousOperative.class);
        Flezha.registerContinuous(CompareFilterContinuousOperative.class);
        Flezha.registerContinuous(StatefulCounterContinuousOperative.class);
        Flezha.registerContinuous(CoalescingCounterContinuousOperative.class);
        Flezha.registerContinuous(PersistenceOffContinuousOperative.class);
        Flezha.registerContinuous(HybridChannelContinuousOperative.class);
        // EchoMessageContinuousOperative is reintroduced once per-Operation `handleMessage` lands.
    }

    private static void registerBoundedOperatives() {
        Flezha.registerBounded(EchoBoundedOperative.class);
        Flezha.registerBounded(CommitBlockBoundedOperative.class);
        Flezha.registerBounded(SagaBlockBoundedOperative.class);
    }

    private static void stdKeeper() {
        try {
            byte[] buf = new byte[1];
            while (true) {
                int n = System.in.read(buf);
                if (n == -1) {
                    System.exit(0);
                }
                Thread.sleep(1_000);
            }
        } catch (Exception e) {
            System.exit(0);
        }
    }
}
